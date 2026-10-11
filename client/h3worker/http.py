"""HTTP client for the server API with bounded retries and error mapping."""

from __future__ import annotations

import asyncio
from contextvars import copy_context
import http.client
import ipaddress
import json
import random
import re
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, FrozenSet, Optional, Tuple

from shared.h3proto import PROTOCOL_SCHEMA_VERSION

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_USER_AGENT = "h3worker/1.0"


class HeartbeatTransport:
    """Bound whole-request latency, including DNS, without blocking the loop.

    urllib cannot cancel a resolver or bound the sum of socket operations.
    Abandoned calls may finish remotely, but their responses are discarded.
    Four dedicated daemon threads bound outstanding work and keep stalled
    renewals out of the executor used by inference, cancellation and finish.
    """

    def __init__(self):
        self._slots = threading.BoundedSemaphore(4)

    async def call(self, func, *args, timeout: float):
        if not self._slots.acquire(blocking=False):
            raise ApiError(0, "NETWORK", "heartbeat transport still stalled")
        loop = asyncio.get_running_loop()
        result = loop.create_future()

        def deliver(value, error):
            if not result.done():
                if error is not None:
                    result.set_exception(error)
                else:
                    result.set_result(value)

        def run():
            value, error = None, None
            try:
                value = func(*args)
            except Exception as exc:
                error = exc
            finally:
                self._slots.release()
            try:
                loop.call_soon_threadsafe(deliver, value, error)
            except RuntimeError:
                pass  # owning loop has shut down

        try:
            context = copy_context()
            threading.Thread(target=context.run, args=(run,), daemon=True,
                             name="h3-heartbeat").start()
        except Exception:
            self._slots.release()
            raise
        try:
            return await asyncio.wait_for(result, timeout)
        except asyncio.TimeoutError as exc:
            raise ApiError(0, "NETWORK", "heartbeat request budget exhausted") from exc


class TransferAborted(Exception):
    """Raised from inside an in-flight PUT/GET body stream when
    abort_check() turns true — a cancel request or a lost lease must
    interrupt a long transfer instead of streaming hundreds of MB to a
    dead attempt (terminal review B-2, C-3)."""


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str,
                 retry_after: Optional[float] = None):
        message = sanitize_error_text(message)
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.retry_after = retry_after

    @property
    def retryable(self) -> bool:
        return self.status >= 500 or self.status == 429

    @property
    def url_expired(self) -> bool:
        """Structured detection of an expired signed storage URL (protocol
        10.10 refresh path).  The server reports FORBIDDEN with a fixed
        message until it grows a dedicated code."""
        return self.status == 403 and "expired" in self.message.lower()


def sanitize_error_text(text: str, *, secrets=(), max_bytes: int = 512) -> str:
    """Preserve diagnostics, redact credentials, then bound UTF-8 bytes.

    Redact before truncation so a credential straddling the limit cannot leak.
    Query strings on URLs may contain signatures; ordinary question marks stay.
    """
    text = str(text)
    for secret in sorted((str(value) for value in secrets if value), key=len, reverse=True):
        text = text.replace(secret, '<redacted>')
    text = re.sub(r"(https?://(?:<redacted>|[^\s<>\"'?#])+)\?(?:<redacted>|[^\s<>\"'])*",
                  r"\1?<redacted>", text, flags=re.I)
    text = re.sub(
        r"(\bauthorization[\"']?\s*[:=]\s*[\"']?(?:Bearer|Basic)\s+)"
        r"[A-Za-z0-9._~+/=-]+", r"\1<redacted>", text, flags=re.I,
    )
    text = re.sub(r"\b(Bearer)\s+[A-Za-z0-9._~+/=-]+",
                  r"\1 <redacted>", text, flags=re.I)
    text = re.sub(
        r"(\b(?:[\w-]*(?:token|password|secret|api[_-]?key)|"
        r"cf[_-]access[_-]client[_-]id)[\"']?\s*[:=]\s*)"
        r"(\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,;&}\]\"')]+)",
        lambda m: m[1] + (m[2][0] + "<redacted>" + m[2][0]
                         if m[2][0] in ("\"", "'") else "<redacted>"),
        text, flags=re.I,
    )
    text = re.sub(
        r"(\bauthorization[\"']?\s*[:=]\s*)(?![\"']?(?:Bearer|Basic)\b)"
        r"(\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,;}\]\"')]+)",
        lambda m: m[1] + (m[2][0] + "<redacted>" + m[2][0]
                         if m[2][0] in ("\"", "'") else "<redacted>"),
        text, flags=re.I,
    )
    # URL userinfo is also a credential, independent of query strings.
    text = re.sub(r"(https?://)(?:<redacted>|[^\s/@<>])+@", r"\1<redacted>@", text, flags=re.I)
    return text.encode('utf-8')[:max_bytes].decode('utf-8', 'ignore')


def sanitize_error_payload(value, *, secrets=()):
    """Sanitize diagnostic fields only; never alter protocol credentials."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key in ('message', 'stderr', 'stdout', 'detail', 'reason', 'note',
                       'unhealthy_reason', 'blocked_reason', 'error', 'exception_message') and isinstance(item, str):
                result[key] = sanitize_error_text(
                    item, secrets=secrets,
                    max_bytes=65536 if key in ('stdout', 'stderr') else 512,
                )
            elif key == 'terminal' and isinstance(item, (list, tuple)) and len(item) == 4:
                result[key] = [
                    *item[:2],
                    sanitize_error_text(item[2], secrets=secrets, max_bytes=65536),
                    sanitize_error_text(item[3], secrets=secrets, max_bytes=65536),
                ]
            else:
                result[key] = sanitize_error_payload(item, secrets=secrets)
        return result
    if isinstance(value, list):
        return [sanitize_error_payload(item, secrets=secrets) for item in value]
    return value


def validate_server_connect_ip(base_url: str, address: str) -> None:
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('server_connect_ip requires an HTTPS server origin')
    ip = ipaddress.ip_address(address)
    if not ip.is_global or '%' in address:
        raise ValueError('server_connect_ip requires a public literal IP address')


class HttpClient:
    """Synchronous urllib-based client.  The worker runs network I/O in a
    thread executor so engine callbacks never block on HTTP (client README 2)."""

    def __init__(self, base_url: str, token: str, connect_timeout: float,
                 timeout: float,
                 allowed_download_hosts: Optional[FrozenSet[str]] = None,
                 cf_access_client_id: str = "",
                 cf_access_client_secret: str = "",
                 server_connect_ip: str = "", allow_redirects: bool = True):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.connect_timeout = connect_timeout
        self.timeout = timeout
        self._server_opener = None
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                raise ValueError('download redirect refused')

        handlers = [] if allow_redirects else [NoRedirect()]
        if server_connect_ip:
            validate_server_connect_ip(base_url, server_connect_ip)
            origin = self._origin(base_url)

            class PinnedHTTPSHandler(urllib.request.HTTPSHandler):
                def https_open(handler, request):
                    def connection(host, **kwargs):
                        conn = http.client.HTTPSConnection(host, **kwargs)
                        if HttpClient._origin(request.full_url) == origin:
                            # Change only TCP destination. HTTPSConnection still
                            # verifies the original hostname and sends its SNI.
                            conn._create_connection = lambda address, *args, **kw: socket.create_connection(
                                (server_connect_ip, address[1]), *args, **kw)
                        return conn
                    return handler.do_open(connection, request, context=handler._context)

            # A proxy would resolve the target itself, defeating this override.
            self._server_opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({}), PinnedHTTPSHandler(), *handlers)
        elif not allow_redirects:
            self._server_opener = urllib.request.build_opener(*handlers)
        if bool(cf_access_client_id) != bool(cf_access_client_secret):
            raise ValueError(
                "Cloudflare Access client ID and client secret must be set together"
            )
        self.cf_access_client_id = cf_access_client_id
        self.cf_access_client_secret = cf_access_client_secret
        # None = allow the API host only (derived from base_url)
        if allowed_download_hosts is None:
            allowed_download_hosts = frozenset(
                {urllib.parse.urlparse(self.base_url).hostname}
            )
        self.allowed_download_hosts = allowed_download_hosts

    @staticmethod
    def _origin(url: str) -> tuple:
        parsed = urllib.parse.urlparse(url)
        port = parsed.port
        if port is None:
            port = 443 if parsed.scheme.lower() == "https" else 80
        return parsed.scheme.lower(), (parsed.hostname or "").lower(), port

    def _urlopen(self, request, *, timeout):
        if self._server_opener and self._origin(request.full_url) == self._origin(self.base_url):
            return self._server_opener.open(request, timeout=timeout)
        return urllib.request.urlopen(request, timeout=timeout)

    def _add_access_headers(self, request: urllib.request.Request,
                            url: str) -> None:
        """Add Access credentials only for the configured app origin.

        Unredirected headers are deliberately not copied by urllib to a
        redirected request, so a cross-origin redirect cannot receive them.
        """
        if not self.cf_access_client_id or \
                self._origin(url) != self._origin(self.base_url):
            return
        request.add_unredirected_header(
            "CF-Access-Client-Id", self.cf_access_client_id)
        request.add_unredirected_header(
            "CF-Access-Client-Secret", self.cf_access_client_secret)

    def request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None,
                timeout: Optional[float] = None,
                headers: Optional[Dict[str, str]] = None) -> Tuple[int, Any]:
        url = self.base_url + path
        data = None
        req_headers = {
            "authorization": f"Bearer {self.token}",
            "accept": "application/json",
            "user-agent": _USER_AGENT,
        }
        if headers:
            req_headers.update(headers)
        if body is not None:
            data = json.dumps(sanitize_error_payload(body, secrets=(
                self.token, self.cf_access_client_id, self.cf_access_client_secret)),
                ensure_ascii=False).encode("utf-8")
            req_headers["content-type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method)
        for k, v in req_headers.items():
            req.add_header(k, v)
        self._add_access_headers(req, url)
        try:
            with self._urlopen(
                req, timeout=timeout or self.timeout
            ) as resp:
                raw = resp.read()
                if resp.status == 204 or not raw:
                    return resp.status, None
                return resp.status, json.loads(raw)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                payload = {"code": "UNKNOWN", "message": raw.decode("utf-8", "replace")}
            raise ApiError(
                e.code,
                payload.get("code", "UNKNOWN"),
                sanitize_error_text(payload.get("message", ""), secrets=(
                    self.token, self.cf_access_client_id, self.cf_access_client_secret)),
                payload.get("retry_after_seconds"),
            ) from e
        except (urllib.error.URLError, TimeoutError, OSError,
                http.client.HTTPException, ValueError) as e:
            # HTTPException (bad status line / remote disconnect mid-header)
            # and ValueError (non-JSON body) are NOT OSError subclasses —
            # without this mapping a malformed proxy response would escape
            # as a raw exception and could kill the lease loop (review B2)
            raise ApiError(0, "NETWORK", sanitize_error_text(str(e), secrets=(
                self.token, self.cf_access_client_id, self.cf_access_client_secret))) from e

    # -- endpoint wrappers -------------------------------------------------

    def register(self, worker_id: str, boot_id: str, capabilities: Dict[str, Any],
                 versions: Dict[str, Any]) -> Dict[str, Any]:
        status, payload = self.request("POST", "/v1/workers/register", {
            "worker_id": worker_id,
            "boot_id": boot_id,
            "protocol_version": PROTOCOL_SCHEMA_VERSION,
            "capacity": 1,
            "capabilities": capabilities,
            **versions,
        })
        return payload

    def worker_heartbeat(self, worker_id: str, boot_id: str, status: str,
                         current_attempt_id: Optional[str],
                         health: Dict[str, Any]) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/workers/{worker_id}/heartbeat",
            {
                "boot_id": boot_id,
                "status": status,
                "current_attempt_id": current_attempt_id,
                "health": health,
            },
            timeout=15.0,
        )
        return payload or {}

    def worker_update_status(self, worker_id: str, request_id: str,
                             status: str, message: Optional[str] = None) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/workers/{worker_id}/update-status",
            {"request_id": request_id, "status": status, "message": message},
            timeout=15.0,
        )
        return payload or {}

    def worker_command_poll(self, worker_id, boot_id, blocked_reason=None):
        return self.request('POST', f'/v1/workers/{worker_id}/command-poll',
                            dict(boot_id=boot_id, blocked_reason=blocked_reason), timeout=3.0)[1] or {}

    def worker_command_claim(self, worker_id, request_id, boot_id, execution_id):
        return self.request('POST', f'/v1/workers/{worker_id}/command-claim',
                            dict(request_id=request_id, boot_id=boot_id,
                                 execution_id=execution_id), timeout=3.0)[1] or {}

    def worker_command_input(self, worker_id, request_id, boot_id, execution_id, expected):
        """Bounded authenticated input read; never include raw responses in errors."""
        import hashlib
        from shared.comfy_workflow import MAX_BYTES, validate_bytes
        if not isinstance(expected, dict):
            raise ValueError('invalid command input descriptor')
        size, digest = expected.get('bytes'), expected.get('sha256')
        if (expected.get('kind') != 'comfy-workflow' or type(size) is not int
                or not 1 <= size <= MAX_BYTES or not isinstance(digest, str)
                or not _HEX64_RE.fullmatch(digest)):
            raise ValueError('invalid command input descriptor')
        url = self.base_url + f'/v1/workers/{worker_id}/command-input'
        req = urllib.request.Request(url, method='POST', data=json.dumps(dict(
            request_id=request_id, boot_id=boot_id, execution_id=execution_id)).encode(),
            headers={'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'})
        self._add_access_headers(req, url)

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                raise ValueError('command input redirect refused')

        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        try:
            with opener.open(req, timeout=10) as response:
                if response.status != 200 or response.geturl() != url:
                    raise ValueError('invalid command input response')
                raw = response.read(MAX_BYTES + 1)
        except urllib.error.HTTPError as exc:
            code = exc.code
            exc.close()
            raise ApiError(code, 'COMMAND_INPUT', 'command input read refused') from None
        except (OSError, ValueError, http.client.HTTPException):
            raise ApiError(0, 'COMMAND_INPUT', 'command input read failed') from None
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError('command input size or digest mismatch')
        return validate_bytes(raw)

    def worker_command_reconcile(self, worker_id, boot_id, record, terminal):
        return self.request('POST', f'/v1/workers/{worker_id}/command-reconcile',
            dict(boot_id=boot_id, request_id=record['request_id'],
                 execution_id=record['execution_id'], previous_boot_id=record['boot_id'],
                 phase=record['phase'], terminal=terminal), timeout=3.0)[1] or {}

    def worker_command_status(self, worker_id: str, request_id: str,
                              status: str, exit_code: Optional[int] = None,
                              stdout: str = "", stderr: str = "", **owner) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/workers/{worker_id}/command-status",
            {"request_id": request_id, "status": status,
             "exit_code": exit_code, "stdout": stdout, "stderr": stderr, **owner},
            timeout=15.0,
        )
        return payload or {}

    def report_status(self, worker_id: str, report: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/workers/{id}/status (protocol 11).  Short timeout: a
        monitoring report must never delay the attempt pipeline."""
        _, payload = self.request(
            "POST", f"/v1/workers/{worker_id}/status",
            report, timeout=8.0,
        )
        return payload or {}

    def cpu_tail_handoff(self, attempt_id, token, proof, boot_id, confirm=False):
        _, result = self.request("POST", f"/v1/attempts/{attempt_id}/cpu-tail-handoff",
            dict(proof, lease_token=token, boot_id=boot_id, confirm=confirm))
        return result

    def claim(self, request_id: str, worker_id: str, boot_id: str,
              wait_seconds: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Returns (claimed, response).  Raises ApiError on hard failures."""
        status, payload = self.request(
            "POST", "/v1/tasks/claim",
            {
                "request_id": request_id,
                "worker_id": worker_id,
                "boot_id": boot_id,
                "wait_seconds": wait_seconds,
            },
            timeout=(wait_seconds + 20.0),
        )
        if status == 204:
            return False, None
        return True, payload

    def reconcile_attempt(self, attempt_id: str, lease_token: str,
                          completed: bool = False) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/attempts/{attempt_id}/reconcile",
            {"lease_token": lease_token, "completed": completed}, timeout=5.0)
        return payload

    def reserve_cpu_tail(self, attempt_id: str, lease_token: str, evidence: Dict[str, Any]) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/attempts/{attempt_id}/cpu-tail",
            dict(evidence, lease_token=lease_token), timeout=5.0)
        return payload

    def attempt_heartbeat(self, attempt_id: str, lease_token: str,
                          heartbeat_seq: int, boot_id: str, phase: Optional[str],
                          last_event_seq: int, process_status: str,
                          resources: Dict[str, Any]) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/attempts/{attempt_id}/heartbeat",
            {
                "lease_token": lease_token,
                "heartbeat_seq": heartbeat_seq,
                "boot_id": boot_id,
                "phase": phase,
                "last_event_seq": last_event_seq,
                "process_status": process_status,
                "resources": resources,
            },
            timeout=5.0,
        )
        return payload

    def send_events(self, attempt_id: str, lease_token: str,
                    events: list) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/attempts/{attempt_id}/events",
            {"lease_token": lease_token, "events": events},
            timeout=20.0,
        )
        return payload

    def resolve_inputs(self, attempt_id: str, lease_token: str,
                       asset_ids: list) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/attempts/{attempt_id}/inputs/resolve",
            {"lease_token": lease_token, "asset_ids": asset_ids},
        )
        return payload

    def prepare_artifacts(self, attempt_id: str, lease_token: str,
                          request_id: str, files: list) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/attempts/{attempt_id}/artifacts/prepare",
            {"request_id": request_id, "lease_token": lease_token,
             "files": files},
        )
        return payload

    def finish(self, attempt_id: str, lease_token: str, request_id: str,
               body: Dict[str, Any]) -> Dict[str, Any]:
        _, payload = self.request(
            "POST", f"/v1/attempts/{attempt_id}/finish",
            {"request_id": request_id, "lease_token": lease_token, **body},
        )
        return payload

    def release(self, attempt_id: str, lease_token: str, reason: str,
                code: Optional[str] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {"lease_token": lease_token, "reason": reason}
        if code is not None:
            # structured cause (protocol 10.5): only a known code triggers
            # the server's node-eligibility negative feedback
            body["code"] = code
        _, payload = self.request(
            "POST", f"/v1/attempts/{attempt_id}/release", body,
        )
        return payload

    def get_attempt(self, attempt_id: str) -> Dict[str, Any]:
        _, payload = self.request("GET", f"/v1/attempts/{attempt_id}", None)
        return payload

    # -- raw download/upload for signed URLs (no auth header; sig in URL) --

    def download(self, url: str, dest_path: str, expected_sha256: str,
                 expected_size: int, max_bytes: int,
                 timeout: float, abort_check=None) -> Tuple[int, str]:
        """Stream to a temp file, verify size+digest, atomic rename.

        The URL must be http(s) and point at an allowed host (client
        README 5: restrict download targets; urllib would happily follow
        file:// or intranet addresses)."""
        import hashlib
        import os
        import tempfile

        self._check_download_url(url)
        if not _HEX64_RE.match(expected_sha256 or ""):
            raise ValueError("server-supplied sha256 is not a hex64 digest")
        tmp_path = dest_path + ".part"
        digest = hashlib.sha256()
        total = 0
        req = urllib.request.Request(
            url, method="GET",
            headers={"Accept": "*/*", "User-Agent": _USER_AGENT},
        )
        self._add_access_headers(req, url)
        try:
            with self._urlopen(req, timeout=timeout) as resp, \
                    open(tmp_path, "wb") as out:
                while True:
                    if abort_check is not None and abort_check():
                        raise TransferAborted()
                    chunk = resp.read(1024 * 256)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes or total > expected_size:
                        out.close()
                        os.unlink(tmp_path)
                        raise ValueError("download exceeds expected size")
                    digest.update(chunk)
                    out.write(chunk)
        except TransferAborted:
            # deliberate stop mid-body: remove the partial file and never
            # retry as a network error
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError,
                http.client.HTTPException) as e:
            # NOTE: ValueError is deliberately NOT mapped here (unlike
            # request()/upload()): download does no JSON parsing, and the
            # size/digest-mismatch ValueErrors below are validation
            # results the worker's retry classification must see as-is
            # (fix-review D-4)
            raise ApiError(0, "NETWORK", sanitize_error_text(str(e), secrets=(
                self.token, self.cf_access_client_id, self.cf_access_client_secret))) from e
        if total != expected_size:
            os.unlink(tmp_path)
            raise ValueError(
                f"download size mismatch: {total} != {expected_size}"
            )
        got = digest.hexdigest()
        if got != expected_sha256:
            os.unlink(tmp_path)
            raise ValueError("download digest mismatch")
        os.replace(tmp_path, dest_path)
        return total, got

    def _check_download_url(self, url: str) -> None:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"unsupported download scheme {parsed.scheme!r}")
        if self.allowed_download_hosts is not None and \
                parsed.hostname not in self.allowed_download_hosts:
            raise ValueError(
                f"download host {parsed.hostname!r} is not allowed"
            )

    def upload(self, url: str, path: str, content_type: str,
               headers: Dict[str, str], timeout: float,
               progress_cb=None, abort_check=None) -> Dict[str, Any]:
        """PUT a file to a signed URL.  ``progress_cb(sent, total)`` fires
        from THIS thread as urllib streams the body — keep it cheap and
        thread-safe (the monitor only stores counters).
        ``abort_check()`` is polled before every body read; returning true
        raises TransferAborted so a cancel/lease-loss interrupts an in-flight
        upload instead of streaming to a dead attempt."""
        import os

        size = os.path.getsize(path)
        fh = open(path, "rb")

        class _Counting:
            def read(self, n=-1):
                if abort_check is not None and abort_check():
                    raise TransferAborted()
                chunk = fh.read(n)
                if chunk and progress_cb is not None:
                    try:
                        progress_cb(fh.tell(), size)
                    except Exception:
                        pass  # monitoring must never fail an upload
                return chunk

            def __len__(self):  # urllib uses len() for Content-Length
                return size

        with fh:
            # Content-Length is set for BOTH paths with the SAME priority
            # (setdefault: a server-signed length wins, else ours):
            # Python 3.14 does not derive the length from __len__ wrappers
            # or arbitrary file objects and falls back to chunked transfer
            # — S3-style presigned PUTs reject that.  _Counting.read
            # returns exactly `size` bytes in total, so the framing stays
            # honest (review C1, fix-review C-4).
            headers_out = {
                "Content-Type": content_type,
                **headers,
                "Accept": "application/json",
                "User-Agent": _USER_AGENT,
            }
            headers_out.setdefault("Content-Length", str(size))
            body_reader = _Counting() if (progress_cb or abort_check) else fh
            req = urllib.request.Request(
                url, data=body_reader, method="PUT",
                headers=headers_out,
            )
            self._add_access_headers(req, url)
            try:
                with self._urlopen(req, timeout=timeout) as resp:
                    raw = resp.read()
                    return json.loads(raw) if raw else {}
            except TransferAborted:
                raise  # deliberate stop — never retried as a network error
            except urllib.error.HTTPError as e:
                raw = e.read()
                try:
                    payload = json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    payload = {"code": "UNKNOWN",
                               "message": raw.decode("utf-8", "replace")}
                raise ApiError(
                    e.code, payload.get("code", "UNKNOWN"),
                    sanitize_error_text(payload.get("message", ""), secrets=(
                        self.token, self.cf_access_client_id, self.cf_access_client_secret))
                ) from e
            except (urllib.error.URLError, TimeoutError, OSError,
                    http.client.HTTPException, ValueError) as e:
                raise ApiError(
                    0, "NETWORK", sanitize_error_text(str(e), secrets=(
                        self.token, self.cf_access_client_id, self.cf_access_client_secret))
                ) from e


async def retry_with_backoff(
    coro_factory, *, max_retries: int, base_delay: float = 0.5,
    max_delay: float = 8.0, retry_on: tuple = (ApiError, OSError),
):
    """Exponential backoff with jitter, respecting retry_after hints."""
    attempt = 0
    while True:
        try:
            return await coro_factory()
        except retry_on as e:
            attempt += 1
            if attempt > max_retries:
                raise
            if isinstance(e, ApiError) and not e.retryable:
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            delay *= 0.5 + random.random()
            if isinstance(e, ApiError) and e.retry_after:
                delay = max(delay, float(e.retry_after))
            await asyncio.sleep(delay)
