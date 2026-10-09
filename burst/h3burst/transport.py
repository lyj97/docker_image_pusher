"""Bounded, authenticated Pod transport. No redirects, proxies or POST retries."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request

MAX_JSON = 2 * 1024 * 1024


class RemoteError(RuntimeError):
    def __init__(self, code, status=0):
        super().__init__(code)
        self.code, self.status = code, status


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RemoteError('redirect_refused')


def endpoint(url):
    u = urllib.parse.urlsplit(url)
    if (u.username is not None or u.password is not None or u.query or u.fragment
            or u.path not in ('', '/') or not u.hostname
            or (u.scheme != 'https' and not
                (u.scheme == 'http' and u.hostname in ('127.0.0.1', '::1', 'localhost')))):
        raise ValueError('Pod endpoint requires HTTPS or an explicit loopback tunnel')
    return url.rstrip('/')


class Transport:
    def __init__(self, url, token, generation, timeout=5):
        self.base = endpoint(url)
        if not isinstance(token, str) or not re.fullmatch('[A-Za-z0-9_-]{32,256}', token):
            raise ValueError('short-lived Pod credential required')
        if not re.fullmatch('[A-Za-z0-9_.:-]{1,128}', generation):
            raise ValueError('invalid generation')
        self.token, self.generation, self.timeout = token, generation, timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        self.opener.addheaders = [('User-Agent', 'h3-service/0.39')]

    def request(self, path, body=None, method=None):
        if not re.fullmatch(r'/v1/[A-Za-z0-9_/-]+', path):
            raise ValueError('invalid protocol path')
        raw = None if body is None else json.dumps(body, allow_nan=False).encode()
        if raw is not None and len(raw) > MAX_JSON:
            raise ValueError('request exceeds bound')
        req = urllib.request.Request(self.base + path, data=raw, method=method,
            headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.token,
                     'X-H3-Generation': self.generation})
        try:
            return self.opener.open(req, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            status = exc.code
            try:
                raw = exc.read(MAX_JSON + 1)
            except (OSError, urllib.error.URLError, http.client.HTTPException):
                raise RemoteError('pod_unreachable') from None
            finally:
                exc.close()
            try:
                error = json.loads(raw) if len(raw) <= MAX_JSON else {}
            except ValueError:
                error = {}
            code = error.get('error') if isinstance(error, dict) and error.get('generation') == self.generation else None
            raise RemoteError(code or 'pod_http_' + str(status), status) from None
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            raise RemoteError('pod_unreachable') from None

    def json(self, path, body=None):
        try:
            with self.request(path, body) as response:
                raw = response.read(MAX_JSON + 1)
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            raise RemoteError('pod_unreachable') from None
        if len(raw) > MAX_JSON:
            raise RemoteError('response_too_large')
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            raise RemoteError('invalid_response') from None
        if not isinstance(value, dict) or value.get('generation') != self.generation:
            raise RemoteError('generation_conflict')
        return value

    def upload(self, source, descriptor, abort=lambda: False):
        from .inputs import verify_descriptor
        verify_descriptor(descriptor)
        size = descriptor['size_bytes'];sha = descriptor['sha256']
        source = Path(source)
        if not source.is_file() or source.is_symlink() or source.stat().st_size != size:
            raise RemoteError('invalid_local_input')
        def chunks():
            h = hashlib.sha256();sent = 0
            with source.open('rb') as stream:
                while chunk := stream.read(1024 * 1024):
                    if abort():raise RemoteError('transfer_aborted')
                    sent += len(chunk)
                    if sent > size:raise RemoteError('input_integrity_failure')
                    h.update(chunk);yield chunk
            if sent != size or h.hexdigest() != sha:raise RemoteError('input_integrity_failure')
        req = urllib.request.Request(self.base + '/v1/inputs/' + sha, data=chunks(), method='PUT',
            headers={'Content-Type':'application/octet-stream', 'Content-Length':str(size),
                'Authorization':'Bearer ' + self.token, 'X-H3-Generation':self.generation})
        try:
            with self.opener.open(req, timeout=max(self.timeout, 60)) as response:
                raw = response.read(MAX_JSON + 1)
            value = json.loads(raw) if len(raw) <= MAX_JSON else {}
            if (value.get('generation') != self.generation or value.get('sha256') != sha
                    or value.get('size_bytes') != size):
                raise RemoteError('input_receipt_conflict')
        except urllib.error.HTTPError as exc:
            status = exc.code;exc.close()
            raise RemoteError('input_upload_http_' + str(status), status) from None
        except (OSError, urllib.error.URLError, ValueError):
            raise RemoteError('input_upload_failed') from None

    def download(self, path, destination, expected_size, expected_sha, abort=lambda: False):
        if (type(expected_size) is not int or expected_size <= 0
                or not isinstance(expected_sha, str) or not re.fullmatch('[0-9a-f]{64}', expected_sha)):
            raise RemoteError('invalid_artifact_descriptor')
        destination = Path(destination)
        partial = destination.with_name(destination.name + '.pending')
        size, digest = 0, hashlib.sha256()
        try:
            with self.request(path) as source, partial.open('wb') as target:
                while chunk := source.read(1024 * 1024):
                    if abort():
                        raise RemoteError('transfer_aborted')
                    size += len(chunk)
                    if size > expected_size:
                        raise RemoteError('artifact_exceeds_bound')
                    digest.update(chunk)
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
            if abort() or size != expected_size or digest.hexdigest() != expected_sha:
                raise RemoteError('artifact_verification_failed')
            os.replace(partial, destination)
        finally:
            partial.unlink(missing_ok=True)
