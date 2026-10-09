"""Worker main loop: registration, claim, execution, lease, upload, finish.

Concurrency model (client README section 2): a single asyncio loop runs the
scheduler; lease heartbeats, event sending and file transfers execute in
thread executors so engine reads never block heartbeats.  Local journal
writes are short transactions.

Lease clock (client README 7): the conservative local deadline tracks BOTH
a monotonic and a wall-clock estimate (min of the two views).  macOS sleeps
pause mach_absolute_time; the wall view keeps the deadline honest after a
wake.  Every deadline is anchored at the REQUEST SEND time, never at the
response receive time.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import math
import os
import queue
from pathlib import Path
import re
import sqlite3
import secrets
import signal
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import wave
from typing import Any, Dict, List, Optional, Tuple

from shared import ltx_policy as ltx
from shared.h3proto import (
    PROTOCOL_SCHEMA_VERSION,
    digest_obj,
    validate_alignment_payload,
    validate_generation,
    validate_seed_string,
    validate_task_request,
    required_h3_optimizations,
    effective_h3_generation,
)

from .config import WorkerConfig
from . import __version__
from .http import ApiError, HeartbeatTransport, HttpClient, TransferAborted, sanitize_error_text, sanitize_error_payload
from .journal import Journal
from .monitor import Monitor
from . import cpu_overlap
from .attempt_runtime import AttemptRuntime, RuntimeField, attempt_scoped, runtime_for, bind_runtime
from . import runner as runmod
from . import comfy_runner as comfy

_ATTEMPT_ID_RE = re.compile(r"^att_[A-Za-z0-9_-]{8,64}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

_AUDIO_SUFFIX_BY_CONTENT_TYPE = {
    "audio/aac": ".aac",
    "audio/flac": ".flac",
    "audio/m4a": ".m4a",
    "audio/mp4": ".m4a",
    "audio/mpeg": ".mp3",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/wav": ".wav",
    "audio/webm": ".webm",
    "audio/x-m4a": ".m4a",
    "audio/x-wav": ".wav",
}


# Dedicated, bounded durability work: a stuck filesystem must not consume the
# default executor or block heartbeats. Cancellation retains the occupied slot.
_DURABILITY_SLOT = threading.BoundedSemaphore(1)


async def _durable_manifest(media_path, manifest_path, attempt_dir, manifest,
                            *, check_alive, timeout_seconds):
    if timeout_seconds is not None and (not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
        raise ValueError("durability_timeout_seconds must be positive and finite")
    if not _DURABILITY_SLOT.acquire(blocking=False):
        raise RuntimeError("local output durability operation still running")
    loop = asyncio.get_running_loop()
    completed = loop.create_future()

    def deliver(value, error):
        if not completed.done():
            if error is not None:
                completed.set_exception(error)
            else:
                completed.set_result(value)

    def run():
        value, error = None, None
        try:
            with open(media_path, "rb") as media:
                os.fsync(media.fileno())
            with open(manifest_path + ".tmp", "w", encoding="utf-8") as fh:
                json.dump(manifest, fh, ensure_ascii=False, indent=1)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(manifest_path + ".tmp", manifest_path)
            directory_fd = os.open(attempt_dir, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            value = file_digest(manifest_path)
        except Exception as exc:
            error = exc
        try:
            loop.call_soon_threadsafe(deliver, value, error)
        except RuntimeError:
            pass  # closed loop; local_completed was never committed
        finally:
            _DURABILITY_SLOT.release()

    try:
        threading.Thread(target=run, daemon=True, name="h3-durability").start()
    except BaseException:
        _DURABILITY_SLOT.release()
        raise
    started_mono, started_wall = time.monotonic(), time.time()
    try:
        while True:
            check_alive()
            remaining = 0.1 if timeout_seconds is None else timeout_seconds - max(
                time.monotonic() - started_mono, time.time() - started_wall,
            )
            if remaining <= 0:
                raise TimeoutError("local output durability timed out")
            if completed.done():
                return completed.result()
            await asyncio.wait({completed}, timeout=min(0.1, remaining))
    finally:
        # The thread alone releases its slot. Discard late results/errors;
        # abandoned filesystem work must never commit local_completed.
        if not completed.done():
            completed.cancel()
        elif not completed.cancelled():
            completed.exception()


def _cache_filename(sha256: str, content_type: str) -> str:
    """Keep a trusted audio suffix for decoders that dispatch by extension.

    The SHA remains the cache identity.  Only a fixed content-type allowlist
    contributes a suffix, so server-controlled metadata can never add path
    separators or arbitrary filenames.
    """
    media_type = str(content_type or "").partition(";")[0].strip().lower()
    return sha256 + _AUDIO_SUFFIX_BY_CONTENT_TYPE.get(media_type, "")


def utcnow_iso() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def parse_iso(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _command_group_alive(pgid):
    """Ignore reaped-or-zombie descendants; fail closed if ps is unavailable."""
    try:
        result = subprocess.run(['ps', '-eo', 'pgid=,stat='], capture_output=True,
                                text=True, timeout=2, check=True)
        return any(int(parts[0]) == pgid and not parts[1].startswith('Z')
                   for line in result.stdout.splitlines()
                   if len(parts := line.split()) >= 2)
    except Exception:
        return None


def _run_operator_command(text: str, timeout: int, on_output=None,
                          report_interval: float = 0.5,
                          cancel_event: Optional[threading.Event] = None,
                          cancel_grace_seconds: float = 5.0,
                          on_start=None,
                          input_stream=None,
                          ) -> Tuple[str, Optional[int], str, str]:
    from .upgrade import _repo_root

    started_at = time.monotonic()
    process = subprocess.Popen(
        ["/bin/sh", "-lc", text],
        cwd=_repo_root(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        stdin=input_stream,
        start_new_session=True,
    )
    if on_start is not None:
        try:
            on_start(process.pid)
        except BaseException:
            # Popen already transferred ownership to us, even if persisting
            # that ownership failed. No pipe readers exist yet.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=5)
                end = time.monotonic() + 3
                alive = _command_group_alive(process.pid)
                while alive is not False and time.monotonic() < end:
                    time.sleep(.05)
                    alive = _command_group_alive(process.pid)
                if alive is not False:
                    return 'recovery_required', None, '', 'cannot confirm process group exit after start failure'
            except subprocess.TimeoutExpired:
                return 'recovery_required', None, '', 'cannot confirm process exit after start failure'
            finally:
                process.stdout.close()
                process.stderr.close()
            raise
    chunks = queue.Queue(maxsize=32)

    def pump(name, stream) -> None:
        try:
            read = getattr(stream, "read1", stream.read)
            while True:
                chunk = read(4096)
                if not chunk:
                    break
                chunks.put((name, chunk))
        finally:
            stream.close()
            chunks.put((name, None))

    for name, stream in (("stdout", process.stdout), ("stderr", process.stderr)):
        threading.Thread(target=pump, args=(name, stream), daemon=True).start()

    tails = {"stdout": bytearray(), "stderr": bytearray()}
    closed = set()
    dirty = False
    timed_out = False
    cancelled = False
    cancel_killed = False
    cancel_deadline = 0.0
    deadline = started_at + timeout
    exit_confirmation_deadline = 0.0
    last_report = time.monotonic()
    while len(closed) < 2 or process.poll() is None:
        now = time.monotonic()
        descendants_may_hold_pipes = len(closed) < 2
        if cancel_event is not None and cancel_event.is_set() \
                and not cancelled and (process.poll() is None
                                       or descendants_may_hold_pipes):
            cancelled = True
            cancel_deadline = now + cancel_grace_seconds
            try:
                os.killpg(process.pid, 15)
            except ProcessLookupError:
                pass
        if cancelled and not cancel_killed and now >= cancel_deadline \
                and (process.poll() is None or descendants_may_hold_pipes):
            try:
                os.killpg(process.pid, 9)
            except ProcessLookupError:
                pass
            cancel_killed = True
            exit_confirmation_deadline = now + 5
        if not timed_out and now >= deadline \
                and (process.poll() is None or descendants_may_hold_pipes):
            timed_out = True
            exit_confirmation_deadline = now + 5
            try:
                os.killpg(process.pid, 9)
            except ProcessLookupError:
                pass
        if exit_confirmation_deadline and now >= exit_confirmation_deadline:
            return ('recovery_required', None,
                    bytes(tails['stdout']).decode('utf-8', 'ignore'),
                    'cannot confirm process exit after KILL')
        try:
            name, chunk = chunks.get(timeout=0.05)
            if chunk is None:
                closed.add(name)
            else:
                tails[name].extend(chunk)
                del tails[name][:-65536]
                dirty = True
        except queue.Empty:
            pass
        now = time.monotonic()
        if dirty and on_output is not None and now - last_report >= report_interval:
            try:
                on_output(
                    bytes(tails["stdout"]).decode("utf-8", "ignore"),
                    bytes(tails["stderr"]).decode("utf-8", "ignore"),
                )
            except Exception as exc:  # output reporting must not kill the command
                print(f"[worker] command output report failed: {sanitize_error_text(str(exc))}", flush=True)
            dirty = False
            last_report = now

    return_code = process.wait()
    # Closed pipes and a dead shell alone do not prove descendants have exited.
    alive = _command_group_alive(process.pid)
    if alive:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        end = time.monotonic() + 3
        while alive and time.monotonic() < end:
            time.sleep(.05)
            alive = _command_group_alive(process.pid)
    if alive is not False:
        return 'recovery_required', None, '', 'cannot confirm command process group exit'

    stdout = bytes(tails["stdout"]).decode("utf-8", "ignore")
    stderr = bytes(tails["stderr"]).decode("utf-8", "ignore")
    if cancelled:
        return "cancelled", None, stdout, stderr
    if timed_out:
        return "timed_out", None, stdout, stderr
    return ("succeeded" if return_code == 0 else "failed",
            return_code, stdout, stderr)


def _run_workflow_input(text, raw, timeout, output, cancel_event, grace, started):
    from shared.comfy_workflow import validate_bytes, save_command
    try:
        argv = shlex.split(text)
        if (len(argv) not in (6, 7) or argv[:4] != [
                '.venv/bin/python', 'client/comfy_admin.py', 'workflow-save', '--name']
                or text != save_command(argv[4], len(argv) == 7)):
            raise ValueError('invalid workflow input command')
        validate_bytes(raw)
    except (ValueError, TypeError):
        return ('failed', 1, '', 'invalid workflow input command or document')
    # Anonymous node-local file avoids blocking writes into a child's stdin pipe.
    # Only stdin carries bytes; neither journal nor argv contains the document.
    with tempfile.TemporaryFile() as stream:
        stream.write(raw)
        stream.seek(0)
        return _run_operator_command(text, timeout, output, .5, cancel_event, grace,
                                     started, input_stream=stream)


def _tail_output(value: str, limit: int = 65536) -> str:
    data = value.encode("utf-8")
    return data[-limit:].decode("utf-8", "ignore")


def file_digest(path: str, chunk: int = 1024 * 1024) -> Tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    with open(path, "rb") as fh:
        while True:
            data = fh.read(chunk)
            if not data:
                break
            total += len(data)
            digest.update(data)
    return total, digest.hexdigest()


def backoff_delay(attempt: int, retry_after: Optional[float] = None,
                  base: float = 0.5, cap: float = 8.0) -> float:
    """Exponential backoff with jitter, honoring server retry_after hints
    (client README 2)."""
    import random

    delay = min(base * (2 ** min(attempt, 6)), cap)
    delay *= 0.5 + random.random()
    if retry_after:
        delay = max(delay, float(retry_after))
    return delay


def _finish_result_payload(
    artifacts: Dict[str, Any], result: Dict[str, Any], seed: str,
) -> Dict[str, Any]:
    if "alignment" in artifacts:
        return {
            "text": result.get("text"),
            "segments": result.get("segments"),
            "tokens": result.get("tokens"),
        }
    if "audio" in artifacts:
        keys = ("sample_rate", "channels", "samples", "duration_seconds")
    else:
        keys = ("width", "height", "frames", "fps", "duration_seconds")
    return {**{key: result.get(key) for key in keys}, "seed": seed}


def _shutdown_requested(worker: Any) -> bool:
    """A legacy execution stub without a stop event is not shutting down."""
    event = getattr(worker, "stop_event", None)
    return event is not None and event.is_set()


class LeaseLost(Exception):
    pass


class CancelRequested(Exception):
    pass


class NativeGroupUnproven(Exception):
    """Keep the attempt and writer fence when native descendants may survive."""


class LocalIncapacity(Exception):
    """Node-local reason the task cannot run here (missing model, low disk):
    the correct action is release, not a task failure (protocol 10.5)."""

    def __init__(self, reason: str, code: str = "MODEL_UNAVAILABLE"):
        super().__init__(reason)
        self.reason = reason
        self.code = code


class Worker:
    # The node scheduler remains serial. These accessors isolate execution,
    # lease tasks and executor callbacks from another attempt's runtime.
    for _runtime_name in (
        'lease_loop_task', 'lease_lost', 'cancel_requested',
        '_attempt_stop_callbacks', 'lease_deadline_mono', 'lease_deadline_wall',
        'lease_hard_deadline_mono', 'lease_hard_deadline_wall',
        '_lease_renewal_pending', 'heartbeat_seq', '_heartbeat_lock',
        '_heartbeat_transport', 'current_attempt', 'active_proc',
        '_transfer_abort_reason', 'monitor',
    ):
        locals()[_runtime_name] = RuntimeField(_runtime_name)
    del _runtime_name

    def _runtime_for_attempt(self, attempt_id):
        current = runtime_for(self)
        if attempt_id in self._attempt_runtimes:
            return self._attempt_runtimes[attempt_id]
        if current.attempt_id is None:
            # Preserve injected dependencies and the initial node monitor.
            current.attempt_id = attempt_id
            runtime = current
        else:
            runtime = AttemptRuntime(attempt_id)
            runtime.values = dict(
                lease_loop_task=None, lease_lost=asyncio.Event(),
                cancel_requested=asyncio.Event(), _attempt_stop_callbacks={},
                lease_deadline_mono=0.0, lease_deadline_wall=0.0,
                lease_hard_deadline_mono=0.0, lease_hard_deadline_wall=0.0,
                _lease_renewal_pending=False, heartbeat_seq=0,
                _heartbeat_lock=asyncio.Lock(),
                _heartbeat_transport=HeartbeatTransport(), current_attempt=None,
                active_proc=None, _transfer_abort_reason='',
                monitor=Monitor(self.journal, self.config.worker_id, self.boot_id, attempt_id),
            )
        # Serial node observers retain the most recently selected runtime.
        # This is not sufficient for plural node admission/reporting; do not
        # relax the existing scheduling fence based on these accessors.
        if getattr(self.config, 'cpu_tail_overlap', False):
            runtime.values['monitor']._monitor_attempt_id = attempt_id
        self._default_runtime = runtime
        self._attempt_runtimes[attempt_id] = runtime
        for old_id in list(self._attempt_runtimes):
            if old_id == attempt_id:
                continue
            old = self.journal.get_attempt(old_id)
            if old and old.get('confirmed_terminal'):
                del self._attempt_runtimes[old_id]
        return runtime

    def __init__(self, config: WorkerConfig):
        # Pin the data directory before controlled updates change cwd.
        config.data_dir = str(Path(config.data_dir).absolute())
        self.config = config
        self._cpu_overlap_negotiated = False
        self._registered_current_boot = False
        self._publication_jobs = {}
        self.journal = Journal(config.journal_path)
        self.http = HttpClient(
            config.server_url, config.worker_token,
            config.http_connect_timeout_seconds, config.http_timeout_seconds,
            cf_access_client_id=config.cf_access_client_id,
            cf_access_client_secret=config.cf_access_client_secret,
            server_connect_ip=config.server_connect_ip,
        )
        self.boot_id = f"boot_{uuid.uuid4().hex[:12]}"
        self.stop_event = asyncio.Event()
        self._ensure_capability_state()
        self._shutdown_watchdog = None
        self._shutdown_forced = False
        self._force_exit_on_shutdown = False
        # lease bookkeeping for the active attempt
        self.lease_loop_task: Optional[asyncio.Task] = None
        self.lease_lost = asyncio.Event()
        self.cancel_requested = asyncio.Event()
        # attempt-scoped stop callbacks fired when the events above are
        # set (used to interrupt in-flight backoff sleeps, C-2)
        self._attempt_stop_callbacks: Dict[str, Any] = {}
        self.lease_deadline_mono = 0.0
        self.lease_deadline_wall = 0.0
        self.lease_hard_deadline_mono = 0.0
        self.lease_hard_deadline_wall = 0.0
        # A renewal chain that started before the conservative deadline may
        # use the reserved margin for its response and prompt retries.  It
        # is cleared only by a successful renewal (and never extends past
        # the authoritative server deadline).
        self._lease_renewal_pending = False
        self.heartbeat_seq = 0
        self.heartbeat_interval = config.heartbeat_seconds
        self.lease_seconds = 90.0
        self._heartbeat_lock = asyncio.Lock()
        self._heartbeat_transport = HeartbeatTransport()
        self.current_attempt: Optional[Dict[str, Any]] = None
        self.active_proc: Optional[subprocess.Popen] = None
        self._command_control_task = None
        self._command_recovery_required = False
        self._command_blocked_reason = None
        self._command_task: Optional[asyncio.Task] = None
        self._command_request_id: Optional[str] = None
        self._last_command_request_id: Optional[str] = None
        self._command_cancel_event: Optional[threading.Event] = None
        # node governance
        self._drain = False
        self._comfy_unhealthy_reason: Optional[str] = None
        self._other_unhealthy_reason: Optional[str] = None
        self._comfy_recovery_task = None
        self._comfy_reconcile_pending = False
        self._comfy_maintenance_lock = asyncio.Lock()
        # monitoring (protocol 11): collects node/attempt/process state and
        # persists it locally; reporting rides the heartbeat cadence
        self.monitor = Monitor(self.journal, config.worker_id, self.boot_id)
        self._monitor_report_lock = asyncio.Lock()
        self._monitor_tasks: set = set()
        # why the last in-flight transfer aborted ("cancel"/"lease_lost")
        self._transfer_abort_reason = ""

    def _safe_error_text(self, text, *, max_bytes=512):
        config = getattr(self, "config", None)
        return sanitize_error_text(text, max_bytes=max_bytes, secrets=(
            getattr(config, 'worker_token', ''),
            getattr(config, 'cf_access_client_id', ''),
            getattr(config, 'cf_access_client_secret', ''),
        ))

    # ==================================================================
    # lifecycle
    # ==================================================================

    @property
    def _unhealthy_reason(self) -> Optional[str]:
        return (getattr(self, '_other_unhealthy_reason', None)
                or getattr(self, '_comfy_unhealthy_reason', None))

    @_unhealthy_reason.setter
    def _unhealthy_reason(self, reason: Optional[str]) -> None:
        # Legacy fault writers own only the non-ComfyUI slot.
        self._other_unhealthy_reason = self._safe_error_text(reason) if reason else None

    COMFY_PROBE_SECONDS = 5.0
    COMFY_PROBE_TIMEOUT = 4.0
    COMFY_SUCCESS_THRESHOLD = 2
    SHUTDOWN_SECONDS = 15.0

    def request_shutdown(self) -> None:
        """Signal callback: no network IO, preserve identity before cancellation."""
        if self._force_exit_on_shutdown and self._shutdown_watchdog is None:
            # asyncio.run waits for executor threads on exit. A wedged socket or
            # native call must not extend the signal budget indefinitely. Only
            # the CLI enables this final process-local backstop; SQLite intent
            # was committed before any engine submission and remains recoverable.
            self._shutdown_watchdog = threading.Timer(
                self.SHUTDOWN_SECONDS + 5, os._exit, args=(1,))
            self._shutdown_watchdog.daemon = True
            self._shutdown_watchdog.start()
        self.stop_event.set()
        if self._command_cancel_event is not None:
            self._command_cancel_event.set()
        self._drain = True
        runtimes = list(getattr(self, '_attempt_runtimes', {}).values()) or [runtime_for(self)]
        for runtime in runtimes:
            for callback in list(runtime.values.get('_attempt_stop_callbacks', {}).values()):
                callback()
        if self.current_attempt:
            attempt_id = self.current_attempt['attempt_id']
            state = comfy.state_for(self, attempt_id)
            if state and not state.get('terminal'):
                state['cancel_requested'] = True
                comfy.persist(self, attempt_id, state)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.request_shutdown)
        task = asyncio.create_task(self._run())
        stopped = asyncio.create_task(self.stop_event.wait())
        try:
            await asyncio.wait((task, stopped), return_when=asyncio.FIRST_COMPLETED)
            if not task.done():
                # Existing execution loop attempts prompt-scoped cancellation.
                # Cancellation of the coroutine never erases engine_state.
                done, _ = await asyncio.wait((task,), timeout=self.SHUTDOWN_SECONDS)
                if not done:
                    self._shutdown_forced = True
                    task.cancel()
            await task
        except asyncio.CancelledError:
            if not self.stop_event.is_set():
                raise
        finally:
            stopped.cancel()
            jobs = list(getattr(self, '_publication_jobs', {}).values())
            if jobs:
                self.request_shutdown()
                try:
                    await asyncio.wait_for(asyncio.gather(*jobs, return_exceptions=True), self.SHUTDOWN_SECONDS)
                except asyncio.TimeoutError:
                    for job in jobs:
                        job.cancel()
                    await asyncio.gather(*jobs, return_exceptions=True)
            for pending in (
                self._capability_task, self._comfy_probe_task, self.lease_loop_task, self._command_control_task, self._comfy_recovery_task
            ):
                if pending:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
            if getattr(self, "_preview_bridge", None):
                await self._preview_bridge.close()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)

    async def _run(self) -> None:
        config = self.config
        config.ensure_dirs()
        os.chmod(config.data_dir, 0o700)
        self._acquire_singleton_lock()
        if config.worker_token == "worker-token":
            print("[worker] WARNING: default worker token in use", flush=True)
        node = self.journal.get_node()
        if node is not None and node.get("worker_id") != config.worker_id:
            raise SystemExit(
                f"data dir bound to worker {node['worker_id']}, "
                f"config says {config.worker_id}"
            )
        self.journal.set_node(config.worker_id, self.boot_id)
        self.monitor.restore()
        if getattr(config, 'comfyui_preview_enabled', False):
            self._drain = True
        # restart recovery before claiming (client README 7)
        await self._recover_on_boot()
        if await self._comfy.recover_orphans(self):
            # Prompt terminal proof and H3 attempt terminal proof are separate.
            # Do not claim again until the normal attempt recovery path has
            # reconciled the now-terminal engine state with the Server.
            self._comfy_reconcile_pending = True

        if getattr(config, 'comfyui_preview_enabled', False):
            from .comfy_preview import PreviewBridge
            from .startup_prepare import prepare_preview, report_failure
            self._preview_bridge = PreviewBridge(self)
            await self._preview_bridge.start()
            self._drain = True
            # Preparation is bounded and local. No registration or update success
            # is possible while the independently owned runtime is stale.
            while not _shutdown_requested(self):
                try:
                    # Retry recovery before maintenance on every preparation pass.
                    await self._recover_on_boot()
                    if await self._comfy.recover_orphans(self):
                        await self._recover_on_boot()
                    await asyncio.to_thread(prepare_preview, self)
                    break
                except Exception as exc:
                    self._drain = True
                    print(f'[worker] {exc}', flush=True)
                    try:
                        await asyncio.to_thread(report_failure, config, exc)
                    except Exception:
                        pass  # retain durable failure marker for retry
                    await asyncio.sleep(30)

        record = self.journal.get_command()
        if record and not record.get('confirmed'):
            # A spawn-intent without a recorded exit is ambiguous. Never replay shell.
            self._command_recovery_required = True
            self._unhealthy_reason = 'command recovery_required: local operator must confirm process exit'
        while not _shutdown_requested(self):
            try:
                await self._register()
                break
            except ApiError as e:
                if e.status in (401, 403):
                    raise
                print(f"[worker] registration deferred: {self._safe_error_text(e)}", flush=True)
                await asyncio.sleep(5.0)
        if _shutdown_requested(self):
            return
        await self._recover_command()
        await self._complete_pending_update()
        self._command_control_task = asyncio.create_task(self._command_control_loop())
        self._capability_task = asyncio.create_task(self._comfy_readiness_loop())
        self._comfy_recovery_task = asyncio.create_task(self._comfy.recovery_loop(self))

        while not _shutdown_requested(self):
            try:
                if getattr(config, 'cpu_tail_overlap', False):
                    await cpu_overlap.tick(self)
                    continue
                if self._comfy_reconcile_pending or self.journal.active_attempt() is not None:
                    await self._recover_on_boot()
                    self._comfy_reconcile_pending = False
                    if self.journal.active_attempt() is not None:
                        await self._node_heartbeat("busy", monitor_status="busy")
                        await asyncio.sleep(5.0)
                        continue
                if self._drain:
                    payload = await self._node_heartbeat(
                        "draining", monitor_status="draining",
                    )
                    await asyncio.sleep(5.0)
                    continue
                if self._disk_free_bytes() < config.min_free_disk_bytes:
                    await self._node_heartbeat(
                        "unhealthy", monitor_status="disk_low",
                        disk_free_bytes=self._disk_free_bytes(),
                    )
                await self._claim_and_execute_once()
            except sqlite3.Error as e:
                # Report the real persistence error and retry on the next pass.
                self._unhealthy_reason = f"journal error: {self._safe_error_text(e)}"
                self.monitor.add_anomaly(
                    "other", f"journal error: {self._safe_error_text(repr(e))}"[:200]
                )
                print(f"[worker] journal error: {self._safe_error_text(e)}; retrying",
                      flush=True)
                await asyncio.sleep(30.0)
            except Exception as e:  # noqa: BLE001 - worker keeps running
                print(f"[worker] loop error: {self._safe_error_text(e)}", flush=True)
                self.monitor.add_anomaly("other", f"loop error: {self._safe_error_text(repr(e))}")
                await asyncio.sleep(2.0)

    # -- singleton lock ---------------------------------------------------

    def _acquire_singleton_lock(self) -> None:
        import fcntl

        self._lock_fh = open(self.config.lock_path, "w")
        try:
            fcntl.flock(self._lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            raise SystemExit(
                "another worker owns the data dir (lock held): " + str(e)
            ) from e
        # lock is released by the OS when the process dies (client README 7)

    # -- registration ------------------------------------------------------

    @property
    def _comfy(self):
        """Per-executor transport; local Workers retain their existing adapter."""
        return getattr(self, '_comfy_adapter', comfy)

    def _ensure_capability_state(self) -> None:
        """Initialize only new boot-local fields for legacy lightweight Workers.

        Keep existing state intact, including in-flight probes and the boot cache.
        config, http and boot_id remain required caller-provided dependencies.
        """
        defaults = {
            "_comfy_probe_task": None,
            "_comfy_successes": 0,
            "_comfy_published": False,
            "_base_capabilities": None,
            "_capability_dirty": False,
            "_comfy_ready": False,
            "_capability_task": None,
        }
        for name, value in defaults.items():
            if not hasattr(self, name):
                setattr(self, name, value)

    async def _build_base_capabilities(self) -> Dict[str, Any]:
        modes = ["t2va", "fl2va", "ref2va"]
        if await asyncio.to_thread(runmod.engine_supports_given_audio, self.config):
            modes.append("a2va")
        from shared.h3proto import H3_OPTIMIZATION_CAPABILITIES
        # Config cannot bypass a failed executable/hardware probe.
        models = [model for model in self.config.capability_models
                  if model not in H3_OPTIMIZATION_CAPABILITIES.values()
                  and not ltx.is_family(model)]
        models.extend(await asyncio.to_thread(runmod.engine_optimization_capabilities, self.config))
        if getattr(self.config, "comfyui_ready", False) and comfy.CAPABILITY not in models:
            models.append(comfy.CAPABILITY)
        if any(
            capability in ("cosyvoice3-mlx", "tts.cosyvoice3.clone")
            or capability.startswith("tts.qwen3.")
            for capability in models
        ):
            modes.append("tts")
        if "audio.qwen3.forced_align" in models:
            modes.append("align")
        from .upgrade import installation_update_sources, _repo_root
        capabilities = {
            "models": models,
            "modes": modes,
            "update_sources": await asyncio.to_thread(
                installation_update_sources, _repo_root(), self.config.data_dir,
            ),
        }
        return capabilities

    async def _capability_snapshot(self, comfy_ready: bool) -> Dict[str, Any]:
        self._ensure_capability_state()
        if self._base_capabilities is None:
            self._base_capabilities = await self._build_base_capabilities()
        # Every payload owns its lists; transitions never mutate the boot cache.
        capabilities = {
            key: list(values) for key, values in self._base_capabilities.items()
        }
        if comfy.CAPABILITY in capabilities["models"]:
            capabilities["modes"].append(comfy.MODE)
        if getattr(self, '_ltx_ready', False):
            from shared.ltx_policy import MODE, REVISION
            capabilities['models'].append(REVISION)
            capabilities['modes'].append(MODE)
        return capabilities

    def _withdraw_ltx_capability(self) -> None:
        self._ltx_ready = False
        self._capability_dirty = True

    async def _probe_comfy(self) -> bool:
        self._ensure_capability_state()
        # A timed-out thread cannot be killed. Retain it until completion and
        # discard its late result; never accumulate overlapping readiness calls.
        if self._comfy_probe_task is not None:
            if not self._comfy_probe_task.done():
                return False
            try:
                self._comfy_probe_task.result()
            except Exception:
                pass
            self._comfy_probe_task = None
        self._comfy_probe_task = asyncio.create_task(
            asyncio.to_thread(comfy.available, self)
        )
        try:
            ready = await asyncio.wait_for(
                asyncio.shield(self._comfy_probe_task), self.COMFY_PROBE_TIMEOUT
            )
        except asyncio.TimeoutError:
            return False
        except Exception:
            self._comfy_probe_task = None
            return False
        self._comfy_probe_task = None
        return ready

    async def _refresh_comfy(self, *, initial: bool = False) -> None:
        self._ensure_capability_state()
        from .ltx_runner import Readiness
        if not hasattr(self, '_ltx_probe'):
            self._ltx_probe = Readiness()
            self._ltx_ready = False
            self._ltx_probe_task = None
        if self._ltx_probe_task is None:
            self._ltx_probe_task = asyncio.create_task(asyncio.to_thread(self._ltx_probe.check))
        if self._ltx_probe_task.done():
            try:
                ltx_ready = self._ltx_probe_task.result()
            except Exception:
                ltx_ready = False
            self._ltx_probe_task = None
            if ltx_ready != self._ltx_ready:
                self._ltx_ready = ltx_ready
                self._capability_dirty = True
        ready = await self._probe_comfy()
        self._comfy_successes = self._comfy_successes + 1 if ready else 0
        self._comfy_ready = self._comfy_successes >= self.COMFY_SUCCESS_THRESHOLD
        if (initial or self._capability_dirty
                or self._comfy_ready != self._comfy_published):
            self._capability_dirty = True
            await self._register(self._comfy_ready)
            self._comfy_published = self._comfy_ready
            self._capability_dirty = False

    async def _comfy_readiness_loop(self) -> None:
        while not _shutdown_requested(self):
            await asyncio.sleep(self.COMFY_PROBE_SECONDS)
            try:
                await self._refresh_comfy()
            except Exception as exc:
                print(f"[worker] capability refresh deferred: {self._safe_error_text(exc)}", flush=True)

    async def _register(self, comfy_ready: Optional[bool] = None) -> None:
        self._ensure_capability_state()
        if comfy_ready is None:
            await self._refresh_comfy(initial=True)
            return
        capabilities = await self._capability_snapshot(comfy_ready)
        capabilities["command_protocol"] = 2
        capabilities["workflow_save_protocol"] = 1
        if getattr(self.config, "comfyui_preview_enabled", False):
            from .comfy_preview import available
            preview_ready = await asyncio.to_thread(available, self)
            if not preview_ready and not self._registered_current_boot:
                from .startup_prepare import PreparationError
                raise PreparationError('Preview startup failed: loaded bridge readiness; inspect dedicated ComfyUI locally')
            if preview_ready:
                capabilities["preview_protocol"] = 1
                from shared.comfy_versions import FRONTENDS
                capabilities["preview_frontend"] = FRONTENDS[self.config.comfyui_version]
                from .comfy_preview import native_available
                if await asyncio.to_thread(native_available, self):
                    capabilities['native_protocol'] = 1
        capabilities["cpu_tail_protocol"] = 1
        if getattr(self.config, 'cpu_tail_overlap', False):
            capabilities["cpu_tail_overlap"] = 1
        versions = {
            "worker_version": __version__,
            "engine_version": "fake" if self.config.fake_runner else "h3-0.1",
        }
        result = await asyncio.to_thread(
            self.http.register,
            self.config.worker_id, self.boot_id, capabilities, versions,
        )
        if getattr(self.config, 'cpu_tail_pilot', False):
            self._performance_revisions = await asyncio.to_thread(cpu_overlap.revisions, self.config)
        self._registered_current_boot = True
        self._cpu_overlap_negotiated = result.get('cpu_tail_overlap') is True
        self.heartbeat_interval = float(
            result.get("heartbeat_interval_seconds")
            or self.config.heartbeat_seconds
        )
        self.lease_seconds = float(result.get("lease_seconds") or 90)
        if self.lease_seconds <= self.config.lease_safety_margin_seconds:
            print(
                f"[worker] WARNING: lease {self.lease_seconds}s <= safety "
                f"margin {self.config.lease_safety_margin_seconds}s; "
                "execution will stop immediately",
                flush=True,
            )
        print(
            f"[worker] registered {self.config.worker_id} boot={self.boot_id} "
            f"hb={self.heartbeat_interval}s lease={self.lease_seconds}s",
            flush=True,
        )

    # ==================================================================
    # restart recovery (client README 7)
    # ==================================================================

    def _defer_recovery(self, attempt_id: str, reason: str = "contact unavailable") -> None:
        record = self.journal.get_attempt(attempt_id)
        if record and not record.get("confirmed_terminal"):
            self.journal.update_attempt(attempt_id, recovery_state="offline", recovery_reason=reason)

    def _quarantine_attempt(self, attempt_id: str, reason: str) -> None:
        # Retain all evidence. Only definitively unrecoverable attempts release
        # admission; unresolved local writers are separately fenced by process identity.
        self.journal.update_attempt(attempt_id, confirmed_terminal=1,
                                    recovery_state="quarantined", recovery_reason=reason)

    async def _recover_on_boot(self) -> None:
        active = self.journal.active_attempts()
        if any(self.journal.cpu_handoff(r['attempt_id']) for r in active) and not getattr(self, '_registered_current_boot', False):
            # Reap every native writer before registering the new boot; handoff
            # confirmation then binds authenticated recovery to that boot.
            for record in active:
                proc = self.journal.get_process(record['attempt_id'])
                if proc:
                    self._reap_leftover_process(record['attempt_id'], proc)
            return
        jobs = getattr(self, '_publication_jobs', {})
        recover = [r for r in active if r['attempt_id'] not in jobs]
        if len(recover) <= 1:
            for record in recover:
                await self._recover_attempt(record['attempt_id'], record)
        else:
            await asyncio.gather(*(self._recover_attempt(r['attempt_id'], r) for r in recover))

    @attempt_scoped
    async def _recover_attempt(self, attempt_id, active) -> None:
        proc_row = self.journal.get_process(attempt_id)
        if proc_row and not self._reap_leftover_process(attempt_id, proc_row):
            return
        try:
            view = await self._reconcile_with_anchor(attempt_id,
                                          active["lease_token"],
                                          bool(active.get("local_completed") or active.get("finish_payload")))
        except ApiError as e:
            if e.status == 0 or e.status >= 500 or e.status == 429:
                self._defer_recovery(attempt_id)
            else:
                self._quarantine_attempt(attempt_id, e.code)
            return
        except OSError:
            self._defer_recovery(attempt_id)
            return
        self.journal.update_attempt(attempt_id, recovery_state="online")
        self.lease_lost.clear()
        self.cancel_requested.clear()
        if active.get("finish_payload"):
            if view.get("status") == "RUNNING" and view.get("is_current"):
                self.current_attempt = dict(attempt_id=attempt_id, task_id=active["task_id"],
                                            lease_token=active["lease_token"])
                self.monitor.begin_attempt(attempt_id, active["task_id"])
                self.monitor.set_node("busy", disk_free_bytes=self._disk_free_bytes())
                self._arm_lease_clock_from_view(view)
                self.heartbeat_seq = int(active.get("heartbeat_seq") or 0)
                self.lease_loop_task = asyncio.create_task(self._lease_loop(attempt_id))
            try:
                await self._replay_finish_intent(attempt_id, active, view)
            finally:
                if self.lease_loop_task:
                    self.lease_loop_task.cancel()
                    await asyncio.gather(self.lease_loop_task, return_exceptions=True)
                    self.lease_loop_task = None
                self.current_attempt = None
            return
        if view.get("status") != "RUNNING" or not view.get("is_current"):
            self._quarantine_attempt(attempt_id, "incompatible terminal result")
            return
        # Durable ComfyUI submission identity permits observation, never another submit.
        if (active["request_snapshot"].get("mode") == comfy.MODE or getattr(self, "_comfy_adapter", None) is not None) and active.get("engine_state") and not active.get("local_completed"):
            claim = dict(view)
            try:
                await self._execute_attempt(attempt_id, claim)
            finally:
                if self.lease_loop_task:
                    self.lease_loop_task.cancel()
                    await asyncio.gather(self.lease_loop_task, return_exceptions=True)
                    self.lease_loop_task = None
                self.current_attempt = None
                self.cancel_requested.clear()
                self.lease_lost.clear()
            return
        if await self._resume_upload_and_finish(attempt_id, active, view):
            return
        if active.get("local_completed"):
            # Failed transfers retain the attempt. Bad local evidence is
            # classified inside the resume path and never re-executed.
            return
        await self._release_unrecoverable(attempt_id, active, view,
                                          "restart without sufficient execution evidence")

    @attempt_scoped
    async def _release_unrecoverable(self, attempt_id: str, active: Dict[str, Any],
                                     view: Dict[str, Any], reason: str) -> None:
        """Release through a durable fenced failure, retaining evidence for replay."""
        self.current_attempt = dict(attempt_id=attempt_id, task_id=active["task_id"],
                                    lease_token=active["lease_token"])
        self.monitor.begin_attempt(attempt_id, active["task_id"])
        self._arm_lease_clock_from_view(view)
        self.heartbeat_seq = int(active.get("heartbeat_seq") or 0)
        self.lease_loop_task = asyncio.create_task(self._lease_loop(attempt_id))
        try:
            await self._finish_failed(attempt_id, active["lease_token"],
                                      "ENGINE_FAILED", reason, "recovery")
            if not self.journal.get_attempt(attempt_id).get("confirmed_terminal"):
                self._defer_recovery(attempt_id, reason)
        except (ApiError, OSError, CancelRequested, LeaseLost) as exc:
            self._defer_recovery(attempt_id, self._safe_error_text(exc))
        finally:
            self.lease_loop_task.cancel()
            await asyncio.gather(self.lease_loop_task, return_exceptions=True)
            self.lease_loop_task = None
            self.current_attempt = None
            self.cancel_requested.clear()
            self.lease_lost.clear()
            self.monitor.set_node("idle", disk_free_bytes=self._disk_free_bytes())
            self._report_monitor_soon()

    async def _reconcile_with_anchor(self, attempt_id, lease_token, completed):
        anchor = (time.monotonic(), time.time())
        view = await asyncio.to_thread(self.http.reconcile_attempt,
                                      attempt_id, lease_token, completed)
        return dict(view, _lease_request_anchor=anchor)

    def _arm_lease_clock_from_view(self, view: Dict[str, Any]) -> None:
        """Anchor authenticated server duration at request send, never receipt.

        An expired or delayed response must arm an expired clock. Completed
        delivery uses its recovery lease independently of the execution budget.
        """
        sent_mono, sent_wall = view["_lease_request_anchor"]
        expiry = parse_iso(view["lease_expires_at"]).timestamp()
        if not view.get("completion_recovery"):
            expiry = min(expiry, parse_iso(view["execution_deadline_at"]).timestamp())
        remaining = expiry - parse_iso(view["server_time"]).timestamp()
        self.lease_hard_deadline_mono = sent_mono + remaining
        self.lease_hard_deadline_wall = sent_wall + remaining
        margin = self.config.lease_safety_margin_seconds
        self.lease_deadline_mono = self.lease_hard_deadline_mono - margin
        self.lease_deadline_wall = self.lease_hard_deadline_wall - margin
        self._lease_renewal_pending = False

    @attempt_scoped
    async def _resume_upload_and_finish(
        self, attempt_id: str, active: Dict[str, Any], view: Dict[str, Any],
    ) -> bool:
        """Crash-recovery fast path after authenticated ownership restoration.
        Upload whatever is missing and submit the SAME deterministic
        finish request (``fin_{attempt_id}``), so the server-side
        idempotency accepts it exactly as if the worker had survived.

        Returns True when the pipeline was resumed (finish submitted or
        resolved); False when resumption is impossible or was abandoned —
        the caller then falls back to the lease-expiry path."""
        rows = self.journal.artifacts_for(attempt_id)
        by_role = {r["role"]: r for r in rows}
        request_snapshot = active.get("request_snapshot") or {}
        required_roles = request_snapshot.get("required_artifacts") \
            or ["video", "manifest"]
        if any(role not in by_role for role in required_roles):
            if active.get("local_completed"):
                await self._release_unrecoverable(attempt_id, active, view, "incomplete local artifact journal")
                return True
            return False
        for r in rows:
            if not os.path.isfile(r["path"]):
                await self._release_unrecoverable(attempt_id, active, view, "missing local artifact")
                return True
        lease_token = active.get("lease_token")
        if not lease_token:
            return False
        try:
            with open(by_role["manifest"]["path"], encoding="utf-8") as fh:
                manifest = json.load(fh)
        except (OSError, ValueError):
            await self._release_unrecoverable(attempt_id, active, view, "invalid local manifest")
            return True
        # re-verify digests (terminal review B-3 step 1): never trust the
        # pre-crash journal record — a torn write or disk issue must be
        # caught here, not at server-side finish verification
        for r in rows:
            size, got = await asyncio.to_thread(file_digest, r["path"])
            if got != r["sha256"] or size != r["size_bytes"]:
                print(
                    f"[worker] resume of {attempt_id} rejected: local "
                    f"artifact {r['role']} digest mismatch",
                    flush=True,
                )
                await self._release_unrecoverable(attempt_id, active, view, "corrupt local artifact")
                return True
        result = active.get("local_result") or manifest.get("result") or {}
        artifacts = {
            r["role"]: {"path": r["path"], "size": r["size_bytes"],
                        "sha256": r["sha256"]}
            for r in rows
        }
        uploaded = sum(
            1 for r in rows if r.get("upload_state") == "uploaded"
        )
        print(
            f"[worker] resuming attempt {attempt_id} from local artifacts "
            f"({uploaded}/{len(rows)} already uploaded)",
            flush=True,
        )
        self.current_attempt = {
            "attempt_id": attempt_id,
            "lease_token": lease_token,
            "task_id": active["task_id"],
        }
        self.monitor.begin_attempt(attempt_id, active["task_id"])
        self.monitor.set_node(
            "busy", disk_free_bytes=self._disk_free_bytes()
        )
        self.monitor.set_stage("uploading")
        self._report_monitor_soon()
        self._arm_lease_clock_from_view(view)
        # restore the per-attempt heartbeat seq from the journal BEFORE
        # any heartbeat (terminal review B-3 precondition): a restart
        # from 0 would replay an already-seen seq and the server would
        # silently stop renewing the lease mid-resume
        async with self._heartbeat_lock:
            self.heartbeat_seq = int(active.get("heartbeat_seq") or 0)
        # The authenticated recovery call already renewed ownership. Do not
        # add a second synchronous contact requirement before delivery.
        # keep the lease alive while the uploads run
        self.lease_loop_task = asyncio.create_task(
            self._lease_loop(attempt_id)
        )
        try:
            await self._upload_artifacts(attempt_id, lease_token, artifacts)
            await self._finish_success(
                attempt_id, lease_token, artifacts, result
            )
            return True
        except CancelRequested:
            # cancel observed during the resumed upload: comply with the
            # still-valid token before handing back
            outcome = await self._submit_cancelled(attempt_id, lease_token)
            self.monitor.end_attempt(outcome)
            return False
        except LeaseLost:
            print(
                f"[worker] resume of {attempt_id} hit a lost lease; "
                "leaving terminal state to the server",
                flush=True,
            )
            self._quarantine_attempt(attempt_id, "authoritative lease loss")
            self.monitor.end_attempt("UNSETTLED")
            return True
        except Exception as e:  # noqa: BLE001 - never block recovery
            print(f"[worker] resume of {attempt_id} failed: {self._safe_error_text(e)}", flush=True)
            if (isinstance(e, ApiError) and 400 <= e.status < 500
                    and e.status != 429 and e.code != "LEASE_LOST"):
                self._quarantine_attempt(attempt_id, e.code)
            else:
                self._defer_recovery(attempt_id, self._safe_error_text(e))
            self.monitor.end_attempt("UNSETTLED")
            return True
        finally:
            if self.lease_loop_task:
                self.lease_loop_task.cancel()
                try:
                    await self.lease_loop_task
                except asyncio.CancelledError:
                    pass
                self.lease_loop_task = None
            self.current_attempt = None
            self.cancel_requested.clear()
            self.lease_lost.clear()
            self.monitor.set_node(
                "idle", disk_free_bytes=self._disk_free_bytes()
            )
            self._report_monitor_soon()

    async def _replay_finish_intent(
        self, attempt_id: str, active: Dict[str, Any], view: Dict[str, Any]
    ) -> None:
        """Finish was submitted but the response was never observed
        (client README 3.8 / 7.4)."""
        request_id = active["finish_request_id"]
        payload = active["finish_payload"]
        accepted = view.get("accepted_finish_request_id")
        if accepted == request_id:
            # already accepted server-side: backfill local confirmation only
            self.journal.update_attempt(
                attempt_id, finish_accepted=1, confirmed_terminal=1
            )
            self.monitor.add_anomaly(
                "other",
                f"boot replay finish {request_id}: already accepted "
                f"remotely ({view.get('status')})",
            )
            print(f"[worker] finish {request_id} was already accepted",
                  flush=True)
            return
        if view.get("status") == "RUNNING" and view.get("is_current"):
            # lease may still be alive: replay the exact same request
            lease_token = active.get("lease_token")
            if lease_token:
                print(f"[worker] replaying finish {request_id}", flush=True)
                status = payload.get("status")
                body = dict(payload)
                body.pop("request_id", None)
                outcome = await self._submit_finish(
                    attempt_id, lease_token, request_id, body,
                    kind=status or "FAILED",
                )
                # C-6 (terminal review): the recovered attempt has no
                # monitor attempt slot this boot — leave an anomaly so
                # its final outcome is visible on the monitoring page
                self.monitor.add_anomaly(
                    "other",
                    f"boot replay finish {request_id}: {outcome}",
                )
                return
        # attempt moved on without our finish: reconciled
        self.journal.update_attempt(attempt_id, confirmed_terminal=1)
        self.journal.drop_outbox(attempt_id)
        self.monitor.add_anomaly(
            "other",
            f"boot replay finish {request_id}: attempt is "
            f"{view.get('status')} remotely, reconciled",
        )

    def _reap_leftover_process(
        self, attempt_id: str, proc_row: Dict[str, Any]
    ) -> bool:
        """Prove the old writer exited before resuming this same attempt."""
        pid = proc_row['pid']
        identity = proc_row['start_identity']
        if not pid:
            self.journal.clear_process(attempt_id)
            return True
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            if (getattr(self.config, 'cpu_tail_overlap', False) or
                    any(self.journal.cpu_handoff(r['attempt_id']) for r in self.journal.active_attempts())):
                if _command_group_alive(proc_row.get('pgid') or pid) is not False:
                    self._unhealthy_reason = f'leftover native group {pid} exit unproven; local recovery required'
                    return False
            self.journal.clear_process(attempt_id)
            return True
        except PermissionError:
            self._unhealthy_reason = f'cannot signal leftover pid {pid}'
            return False
        current = runmod.process_start_identity(pid)
        if not current or not identity or current != identity:
            # Keep the durable ownership fence; never signal by PID guess.
            self._unhealthy_reason = f'leftover pid {pid} identity unproven; refusing to kill'
            return False
        try:
            if os.getpgid(pid) != pid:
                self._unhealthy_reason = f'leftover pid {pid} process group identity unproven'
                return False
            os.killpg(pid, 15)
            time.sleep(self.config.cancel_grace_seconds)
            os.killpg(pid, 9)
        except ProcessLookupError:
            pass
        except PermissionError:
            self._unhealthy_reason = f'cannot signal leftover process group {pid}'
            return False
        if _command_group_alive(pid) is not False:
            self._unhealthy_reason = f'leftover process group {pid} exit unproven'
            return False
        self.journal.clear_process(attempt_id)
        return True

    # ==================================================================
    # claim + execute
    # ==================================================================

    async def _claim_and_execute_once(self) -> None:
        if not hasattr(self, '_claim_lock'):
            self._claim_lock = asyncio.Lock()
        async with self._claim_lock:
            await self._claim_once_unlocked()

    async def _claim_once_unlocked(self) -> None:
        if (_shutdown_requested(self) or self._drain or not cpu_overlap.can_claim(self)):
            return
        request_id = self.journal.new_claim_request_id()
        hard_timeout = max(0.05, min(
            self.config.claim_wait_seconds + 20.0,
            self.lease_seconds
            - self.config.lease_safety_margin_seconds
            - 1.0,
        ))
        try:
            claimed, response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.http.claim,
                    request_id, self.config.worker_id, self.boot_id,
                    self.config.claim_wait_seconds,
                ),
                timeout=hard_timeout,
            )
        except asyncio.TimeoutError:
            print(
                f"[worker] claim exceeded hard timeout {hard_timeout:.1f}s; "
                "exiting for journal replay",
                flush=True,
            )
            # The request id was committed before the call.  A normal
            # asyncio shutdown would wait forever for the wedged urllib
            # thread, so let the service manager restart us and replay the
            # same idempotent claim from the journal.
            os._exit(75)
        except ApiError as e:
            if e.status in (401, 403):
                print("[worker] credentials rejected; stopping", flush=True)
                self.stop_event.set()
                return
            if 400 <= e.status < 500:
                # permanent rejection: this request_id is burned (protocol
                # 10.3: 4xx does not pin idempotency); do not hot-loop
                self.journal.close_claim(request_id)
                print(f"[worker] claim rejected: {self._safe_error_text(e)}", flush=True)
                await asyncio.sleep(30.0)
                return
            raise
        if not claimed:
            # definitive empty: next claim uses a new request id
            self.journal.close_claim(request_id)
            # idle node heartbeat (protocol 4)
            payload = await self._node_heartbeat(
                "unhealthy" if self._unhealthy_reason else "idle")
            return

        # B2: replayed claims carry the CURRENT attempt/task status; a
        # terminal or expired allocation must never be executed again
        attempt_status = response.get("attempt_status")
        task_status = response.get("task_status")
        if attempt_status is not None and attempt_status != "RUNNING":
            print(
                f"[worker] claim replay returned {attempt_status} attempt "
                f"(task {task_status}); reconciling without executing",
                flush=True,
            )
            self.journal.resolve_claim(request_id, response["attempt_id"])
            self.journal.reconcile_attempt(response["attempt_id"])
            self.journal.close_claim(request_id)
            return
        if "lease_token" not in response or not response.get("lease_expires_at"):
            # RUNNING but no usable credentials: treat as lost, close the id
            print("[worker] claim replay without usable lease; closing",
                  flush=True)
            self.journal.close_claim(request_id)
            return

        attempt_id = response["attempt_id"]
        self.journal.resolve_claim(request_id, attempt_id)
        proc_row = self.journal.get_process(attempt_id)
        if proc_row and not self._reap_leftover_process(attempt_id, proc_row):
            self.journal.close_claim(request_id)
            return  # A replay cannot replace this attempt's unresolved writer.

        self.journal.start_attempt(
            attempt_id,
            response["task_id"],
            response["lease_token"],
            response["task"],
            response["lease_expires_at"],
            response["execution_deadline_at"],
        )
        self.journal.update_attempt(attempt_id, operation_key=response.get('operation_key'))
        print(
            f"[worker] claimed {response['task_id']} attempt {attempt_id}",
            flush=True,
        )
        if cpu_overlap.enabled(self):
            job = asyncio.create_task(self._execute_job(attempt_id, response))
            self._publication_jobs[attempt_id] = job
            return
        await self._execute_job(attempt_id, response)

    async def _execute_job(self, attempt_id, response):
        runtime = self._runtime_for_attempt(attempt_id)
        with bind_runtime(self, runtime):
            await self._execute_and_cleanup(attempt_id, response)

    async def _execute_and_cleanup(self, attempt_id, response):
        try:
            await self._execute_attempt(attempt_id, response)
        finally:
            self.current_attempt = None
            if self.lease_loop_task:
                self.lease_loop_task.cancel()
                try:
                    await self.lease_loop_task
                except asyncio.CancelledError:
                    pass
                self.lease_loop_task = None
            self.cancel_requested.clear()
            self.lease_lost.clear()
            # back to idle for monitoring (protocol 11): the finished
            # attempt stays in the last snapshot until the next claim.
            # A fine-grained incapacity reason (model_unavailable etc.)
            # survives until the NEXT successful claim (review C2) — the
            # server's eligibility backoff window outlives this loop.
            if self.monitor.node_status() in (
                "model_unavailable", "disk_low", "unhealthy"
            ):
                self.monitor.set_node(
                    self.monitor.node_status(),
                    disk_free_bytes=self._disk_free_bytes(),
                )
            else:
                self.monitor.set_node(
                    "busy" if self.journal.active_attempts() else "idle", disk_free_bytes=self._disk_free_bytes()
                )
            await self._report_monitor()

    # ==================================================================
    # one attempt execution
    # ==================================================================

    @attempt_scoped
    async def _execute_attempt(self, attempt_id: str, claim: Dict[str, Any]) -> None:
        record = self.journal.get_attempt(attempt_id)
        task_request = record["request_snapshot"]
        lease_token = record["lease_token"]
        self.current_attempt = {
            "attempt_id": attempt_id,
            "lease_token": lease_token,
            "task_id": record["task_id"],
        }
        # heartbeat seq is PER ATTEMPT on the server: a fresh claim starts
        # from the journaled value (0 for a new attempt row)
        async with self._heartbeat_lock:
            self.heartbeat_seq = int(record.get("heartbeat_seq") or 0)
        self.monitor.begin_attempt(attempt_id, record["task_id"])
        self.monitor.set_node(
            "busy", disk_free_bytes=self._disk_free_bytes(),
            unhealthy_reason=self._unhealthy_reason,
        )
        self._report_monitor_soon()

        # Keep input validation; runtime readiness is diagnosed by execution.
        problems = validate_task_request(task_request)
        seed = task_request.get("seed")
        if seed is not None and not validate_seed_string(seed):
            problems.append("seed is not a uint64 decimal string")
        if problems:
            await self._finish_failed(
                attempt_id, lease_token, "INPUT_INVALID",
                "; ".join(problems), stage="downloading",
            )
            return

        # start lease loop; the claim's lease is created server-side at
        # GRANT time (the end of a possibly long poll), so the initial
        # clock anchors at the response receive time.  Heartbeats below
        # anchor at their request SEND time (conservative for in-flight
        # renewals) per client README 7.
        self._init_lease_clock(claim)
        self.heartbeat_seq = int(record.get("heartbeat_seq") or 0)
        self.lease_loop_task = asyncio.create_task(self._lease_loop(attempt_id))

        try:
            if task_request.get('mode') == ltx.MODE and any(
                    a['attempt_id'] != attempt_id for a in self.journal.active_attempts()):
                raise LocalIncapacity('LTX requires exclusive GPU/publication slot')
            # phase 1: downloading (skip when no references)
            local_inputs: Dict[str, str] = {}
            references = task_request.get("references") or []
            anchors = task_request.get("anchors") or {}
            if (references or anchors) and not (task_request.get('mode') == 'comfyui_video' and task_request.get('_native')):
                self.monitor.set_stage("downloading")
                self._report_monitor_soon()
                local_inputs = await self._download_inputs(
                    attempt_id, lease_token, task_request
                )

            # phase 2: run inference
            self.monitor.set_stage("loading")
            self._report_monitor_soon()
            exit_ok, result = await self._run_engine(
                attempt_id, lease_token, task_request, local_inputs
            )
            if not exit_ok:
                failure = result or {}
                self.monitor.add_anomaly(
                    "engine_failed",
                    f"{failure.get('code')}: "
                    f"{self._safe_error_text(failure.get('message'))[:120]}",
                )
                await self._finish_failed(
                    attempt_id, lease_token,
                    failure.get("code", "ENGINE_FAILED"),
                    failure.get("message", "engine exited non-zero"),
                    stage=failure.get("stage") or self._current_stage(attempt_id),
                )
                return

            self._checkpoint_cpu_tail(attempt_id, "engine_exited")

            # phase 3: validating + manifest
            self.monitor.set_stage("validating")
            self._report_monitor_soon()
            artifacts = await self._validate_and_manifest(
                attempt_id, lease_token, task_request, local_inputs,
                result or {}
            )

            # phase 4: uploading
            self.monitor.set_stage("uploading")
            self._report_monitor_soon()
            await self._upload_artifacts(attempt_id, lease_token, artifacts)

            # phase 5: finish (finalizing = awaiting server confirmation)
            self.monitor.set_stage("finalizing")
            self._report_monitor_soon()
            await self._finish_success(
                attempt_id, lease_token, artifacts, result or {}
            )
        except (LeaseLost, CancelRequested):
            await self._shutdown_after_cancel_or_loss(attempt_id, lease_token)
        except LocalIncapacity as exc:
            await self._release_or_fail(attempt_id, lease_token, exc)
        except NativeGroupUnproven as exc:
            self._defer_recovery(attempt_id, str(exc))
            self._unhealthy_reason = str(exc)
            self._drain = True
        except Exception as e:  # noqa: BLE001 - unknown local failure
            record = self.journal.get_attempt(attempt_id)
            if record.get("local_completed") or record.get("finish_payload"):
                if isinstance(e, ApiError) and e.code == 'CANCEL_REQUESTED':
                    self._set_cancel_requested()
                    await self._shutdown_after_cancel_or_loss(attempt_id, lease_token)
                elif (isinstance(e, ApiError) and 400 <= e.status < 500
                        and e.status != 429 and e.code != "LEASE_LOST"):
                    self._quarantine_attempt(attempt_id, e.code)
                else:
                    self._defer_recovery(attempt_id, self._safe_error_text(e))
                return
            # review C15: never abandon an attempt without a verdict
            print(f"[worker] unexpected error: {self._safe_error_text(repr(e))}", flush=True)
            try:
                await self._finish_failed(
                    attempt_id, lease_token,
                    "INPUT_UNAVAILABLE" if isinstance(e, ApiError) and e.code == "INPUT_UNAVAILABLE" else "ENGINE_FAILED",
                    self._safe_error_text(e),
                    stage=self._current_stage(attempt_id),
                )
            except Exception as e2:  # noqa: BLE001
                print(f"[worker] fallback finish failed: {self._safe_error_text(e2)}", flush=True)
                self._defer_recovery(attempt_id)

    def _checkpoint_cpu_tail(self, attempt_id: str, stage: str) -> None:
        if getattr(self.config, "cpu_tail_pilot", False):
            snapshot = self.journal.checkpoint_cpu_tail(
                attempt_id, stage, self.config.cpu_tail_max_bytes)
            print(json.dumps(dict(type="cpu_tail_pilot", **snapshot),
                             sort_keys=True), flush=True)

    def _current_stage(self, attempt_id: str) -> str:
        record = self.journal.get_attempt(attempt_id)
        return (record or {}).get("current_stage") or "downloading"

    # ==================================================================
    # lease management
    # ==================================================================

    def _init_lease_clock(self, claim: Dict[str, Any]) -> None:
        """Initial claim clock, or the request-send clock for recovered execution.

        A claim lease is granted at the end of the long poll. Recovered claims
        retain their authenticated reconcile request anchors and server_time.
        Both clock views cover monotonic clocks that pause during system sleep.
        """
        received_mono, received_wall = claim.get("_lease_request_anchor") or (time.monotonic(), time.time())
        server_time = parse_iso(claim["server_time"]).timestamp()
        lease_expires = parse_iso(claim["lease_expires_at"]).timestamp()
        deadline = parse_iso(claim["execution_deadline_at"]).timestamp()
        remaining = (lease_expires if claim.get("completion_recovery") else min(lease_expires, deadline)) - server_time
        margin = self.config.lease_safety_margin_seconds
        self.lease_hard_deadline_mono = received_mono + remaining
        self.lease_hard_deadline_wall = received_wall + remaining
        self.lease_deadline_mono = self.lease_hard_deadline_mono - margin
        self.lease_deadline_wall = self.lease_hard_deadline_wall - margin
        self._lease_renewal_pending = False
        self.heartbeat_seq = 0

    def _check_lease_alive(self) -> None:
        if _shutdown_requested(self):
            raise CancelRequested()
        if getattr(self, "cancel_requested", None) is not None and self.cancel_requested.is_set():
            raise CancelRequested()
        if self.lease_lost.is_set():
            raise LeaseLost()
        if self._local_lease_expired() and getattr(self, "current_attempt", None):
            record = self.journal.get_attempt(self.current_attempt["attempt_id"])
            if record and record.get("recovery_state") != "offline":
                self._defer_recovery(record["attempt_id"], "local lease clock elapsed")

    def _local_lease_expired(self) -> bool:
        """Return whether remote delivery needs ownership reconciliation.

        ``lease_deadline_*`` is deliberately conservative by one safety
        margin.  If a renewal chain started before that line, allow its
        response and prompt retries during the reserved margin; otherwise
        the engine can be stopped milliseconds before a successful renewal
        is applied. This clock gates delivery, never local inference.
        """
        if self.lease_deadline_mono == 0.0 and self.lease_deadline_wall == 0.0:
            return False
        now_mono = time.monotonic()
        now_wall = time.time()
        safe_expired = (
            now_mono >= self.lease_deadline_mono
            or now_wall >= self.lease_deadline_wall
        )
        if not safe_expired:
            return False
        if self._lease_renewal_pending:
            hard_expired = (
                now_mono >= self.lease_hard_deadline_mono
                or now_wall >= self.lease_hard_deadline_wall
            )
            if not hard_expired:
                return False
        return True

    def _set_cancel_requested(self) -> None:
        if getattr(self, "current_attempt", None):
            self.journal.update_attempt(self.current_attempt["attempt_id"], cancel_intent=1)
        self.cancel_requested.set()
        for cb in list(self._attempt_stop_callbacks.values()):
            cb()

    def _set_lease_lost(self) -> None:
        self.lease_lost.set()
        for cb in list(self._attempt_stop_callbacks.values()):
            cb()

    def _transfer_should_abort(self) -> bool:
        """Non-raising predicate polled from transfer body reader threads:
        process shutdown, cancel requested, or the local conservative lease
        clock ran out.
        A 0.0 deadline means unarmed (recovery before any heartbeat) —
        do not treat it as already expired.  The reason is recorded so the
        TransferAborted handler classifies what WAS true at the decision
        moment, not a possibly-changed state re-read later (C-1)."""
        if _shutdown_requested(self):
            self._transfer_abort_reason = "shutdown"
            return True
        if self.cancel_requested.is_set():
            self._transfer_abort_reason = "cancel"
            return True
        if self.lease_lost.is_set():
            self._transfer_abort_reason = "lease_lost"
            return True
        if self._local_lease_expired():
            self._transfer_abort_reason = "offline"
            return True
        return False

    async def _lease_loop(self, attempt_id: str) -> None:
        """Independent heartbeat loop; never blocks inference or uploads.
        Also reports the monitoring snapshot on the same cadence (protocol
        11) so stage/progress updates reach the server within one
        heartbeat interval even during long engine phases."""
        delay = self.heartbeat_interval
        while True:
            await asyncio.sleep(delay)
            delay = self.heartbeat_interval
            try:
                await self._send_heartbeat(attempt_id)
            except LeaseLost:
                self._set_lease_lost()
                return
            except CancelRequested:
                return
            except ApiError as e:
                self.monitor.add_anomaly(
                    "lease_renew_failed",
                    f"heartbeat {e.status} {e.code}: {self._safe_error_text(e.message)[:120]}",
                )
                if e.code == "CANCEL_REQUESTED":
                    self._set_cancel_requested()
                    continue
                if e.code in ("LEASE_LOST", "ATTEMPT_NOT_CURRENT"):
                    self._set_lease_lost()
                    return
                if e.status in (401, 403):
                    self._set_lease_lost()
                    return
                # A heartbeat timeout already consumed part of the lease.
                # Retry promptly and skip the non-critical monitor report;
                # otherwise timeout + report + full cadence can exhaust a
                # healthy 90-second lease after only two network attempts.
                self._defer_recovery(attempt_id)
                delay = 2.0
                continue
            except Exception as e:  # noqa: BLE001
                self.monitor.add_anomaly(
                    "lease_renew_failed", f"heartbeat error: {self._safe_error_text(repr(e))}"[:200]
                )
                print(f"[worker] heartbeat error: {self._safe_error_text(e)}", flush=True)
                self._defer_recovery(attempt_id)
                delay = 2.0
                continue
            # Monitoring is best-effort and must not stretch the lease
            # heartbeat cadence when its own request is slow.
            self._report_monitor_soon()

    async def _send_heartbeat(self, attempt_id: str) -> None:
        # Local expiry requires reconciliation; only an explicit rejection stops inference.
        Worker._check_lease_alive(self)
        record = self.journal.get_attempt(attempt_id)
        if record is None:
            return
        sent_at_mono = time.monotonic()
        sent_at_wall = time.time()
        # Once a renewal chain starts inside the conservative window, keep
        # it pending across transient failures and the 2-second retry gap.
        # A retry that starts only after an already-unprotected safe expiry
        # must not manufacture additional execution authority.
        if (
            sent_at_mono < self.lease_deadline_mono
            and sent_at_wall < self.lease_deadline_wall
        ):
            self._lease_renewal_pending = True
        async with self._heartbeat_lock:
            self.heartbeat_seq += 1
            seq = self.heartbeat_seq
        # persist BEFORE the send (terminal review B-3 precondition): the
        # journal seq may end up ahead of the server after a crash — gaps
        # are fine, a restart-from-1 would look like a DUPLICATE and the
        # server would silently stop renewing the lease on resume
        self.journal.update_attempt(attempt_id, heartbeat_seq=seq)
        proc_status = (
            "running"
            if self.active_proc and self.active_proc.poll() is None
            else "idle"
        )
        # Budget the entire request, not each socket operation. At the
        # default lease this permits retries at 10, 22, 34, 46, ... seconds.
        budget = min(10.0, self.lease_seconds / 9.0)
        if record.get("recovery_state") == "offline" or self._local_lease_expired():
            payload = await self._heartbeat_transport.call(
                self.http.reconcile_attempt, attempt_id, record["lease_token"],
                bool(record.get("local_completed") or record.get("finish_payload")), timeout=budget)
            if payload.get("status") != "RUNNING" or not payload.get("is_current"):
                if record.get("finish_request_id") and payload.get("accepted_finish_request_id") == record["finish_request_id"]:
                    self.journal.update_attempt(attempt_id, finish_accepted=1, confirmed_terminal=1)
                    return
                raise LeaseLost()
        else:
            try:
                payload = await self._heartbeat_transport.call(
                    self.http.attempt_heartbeat,
                    attempt_id, record["lease_token"], seq, self.boot_id,
                    record.get("phase"), record.get("event_seq") or 0, proc_status,
                    {"disk_free_bytes": self._disk_free_bytes()},
                    timeout=budget,
                )
            except ApiError as e:
                if e.code != "LEASE_LOST":
                    raise
                # An expired heartbeat is not an authoritative recovery
                # verdict. The original token can still own its reservation.
                self._defer_recovery(attempt_id)
                payload = await self._heartbeat_transport.call(
                    self.http.reconcile_attempt, attempt_id, record["lease_token"],
                    bool(record.get("local_completed") or record.get("finish_payload")), timeout=budget)
                if payload.get("status") != "RUNNING" or not payload.get("is_current"):
                    raise LeaseLost()
        Worker._check_lease_alive(self)
        server_time = parse_iso(payload["server_time"]).timestamp()
        lease_expires = parse_iso(payload["lease_expires_at"]).timestamp()
        deadline = parse_iso(payload["execution_deadline_at"]).timestamp()
        remaining = (lease_expires if payload.get("completion_recovery") else min(lease_expires, deadline)) - server_time
        margin = self.config.lease_safety_margin_seconds
        new_hard_mono = sent_at_mono + remaining
        new_hard_wall = sent_at_wall + remaining
        # Concurrent callers are not expected in the normal attempt path,
        # but max() prevents a slower, older response from moving a newer
        # deadline backwards during recovery or future call-site changes.
        self.lease_deadline_mono = max(
            self.lease_deadline_mono,
            new_hard_mono - margin,
        )
        self.lease_deadline_wall = max(
            self.lease_deadline_wall,
            new_hard_wall - margin,
        )
        self.lease_hard_deadline_mono = max(
            self.lease_hard_deadline_mono,
            new_hard_mono,
        )
        self.lease_hard_deadline_wall = max(
            self.lease_hard_deadline_wall,
            new_hard_wall,
        )
        self._lease_renewal_pending = False
        self.journal.update_attempt(attempt_id, recovery_state="online",
                                    lease_expires_at=payload["lease_expires_at"],
                                    execution_deadline_at=payload["execution_deadline_at"])
        if payload.get("cancel_requested"):
            self._set_cancel_requested()
            self.monitor.set_cancel_requested()
            self.monitor.add_anomaly(
                "cancel_observed", "cancel marker seen in heartbeat"
            )
            if not record.get("cancel_intent"):
                # persist the cancel intent before acting (README 7)
                self.journal.update_attempt(attempt_id, cancel_intent=1)
        if "drain_requested" in payload:
            self._drain = bool(payload["drain_requested"])
        await self._maybe_run_command(payload)
        self._apply_command_control(payload)

    async def _report_monitor(self) -> None:
        """Send the monitoring snapshot; failures never affect the attempt
        (protocol 11) — the catch is exhaustive on purpose (review B2):
        any unexpected exception here would otherwise kill the lease loop.
        Note ApiError(0, NETWORK) covers malformed-response exceptions."""
        async with self._monitor_report_lock:
            try:
                # payload built INSIDE the try (fix-review C-3): even a
                # theoretically impossible snapshot error must not escape
                payload = await asyncio.to_thread(self.monitor.report_payload)
                report = payload['status_report']
                active = self.journal.active_attempts() if hasattr(self, 'journal') else []
                if (getattr(self.config, 'cpu_tail_overlap', False) or len(active) > 1 or
                        (active and runtime_for(self).attempt_id not in {r['attempt_id'] for r in active})):
                    report['worker_id'] = self.config.worker_id
                    reports = []
                    for record in active:
                        runtime = self._attempt_runtimes.get(record['attempt_id'])
                        if runtime is None:
                            continue
                        snapshot = await asyncio.to_thread(runtime.values['monitor'].snapshot)
                        handoff = self.journal.cpu_handoff(record['attempt_id'])
                        snapshot['occupancy'] = 'tail' if (handoff and handoff['acknowledgement'] and
                            handoff['acknowledgement']['gpu_occupancy_released']) else 'gpu'
                        reports.append(snapshot)
                    reports.sort(key=lambda snapshot: snapshot['occupancy'] != 'gpu')
                    report['attempts'] = reports
                    if reports:
                        report['attempt'] = reports[0]['attempt']
                    elif active:
                        report['attempt'] = None
                    if active:
                        report['node']['status'] = 'draining' if self._drain else 'busy'
                    self.journal.set_monitor_state(report)
                payload = sanitize_error_payload(payload, secrets=(
                    getattr(self.config, 'worker_token', ''),
                    getattr(self.config, 'cf_access_client_id', ''),
                    getattr(self.config, 'cf_access_client_secret', '')))
                await asyncio.to_thread(
                    self.http.report_status,
                    self.config.worker_id, payload,
                )
                self.monitor.note_report_ok()
            except ApiError as e:
                self.monitor.note_report_error(
                    f"{e.status} {e.code}: {self._safe_error_text(e.message)[:120]}"
                )
            except Exception as e:  # noqa: BLE001 - never propagate
                self.monitor.note_report_error(self._safe_error_text(repr(e))[:160])

    def _report_monitor_soon(self) -> None:
        """Fire-and-forget variant for the execution hot path: a slow or
        dead server must never delay stage transitions (protocol 11)."""
        task = asyncio.create_task(self._report_monitor())
        self._monitor_tasks.add(task)
        task.add_done_callback(self._monitor_tasks.discard)

    def _disk_free_bytes(self) -> int:
        try:
            st = os.statvfs(self.config.data_dir)
            return st.f_bavail * st.f_frsize
        except OSError:
            return 0

    async def _node_heartbeat(self, status: str, *,
                              monitor_status: Optional[str] = None,
                              disk_free_bytes: Optional[int] = None,
                              unhealthy_reason: Optional[str] = None) -> Dict[str, Any]:
        """Idle-path node heartbeat; also refreshes and reports the
        monitoring snapshot (protocol 11) so an idle node stays visible."""
        self.monitor.set_node(
            monitor_status or status,
            disk_free_bytes=disk_free_bytes
            if disk_free_bytes is not None
            else self._disk_free_bytes(),
            # Preserve current diagnostics even while ordinary work proceeds.
            unhealthy_reason=self._safe_error_text(unhealthy_reason or self._unhealthy_reason)
            if unhealthy_reason or self._unhealthy_reason else None,
        )

        active = self.journal.active_attempts()
        if active and status == 'idle':
            status = 'busy'
        pending = self.current_attempt or self.journal.active_attempt()

        def _do() -> Dict[str, Any]:
            try:
                return self.http.worker_heartbeat(
                    self.config.worker_id, self.boot_id, status,
                    pending and pending["attempt_id"],
                    {"unhealthy_reason": self._unhealthy_reason},
                )
            except Exception as e:  # noqa: BLE001 - monitoring-adjacent
                print(f"[worker] node heartbeat failed: {self._safe_error_text(e)}", flush=True)
                return {}

        payload = await asyncio.to_thread(_do)
        if 'cpu_tail_overlap' in payload:
            self._cpu_overlap_negotiated = payload['cpu_tail_overlap'] is True
        if "drain_requested" in payload:
            self._drain = bool(payload["drain_requested"])
        await self._maybe_run_command(payload)
        self._apply_command_control(payload)
        await self._maybe_apply_update(payload)
        await self._report_monitor()
        return payload

    async def _complete_pending_update(self) -> None:
        path = self.config.update_marker_path
        if not os.path.isfile(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                marker = json.load(fh)
            if marker.get('startup_failed'):
                from .startup_prepare import report_failure
                await asyncio.to_thread(report_failure, self.config)
                self._drain = True
                return
            if getattr(self.config, 'comfyui_preview_enabled', False):
                from .startup_prepare import PreparationError, report_failure
                from .comfy_preview import available
                try:
                    await asyncio.to_thread(comfy.runtime, self)
                    if not await asyncio.to_thread(available, self):
                        raise PreparationError('Preview startup failed: loaded bridge readiness before update completion')
                except Exception:
                    self._drain = True
                    await asyncio.to_thread(report_failure, self.config,
                        PreparationError('Preview startup failed: runtime/bridge readiness before update completion'))
                    return
            await asyncio.to_thread(
                self.http.worker_update_status,
                self.config.worker_id, marker["request_id"], "succeeded",
                f"running {marker['revision']}",
            )
            os.unlink(path)
            # Refresh authoritative operator/update drain on the next heartbeat.
            self._drain = True
            print(f"[worker] update {marker['request_id']} completed", flush=True)
        except Exception as exc:  # noqa: BLE001 - keep node safely drained
            self._drain = True
            print(f"[worker] update completion report deferred: {self._safe_error_text(exc)}", flush=True)

    async def _maybe_apply_update(self, heartbeat: Dict[str, Any]) -> None:
        if not isinstance(heartbeat, dict) or not isinstance(heartbeat.get('update'), dict):
            return
        async with self._comfy_maintenance_lock:
            await self._apply_controlled_update(heartbeat)

    async def _apply_controlled_update(self, heartbeat: Dict[str, Any]) -> None:
        command = heartbeat.get("update") if isinstance(heartbeat, dict) else None
        if (_shutdown_requested(self) or self._command_recovery_required or not isinstance(command, dict) or self.current_attempt is not None
                or (self._command_task is not None
                    and not self._command_task.done())):
            return
        if command.get("worker_id") != self.config.worker_id:
            return
        record = self.journal.get_command()
        if record and not record.get("confirmed") and record["request_id"] != command.get("request_id"):
            return  # Preserve the single command owner until its exit is reconciled.
        request_id = command.get("request_id")
        revision = command.get("revision")
        package_url = command.get("package_url")
        package_sha256 = command.get("package_sha256")
        if not isinstance(request_id, str) or not isinstance(revision, str):
            return
        if (package_url is None) != (package_sha256 is None):
            return
        self._drain = True
        try:
            recovery_update = (package_url is None
                               and heartbeat.get('recovery_update_safe') is True
                               and heartbeat.get('drain_requested') is True
                               and self._other_unhealthy_reason is None)
            from .upgrade import (
                _apply_package_update, _apply_update, _package_headers,
                _prepare_update, _repo_root, installation_update_sources,
                finish_git_update,
            )
            repo = _repo_root()
            source = "git" if package_url is None else "package"
            if source not in await asyncio.to_thread(installation_update_sources, repo, self.config.data_dir):
                raise RuntimeError(f"{source} update transport does not match installation mode")
            await asyncio.to_thread(comfy.maintenance_preflight, self.config,
                                   self.config.journal_path, recovery_update=recovery_update)
            await asyncio.to_thread(
                self.http.worker_update_status,
                self.config.worker_id, request_id, "applying", None,
            )
            if package_url is None:
                await asyncio.to_thread(_prepare_update, repo, revision, self.config.data_dir)
                if recovery_update:
                    await asyncio.to_thread(comfy.maintenance_preflight, self.config,
                                           self.config.journal_path, recovery_update=True)
                await asyncio.to_thread(_apply_update, repo, revision, self.config.data_dir)
                await asyncio.to_thread(finish_git_update, repo,
                                      self.config.data_dir, revision)
            elif isinstance(package_url, str) and isinstance(package_sha256, str):
                await asyncio.to_thread(
                    _apply_package_update, repo, self.config.data_dir,
                    package_url, package_sha256,
                    _package_headers(self.config, package_url), revision=revision,
                )
            else:
                raise ValueError("invalid package update payload")
            from .startup_prepare import _write_private
            _write_private(Path(self.config.update_marker_path), {
                "request_id": request_id,
                "revision": revision,
                "source": "package" if package_url else "git",
            })
            os.environ["H3WORKER_DATA_DIR"] = str(Path(self.config.data_dir).absolute())
            os.chdir(repo / "client")
            print(f"[worker] exec verified update {revision}", flush=True)
            launcher = str(repo / "client" / "start.sh")
            os.execv(launcher, [launcher])
        except Exception as exc:  # noqa: BLE001 - keep failed update drained
            message = self._safe_error_text(exc)[:500]
            if recovery_update:
                # A read-only admission probe may have seen reappearance or
                # broken runtime evidence. Do not let the next recovery pass
                # reuse an earlier absence window. Explicit contradictions
                # also invalidate the earlier accepted cancellation.
                try:
                    rows = self.journal.conn.execute(
                        'SELECT attempt_id, engine_state FROM attempts WHERE engine_state IS NOT NULL').fetchall()
                    for row in rows:
                        state = json.loads(row['engine_state'])
                        if isinstance(state, dict) and not state.get('terminal'):
                            comfy.reset_absence(state)
                            if isinstance(exc, comfy.PromptAbsenceConflict):
                                state.pop('cancel_accepted_at', None)
                            comfy.persist(self, row['attempt_id'], state)
                            comfy.quarantine(self, 'ComfyUI recovery update failed; fresh reconciliation pending')
                except Exception as journal_exc:
                    if self._other_unhealthy_reason is None:
                        self._unhealthy_reason = 'recovery update journal error: ' + str(journal_exc)[:200]
            try:
                await asyncio.to_thread(
                    self.http.worker_update_status,
                    self.config.worker_id, request_id, "failed", message,
                )
            except Exception as report_exc:  # noqa: BLE001
                print(f"[worker] update failure report failed: {self._safe_error_text(report_exc)}", flush=True)
            self.monitor.add_anomaly("other", f"update failed: {message}")
            print(f"[worker] update failed safely: {message}", flush=True)

    async def _maybe_run_command(self, heartbeat: Dict[str, Any]) -> None:
        command = heartbeat.get("command") if isinstance(heartbeat, dict) else None
        if not isinstance(command, dict):
            return
        if command.get("worker_id") != self.config.worker_id:
            return
        record = self.journal.get_command()
        if record and not record.get("confirmed") and record["request_id"] != command.get("request_id"):
            return  # Preserve the single command owner until its exit is reconciled.
        request_id = command.get("request_id")
        text = command.get("command")
        timeout = command.get("timeout_seconds")
        if not isinstance(request_id, str) or not isinstance(text, str) \
                or not isinstance(timeout, int):
            return
        if command.get("drain") is True:
            # Server owns the durable drain and serializes it against claims.
            # Also refuse local leftovers not represented by a live Server lease.
            self._drain = True
            if (self.current_attempt is not None or self.journal.active_attempt()
                    or (self.active_proc is not None and self.active_proc.poll() is None)):
                return
        if request_id == self._last_command_request_id:
            return
        if self._command_task is not None and not self._command_task.done():
            return
        self._command_request_id = request_id
        self._command_cancel_event = threading.Event()
        self._command_task = asyncio.create_task(
            self._run_direct_command(command) if command.get("protocol") == 2
            else self._run_command(request_id, text, timeout)
        )
        self._command_task.add_done_callback(self._command_done)

    def _command_done(self, task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # keep the Worker loop alive and observable
            print(f"[worker] command task failed: {self._safe_error_text(exc)}", flush=True)

    def _apply_command_control(self, heartbeat: Dict[str, Any]) -> None:
        control = (heartbeat.get("command_control")
                   if isinstance(heartbeat, dict) else None)
        if (isinstance(control, dict)
                and control.get("request_id") == self._command_request_id
                and control.get("cancel_requested") is True
                and self._command_cancel_event is not None):
            self._command_cancel_event.set()

    async def _recover_command(self):
        record = self.journal.get_command()
        if not record or record.get('confirmed'):
            return
        # intent precedes claim and spawn_intent precedes Popen. Only these two
        # durable facts prove no process started / supervisor observed exit.
        if record['phase'] not in ('intent', 'exited'):
            pid = record.get('pid')
            if not pid or await asyncio.to_thread(_command_group_alive, pid) is not False:
                return
            record['phase'] = 'exited'
            record['terminal'] = ['failed', None, '', 'restart: process group has exited; result unknown']
            self.journal.save_command(record)
        terminal = record.get('terminal') or ['cancelled', None, '', 'restart before spawn']
        terminal = [
            *terminal[:2],
            self._safe_error_text(terminal[2], max_bytes=65536),
            self._safe_error_text(terminal[3], max_bytes=65536),
        ]
        record['terminal'] = terminal
        try:
            await asyncio.to_thread(self.http.worker_command_reconcile,
                self.config.worker_id, self.boot_id, record, terminal)
        except Exception:
            return
        record['confirmed'] = True
        self.journal.save_command(record)
        self._command_recovery_required = False
        if self._unhealthy_reason and self._unhealthy_reason.startswith('command recovery_required'):
            self._unhealthy_reason = None

    async def _command_control_loop(self):
        while not _shutdown_requested(self):
            try:
                if self._command_recovery_required:
                    await self._recover_command()
                payload = await asyncio.to_thread(self.http.worker_command_poll,
                    self.config.worker_id, self.boot_id,
                    self._command_blocked_reason)
                if 'drain_requested' in payload:
                    self._drain = bool(payload['drain_requested'])
                self._apply_command_control(payload)
                await self._maybe_run_command(payload)
            except Exception as exc:
                print(f'[worker] command poll deferred: {self._safe_error_text(exc)}', flush=True)
            await asyncio.sleep(1)

    async def _run_direct_command(self, command):
        request_id = command['request_id']
        record = self.journal.get_command()
        if record and record['request_id'] == request_id:
            if record.get('confirmed'):
                self._last_command_request_id = request_id
                return
            if record['phase'] != 'intent':
                self._command_recovery_required = True
                self._unhealthy_reason = 'command recovery_required'
                return
        else:
            record = dict(request_id=request_id, execution_id=uuid.uuid4().hex,
                          boot_id=self.boot_id, command=command, phase='intent', report_seq=0)
            self.journal.save_command(record)
        # Failed/lost claim response retries this durable owner; no spawn before acknowledgement.
        while True:
            if command.get('drain') and (
                    self.current_attempt or self.journal.active_attempt() or
                    (self.active_proc is not None and self.active_proc.poll() is None)):
                self._command_blocked_reason = 'active local attempt/process'
                await asyncio.sleep(1)
                continue
            self._command_blocked_reason = None
            try:
                result = await asyncio.to_thread(self.http.worker_command_claim, self.config.worker_id,
                    request_id, self.boot_id, record['execution_id'])
                if result.get('claimed') is not True:
                    await asyncio.sleep(1)
                    continue
                break
            except ApiError as exc:
                if exc.status == 409:
                    record.update(confirmed=True, phase='not_started')
                    self.journal.save_command(record)
                    self._last_command_request_id = request_id
                    return
                await asyncio.sleep(1)
            except Exception:
                await asyncio.sleep(1)
        record['phase'] = 'spawn_intent'
        self.journal.save_command(record)
        self._last_command_request_id = request_id
        loop = asyncio.get_running_loop()
        latest = [None]
        def output(stdout, stderr):
            loop.call_soon_threadsafe(latest.__setitem__, 0, (stdout, stderr))
        def started(pid):
            identity = runmod.process_start_identity(pid)
            async def persist():
                record.update(pid=pid, process_identity=identity, phase='running')
                self.journal.save_command(record)
                latest[0] = ('', '')
            asyncio.run_coroutine_threadsafe(persist(), loop).result()
        async def report(status, code, stdout, stderr):
            stdout = self._safe_error_text(stdout, max_bytes=65536)
            stderr = self._safe_error_text(stderr, max_bytes=65536)
            record['report_seq'] += 1
            record['report'] = dict(status=status, exit_code=code, stdout=stdout, stderr=stderr)
            self.journal.save_command(record)
            await asyncio.to_thread(self.http.worker_command_status, self.config.worker_id,
                request_id, status, code, stdout, stderr, boot_id=self.boot_id,
                execution_id=record['execution_id'], report_seq=record['report_seq'])
        async def reporter():
            while True:
                if latest[0] is not None:
                    stdout, stderr = latest[0]
                    latest[0] = None
                    try:
                        await report('running', None, stdout, stderr)
                    except Exception:
                        pass
                await asyncio.sleep(.25)
        reporting = asyncio.create_task(reporter())
        try:
            if result.get('input') is not None:
                descriptor = result['input']
                try:
                    raw = await asyncio.to_thread(self.http.worker_command_input,
                        self.config.worker_id, request_id, self.boot_id, record['execution_id'], descriptor)
                except Exception:
                    result = ('failed', 1, '', 'workflow command input failed; no automatic retry')
                else:
                    result = await asyncio.to_thread(_run_workflow_input, command['command'],
                        raw, command['timeout_seconds'], output, self._command_cancel_event,
                        self.config.cancel_grace_seconds, started)
            else:
                result = await asyncio.to_thread(_run_operator_command, command['command'],
                    command['timeout_seconds'], output, .5, self._command_cancel_event,
                    self.config.cancel_grace_seconds, started)
            result = (
                *result[:2],
                self._safe_error_text(result[2], max_bytes=65536),
                self._safe_error_text(result[3], max_bytes=65536),
            )
            if result[0] == 'recovery_required':
                self._command_recovery_required = True
                self._unhealthy_reason = 'command recovery_required: cannot confirm process group exit'
                record['phase'] = 'recovery_required'
            else:
                record['phase'] = 'exited'
                record['terminal'] = list(result)
            self.journal.save_command(record)
            reporting.cancel()
            await asyncio.gather(reporting, return_exceptions=True)
            reconcile_terminal = False
            def recovery_handed_off():
                if result[0] != 'recovery_required':
                    return False
                saved = self.journal.get_command()
                return (saved is not None
                        and all(saved.get(key) == record.get(key)
                                for key in ('request_id', 'boot_id', 'execution_id'))
                        and (saved.get('confirmed') or saved.get('phase') == 'exited'))

            while True:
                # Recovery owns the durable exit once it starts reconciliation.
                # Never overwrite it with this task's stale recovery snapshot.
                if recovery_handed_off():
                    break
                try:
                    if reconcile_terminal:
                        # A committed terminal report can lose its ACK and then
                        # lose its slot. Reconcile the durable exit with the same
                        # owner; never claim or spawn this execution again.
                        await asyncio.to_thread(self.http.worker_command_reconcile,
                            self.config.worker_id, self.boot_id, record, record['terminal'])
                    else:
                        await report(*result)
                    if recovery_handed_off():
                        break
                    record['confirmed'] = result[0] != 'recovery_required'
                    self.journal.save_command(record)
                    break
                except ApiError as exc:
                    if exc.status == 409 and record['phase'] == 'exited':
                        reconcile_terminal = True
                    await asyncio.sleep(1)
                except Exception:
                    await asyncio.sleep(1)
        except BaseException:
            self._command_recovery_required = True
            self._unhealthy_reason = 'command recovery_required: supervisor interrupted'
            if self._command_cancel_event is not None:
                self._command_cancel_event.set()
            raise
        finally:
            reporting.cancel()
            self._command_request_id = None
            self._command_cancel_event = None

    async def _run_command(self, request_id: str, text: str,
                           timeout: int) -> None:
        try:
            await asyncio.to_thread(
                self.http.worker_command_status,
                self.config.worker_id, request_id, "running",
            )
        except ApiError as exc:
            if exc.status == 409:
                self._last_command_request_id = request_id
                print(
                    f"[worker] command {request_id} no longer current",
                    flush=True,
                )
                return
            raise
        self._last_command_request_id = request_id
        cancel_event = self._command_cancel_event
        if cancel_event is None:
            return

        outputs = queue.Queue(maxsize=1)
        def report_output(stdout: str, stderr: str) -> None:
            try:
                outputs.get_nowait()
            except queue.Empty:
                pass
            outputs.put_nowait((
                self._safe_error_text(stdout, max_bytes=65536),
                self._safe_error_text(stderr, max_bytes=65536),
            ))
        async def reporter():
            while True:
                if not outputs.empty():
                    stdout, stderr = outputs.get_nowait()
                    try:
                        await asyncio.to_thread(self.http.worker_command_status,
                            self.config.worker_id, request_id, "running", None, stdout, stderr)
                    except Exception:
                        pass
                await asyncio.sleep(.1)
        output_task = asyncio.create_task(reporter())

        command_task = asyncio.create_task(asyncio.to_thread(
            _run_operator_command, text, timeout, report_output, 0.5,
            cancel_event, self.config.cancel_grace_seconds,
        ))
        try:
            while not command_task.done():
                done, _ = await asyncio.wait(
                    {command_task}, timeout=1.0,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    try:
                        payload = await asyncio.to_thread(
                            self.http.worker_heartbeat,
                            self.config.worker_id, self.boot_id,
                            "busy" if self.current_attempt else "idle",
                            self.current_attempt and
                            self.current_attempt["attempt_id"],
                            {"unhealthy_reason": self._unhealthy_reason},
                        )
                        if "drain_requested" in payload:
                            self._drain = bool(payload["drain_requested"])
                        self._apply_command_control(payload)
                    except Exception as exc:  # command control is best-effort
                        print(f"[worker] command control poll failed: {self._safe_error_text(exc)}",
                              flush=True)
            status, exit_code, stdout, stderr = await command_task
            stdout = self._safe_error_text(stdout, max_bytes=65536)
            stderr = self._safe_error_text(stderr, max_bytes=65536)
            output_task.cancel()
            await asyncio.gather(output_task, return_exceptions=True)
            while True:
                try:
                    await asyncio.to_thread(
                        self.http.worker_command_status,
                        self.config.worker_id, request_id, status, exit_code,
                        stdout, stderr,
                    )
                    print(f"[worker] command {request_id} {status}",
                          flush=True)
                    break
                except ApiError as exc:
                    if exc.status == 409:
                        print(
                            f"[worker] command {request_id} terminal status "
                            "no longer current",
                            flush=True,
                        )
                        break
                    if exc.status in (401, 403):
                        raise
                    print(
                        f"[worker] command {request_id} terminal report "
                        f"failed: {self._safe_error_text(exc)}; retrying",
                        flush=True,
                    )
                except Exception as exc:  # network/client failures are retryable
                    print(
                        f"[worker] command {request_id} terminal report "
                        f"failed: {self._safe_error_text(exc)}; retrying",
                        flush=True,
                    )
                await asyncio.sleep(2.0)
        finally:
            output_task.cancel()
            if self._command_request_id == request_id:
                self._command_request_id = None
                self._command_cancel_event = None

    # ==================================================================
    # events
    # ==================================================================

    def _next_phase_instance(self, attempt_id: str, phase: str) -> int:
        """phase_instance increments per distinct phase WITHIN one attempt
        (protocol section 2; review C11)."""
        record = self.journal.get_attempt(attempt_id)
        last_phase = (record or {}).get("phase")
        last_instance = int((record or {}).get("phase_instance") or 0)
        if phase != last_phase:
            last_instance += 1
        return max(1, last_instance)

    async def _emit_event(
        self, attempt_id: str, body: Dict[str, Any]
    ) -> None:
        record = self.journal.get_attempt(attempt_id)
        if record is None:
            return
        if body.get("type") == "phase":
            body["phase_instance"] = self._next_phase_instance(
                attempt_id, body["phase"]
            )
            if body.pop("new_phase_instance", False):
                body["phase_instance"] = max(body["phase_instance"],
                    int(record.get("phase_instance") or 0) + 1)
        elif body.get("type") == "progress" and not body.get("phase_instance"):
            # progress events carry the same instance as their phase
            body["phase_instance"] = int(record.get("phase_instance") or 1)
        body["observed_at"] = utcnow_iso()
        performance = None
        if body.get("type") == "phase" and getattr(self.config, "cpu_tail_pilot", False):
            performance = dict(
                variant="validation_hash_upload_overlap_v1" if cpu_overlap.enabled(self) else "serial_reservation_observation_v1", boot_id=self.boot_id,
                phase=body["phase"], detail_phase=body.get("detail_phase"),
                phase_instance=body["phase_instance"], observed_at=body["observed_at"],
                monotonic_seconds=time.monotonic(),
                page_in_bytes=None, page_out_bytes=None,
                paging_counter_status="unavailable_worker_phase_sampler",
                artifact_bytes=sum(a["size_bytes"] for a in self.journal.artifacts_for(attempt_id)))
            performance.update(await asyncio.to_thread(cpu_overlap.paging_counters))
            performance.update(getattr(self, '_performance_revisions', dict(worker_revision=None, engine_revision=None)))
            performance['model_revision'] = record['request_snapshot'].get('model_revision')
            performance['fixture_digest'] = digest_obj(record['request_snapshot'])
            performance['operation_key'] = record.get('operation_key')
            body["performance"] = performance
        seq = self.journal.append_event(attempt_id, body, performance=performance)
        update_fields: Dict[str, Any] = {"event_seq": seq}
        if body.get("type") == "phase":
            update_fields["phase"] = body.get("phase")
            update_fields["phase_instance"] = body.get("phase_instance")
            update_fields["current_stage"] = body.get("phase")
        self.journal.update_attempt(attempt_id, **update_fields)
        # monitoring mirror (protocol 11): stage transitions and progress
        # counters, semantics preserved (GPU enqueue is not completion).
        # Phase changes report IMMEDIATELY (fire-and-forget) so short
        # engine stages are never skipped on the monitor view; progress
        # rides the heartbeat cadence.
        if body.get("type") == "phase":
            self.monitor.set_stage(
                body["phase"],
                detail_phase=body.get("detail_phase"),
                phase_instance=body.get("phase_instance"),
            )
            self._report_monitor_soon()
        elif body.get("type") == "progress":
            self.monitor.set_progress(
                body.get("completed") or 0,
                body.get("total") or 0,
                body.get("unit") or "steps",
                body.get("semantics") or "completed",
            )

    async def _flush_events(self, attempt_id: str, lease_token: str) -> None:
        """Send pending events.  Transient failures are NOT fatal (review
        B5): the outbox keeps them for the next window.  Only an
        EVENT_CONFLICT (a protocol violation on our side) is raised."""
        record = self.journal.get_attempt(attempt_id)
        if record and (record.get("recovery_state") == "offline" or self._local_lease_expired()):
            self._defer_recovery(attempt_id)
            return
        pending = self.journal.pending_events(attempt_id)
        if not pending:
            return
        events = [dict(item["payload"], seq=item["seq"]) for item in pending]
        try:
            resp = await asyncio.to_thread(
                self.http.send_events, attempt_id, lease_token, events
            )
        except ApiError as e:
            if e.code == "LEASE_LOST":
                # Contact expiry is provisional; the heartbeat loop obtains an
                # authenticated reconcile verdict before stopping inference.
                self._defer_recovery(attempt_id)
                return
            if e.code == "CANCEL_REQUESTED":
                self._set_cancel_requested()
                return
            if e.code == "ATTEMPT_NOT_CURRENT" or e.status in (401, 403):
                # Definitive rejection fences further local execution.
                self.journal.drop_outbox(attempt_id)
                self._set_lease_lost()
                return
            if e.code == "EVENT_CONFLICT":
                # protocol violation: keep evidence, surface loudly
                print(f"[worker] EVENT_CONFLICT: {self._safe_error_text(e.message)}", flush=True)
                raise
            if e.status == 0 or e.status >= 500 or e.status == 429:
                self._defer_recovery(attempt_id)
            # transient (5xx / network): keep the outbox, retry next window
            print(f"[worker] event flush deferred: {self._safe_error_text(e)}", flush=True)
            return
        acked = resp.get("acked_seqs") or []
        self.journal.ack_events(attempt_id, acked)
        if acked:
            self.journal.prune_outbox(attempt_id)

    async def _flush_events_safe(self, attempt_id: str, lease_token: str) -> None:
        """Flush used from the engine read loop: never lets a transient
        network error kill a healthy inference (review B5)."""
        try:
            await self._flush_events(attempt_id, lease_token)
        except ApiError:
            # EVENT_CONFLICT is a real protocol break: stop the engine
            raise LeaseLost()

    # ==================================================================
    # input download
    # ==================================================================

    async def _download_inputs(
        self, attempt_id: str, lease_token: str, task_request: Dict[str, Any]
    ) -> Dict[str, str]:
        await self._emit_event(attempt_id, {
            "type": "phase", "phase": "downloading",
        })
        await self._flush_events_safe(attempt_id, lease_token)
        references = task_request.get("references") or []
        anchors = task_request.get("anchors") or {}
        asset_ids: List[str] = []
        key_by_asset: Dict[str, str] = {}
        for ref in references:
            asset_id = ref.get("asset_id")
            if asset_id:
                asset_ids.append(asset_id)
                key_by_asset[asset_id] = f"ref:{asset_id}"
        for name in ("first", "last"):
            anchor = anchors.get(name)
            if isinstance(anchor, dict) and anchor.get("asset_id"):
                asset_ids.append(anchor["asset_id"])
                key_by_asset[anchor["asset_id"]] = f"anchors.{name}"
        if not asset_ids:
            return {}

        for attempt in range(max(1, self.config.max_retries)):
            self._check_lease_alive()
            if self.cancel_requested.is_set():
                raise CancelRequested()
            try:
                resp = await asyncio.to_thread(
                    self.http.resolve_inputs, attempt_id, lease_token, asset_ids)
                break
            except ApiError as error:
                # Resolving signed URLs is read-only. Retry temporary transport
                # failures, never authorization, cancellation or fencing errors.
                if error.code in ("UNAUTHORIZED", "FORBIDDEN", "LEASE_LOST",
                                  "ATTEMPT_NOT_CURRENT", "CANCEL_REQUESTED", "TASK_TERMINAL") \
                        or not (error.status == 0 and error.code == "NETWORK"
                                or error.status == 429 or error.status >= 500):
                    raise
                if attempt + 1 >= max(1, self.config.max_retries):
                    raise ApiError(error.status, "INPUT_UNAVAILABLE",
                                   "input resolution retries exhausted: " + self._safe_error_text(error)) from error
                self.monitor.add_anomaly("download_retry",
                    "input resolution retry: " + self._safe_error_text(error)[:120])
                await self._backoff_sleep(backoff_delay(attempt, error.retry_after))
        local_paths: Dict[str, str] = {}
        cache_root = os.path.join(self.config.data_dir, "cache", "sha256")
        for asset in resp.get("assets", []):
            self._check_lease_alive()
            if self.cancel_requested.is_set():
                raise CancelRequested()
            asset_id = asset["asset_id"]
            sha = str(asset["sha256"])
            size = int(asset["size_bytes"])
            if not _HEX64_RE.match(sha):
                # never join an unvalidated external id into a path (C6)
                raise ValueError(f"asset {asset_id} has invalid sha256")
            cached = os.path.join(
                cache_root, _cache_filename(sha, asset.get("content_type", ""))
            )
            if os.path.isfile(cached) and os.path.getsize(cached) == size:
                # cache hit: verify digest too (README 5, review C6)
                _, got = await asyncio.to_thread(file_digest, cached)
                if got == sha:
                    local_paths[key_by_asset[asset_id]] = cached
                    continue
            dest = cached

            def _dl(url=asset["url"], dest=dest, sha=sha, size=size):
                return self.http.download(
                    url, dest, sha, size, self.config.max_asset_bytes,
                    self.config.download_timeout_seconds,
                    # cancel/lease-loss interrupts an in-flight download
                    # too (terminal review C-3), not just uploads
                    abort_check=self._transfer_should_abort,
                )

            last_error: Optional[Exception] = None
            for attempt in range(self.config.max_retries):
                try:
                    await asyncio.to_thread(_dl)
                    last_error = None
                    break
                except TransferAborted:
                    if self._transfer_abort_reason == "offline":
                        raise ApiError(0, "NETWORK", "lease contact requires reconciliation")
                    if self._transfer_abort_reason in ("cancel", "shutdown"):
                        raise CancelRequested()
                    raise LeaseLost()
                except ApiError as e:
                    if e.status in (401, 403, 404):
                        raise
                    last_error = e
                except (OSError, ValueError) as e:
                    last_error = e
                self.monitor.add_anomaly(
                    "download_retry",
                    f"asset {asset_id} attempt {attempt + 1}: "
                    f"{self._safe_error_text(last_error)[:120]}",
                )
                await self._backoff_sleep(
                    backoff_delay(attempt, getattr(last_error, "retry_after", None))
                )
            if last_error is not None:
                raise RuntimeError(
                    f"download failed for asset {asset_id}: "
                    f"{self._safe_error_text(last_error)}"
                )
            local_paths[key_by_asset[asset_id]] = cached
        if task_request.get("mode") == "a2va":
            ref = task_request["references"][0]
            source = local_paths.get(f"ref:{ref['asset_id']}")
            if not source or ((task_request.get("anchors") or {}).get("first") and
                              "anchors.first" not in local_paths):
                raise ValueError("a2va input resolution is incomplete")
            await asyncio.to_thread(runmod.validate_a2va_input, source, self.config)
        return local_paths

    # ==================================================================
    # engine execution
    # ==================================================================

    async def _run_engine(
        self, attempt_id: str, lease_token: str,
        task_request: Dict[str, Any], local_inputs: Dict[str, str],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        if task_request.get('mode') == 'ltx2_video':
            from shared.ltx_policy import validate
            problems = validate(task_request)
            if problems:
                return False, dict(code='INPUT_INVALID',message='; '.join(problems),stage='loading')
            others = [a for a in self.journal.active_attempts() if a['attempt_id'] != attempt_id]
            if others:
                raise LocalIncapacity('LTX requires exclusive GPU/publication slot')
            if not self.config.fake_runner and not getattr(self, '_ltx_ready', False):
                raise LocalIncapacity('LTX acceptance evidence unavailable')
        if task_request.get("mode") == comfy.MODE:
            return await self._comfy.run(self, attempt_id, lease_token, task_request, local_inputs)
        await self._emit_event(attempt_id, {
            "type": "phase", "phase": "loading",
        })
        await self._flush_events_safe(attempt_id, lease_token)
        attempt_dir = self.config.attempt_dir(attempt_id)
        is_tts = task_request.get("mode") == "tts"
        is_align = task_request.get("mode") == "align"
        output_name = (
            "result.wav" if is_tts else
            "alignment.json" if is_align else
            "result.mp4"
        )
        output_path = os.path.join(attempt_dir, output_name)

        if self.config.fake_runner:
            if required_h3_optimizations(task_request.get("generation") or {}):
                return False, {
                    "code": "INPUT_INVALID", "message": "fake runner does not support h3 optimizations",
                    "stage": "loading",
                }
            argv = [
                sys.executable or "python3", os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "runner.py")
                ),
                "--frames",
                str((task_request.get("generation") or {}).get("frames", 22)),
                "--steps",
                str((task_request.get("generation") or {}).get("steps", 20)),
                "--sleep", str(self.config.fake_runner_sleep_seconds),
                "-o", output_path,
            ]
            if self.config.fake_runner_fail_after > 0:
                argv += ["--fail-after", str(self.config.fake_runner_fail_after)]
            if task_request.get('mode') == 'ltx2_video':
                argv += ['--ltx-fixture']
            if is_tts:
                argv.append("--audio")
            elif is_align:
                argv += ["--alignment", "--text", str(task_request.get("prompt") or "")]
            cwd = "."
        else:
            try:
                if task_request.get('mode') == 'ltx2_video':
                    from .ltx_runner import build_command
                    argv = build_command(task_request, local_inputs, output_path,
                                         os.path.join(attempt_dir, 'ltx-request.json'))
                elif is_tts:
                    capability = str(
                        task_request.get("model_revision") or "cosyvoice3-mlx"
                    )
                    argv = runmod.build_tts_command(
                        task_request, output_path, local_inputs,
                        runmod.configured_model(self.config, capability),
                        python_path=runmod.capability_python_path(capability),
                    )
                elif is_align:
                    capability = "audio.qwen3.forced_align"
                    argv = runmod.build_align_command(
                        task_request, output_path, local_inputs,
                        runmod.configured_model(self.config, capability),
                        python_path=runmod.capability_python_path(capability),
                    )
                else:
                    argv = runmod.build_h3_command(
                        task_request, output_path, local_inputs,
                        self.config.h3_binary, self.config.model_dir,
                        optimization_capabilities=(
                            await asyncio.to_thread(runmod.engine_optimization_capabilities, self.config)
                            if required_h3_optimizations(task_request.get("generation") or {}) else []
                        ),
                    )
            except ValueError as e:
                # argv-level content problem: INPUT_INVALID, not release
                return False, {
                    "code": "INPUT_INVALID", "message": str(e),
                    "stage": "loading",
                }
            cwd = (
                os.path.dirname(runmod._HERE)
                if is_tts or is_align else self.config.h3_working_dir
            )

        env = runmod.subprocess_env(self.config)
        if task_request.get('mode') == 'ltx2_video' and not self.config.fake_runner:
            from .ltx_runner import environment
            env = environment()
            cwd = runmod._SERVICE_ROOT
        stdout_path = os.path.join(attempt_dir, "engine-stdout.log")
        stderr_path = os.path.join(attempt_dir, "stderr.log")

        if cpu_overlap.enabled(self) and task_request.get("mode") != "ltx2_video":
            argv = [sys.executable or 'python3', os.path.abspath(runmod.__file__),
                    '--bounded-exec', str(cpu_overlap.MAX_BYTES), '--', *argv]
        proc = await asyncio.to_thread(
            runmod.start_process, argv, cwd, env, stdout_path, stderr_path
        )
        self.active_proc = proc
        self.journal.record_process(
            attempt_id, proc.pid, proc.pid,
            runmod.process_start_identity(proc.pid), argv[0],
        )
        self.monitor.process_started(proc.pid, os.path.basename(argv[0]))
        self._report_monitor_soon()

        # pump stdout lines to the asyncio loop via a bounded queue; the
        # reader thread blocks on the file, never on the loop
        line_queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=4096)
        loop = asyncio.get_event_loop()

        def _read_lines():
            with open(stdout_path, "r", errors="replace") as fh:
                while True:
                    line = fh.readline()
                    if line:
                        yield line
                    elif proc.poll() is not None:
                        tail = fh.readline()
                        if not tail:
                            break
                        yield tail
                    else:
                        time.sleep(0.05)

        def _pump():
            try:
                for line in _read_lines():
                    asyncio.run_coroutine_threadsafe(
                        line_queue.put(line), loop
                    ).result(timeout=5)
            except Exception:
                pass

        pump_thread = threading.Thread(target=_pump, daemon=True)
        pump_thread.start()

        engine_failed: Optional[Dict[str, Any]] = None
        result: Optional[Dict[str, Any]] = None
        last_progress_flush = 0.0
        try:
            while True:
                if self.lease_lost.is_set():
                    raise LeaseLost()
                if self.cancel_requested.is_set():
                    raise CancelRequested()
                self._check_lease_alive()
                if cpu_overlap.enabled(self) and self._disk_free_bytes() < self.config.min_free_disk_bytes:
                    self._unhealthy_reason = 'overlap disk headroom exhausted'
                    self._drain = True
                    raise CancelRequested()
                try:
                    line = await asyncio.wait_for(
                        line_queue.get(), timeout=0.5
                    )
                except asyncio.TimeoutError:
                    if proc.poll() is not None and line_queue.empty():
                        break
                    continue
                event = runmod.parse_engine_line(line)
                if event is None:
                    continue
                if event.kind == "error":
                    engine_failed = {
                        "code": ("MODEL_UNAVAILABLE" if task_request.get('mode') == ltx.MODE
                                 and event.message.startswith('MODEL_UNAVAILABLE:') else "ENGINE_FAILED"),
                        "message": event.message,
                    }
                    continue
                if event.kind == "result":
                    result = event.result
                    continue
                mapped = runmod.map_engine_event(event, 0)
                if mapped is None:
                    continue
                if event.kind == "phase":
                    await self._emit_event(attempt_id, mapped)
                    await self._flush_events_safe(attempt_id, lease_token)
                elif event.kind == "progress":
                    await self._emit_event(attempt_id, mapped)
                    now = time.monotonic()
                    if now - last_progress_flush >= \
                            self.config.progress_merge_seconds:
                        await self._flush_events_safe(
                            attempt_id, lease_token
                        )
                        last_progress_flush = now
        except (LeaseLost, CancelRequested):
            # stop the process group (primary stop mechanism, README 6/D5);
            # the grace period is bounded by the remaining safe window
            grace = min(
                self.config.cancel_grace_seconds,
                max(
                    0.0,
                    self.lease_deadline_mono - time.monotonic(),
                    self.lease_deadline_wall - time.time(),
                ),
            )
            if grace < 1.0:
                grace = 0.0  # window nearly exhausted: immediate hard kill
            await asyncio.to_thread(runmod.terminate_group, proc, grace)
            raise
        finally:
            if proc.poll() is None:
                await asyncio.to_thread(
                    runmod.terminate_group, proc, self.config.cancel_grace_seconds
                )
            pump_thread.join(timeout=2)
            if getattr(self.config, 'cpu_tail_overlap', False):
                if await asyncio.to_thread(_command_group_alive, proc.pid) is not False:
                    # A reaped primary is insufficient proof for releasing GPU
                    # occupancy: retain the durable process row and attempt.
                    self.active_proc = None
                    self.monitor.process_ended(proc.returncode)
                    raise NativeGroupUnproven('native process group exit unproven; local recovery required')
            self.journal.clear_process(attempt_id)
            self.active_proc = None
            self.monitor.process_ended(proc.returncode)
            self._report_monitor_soon()
            try:
                await self._flush_events_safe(attempt_id, lease_token)
            except LeaseLost:
                pass

        return_code = proc.returncode
        stage = self._current_stage(attempt_id)
        if engine_failed is not None and engine_failed.get("code") == "MODEL_UNAVAILABLE":
            # Withdraw before release so this Worker cannot immediately claim
            # the same task again while background registration catches up.
            self._withdraw_ltx_capability()
            raise LocalIncapacity(engine_failed["message"])
        if engine_failed is not None:
            # Preserve the engine's last stderr diagnostic and real exit code;
            # sanitize only credential material at the reporting boundary.
            engine_failed["message"] = self._safe_error_text(
                runmod.format_engine_failure(
                    str(engine_failed.get("message") or ""),
                    return_code,
                    stderr_path,
                )
            )
            engine_failed.setdefault("stage", stage)
            return False, engine_failed
        if return_code != 0:
            return False, {
                "code": "ENGINE_FAILED",
                "message": self._safe_error_text(
                    runmod.format_engine_failure("", return_code, stderr_path)),
                "stage": stage,
            }
        if result is None:
            return False, {
                "code": "ENGINE_FAILED",
                "message": "engine produced no result object",
                "stage": stage,
            }
        return True, result

    # ==================================================================
    # validation + manifest
    # ==================================================================

    async def _validate_and_manifest(
        self, attempt_id: str, lease_token: str,
        task_request: Dict[str, Any], local_inputs: Dict[str, str],
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        await self._emit_event(attempt_id, {
            "type": "phase", "phase": "validating",
        })
        await self._flush_events_safe(attempt_id, lease_token)
        attempt_dir = self.config.attempt_dir(attempt_id)
        is_tts = task_request.get("mode") == "tts"
        is_align = task_request.get("mode") == "align"
        media_role = "audio" if is_tts else "alignment" if is_align else "video"
        media_name = "result.wav" if is_tts else "alignment.json" if is_align else "result.mp4"
        media_path = os.path.join(attempt_dir, media_name)
        if not os.path.isfile(media_path) or os.path.getsize(media_path) == 0:
            raise RuntimeError(f"{os.path.basename(media_path)} missing or empty")

        if is_tts:
            with wave.open(media_path, "rb") as wav:
                actual = {
                    "sample_rate": wav.getframerate(),
                    "channels": wav.getnchannels(),
                    "samples": wav.getnframes(),
                }
            for key, value in actual.items():
                if value <= 0 or int(result.get(key) or -1) != value:
                    raise RuntimeError(
                        f"engine result.{key} differs from WAV ({result.get(key)} != {value})"
                    )
            generation = task_request.get("tts") or {}
        elif is_align:
            with open(media_path, "r", encoding="utf-8") as fh:
                alignment = json.load(fh)
            problems = validate_alignment_payload(
                alignment, expected_text=str(result.get("text") or ""),
                requested_text=str(task_request.get("prompt") or ""),
            )
            if problems:
                raise RuntimeError("alignment.json invalid: " + "; ".join(problems[:3]))
            if int(result.get("segments") or -1) != len(alignment["segments"]):
                raise RuntimeError("engine result.segments differs from alignment JSON")
            generation = {}
        else:
            generation = task_request.get("generation") or {}
            for key, result_key in (
                ("frames", "frames"), ("width", "width"), ("height", "height"),
            ):
                want, got = generation.get(key), result.get(result_key)
                if want is not None and got is not None and int(want) != int(got):
                    raise RuntimeError(
                        f"engine result.{result_key}={got} differs from request "
                        f"{key}={want}"
                    )

        soundtrack = None
        if task_request.get("mode") == "a2va":
            ref = task_request["references"][0]
            source = local_inputs.get(f"ref:{ref['asset_id']}")
            if not source:
                raise RuntimeError("a2va soundtrack missing during artifact validation")
            soundtrack = await asyncio.to_thread(
                runmod.verify_a2va_mux, media_path, source, generation, self.config,
            )
            _, source_sha = await asyncio.to_thread(file_digest, source)
            if ref.get("sha256") != source_sha:
                raise RuntimeError("a2va soundtrack digest differs from request")
            soundtrack.update(asset_id=ref["asset_id"], source_sha256=source_sha)

        # heavy hashing off the event loop (review C4)
        size, sha = await asyncio.to_thread(file_digest, media_path)
        self.journal.put_artifact(
            attempt_id, media_role, media_path, size, sha
        )

        record = self.journal.get_attempt(attempt_id)
        manifest = {
            "task_id": record["task_id"],
            "attempt_id": attempt_id,
            "schema_version": PROTOCOL_SCHEMA_VERSION,
            "request_digest": digest_obj(task_request),
            "engine": "fake-runner" if self.config.fake_runner else (
                str(task_request.get("model_revision") or "cosyvoice3-mlx")
                if is_tts or is_align else "h3"
            ),
            "engine_version": "fake" if self.config.fake_runner else (
                runmod.configured_model(
                    self.config,
                    str(task_request.get("model_revision") or "cosyvoice3-mlx"),
                ) if is_tts or is_align else "h3-0.1"
            ),
            "worker_version": __version__,
            "model_revision": task_request.get("model_revision"),
            "seed": str(task_request.get("seed", "")),
            "effective_params": (
                dict(generation) if is_tts or is_align or task_request.get("mode") == comfy.MODE
                else effective_h3_generation(generation)
            ),
            "input_digests": {
                key: os.path.basename(path)
                for key, path in (local_inputs or {}).items()
            },
            "result": result,
            "artifacts": {
                media_role: {"sha256": sha, "size_bytes": size},
                # manifest's own entry: presence only (no self digest, and
                # size is self-referential so it is not checked either)
                "manifest": {"present": True},
            },
        }
        if task_request.get('mode') == 'ltx2_video':
            from shared import ltx_policy as ltx
            manifest.update(engine='ltx2-mlx', engine_version='0.15.12', preset_revision=ltx.PRESET,
                effective_params=dict(generation, **ltx.SETTINGS), provenance=dict(ltx.PROVENANCE),
                input_digests=ltx.input_hashes(task_request))
            for ref in task_request['references']:
                _, actual_sha = await asyncio.to_thread(file_digest, local_inputs['ref:' + ref['asset_id']])
                if actual_sha != ref['sha256']:
                    raise RuntimeError('LTX local input hash mismatch')
            if any(result.get(k) != v for k,v in generation.items()):
                raise RuntimeError('LTX result settings mismatch')
        elif task_request.get("mode") == comfy.MODE:
            manifest["engine"] = "comfyui"
            manifest["engine_version"] = self.config.comfyui_version
            manifest["workflow_digest"] = digest_obj(task_request["workflow"])
            manifest["engine_prompt_id"] = result["prompt_id"]
            if task_request.get('_native'):
                expected = {ref['asset_id']: ref['sha256'] for ref in task_request['references']}
                if result.get('input_digests') != expected:
                    raise RuntimeError('native input verification incomplete')
                manifest['input_digests'] = expected
        elif not is_tts and not is_align:
            from shared.h3proto import required_h3_optimizations, H3_OPTIMIZATION_REVISION
            selected = required_h3_optimizations(generation)
            if selected:
                manifest["optimization_capabilities"] = selected
                manifest["optimization_contract_revision"] = H3_OPTIMIZATION_REVISION
        if getattr(self, '_remote_profile_metadata', None):
            manifest.update(self._remote_profile_metadata(task_request, result))
        if soundtrack is not None:
            manifest["soundtrack"] = soundtrack
        manifest_path = os.path.join(attempt_dir, "manifest.json")
        msize, msha = await _durable_manifest(
            media_path, manifest_path, attempt_dir, manifest,
            check_alive=self._check_lease_alive,
            timeout_seconds=self.config.durability_timeout_seconds,
        )
        self.journal.complete_local_output(attempt_id, manifest_path, msize, msha, result)
        self._checkpoint_cpu_tail(attempt_id, "durable")
        return {
            media_role: {"path": media_path, "size": size, "sha256": sha},
            "manifest": {"path": manifest_path, "size": msize, "sha256": msha},
        }

    # ==================================================================
    # upload + finish
    # ==================================================================

    async def _handoff_publication(self, attempt_id, lease_token):
        intent = self.journal.cpu_handoff(attempt_id)
        record = self.journal.get_attempt(attempt_id)
        if intent is None:
            # Rollback stops new handoffs, but must still serialize a GPU
            # owner's publication behind an already-admitted immutable tail.
            start = time.monotonic()
            def other_tail():
                for active in self.journal.active_attempts():
                    if active['attempt_id'] == attempt_id:
                        continue
                    prior = self.journal.cpu_handoff(active['attempt_id'])
                    if prior and prior['acknowledgement'] and prior['acknowledgement']['gpu_occupancy_released']:
                        return True
                return False
            while other_tail():
                self._check_lease_alive()
                if (self.config.durability_timeout_seconds is not None
                        and time.monotonic() - start > self.config.durability_timeout_seconds):
                    raise TimeoutError('previous publication tail did not drain')
                await asyncio.sleep(.1)
            if not cpu_overlap.enabled(self) or record['request_snapshot'].get('mode') not in cpu_overlap.NATIVE_MODES:
                return
            rows = self.journal.artifacts_for(attempt_id)
            if sum(r['size_bytes'] for r in rows) > min(self.config.cpu_tail_max_bytes, cpu_overlap.MAX_BYTES):
                return  # oversize stays GPU-owned and serial through verification
            headroom = max(self.config.min_free_disk_bytes, cpu_overlap.MIN_HEADROOM)
            free = self._disk_free_bytes()
            if free < headroom + cpu_overlap.MAX_BYTES or self._drain:
                return
            proof = dict(engine_exited=True, durable=True,
                         max_bytes=min(self.config.cpu_tail_max_bytes, cpu_overlap.MAX_BYTES),
                         files=sorted([dict(role=r['role'],size_bytes=r['size_bytes'],sha256=r['sha256'])
                             for r in rows], key=lambda f:f['role']), disk_free_bytes=free, headroom_bytes=headroom)
            intent = self.journal.begin_cpu_handoff(attempt_id, proof)
        actual = sorted([dict(role=r['role'],size_bytes=r['size_bytes'],sha256=r['sha256'])
                         for r in self.journal.artifacts_for(attempt_id)], key=lambda f:f['role'])
        if actual != intent['evidence']['files'] or self.journal.get_process(attempt_id):
            raise ValueError('handoff evidence differs from durable output')
        if intent['acknowledgement'] is None:
            try:
                ack = await asyncio.to_thread(self.http.cpu_tail_handoff, attempt_id, lease_token,
                                             intent['evidence'], self.boot_id)
            except ApiError as exc:
                if exc.code in ('CONFLICT', 'IDEMPOTENCY_CONFLICT', 'INVALID_ARGUMENT', 'NOT_FOUND'):
                    raise ValueError('unresolved handoff: ' + exc.code) from exc
                raise
            self.journal.ack_cpu_handoff(attempt_id, ack, self.boot_id)
            intent = self.journal.cpu_handoff(attempt_id)
        if not intent['acknowledgement']['gpu_occupancy_released']:
            return  # definitive authenticated serial denial; no occupancy transition
        if intent['confirmed_boot'] != self.boot_id:
            # Persisted original ACK is a precondition of the server admission ACK.
            try:
                ack = await asyncio.to_thread(self.http.cpu_tail_handoff, attempt_id, lease_token,
                    dict(handoff_id=intent['acknowledgement']['handoff_id']), self.boot_id, True)
            except ApiError as exc:
                if exc.code in ('CONFLICT', 'IDEMPOTENCY_CONFLICT', 'INVALID_ARGUMENT', 'NOT_FOUND'):
                    raise ValueError('unresolved handoff confirmation: ' + exc.code) from exc
                raise
            self.journal.ack_cpu_handoff(attempt_id, ack, self.boot_id, confirmed=True)
        await self._emit_event(attempt_id, dict(type='phase',phase='uploading',
            detail_phase='cpu_tail_handoff_confirmed', handoff_id=intent['acknowledgement']['handoff_id'],
            gpu_occupancy_released=True, artifact_bytes=sum(f['size_bytes'] for f in actual)))

    async def _upload_artifacts(
        self, attempt_id: str, lease_token: str, artifacts: Dict[str, Any]
    ) -> None:
        # Register durable completion before delivery, even with a healthy
        # execution lease: uploads can cross the execution budget. Reconcile
        # is authenticated and idempotent, including on boot replay.
        try:
            view = await self._reconcile_with_anchor(attempt_id, lease_token, True)
        except ApiError as e:
            # LEASE_LOST is definitive only from authenticated reconcile,
            # not from prepare, credential refresh, or signed PUT delivery.
            if 400 <= e.status < 500 and e.status != 429:
                self._quarantine_attempt(attempt_id, e.code)
            raise
        if view.get("status") != "RUNNING" or not view.get("is_current"):
            raise LeaseLost()
        self._arm_lease_clock_from_view(view)
        self.journal.update_attempt(attempt_id, recovery_state="online")
        self._check_lease_alive()
        intent = self.journal.cpu_tail_intent(attempt_id)
        fresh_intent = False
        if (intent is None and getattr(self.config, "cpu_tail_pilot", False)
                and self.journal.get_attempt(attempt_id)["request_snapshot"].get("mode") != "ltx2_video"):
            checkpoints = self.journal.cpu_tail_checkpoints(attempt_id)
            rows = self.journal.artifacts_for(attempt_id)
            # Old journals without observed engine exit stay on the serial path.
            if ("engine_exited" in checkpoints and "durable" in checkpoints
                    and not self.journal.get_process(attempt_id)
                    and sum(r["size_bytes"] for r in rows) <= min(
                        self.config.cpu_tail_max_bytes, 512 * 1024 * 1024)):
                proof = dict(engine_exited=True, durable=True,
                             max_bytes=min(self.config.cpu_tail_max_bytes, 512 * 1024 * 1024),
                             files=sorted([dict(role=r["role"], size_bytes=r["size_bytes"],
                                         sha256=r["sha256"]) for r in rows], key=lambda f: f["role"]))
                intent = self.journal.begin_cpu_tail_intent(attempt_id, proof)
                fresh_intent = True
        if intent is not None:
            # Persisted evidence, never current configuration, is the replay key.
            # Disabling the pilot cannot bypass an uncertain prior request.
            actual = sorted([dict(role=r["role"], size_bytes=r["size_bytes"], sha256=r["sha256"])
                             for r in self.journal.artifacts_for(attempt_id)], key=lambda f: f["role"])
            if actual != intent["evidence"]["files"] or self.journal.get_process(attempt_id):
                raise ValueError("CPU tail intent differs from durable local output")
            if intent["acknowledgement"] is None:
                try:
                    response = await asyncio.to_thread(self.http.reserve_cpu_tail,
                                            attempt_id, lease_token, intent["evidence"])
                except ApiError as e:
                    # Only a definitive first response establishes legacy support.
                    # A missing route after an uncertain request cannot settle it.
                    if e.status != 404 or not fresh_intent:
                        raise
                    self.journal.cpu_tail_unsupported(attempt_id)
                else:
                    self.journal.acknowledge_cpu_tail_intent(attempt_id, response)
        await self._handoff_publication(attempt_id, lease_token)
        await self._emit_event(attempt_id, {
            "type": "phase", "phase": "uploading",
        })
        await self._flush_events_safe(attempt_id, lease_token)
        # journal state decides what still needs uploading: a role already
        # uploaded (e.g. before a crash — the resume path re-enters here)
        # is skipped; the server already holds those bytes (terminal-state
        # review issue 3)
        rows = {
            r["role"]: r for r in self.journal.artifacts_for(attempt_id)
        }
        files = []
        pending: List[Tuple[str, Dict[str, Any]]] = []
        for role, info in artifacts.items():
            row = rows.get(role)
            if row and row.get("artifact_id") \
                    and row.get("upload_state") == "uploaded":
                continue
            content_type = (
                "video/mp4" if role == "video" else
                "audio/wav" if role == "audio" else
                "application/json"
            )
            spec = {
                "role": role,
                "content_type": content_type,
                "size_bytes": info["size"],
                "sha256": info["sha256"],
            }
            if row and row.get("artifact_id"):
                # prepared before the crash: refresh the credential for
                # the SAME artifact (protocol 10.10)
                spec["artifact_id"] = row["artifact_id"]
            files.append(spec)
            pending.append((role, info))
        # mirror the artifact list into the monitoring snapshot with each
        # role's CURRENT state (protocol 11: artifact status)
        self.monitor.set_artifacts([
            {
                "role": role,
                "size_bytes": info["size"],
                "sha256": info["sha256"],
                "state": (rows.get(role) or {}).get("upload_state")
                or "local",
            }
            for role, info in artifacts.items()
        ])
        if not files:
            # nothing to do (resume after a full upload)
            self._report_monitor_soon()
            return
        request_id = f"prep_{attempt_id}_{secrets.token_hex(4)}"
        prep = await asyncio.to_thread(
            self.http.prepare_artifacts, attempt_id, lease_token,
            request_id, files,
        )
        upload_map = {}
        for spec in prep.get("files", []):
            upload_map[spec["role"]] = spec
            self.journal.set_artifact_remote(
                attempt_id, spec["role"], spec["artifact_id"], "prepared"
            )
            self.monitor.set_artifact_state(spec["role"], "prepared")
        self._report_monitor_soon()
        for role, info in pending:
            self._check_lease_alive()
            if self.cancel_requested.is_set():
                raise CancelRequested()
            spec = upload_map[role]
            content_type = (
                "video/mp4" if role == "video" else
                "audio/wav" if role == "audio" else
                "application/json"
            )

            last_error: Optional[Exception] = None
            for attempt in range(self.config.max_retries):
                try:
                    self._check_lease_alive()

                    # defined INSIDE the loop (final review B4): a refreshed
                    # URL must be picked up by the next attempt, not closed
                    # over from before the refresh
                    def _up(spec=spec, content_type=content_type,
                             role=role):
                        # byte-accurate upload counter (protocol 11); the
                        # callback fires on the executor thread and only
                        # stores numbers.  abort_check interrupts an
                        # in-flight PUT on cancel/lease-loss instead of
                        # streaming to a dead attempt.
                        return self.http.upload(
                            spec["url"], info["path"], content_type,
                            spec.get("headers") or {},
                            self.config.upload_timeout_seconds,
                            progress_cb=lambda sent, total, _r=role:
                                self.monitor.upload_progress(
                                    _r, sent, total),
                            abort_check=self._transfer_should_abort,
                        )

                    await asyncio.to_thread(_up)
                    last_error = None
                    break
                except TransferAborted:
                    # deliberate stop mid-body: classify by the RECORDED
                    # reason (what was true at the abort decision)
                    if self._transfer_abort_reason == "offline":
                        raise ApiError(0, "NETWORK", "lease contact requires reconciliation")
                    if self._transfer_abort_reason in ("cancel", "shutdown"):
                        raise CancelRequested()
                    raise LeaseLost()
                except ApiError as e:
                    last_error = e
                    if e.code == "LEASE_LOST":
                        raise  # retry only after authenticated reconciliation
                    if e.url_expired:
                        self.monitor.add_anomaly(
                            "url_refresh",
                            f"upload URL expired for {role}; refreshing",
                        )
                        # B4: refresh the credential BEFORE any raise; new
                        # request_id + original artifact_id (protocol 10.10)
                        refresh = await asyncio.to_thread(
                            self.http.prepare_artifacts, attempt_id,
                            lease_token,
                            f"prep_{attempt_id}_{secrets.token_hex(4)}",
                            [{
                                "role": role,
                                "artifact_id": spec["artifact_id"],
                                "content_type": content_type,
                                "size_bytes": info["size"],
                                "sha256": info["sha256"],
                            }],
                        )
                        for rspec in refresh.get("files", []):
                            upload_map[rspec["role"]] = rspec
                        spec = rspec
                        continue
                    if e.status in (400, 401, 403, 409):
                        raise
                except OSError as e:
                    last_error = e
                self.monitor.add_anomaly(
                    "upload_retry",
                    f"{role} attempt {attempt + 1}: "
                    f"{self._safe_error_text(last_error)[:120]}",
                )
                await self._backoff_sleep(backoff_delay(attempt))
            if last_error is not None:
                raise RuntimeError(
                    f"upload failed for {role}: "
                    f"{self._safe_error_text(last_error)}"
                )
            self.journal.set_artifact_remote(
                attempt_id, role, upload_map[role]["artifact_id"], "uploaded"
            )
            self.monitor.set_artifact_state(role, "uploaded")
            self._report_monitor_soon()

    async def _finish_success(
        self, attempt_id: str, lease_token: str,
        artifacts: Dict[str, Any], result: Dict[str, Any],
    ) -> None:
        await self._emit_event(attempt_id, {
            "type": "phase", "phase": "finalizing",
        })
        await self._flush_events_safe(attempt_id, lease_token)
        record = self.journal.get_attempt(attempt_id)
        request_id = f"fin_{attempt_id}"
        remote_artifacts = self.journal.artifacts_for(attempt_id)
        by_role = {a["role"]: a for a in remote_artifacts}
        body = {
            "status": "SUCCEEDED",
            "artifacts": [
                {
                    "artifact_id": by_role[role]["artifact_id"],
                    "sha256": info["sha256"],
                }
                for role, info in artifacts.items()
            ],
            "result": _finish_result_payload(
                artifacts,
                result,
                str(record["request_snapshot"].get("seed", "")),
            ),
        }
        self._checkpoint_cpu_tail(attempt_id, "uploaded")
        outcome = await self._submit_finish(
            attempt_id, lease_token, request_id, body, kind="SUCCEEDED"
        )
        if outcome == "SUCCEEDED":
            self._checkpoint_cpu_tail(attempt_id, "verified")
            # server confirmed the finish: artifacts are verified remotely
            for role in artifacts:
                self.monitor.set_artifact_state(role, "verified")
        # any other resolution (CANCELLED / UNSETTLED / EXPIRED / a racing
        # verdict) is displayed as-is — monitoring shows the remote truth,
        # never the local intent (terminal-state review issue 1); artifacts
        # honestly stay "uploaded" until a confirmed success verifies them
        self.monitor.end_attempt(outcome)
        await self._report_monitor()

    async def _finish_failed(
        self, attempt_id: str, lease_token: str, code: str, message: str,
        stage: Optional[str] = None,
    ) -> None:
        request_id = f"finf_{attempt_id}"
        body = {
            "status": "FAILED",
            "error": {
                "code": code,
                "message": self._safe_error_text(message),
                "stage": stage,
            },
        }
        outcome = await self._submit_finish(
            attempt_id, lease_token, request_id, body, kind="FAILED"
        )
        # show the resolved outcome: a FAILED finish that raced into a
        # cancel or lost its lease is NOT displayed as FAILED
        self.monitor.end_attempt(outcome)
        await self._report_monitor()

    async def _submit_finish(
        self, attempt_id: str, lease_token: str, request_id: str,
        body: Dict[str, Any], kind: str,
    ) -> str:
        """Submit a finish with retry + reconciliation (review B3/C14).

        The intent is persisted BEFORE the first send; on a lost response
        the SAME request_id and payload are retried, and GET attempt
        reconciles an accepted-but-unobserved finish.

        Returns the RESOLVED outcome for monitoring (terminal-state review
        issue 1: the monitor must reflect the remote truth, never the
        local intent):
        - ``kind`` ("SUCCEEDED"/"FAILED"): the server accepted our finish
        - "CANCELLED": the server demanded cancel semantics and we complied
        - the remote attempt status (e.g. "EXPIRED", "CANCELLED") when a
          racing verdict had already landed
        - "UNSETTLED": confirmation impossible (lease lost / permanent
          rejection / reconcile failed) — the remote verdict is pending
          and must NOT be displayed as the intended outcome"""
        self.journal.update_attempt(
            attempt_id,
            finish_request_id=request_id,
            finish_payload={"request_id": request_id, **body},
        )
        # B-4 (terminal review): when reconcile shows the attempt is still
        # RUNNING+current (our finish simply has not landed), keep
        # replaying within the lease budget instead of abandoning it
        outer_rounds = 0
        while True:
            last_error: Optional[ApiError] = None
            replay = False  # budget-approved replay of the whole round
            for attempt in range(self.config.max_retries + 2):
                if self._local_lease_expired():
                    self._defer_recovery(attempt_id)
                    return "UNSETTLED"
                try:
                    await asyncio.to_thread(
                        self.http.finish, attempt_id, lease_token,
                        request_id, body,
                    )
                    self.journal.update_attempt(
                        attempt_id, finish_accepted=1, confirmed_terminal=1
                    )
                    print(f"[worker] attempt {attempt_id} {kind}", flush=True)
                    return kind
                except ApiError as e:
                    last_error = e
                    if e.code == "CANCEL_REQUESTED":
                        # protocol 10.3: switch to CANCELLED with a new id;
                        # propagate whatever that submission resolved to
                        return await self._submit_cancelled(
                            attempt_id, lease_token
                        )
                    if e.code in ("TASK_TERMINAL", "ATTEMPT_NOT_CURRENT", "LEASE_LOST"):
                        # a racing verdict may have landed (e.g. our own
                        # replay, or the scanner): reconcile, do not error
                        if e.code == "LEASE_LOST":
                            self._defer_recovery(attempt_id, e.code)
                        resolution = await self._reconcile_remote(
                            attempt_id, request_id
                        )
                        if resolution == "OURS":
                            return kind
                        if resolution in ("RUNNING", "UNREACHABLE"):
                            # RUNNING: still ours, replay.  UNREACHABLE:
                            # the reconcile GET failed transiently — the
                            # idempotent finish replay doubles as a retry
                            # round for the reconcile itself
                            if self._finish_budget_ok(outer_rounds):
                                outer_rounds += 1
                                replay = True
                                break  # leave the inner retry loop only
                            return "UNSETTLED"  # alive/unreachable, budget spent
                        return resolution or "UNSETTLED"
                    if 400 <= e.status < 500 and e.status != 429:
                        # permanent rejection: surface loudly, stop
                        # retrying; mark confirmed so a later boot does not
                        # replay a doomed finish forever (terminal review
                        # B-1 path 5 note)
                        self._quarantine_attempt(attempt_id, e.code)
                        self.journal.drop_outbox(attempt_id)
                        self.monitor.add_anomaly(
                            "other", f"finish {kind} rejected: {e.code}"
                        )
                        print(
                            f"[worker] finish {kind} rejected: {self._safe_error_text(e)}",
                            flush=True,
                        )
                        return "UNSETTLED"
                    await self._backoff_sleep(
                    backoff_delay(attempt, e.retry_after)
                )
            if replay:
                # a reconcile verdict of RUNNING/UNREACHABLE within budget:
                # replay the idempotent finish for a fresh round — no
                # extra reconcile here, the round's own error handling
                # reconciles as needed
                continue
            # inner retries exhausted while the lease may still be valid
            resolution = await self._reconcile_remote(attempt_id, request_id)
            if resolution == "OURS":
                return kind
            if resolution in ("RUNNING", "UNREACHABLE"):
                if self._finish_budget_ok(outer_rounds):
                    outer_rounds += 1
                    continue
                return "UNSETTLED"  # alive/unreachable, budget spent
            return resolution or "UNSETTLED"

    async def _backoff_sleep(self, delay: float) -> None:
        """Backoff sleep interruptible by cancel/lease-loss (terminal
        review C-2): a stop request during the backoff must take effect
        at once, not after the full delay."""
        if delay <= 0:
            return
        wake = asyncio.Event()

        def _on_cancel() -> None:
            wake.set()

        cancel_cb = self._attempt_stop_callbacks["cancel"] = _on_cancel
        try:
            try:
                await asyncio.wait_for(wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                return  # delay elapsed normally
            # woken early: classify
            if self.cancel_requested.is_set():
                raise CancelRequested()
            raise LeaseLost()
        finally:
            self._attempt_stop_callbacks.pop("cancel", None)

    def _finish_budget_ok(self, outer_rounds: int) -> bool:
        """B-4 replay budget: the lease clock is still armed-and-future
        (or unarmed in a recovery replay) and the round cap is not hit."""
        if self.lease_lost.is_set():
            return False
        if outer_rounds >= 3:
            return False
        if self.lease_deadline_mono == 0.0 \
                and self.lease_deadline_wall == 0.0:
            return True  # unarmed (recovery): one bounded extra round
        return not (
            time.monotonic() >= self.lease_deadline_mono
            or time.time() >= self.lease_deadline_wall
        )

    async def _reconcile_remote(
        self, attempt_id: str, request_id: str
    ) -> Optional[str]:
        """GET attempt and check accepted_finish_request_id (protocol 10.7).

        Returns "OURS" when our finish had landed, "RUNNING" when the
        attempt is still alive and ours (caller replays), "UNREACHABLE"
        when the GET failed TRANSIENTLY (caller retries within budget and
        the next boot's replay path re-reconciles — never confirm on a
        maybe), the remote attempt status when another verdict moved it,
        or None when the attempt is permanently unreachable (404/401/403)."""
        try:
            view = await asyncio.to_thread(self.http.get_attempt, attempt_id)
        except ApiError as e:
            print(f"[worker] reconcile GET failed: {self._safe_error_text(e)}", flush=True)
            if e.status == 0 or e.status == 429 or e.status >= 500:
                # transient: do NOT confirm locally — the finish may have
                # landed unobserved; the journaled intent must survive so
                # a later boot backfills the acceptance (follow-up to the
                # terminal review: premature confirm abandoned the replay)
                return "UNREACHABLE"
            # permanent (401/403/404): the attempt is unknown to us or we
            # cannot authenticate — replaying will never succeed
            self.journal.update_attempt(attempt_id, confirmed_terminal=1)
            return None
        if view.get("accepted_finish_request_id") == request_id:
            self.journal.update_attempt(
                attempt_id, finish_accepted=1, confirmed_terminal=1
            )
            print(f"[worker] finish {request_id} confirmed via GET",
                  flush=True)
            return "OURS"
        record = self.journal.get_attempt(attempt_id)
        if (view.get("status") == "RUNNING" and view.get("is_current")
                and record.get("recovery_state") != "offline"
                and not view.get("recovery_expires_at") and not self._local_lease_expired()):
            # B-4 (terminal review): the attempt is still ours and alive —
            # our finish simply has not landed.  Do NOT confirm/drop: the
            # caller replays within its lease budget, and a later boot
            # must still see a resumable intent
            return "RUNNING"
        if view.get("status") in ("RUNNING", "EXPIRED"):
            record = self.journal.get_attempt(attempt_id)
            try:
                recovered = await self._reconcile_with_anchor(
                    attempt_id, record["lease_token"], True)
            except ApiError as e:
                if e.status == 0 or e.status >= 500 or e.status == 429:
                    self._defer_recovery(attempt_id)
                    return "UNREACHABLE"
                self._quarantine_attempt(attempt_id, e.code)
                return "UNSETTLED"
            if recovered.get("status") == "RUNNING" and recovered.get("is_current"):
                self._arm_lease_clock_from_view(recovered)
                self.journal.update_attempt(attempt_id, recovery_state="online")
                return "RUNNING"
        # someone else moved the attempt (scanner expiry / superseded)
        self.journal.update_attempt(attempt_id, confirmed_terminal=1)
        self.journal.drop_outbox(attempt_id)
        print(
            f"[worker] attempt {attempt_id} is {view.get('status')} "
            "remotely; reconciled",
            flush=True,
        )
        return view.get("status")

    async def _submit_cancelled(
        self, attempt_id: str, lease_token: str
    ) -> str:
        """Submit the CANCELLED finish.  Returns the resolved outcome for
        monitoring (terminal review C-7): "CANCELLED" only when the server
        accepted it; "UNSETTLED" when unconfirmed (network error) or a
        racing verdict landed first."""
        request_id = f"finc_{attempt_id}"
        body = {"status": "CANCELLED", "reason": "local stop confirmed"}
        self.journal.update_attempt(attempt_id, cancel_intent=1,
            finish_request_id=request_id, finish_payload={"request_id": request_id, **body})
        if self._local_lease_expired():
            self._defer_recovery(attempt_id, "cancellation awaiting Server expiry")
            return "UNSETTLED"
        try:
            await asyncio.to_thread(
                self.http.finish, attempt_id, lease_token, request_id, body
            )
            self.journal.update_attempt(attempt_id, confirmed_terminal=1)
            print(f"[worker] attempt {attempt_id} CANCELLED", flush=True)
            return "CANCELLED"
        except ApiError as e:
            if e.code == "LEASE_LOST":
                self._defer_recovery(attempt_id, e.code)
                return "UNSETTLED"
            if e.code in ("TASK_TERMINAL", "ATTEMPT_NOT_CURRENT"):
                # legal race: another verdict landed first — we are done,
                # but we cannot claim the remote state is CANCELLED
                self.journal.update_attempt(attempt_id, confirmed_terminal=1)
                print(
                    f"[worker] attempt {attempt_id} already terminal "
                    f"remotely ({e.code})",
                    flush=True,
                )
                return "UNSETTLED"
            print(f"[worker] finish CANCELLED rejected: {self._safe_error_text(e)}", flush=True)
            return "UNSETTLED"

    async def _release_or_fail(
        self, attempt_id: str, lease_token: str, incapacity: LocalIncapacity
    ) -> None:
        """Node-local incapacity: release so another node can pick the task
        up immediately (protocol 10.5).  The structured code rides along so
        the server applies the eligibility backoff (final review B1)."""
        last_error: Optional[ApiError] = None
        for attempt in range(3):
            try:
                await asyncio.to_thread(
                    self.http.release, attempt_id, lease_token,
                    incapacity.reason, incapacity.code,
                )
                self.journal.update_attempt(attempt_id, confirmed_terminal=1)
                print(
                    f"[worker] attempt {attempt_id} released "
                    f"({incapacity.code})",
                    flush=True,
                )
                # belt-and-braces local backoff: even if the server-side
                # ineligible window were misconfigured, do not spin on the
                # same task immediately
                self.monitor.end_attempt("RELEASED")
                if incapacity.code == "MODEL_UNAVAILABLE":
                    self.monitor.set_node("model_unavailable")
                await self._report_monitor()
                await asyncio.sleep(5.0)
                return
            except ApiError as e:
                last_error = e
                if e.code in ("LEASE_LOST", "ATTEMPT_NOT_CURRENT",
                              "TASK_TERMINAL"):
                    self.journal.update_attempt(attempt_id, confirmed_terminal=1)
                    return
                if 400 <= e.status < 500 and e.status != 429:
                    break
                await self._backoff_sleep(
                    backoff_delay(attempt, e.retry_after)
                )
        # release not achievable: report as a failure with the real cause
        await self._finish_failed(
            attempt_id, lease_token, incapacity.code, incapacity.reason,
            stage="downloading",
        )
        if last_error is not None:
            print(f"[worker] release failed: {self._safe_error_text(last_error)}", flush=True)

    async def _shutdown_after_cancel_or_loss(
        self, attempt_id: str, lease_token: str
    ) -> None:
        """Local stop is confirmed.  Two distinct cases:
        - cancel requested AND lease still valid: submit CANCELLED now
        - lease lost (either clock or server verdict): we hold no execution
          right; stop silently — the server scanner expires the attempt and
          applies the cancel-marked CANCELLED semantics itself."""
        state = comfy.state_for(self, attempt_id)
        if state and not state.get('terminal'):
            comfy.quarantine(self, 'ComfyUI cancellation unresolved; automatic reconciliation pending')
            return
        if self.lease_lost.is_set() or not self.cancel_requested.is_set():
            if _shutdown_requested(self) and not self.lease_lost.is_set():
                self._defer_recovery(attempt_id, "worker shutdown")
            else:
                self._quarantine_attempt(attempt_id, "authoritative lease loss")
            self.monitor.end_attempt(
                "CANCELLED" if self.cancel_requested.is_set() else "STOPPED"
            )
            print(
                f"[worker] attempt {attempt_id} stopped "
                f"({'lease lost' if self.lease_lost.is_set() else 'no cancel request'})",
                flush=True,
            )
            return
        outcome = await self._submit_cancelled(attempt_id, lease_token)
        self.monitor.end_attempt(outcome)


async def main_async(worker: Optional[Worker] = None) -> None:
    if worker is None:
        worker = Worker(WorkerConfig.from_env())
    await worker.run()


def main() -> None:
    worker = Worker(WorkerConfig.from_env())
    worker._force_exit_on_shutdown = True
    try:
        asyncio.run(main_async(worker))
    except KeyboardInterrupt:
        raise SystemExit(130)
    else:
        # Keep the backstop armed until asyncio.run has also joined executor
        # threads. Cancelling it in run() would leave socket/native hangs unbounded.
        if worker._shutdown_watchdog is not None:
            worker._shutdown_watchdog.cancel()
        if worker._shutdown_forced:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
