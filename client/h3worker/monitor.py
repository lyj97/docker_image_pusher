"""Monitoring state collector (protocol 11).

The Monitor gathers node / attempt / process / artifact state, persists the
latest snapshot to the journal (so the local status command works offline
and crash recovery keeps the last known state), and hands it to the Worker
for reporting.  Display boundaries it enforces:

* engine progress keeps its ``semantics`` — "denoise enqueue" is a GPU
  submission, never a completion;
* only current-phase counters are tracked — no global percentage in v1;
* ``last_progress_at`` is recorded separately from heartbeat time.

All mutations are cheap and thread-safe: the upload progress callback fires
from an executor thread while the asyncio loop mutates stages.
"""

from __future__ import annotations

import collections
import copy
import ctypes
import datetime as dt
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional

from .http import sanitize_error_text

from shared.h3proto import (
    MONITOR_ARTIFACT_STATES,
    MONITOR_MAX_ANOMALIES,
    MONITOR_TEXT_LIMIT,
)


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_ms_iso() -> str:
    """Sub-second ordering stamp for reported_at only: the server drops
    out-of-order snapshots by string comparison, and second precision
    left a ~1s window where a cancelled in-flight report could still
    overwrite a newer one (fix-review C-1)."""
    return dt.datetime.now(dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


class Monitor:
    """Bounded, thread-safe monitoring snapshot (protocol 11)."""

    def __init__(self, journal, worker_id: str, boot_id: str, attempt_id=None):
        self._monitor_attempt_id = attempt_id
        self._journal = journal
        self._worker_id = worker_id
        self._boot_id = boot_id
        self._lock = threading.Lock()
        self._node: Dict[str, Any] = {
            "status": "idle",
            "disk_free_bytes": 0,
            "unhealthy_reason": None,
            "note": None,
        }
        self._cpu_ticks = self._read_cpu_ticks()
        self._mac_metrics_at = 0.0
        self._mac_metrics: Dict[str, Any] = {}
        self._mac_identity: Optional[Dict[str, Any]] = None
        self._attempt: Optional[Dict[str, Any]] = None
        self._anomalies: "collections.deque[Dict[str, Any]]" = collections.deque(
            maxlen=MONITOR_MAX_ANOMALIES
        )
        self._report_error_logged = False
        # persistence throttle: progress-only updates coalesce to at most
        # one journal write per interval; stage/process/artifact changes
        # always persist immediately
        self._last_persist = 0.0
        import time as _time

        self._time = _time
        self._persist_min_interval = 0.5

    # -- node --------------------------------------------------------------

    def set_node(self, status: str, **fields: Any) -> None:
        """Update node status.  Optional fields: disk_free_bytes,
        unhealthy_reason, note — passing a key with an explicit None
        CLEARS it (review C10: no permanently-sticky fields)."""
        with self._lock:
            self._node["status"] = status
            for key in ("disk_free_bytes", "unhealthy_reason", "note"):
                if key in fields:
                    value = fields[key]
                    if key in ("unhealthy_reason", "note") and isinstance(value, str):
                        value = sanitize_error_text(value)
                    self._node[key] = (
                        int(value) if key == "disk_free_bytes"
                        and value is not None else value
                    )
            self._persist_locked()

    # -- attempt lifecycle ---------------------------------------------------

    def begin_attempt(self, attempt_id: str, task_id: str) -> None:
        with self._lock:
            self._attempt = {
                "attempt_id": attempt_id,
                "task_id": task_id,
                "stage": "downloading",
                "detail_phase": None,
                "phase_instance": 0,
                "started_at": _now_iso(),
                "last_progress_at": None,
                "progress": None,
                "cancel_requested": False,
                "outcome": None,
                "process": None,
                "artifacts": [],
            }
            self._persist_locked()

    def end_attempt(self, outcome: str) -> None:
        with self._lock:
            if self._attempt is not None:
                self._attempt["outcome"] = outcome
                # freeze the elapsed anchor for server-side display
                # (review C6): the server stops recomputing once outcome
                # is set, so this timestamp is what fixes the final value
                self._attempt["ended_at"] = _now_iso()
                self._persist_locked()

    def clear_attempt(self) -> None:
        with self._lock:
            self._attempt = None
            self._persist_locked()

    # -- stage + progress -----------------------------------------------------

    def set_stage(self, stage: str, detail_phase: Optional[str] = None,
                  phase_instance: Optional[int] = None) -> None:
        with self._lock:
            if self._attempt is None:
                return
            changed = (stage != self._attempt.get("stage") or
                       (phase_instance is not None and
                        int(phase_instance) != self._attempt.get("phase_instance")))
            self._attempt["stage"] = stage
            if detail_phase is not None:
                self._attempt["detail_phase"] = detail_phase
            if phase_instance is not None:
                self._attempt["phase_instance"] = int(phase_instance)
            if changed:
                # a new phase resets the current-phase counters
                self._attempt["progress"] = None
                self._attempt["last_progress_at"] = None
            self._persist_locked()

    def set_progress(self, completed: int, total: int, unit: str,
                     semantics: str) -> None:
        with self._lock:
            if self._attempt is None:
                return
            self._attempt["progress"] = {
                "completed": int(completed),
                "total": int(total),
                "unit": unit,
                # GPU enqueue must never display as completed work
                "semantics": semantics,
            }
            self._attempt["last_progress_at"] = _now_iso()
            self._persist_locked(force=False)

    def set_cancel_requested(self) -> None:
        with self._lock:
            if self._attempt is not None:
                self._attempt["cancel_requested"] = True
                self._persist_locked()

    # -- process ---------------------------------------------------------------

    def process_started(self, pid: int, program: str) -> None:
        with self._lock:
            if self._attempt is None:
                return
            self._attempt["process"] = {
                "pid": int(pid),
                "program": program,
                "alive": True,
                "exit_code": None,
                "started_at": _now_iso(),
                "ended_at": None,
            }
            self._persist_locked()

    def process_ended(self, exit_code: Optional[int]) -> None:
        with self._lock:
            if self._attempt is None or self._attempt.get("process") is None:
                return
            self._attempt["process"]["alive"] = False
            self._attempt["process"]["exit_code"] = (
                int(exit_code) if exit_code is not None else None
            )
            self._attempt["process"]["ended_at"] = _now_iso()
            self._persist_locked()

    # -- artifacts ---------------------------------------------------------------

    def set_artifacts(self, artifacts: List[Dict[str, Any]]) -> None:
        """Replace the artifact list (roles with size/sha/state)."""
        with self._lock:
            if self._attempt is None:
                return
            self._attempt["artifacts"] = [
                {
                    "role": a["role"],
                    "size_bytes": int(a.get("size_bytes") or 0),
                    "sha256": a.get("sha256"),
                    "state": a.get("state") or "local",
                    "uploaded_bytes": int(a.get("uploaded_bytes") or 0),
                }
                for a in artifacts
            ]
            self._persist_locked()

    def set_artifact_state(self, role: str, state: str) -> None:
        if state not in MONITOR_ARTIFACT_STATES:
            return
        with self._lock:
            if self._attempt is None:
                return
            for item in self._attempt["artifacts"]:
                if item["role"] == role:
                    item["state"] = state
                    if state == "uploaded":
                        item["uploaded_bytes"] = item["size_bytes"]
                    self._persist_locked()
                    return

    def upload_progress(self, role: str, sent: int, total: int) -> None:
        """Byte-accurate upload counter; called from the upload thread."""
        with self._lock:
            if self._attempt is None:
                return
            for item in self._attempt["artifacts"]:
                if item["role"] == role:
                    item["uploaded_bytes"] = int(min(sent, total))
                    return

    # -- anomalies ----------------------------------------------------------------

    def add_anomaly(self, kind: str, detail: str) -> None:
        with self._lock:
            self._anomalies.append({
                "at": _now_iso(),
                "kind": kind,
                "detail": sanitize_error_text(detail),
            })
            self._persist_locked()

    def note_report_error(self, message: str) -> None:
        """A failed status report is itself an anomaly; log once per streak
        so a long outage does not spam the ring."""
        with self._lock:
            if self._report_error_logged:
                return
            self._report_error_logged = True
        self.add_anomaly("network_retry", f"status report failed: {message}")

    def note_report_ok(self) -> None:
        with self._lock:
            self._report_error_logged = False

    # -- output --------------------------------------------------------------------

    def node_status(self) -> str:
        with self._lock:
            return self._node["status"]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            self._node.update(self._system_metrics_locked())
            self._update_process_metrics_locked()
            node = dict(self._node)
            return {
                "schema_version": 1,
                "boot_id": self._boot_id,
                "worker_id": self._worker_id,
                # sub-second: ordering only, never displayed
                "reported_at": _now_ms_iso(),
                "node": node,
                "attempt": copy.deepcopy(self._attempt),
                "anomalies": list(self._anomalies),
            }

    def _update_process_metrics_locked(self) -> None:
        process = self._attempt and self._attempt.get("process")
        if not process or not process.get("alive"):
            return
        process["cpu_percent"] = None
        process["rss_bytes"] = None
        pid = process.get("pid")
        try:
            result = subprocess.run(
                ["/bin/ps", "-o", "%cpu=,rss=", "-p", str(pid)],
                capture_output=True, text=True, timeout=1,
            )
            fields = result.stdout.split()
            if result.returncode == 0 and len(fields) >= 2:
                cpu = float(fields[0])
                rss = int(fields[1]) * 1024
                if 0 <= cpu and 0 <= rss:
                    process["cpu_percent"] = round(cpu, 1)
                    process["rss_bytes"] = rss
        except (OSError, subprocess.TimeoutExpired, ValueError):
            pass

    @staticmethod
    def _read_cpu_ticks() -> Optional[tuple[int, ...]]:
        """Return aggregate CPU counters without starting a helper process."""
        if sys.platform == "darwin":
            try:
                lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
                ticks = (ctypes.c_uint32 * 4)()
                count = ctypes.c_uint32(4)
                result = lib.host_statistics(
                    lib.mach_host_self(), 3,
                    ctypes.cast(ticks, ctypes.POINTER(ctypes.c_int32)),
                    ctypes.byref(count),
                )
                if result == 0 and count.value >= 4:
                    # HOST_CPU_LOAD_INFO: user, system, idle, nice.  Reorder
                    # idle last so the shared delta helper can sum busy ticks.
                    return ticks[0], ticks[1], ticks[3], ticks[2]
            except (AttributeError, OSError, TypeError, ValueError):
                return None
            return None
        try:
            with open("/proc/stat", encoding="ascii") as fh:
                fields = [int(value) for value in fh.readline().split()[1:]]
            idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
            return sum(fields) - idle, idle
        except (OSError, ValueError, IndexError):
            return None

    @staticmethod
    def _cpu_percent(previous: tuple[int, ...],
                     current: tuple[int, ...]) -> Optional[float]:
        if len(previous) != len(current) or len(current) < 2:
            return None
        if len(current) == 4:
            # Darwin counters are uint32 and wrap independently.
            deltas = [
                (now - before) % (1 << 32)
                for before, now in zip(previous, current)
            ]
            busy_delta, idle_delta = sum(deltas[:-1]), deltas[-1]
        else:
            busy_delta = current[0] - previous[0]
            idle_delta = current[1] - previous[1]
        total_delta = busy_delta + idle_delta
        if total_delta <= 0 or busy_delta < 0 or idle_delta < 0:
            return None
        return round(max(0.0, min(100.0,
                                  busy_delta * 100 / total_delta)), 1)

    def _mac_memory_metrics(self) -> Dict[str, Any]:
        now = time.monotonic()
        if self._mac_metrics and now - self._mac_metrics_at < 10:
            return self._mac_metrics
        metrics: Dict[str, Any] = {
            "memory_pressure_free_percent": None,
            "memory_compressed_bytes": None,
            "swap_used_bytes": None,
            "gpu_percent": None,
        }

        def output(argv: list[str]) -> str:
            try:
                result = subprocess.run(
                    argv, capture_output=True, text=True, timeout=1,
                )
                return result.stdout if result.returncode == 0 else ""
            except (OSError, subprocess.TimeoutExpired):
                return ""

        pressure = output(["/usr/bin/memory_pressure", "-Q"])
        total = re.search(r"The system has (\d+)", pressure)
        free = re.search(r"System-wide memory free percentage: (\d+)%", pressure)
        if total:
            metrics["memory_total_bytes"] = int(total.group(1))
        if free:
            percent = int(free.group(1))
            if percent <= 100:
                metrics["memory_pressure_free_percent"] = percent

        vm = output(["/usr/bin/vm_stat"])
        page = re.search(r"page size of (\d+) bytes", vm)
        occupied = re.search(r"Pages occupied by compressor:\s*(\d+)\.", vm)
        if page and occupied:
            metrics["memory_compressed_bytes"] = (
                int(page.group(1)) * int(occupied.group(1))
            )

        swap = output(["/usr/sbin/sysctl", "vm.swapusage"])
        used = re.search(r"\bused\s*=\s*([\d.]+)([KMGT])\b", swap)
        if used:
            metrics["swap_used_bytes"] = round(
                float(used.group(1)) * 1024 ** ("KMGT".index(used.group(2)) + 1)
            )

        # Apple Silicon exposes aggregate AGX activity without privileges.
        # This is best-effort because keys vary across macOS / GPU versions.
        ioreg = output(["/usr/sbin/ioreg", "-r", "-d", "1",
                        "-c", "AGXAccelerator"])
        gpu = re.search(r'"Device Utilization %"\s*=\s*(\d+)', ioreg)
        if gpu:
            value = int(gpu.group(1))
            if value <= 100:
                metrics["gpu_percent"] = value

        if self._mac_identity is None:
            self._mac_identity = {}
            try:
                identity_result = subprocess.run(
                    ["/usr/sbin/system_profiler", "SPHardwareDataType",
                     "SPDisplaysDataType", "-json"],
                    capture_output=True, text=True, timeout=3,
                )
                if identity_result.returncode == 0:
                    info = json.loads(identity_result.stdout)
                    hardware = (info.get("SPHardwareDataType") or [{}])[0]
                    displays = (info.get("SPDisplaysDataType") or [{}])[0]
                    cores = displays.get("sppci_cores")
                    if isinstance(cores, str) and cores.isdigit():
                        cores = int(cores)
                    if not isinstance(cores, int):
                        cores = None
                    mapping = {
                        "device_model": hardware.get("machine_model"),
                        "chip_name": hardware.get("chip_type"),
                        "gpu_name": displays.get("sppci_model"),
                        "gpu_core_count": cores,
                    }
                    self._mac_identity.update({
                        key: value for key, value in mapping.items()
                        if isinstance(value, (str, int)) and value != ""
                    })
            except (OSError, subprocess.TimeoutExpired, ValueError,
                    json.JSONDecodeError):
                pass
        metrics.update(self._mac_identity)
        self._mac_metrics = metrics
        self._mac_metrics_at = now
        return metrics

    def _system_metrics_locked(self) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {
            "hostname": socket.gethostname()[:MONITOR_TEXT_LIMIT],
            "cpu_count": os.cpu_count(),
            "cpu_percent": None,
            "memory_total_bytes": None,
            "memory_available_bytes": None,
            "uptime_seconds": int(time.monotonic()),
        }
        try:
            load = os.getloadavg()
            metrics.update({
                "load_1m": round(load[0], 2),
                "load_5m": round(load[1], 2),
                "load_15m": round(load[2], 2),
            })
        except OSError:
            metrics.update({"load_1m": None, "load_5m": None,
                            "load_15m": None})

        sample = self._read_cpu_ticks()
        if sample is not None and self._cpu_ticks is not None:
            metrics["cpu_percent"] = self._cpu_percent(
                self._cpu_ticks, sample
            )
        self._cpu_ticks = sample

        if sys.platform == "darwin":
            metrics.update(self._mac_memory_metrics())
            # memory_pressure's free percentage is a pressure measure, not
            # MemAvailable bytes.  Leave the latter unset on macOS.
            if metrics["memory_total_bytes"] is None:
                try:
                    metrics["memory_total_bytes"] = (
                        os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
                    )
                except (OSError, ValueError):
                    pass
            return metrics
        try:
            with open("/proc/meminfo", encoding="ascii") as fh:
                memory = {
                    key.rstrip(":"): int(value) * 1024
                    for key, value, *_ in (line.split() for line in fh)
                }
            metrics["memory_total_bytes"] = memory["MemTotal"]
            metrics["memory_available_bytes"] = memory["MemAvailable"]
        except (OSError, ValueError, KeyError):
            try:
                page_size = os.sysconf("SC_PAGE_SIZE")
                total = page_size * os.sysconf("SC_PHYS_PAGES")
                available = page_size * os.sysconf("SC_AVPHYS_PAGES")
                if total >= 0 and available >= 0:
                    metrics["memory_total_bytes"] = total
                    metrics["memory_available_bytes"] = available
            except (OSError, ValueError):
                pass
        return metrics

    def report_payload(self) -> Dict[str, Any]:
        """The POST body for /v1/workers/{id}/status."""
        snap = self.snapshot()
        snap.pop("worker_id", None)
        return {"schema_version": 1, "status_report": snap}

    def restore(self) -> None:
        """Load the last persisted snapshot (boot recovery display only).

        The crashed boot's attempt is DELIBERATELY dropped (review C9): a
        fresh boot holds no execution right — showing a stale 'alive'
        process would mislead offline debugging.  The attempt lives on in
        the server's view until the scanner expires it."""
        state = (self._journal.get_monitor_state() if self._monitor_attempt_id is None
                 else self._journal.get_monitor_state(self._monitor_attempt_id))
        if not state:
            return
        node = state.get("node")
        if isinstance(node, dict):
            restored = dict(self._node)
            restored.update(node)
            # a new boot starts claimable unless the crash left it unhealthy
            if restored.get("status") in ("busy",):
                restored["status"] = "idle"
            self._node = restored
        anomalies = state.get("anomalies")
        if isinstance(anomalies, list):
            for item in anomalies[-MONITOR_MAX_ANOMALIES:]:
                if isinstance(item, dict) and item.get("kind"):
                    self._anomalies.append(item)

    # -- persistence ------------------------------------------------------------

    def _persist_locked(self, force: bool = True) -> None:
        """Persist the snapshot; called with the lock held.  Progress-only
        churn is throttled to one journal write per _persist_min_interval
        so per-step engine events do not write-amplify the hot path."""
        if not force:
            now = self._time.monotonic()
            if now - self._last_persist < self._persist_min_interval:
                return
            self._last_persist = now
        # the journal runs its own short transaction
        snapshot = {
            "worker_id": self._worker_id,
            "boot_id": self._boot_id,
            "node": dict(self._node),
            "attempt": copy.deepcopy(self._attempt),
            "anomalies": list(self._anomalies),
        }
        if self._monitor_attempt_id is None:
            self._journal.set_monitor_state(snapshot)
        else:
            self._journal.set_monitor_state(snapshot, self._monitor_attempt_id)


def status_text(state: Dict[str, Any], journal) -> str:
    """Render the local read-only status report (h3worker.status)."""
    lines: List[str] = []
    node = state.get("node") or {}
    lines.append("节点")
    lines.append(f"  worker_id     : {state.get('worker_id')}")
    lines.append(f"  boot_id       : {state.get('boot_id')}")
    lines.append(f"  状态          : {node.get('status', 'unknown')}")
    lines.append(
        f"  磁盘可用      : "
        f"{node.get('disk_free_bytes', 0) / (1 << 30):.1f} GiB"
    )
    if node.get("cpu_count") is not None:
        cpu = node.get("cpu_percent")
        cpu_text = f"{cpu:.1f}%" if cpu is not None else "未知"
        lines.append(
            f"  CPU           : {cpu_text} / {node['cpu_count']} 核"
        )
    if node.get("memory_total_bytes"):
        total = node["memory_total_bytes"]
        available = node.get("memory_available_bytes")
        if available is not None:
            lines.append(
                f"  内存          : {(total - available) / (1 << 30):.1f} / "
                f"{total / (1 << 30):.1f} GiB"
            )
    if node.get("unhealthy_reason"):
        lines.append(f"  不健康原因    : {node['unhealthy_reason']}")

    attempt = state.get("attempt")
    lines.append("")
    if not attempt:
        lines.append("当前任务: 无（空闲）")
    else:
        lines.append("当前任务")
        lines.append(f"  task_id       : {attempt.get('task_id')}")
        lines.append(f"  attempt_id    : {attempt.get('attempt_id')}")
        lines.append(f"  阶段          : {attempt.get('stage')}")
        if attempt.get("detail_phase"):
            lines.append(f"  引擎阶段      : {attempt['detail_phase']}")
        lines.append(f"  开始时间      : {attempt.get('started_at')}")
        if attempt.get("last_progress_at"):
            lines.append(
                f"  最后进度      : {attempt['last_progress_at']}"
            )
        progress = attempt.get("progress")
        if progress:
            suffix = (
                "（已提交，非计算完成）"
                if progress.get("semantics") == "submitted" else ""
            )
            lines.append(
                f"  进度          : {progress.get('completed')}/"
                f"{progress.get('total')} {progress.get('unit')}{suffix}"
            )
        if attempt.get("cancel_requested"):
            lines.append("  取消请求      : 已收到")
        if attempt.get("outcome"):
            lines.append(f"  结果          : {attempt['outcome']}")
        process = attempt.get("process")
        if process:
            alive = "存活" if process.get("alive") else "已退出"
            lines.append(
                f"  进程          : PID {process.get('pid')} {alive}"
                + (
                    f"（退出码 {process['exit_code']}）"
                    if process.get("exit_code") is not None else ""
                )
            )
        for item in attempt.get("artifacts") or []:
            lines.append(
                f"  产物 {item.get('role'):<8}: {item.get('state')}"
                f"  {item.get('uploaded_bytes', 0)}/"
                f"{item.get('size_bytes', 0)} bytes"
            )

    anomalies = state.get("anomalies") or []
    lines.append("")
    lines.append(f"近期异常（最近 {len(anomalies)} 条）")
    if not anomalies:
        lines.append("  无")
    for item in anomalies[-10:]:
        lines.append(
            f"  {item.get('at')} {item.get('kind')}: {item.get('detail', '')}"
        )

    if journal is not None:
        pending = journal.pending_event_count()
        active = journal.active_attempt()
        lines.append("")
        lines.append(f"待上报事件: {pending}")
        if active is not None:
            lines.append(f"未终结 attempt: {active['attempt_id']}")
    return "\n".join(lines)
