"""Shared protocol constants for the h3 task service (Client / Server contract v1).

This module is the machine-readable source of truth for the field-level
contract described in service/protocol.md (including its section 10
"v1 implementation supplements").  Both the server (h3server) and the
worker client (h3worker) import from here so the two ends cannot drift.

Any change to values in this file is a protocol change and must be
reflected in service/protocol.md.
"""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from typing import Any, Dict, List, Optional

PROTOCOL_SCHEMA_VERSION = 1

# ---------------------------------------------------------------------------
# States
# ---------------------------------------------------------------------------

TASK_STATES = ("QUEUED", "RUNNING", "RETRY_WAIT", "SUCCEEDED", "FAILED", "CANCELLED")
TASK_TERMINAL_STATES = ("SUCCEEDED", "FAILED", "CANCELLED")

ATTEMPT_STATES = ("RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "EXPIRED")
ATTEMPT_TERMINAL_STATES = ("SUCCEEDED", "FAILED", "CANCELLED", "EXPIRED")

# Execution phases (protocol section 2)
PHASES = (
    "downloading",  # resolving + downloading + verifying reference assets
    "loading",      # model loading / init
    "encoding",     # text/vision/audio encoding
    "denoise",      # diffusion sampling
    "decode",       # VAE decode
    "mux",          # ffmpeg container writing
    "validating",   # local output validation + manifest build
    "uploading",    # artifact upload
    "finalizing",   # finish submission
)

# Event types (protocol section 5)
EVENT_TYPES = ("phase", "progress", "diagnostic", "artifact_ready")

# Progress counter semantics
SEMANTICS = ("submitted", "completed")

# Units seen from the engine (used for validation only; open set for forward
# compatibility, but we validate against this list in v1).
UNITS = ("steps", "blocks", "layers", "stages", "tiles", "frames", "bytes", "items")

# Artifact roles (protocol section 10.8)
ARTIFACT_ROLES = ("video", "audio", "alignment", "manifest", "cover", "log")
REQUIRED_ROLES = ("video", "manifest")
ROLE_FILENAME = {
    "video": "result.mp4",
    "audio": "result.wav",
    "alignment": "alignment.json",
    "manifest": "manifest.json",
}

# Modes
MODES = ("t2va", "fl2va", "ref2va", "a2va", "tts", "align", "comfyui_video", "ltx2_video")

# Reference asset kinds
REFERENCE_KINDS = ("image", "video", "audio", "video_audio")

# Finish statuses a worker may submit
FINISH_STATUSES = ("SUCCEEDED", "FAILED", "CANCELLED")

# Error codes (protocol section 8)
ERROR_CODES = (
    "INVALID_ARGUMENT",
    "INPUT_INVALID",
    "INPUT_UNAVAILABLE",
    "MODEL_UNAVAILABLE",
    "RESOURCE_EXHAUSTED",
    "ENGINE_FAILED",
    "ENGINE_TIMEOUT",
    "ARTIFACT_INVALID",
    "UPLOAD_FAILED",
    "IDEMPOTENCY_CONFLICT",
    "LEASE_LOST",
    "ATTEMPT_NOT_CURRENT",
    "CANCEL_REQUESTED",
    "TASK_TERMINAL",
    "EVENT_CONFLICT",
    "ARTIFACT_MISMATCH",
    "UNAUTHORIZED",
    "FORBIDDEN",
    "NOT_FOUND",
    "RATE_LIMITED",
    "INTERNAL",
    # server-synthesized terminal/synthetic codes (visible in attempt.error
    # and task.last_error; protocol section 10)
    "LEASE_EXPIRED",
    "RELEASED",
    "TASK_DEADLINE",
    "EXECUTION_DEADLINE",
)

# Execution error codes that are candidates for business retry (server policy).
# LEASE_EXPIRED is the server's own scanner error for expired attempts and is
# always retryable within the attempt budget.
# EXECUTION_DEADLINE is not retryable: the unchanged execution budget would
# interrupt the next attempt again. Resubmit explicitly with a sufficient budget.
RETRYABLE_ERROR_CODES = {
    "INPUT_UNAVAILABLE",
    "ENGINE_FAILED",
    "ENGINE_TIMEOUT",
    "UPLOAD_FAILED",
    "RESOURCE_EXHAUSTED",
    "LEASE_EXPIRED",
}

# Structured causes for POST /attempts/{id}/release (protocol 10.5): a known
# code triggers the server's node-eligibility negative feedback; the free
# text reason never does.
RELEASE_CODES = ("MODEL_UNAVAILABLE", "RESOURCE_EXHAUSTED")

# ---------------------------------------------------------------------------
# Monitoring (protocol 10.14): node/task/process status the worker reports
# ---------------------------------------------------------------------------

# Fine-grained node status for the monitoring view.  The coarse scheduling
# status on workers.status stays within (idle, busy, draining, unhealthy);
# these add the reasons a human cares about (model missing, disk watermark).
# "offline" is never reported by a worker — the server derives it from
# heartbeat age.
MONITOR_NODE_STATUSES = (
    "idle", "busy", "draining", "unhealthy",
    "model_unavailable", "disk_low",
)

# Anomaly kinds for the bounded recent-events ring in a status report.
ANOMALY_KINDS = (
    "network_retry",       # transient request failure, retried
    "lease_renew_failed",  # heartbeat rejected or unreachable
    "engine_failed",       # inference process failure
    "upload_retry",        # artifact PUT retried
    "download_retry",      # input download retried
    "url_refresh",         # signed URL expired and was refreshed
    "cancel_observed",     # cancel marker seen locally
    "other",
)

# Artifact upload states in the monitoring snapshot (mirrors the journal).
MONITOR_ARTIFACT_STATES = ("local", "prepared", "uploaded", "verified")

MONITOR_MAX_ANOMALIES = 32          # per report payload
MONITOR_MAX_ARTIFACTS = 16
MONITOR_TEXT_LIMIT = 512            # free-text fields in a report


def validate_monitor_report(report) -> list:
    """Validate a monitoring status report (protocol 10.14).  Returns a list
    of human-readable problems; empty means valid.

    Everything the HTML page renders is typed here: a wrong type must be
    REJECTED at ingest, not blow up fmtBytes during rendering (review C4).
    """
    problems = []
    if not isinstance(report, dict):
        return ["report must be an object"]
    if report.get("schema_version") != 1:
        problems.append("schema_version must be 1")
    if not isinstance(report.get("boot_id"), str) or not report["boot_id"]:
        problems.append("boot_id is required")

    def _short_str(value):
        return (
            value is None
            or (isinstance(value, str) and len(value) <= MONITOR_TEXT_LIMIT)
        )

    def _nonneg_int(value):
        return (
            value is None
            or (isinstance(value, int) and not isinstance(value, bool)
                and value >= 0)
        )

    def _nonneg_number(value):
        return (
            value is None
            or (isinstance(value, (int, float))
                and not isinstance(value, bool) and value >= 0)
        )

    node = report.get("node")
    if not isinstance(node, dict):
        problems.append("node is required")
    else:
        if node.get("status") not in MONITOR_NODE_STATUSES:
            problems.append(
                f"node.status must be one of {MONITOR_NODE_STATUSES}"
            )
        if not _nonneg_int(node.get("disk_free_bytes")):
            problems.append("node.disk_free_bytes must be a non-negative "
                            "int or null")
        for key in ("cpu_count", "memory_total_bytes",
                    "memory_available_bytes", "memory_compressed_bytes",
                    "swap_used_bytes", "uptime_seconds"):
            if not _nonneg_int(node.get(key)):
                problems.append(f"node.{key} must be a non-negative int "
                                "or null")
        cpu_percent = node.get("cpu_percent")
        if not _nonneg_number(cpu_percent) or (
            cpu_percent is not None and cpu_percent > 100
        ):
            problems.append("node.cpu_percent must be between 0 and 100 "
                            "or null")
        gpu_percent = node.get("gpu_percent")
        if not _nonneg_number(gpu_percent) or (
            gpu_percent is not None and gpu_percent > 100
        ):
            problems.append("node.gpu_percent must be between 0 and 100 "
                            "or null")
        pressure_free = node.get("memory_pressure_free_percent")
        if not _nonneg_int(pressure_free) or (
            pressure_free is not None and pressure_free > 100
        ):
            problems.append("node.memory_pressure_free_percent must be "
                            "between 0 and 100 or null")
        for key in ("load_1m", "load_5m", "load_15m"):
            if not _nonneg_number(node.get(key)):
                problems.append(f"node.{key} must be a non-negative number "
                                "or null")
        for key in ("hostname", "device_model", "chip_name", "gpu_name"):
            if not _short_str(node.get(key)):
                problems.append(f"node.{key} must be a short string or null")
        if not _nonneg_int(node.get("gpu_core_count")):
            problems.append("node.gpu_core_count must be a non-negative int "
                            "or null")
        for key in ("unhealthy_reason", "note"):
            if not _short_str(node.get(key)):
                problems.append(f"node.{key} must be a short string or null")

    attempt = report.get("attempt")
    if attempt is not None:
        if not isinstance(attempt, dict):
            problems.append("attempt must be an object")
        else:
            if attempt.get("stage") not in PHASES:
                problems.append(f"attempt.stage must be one of {PHASES}")
            if not _short_str(attempt.get("detail_phase")):
                problems.append("attempt.detail_phase must be short or null")
            if not _nonneg_int(attempt.get("phase_instance")):
                problems.append("attempt.phase_instance must be a "
                                "non-negative int or null")
            for key in ("attempt_id", "task_id"):
                if not _short_str(attempt.get(key)):
                    problems.append(f"attempt.{key} must be a short string "
                                    "or null")
            for key in ("started_at", "last_progress_at", "ended_at"):
                if not _short_str(attempt.get(key)):
                    problems.append(f"attempt.{key} must be short or null")
            if attempt.get("cancel_requested") not in (None, True, False):
                problems.append("attempt.cancel_requested must be boolean")
            if not _short_str(attempt.get("outcome")):
                problems.append("attempt.outcome must be short or null")
            progress = attempt.get("progress")
            if progress is not None:
                if not isinstance(progress, dict):
                    problems.append("attempt.progress must be an object")
                else:
                    if progress.get("semantics") not in (None, *SEMANTICS):
                        problems.append(
                            "attempt.progress.semantics must be one of "
                            f"{SEMANTICS}"
                        )
                    for key in ("completed", "total"):
                        if not _nonneg_int(progress.get(key)):
                            problems.append(
                                f"attempt.progress.{key} must be a "
                                "non-negative int or null"
                            )
                    if not _short_str(progress.get("unit")):
                        problems.append("attempt.progress.unit must be "
                                        "short or null")
            process = attempt.get("process")
            if process is not None:
                if not isinstance(process, dict):
                    problems.append("attempt.process must be an object")
                else:
                    if not _nonneg_int(process.get("pid")):
                        problems.append("attempt.process.pid must be a "
                                        "non-negative int or null")
                    for key in ("program", "started_at", "ended_at"):
                        if not _short_str(process.get(key)):
                            problems.append(f"attempt.process.{key} must "
                                            "be short or null")
                    if not _nonneg_number(process.get("cpu_percent")):
                        problems.append("attempt.process.cpu_percent must be "
                                        "a non-negative number or null")
                    if not _nonneg_int(process.get("rss_bytes")):
                        problems.append("attempt.process.rss_bytes must be a "
                                        "non-negative int or null")
                    if process.get("alive") not in (None, True, False):
                        problems.append("attempt.process.alive must be "
                                        "boolean")
                    exit_code = process.get("exit_code")
                    if exit_code is not None and (
                        not isinstance(exit_code, int)
                        or isinstance(exit_code, bool)
                    ):
                        # negative exit codes are LEGAL: POSIX returncode is
                        # -signal when the engine is killed (SIGTERM=-15,
                        # SIGKILL=-9) — exactly the cancel/lease-loss path
                        # monitoring exists to show (fix-review B-1)
                        problems.append("attempt.process.exit_code must be "
                                        "an int or null (negative = signal)")
            artifacts = attempt.get("artifacts")
            if artifacts is not None:
                if not isinstance(artifacts, list) or len(artifacts) > \
                        MONITOR_MAX_ARTIFACTS:
                    problems.append(
                        "attempt.artifacts must be a short list"
                    )
                else:
                    for item in artifacts:
                        if not isinstance(item, dict) or \
                                item.get("state") not in \
                                MONITOR_ARTIFACT_STATES:
                            problems.append(
                                "attempt.artifacts[].state must be one of "
                                f"{MONITOR_ARTIFACT_STATES}"
                            )
                            break
                        if not _short_str(item.get("role")) or \
                                not _short_str(item.get("sha256")):
                            problems.append(
                                "attempt.artifacts[].role/sha256 must be "
                                "short strings"
                            )
                            break
                        for key in ("size_bytes", "uploaded_bytes"):
                            if not _nonneg_int(item.get(key)):
                                problems.append(
                                    f"attempt.artifacts[].{key} must be a "
                                    "non-negative int or null"
                                )
                                break

    anomalies = report.get("anomalies")
    if anomalies is not None:
        if not isinstance(anomalies, list) or len(anomalies) > \
                MONITOR_MAX_ANOMALIES:
            problems.append("anomalies must be a short list")
        else:
            for item in anomalies:
                if not isinstance(item, dict) or \
                        item.get("kind") not in ANOMALY_KINDS:
                    problems.append(
                        f"anomalies[].kind must be one of {ANOMALY_KINDS}"
                    )
                    break
                if not _short_str(item.get("at")) or \
                        not _short_str(item.get("detail")):
                    problems.append(
                        "anomalies[].at/detail must be short strings or "
                        "null"
                    )
                    break
    plural = report.get('attempts')
    if plural is not None:
        if not isinstance(plural, list) or len(plural) > 2:
            problems.append('attempts must contain at most two reports')
        else:
            ids, slots = set(), set()
            for item in plural:
                if not isinstance(item, dict) or 'attempts' in item:
                    problems.append('invalid plural report')
                    continue
                attempt = item.get('attempt')
                if not isinstance(attempt, dict):
                    problems.append('plural attempt must be an object')
                    continue
                slot = item.get('occupancy')
                aid = attempt.get('attempt_id')
                if not isinstance(slot, str) or not isinstance(aid, str):
                    problems.append('plural occupancy and identity must be strings')
                    continue
                if item.get('boot_id') != report.get('boot_id') or item.get('worker_id') != report.get('worker_id'):
                    problems.append('plural report must belong to the current Worker boot')
                if slot not in ('gpu', 'tail') or slot in slots or not aid or aid in ids:
                    problems.append('duplicate or ambiguous plural occupancy')
                slots.add(slot); ids.add(aid)
                process = attempt.get('process')
                if slot == 'tail' and isinstance(process, dict) and process.get('alive'):
                    problems.append('CPU tail cannot contain a live native process')
                problems.extend(validate_monitor_report(item))
    return problems

# ---------------------------------------------------------------------------
# Lease / scheduling defaults (service/README.md suggested initial config)
# ---------------------------------------------------------------------------

DEFAULT_LEASE_SECONDS = 90
DEFAULT_HEARTBEAT_SECONDS = 10
DEFAULT_LEASE_SAFETY_MARGIN_SECONDS = 15
DEFAULT_CLAIM_WAIT_SECONDS = 25
DEFAULT_PROGRESS_MERGE_SECONDS = 1.5
DEFAULT_EVENT_BATCH_MAX = 64
VERIFICATION_MARGIN_SECONDS = 120  # protocol 10.11

# ---------------------------------------------------------------------------
# Engine limits (verified against h3.c / h3.h by the 2026-09-09 review, C1)
# ---------------------------------------------------------------------------

ENGINE_LIMITS = {
    "fps": 24,
    "frames_min": 22,       # h3.c:861-865 after alignment
    "frames_max": 362,      # h3.c:514-517 after alignment
    "width_min": 64,
    "height_min": 64,
    "width_max": 1344,      # landscape/portrait share the same pixel budget
    "height_max": 1344,
    "dim_multiple": 32,
    "pixels_max": 768 * 1344,
    "steps_min": 2,
    "steps_max": 1000,
    "dit_layers_min": 35,
    "dit_layers_max": 50,
    "references_max": 12,
    "reference_images_max": 9,
    "reference_videos_max": 3,
    "reference_audios_max": 3,
    "reference_audio_seconds_max": 15.0,
    "video_audio_min_frames": 56,  # refs with audio require >= ~56 output frames
}


def align_frames(requested: int) -> int:
    """Align a frame request upward to the engine temporal shape 5 + 17*n.

    Mirrors h3_host.c h3_align_frame_count: max(request, 5) then round up to
    the next value expressible as 5 + 17*n.
    """
    if requested < 5:
        return 22  # smallest legal temporal shape (5+17*1=22 satisfies >=22)
    n = math.ceil((requested - 5) / 17)
    return 5 + 17 * n


def frames_from_seconds(seconds: float) -> int:
    """Convert a seconds request to frames the way the CLI does: 24 fps,
    llround, then upward alignment (main.c:87 + engine alignment)."""
    return align_frames(int(math.floor(float(seconds) * ENGINE_LIMITS["fps"] + 0.5)))


H3_OPTIMIZATION_REVISION = "b6692ae0e52343ef80ba72f53b9e666d28ee446e"
H3_OPTIMIZATION_CAPABILITIES = {
    "use_int8_row_fc2": "h3.m5.int8-row-fc2.v1",
    "token_reduction": "h3.token-reduction.v1",
    "render_width": "h3.internal-render.v1",
}


def required_h3_optimizations(generation: Dict[str, Any]) -> List[str]:
    return [cap for key, cap in H3_OPTIMIZATION_CAPABILITIES.items()
            if generation.get(key)]


def effective_h3_generation(generation: Dict[str, Any]) -> Dict[str, Any]:
    """Mirror argv defaults/conversions, including legacy normalized requests."""
    effective = {key: int(generation[key]) for key in ("width", "height", "frames")}
    effective.update(steps=int(generation.get("steps", 20)),
                     dit_layers=int(generation.get("dit_layers", 50)))
    for key in ("denoise_reuse", "core_reuse"):
        effective[key] = int(generation.get(key, 1) or 1)
    for key in ("ssd_streaming", "use_int8_row_fc2", "token_reduction"):
        effective[key] = bool(generation.get(key, False))
    for axis in ("width", "height"):
        effective["render_" + axis] = generation.get("render_" + axis) or effective[axis]
    return effective


def validate_generation(generation: Dict[str, Any]) -> List[str]:
    """Validate a normalized generation parameter block.

    Returns a list of human-readable problems (empty when valid).  Mirrors
    the engine's own checks so the server can reject early and the worker
    can re-check locally (client README section 5).
    """
    problems: List[str] = []
    lim = ENGINE_LIMITS
    try:
        width = int(generation["width"])
        height = int(generation["height"])
    except (KeyError, TypeError, ValueError):
        return ["generation.width/height are required integers"]
    for key in ("use_int8_row_fc2", "token_reduction"):
        if key in generation and not isinstance(generation[key], bool):
            problems.append(f"{key} must be a boolean")
    rw, rh = generation.get("render_width"), generation.get("render_height")
    if "render_width" in generation or "render_height" in generation:
        if any(type(value) is not int for value in (rw, rh)):
            problems.append("render_width/render_height must be supplied together as integers")
        elif (rw < 32 or rh < 32 or rw % 32 or rh % 32
              or rw > width or rh > height or rw * height != rh * width):
            problems.append("internal render dimensions must be same-aspect multiples of 32, at least 32, and no larger than output")
    frames = generation.get("frames")
    steps = generation.get("steps", 20)
    layers = generation.get("dit_layers", 50)
    if width % lim["dim_multiple"] != 0 or height % lim["dim_multiple"] != 0:
        problems.append(f"width/height must be multiples of {lim['dim_multiple']}")
    if not (lim["width_min"] <= width <= lim["width_max"]):
        problems.append(f"width out of range [{lim['width_min']}, {lim['width_max']}]")
    if not (lim["height_min"] <= height <= lim["height_max"]):
        problems.append(f"height out of range [{lim['height_min']}, {lim['height_max']}]")
    if width * height > lim["pixels_max"]:
        problems.append("resolution exceeds maximum pixel budget")
    if frames is not None:
        frames = int(frames)
        if not (lim["frames_min"] <= frames <= lim["frames_max"]):
            problems.append(f"frames out of range [{lim['frames_min']}, {lim['frames_max']}]")
        if frames != align_frames(frames):
            problems.append("frames not aligned to 5 + 17*n")
    if steps is not None:
        steps = int(steps)
        if not (lim["steps_min"] <= steps <= lim["steps_max"]):
            problems.append(f"steps out of range [{lim['steps_min']}, {lim['steps_max']}]")
    if layers is not None:
        layers = int(layers)
        if not (lim["dit_layers_min"] <= layers <= lim["dit_layers_max"]):
            problems.append(
                f"dit_layers out of range [{lim['dit_layers_min']}, {lim['dit_layers_max']}]"
            )
    core_reuse = int(generation.get("core_reuse", 1) or 1)
    denoise_reuse = int(generation.get("denoise_reuse", 1) or 1)
    if core_reuse > 1 and denoise_reuse > 1:
        problems.append("core_reuse>1 and denoise_reuse>1 are mutually exclusive")
    if generation.get("ssd_streaming") and generation.get("use_int8_row_fc2"):
        problems.append("ssd_streaming and use_int8_row_fc2 are mutually exclusive")
    return problems


VIDEO_PROMPT_MAX_CHARS = 32768


def validate_task_request(task: Dict[str, Any]) -> List[str]:
    """Validate a business task creation request (mode, references, anchors,
    generation).  Returns a list of problems (empty when valid)."""
    problems: List[str] = []
    mode = task.get("mode")
    if mode not in MODES:
        problems.append(f"mode must be one of {MODES}")
        return problems
    if mode == "ltx2_video":
        from shared.ltx_policy import validate
        return validate(task)
    if mode == "comfyui_video":
        from shared.comfy_policy import validate
        return validate(task)
    if mode in ("t2va", "fl2va", "ref2va"):
        prompt = task.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > VIDEO_PROMPT_MAX_CHARS:
            problems.append(f"video prompt must be a non-empty string of at most {VIDEO_PROMPT_MAX_CHARS} chars")
    references = task.get("references") or []
    anchors = task.get("anchors") or {}
    if mode == "tts":
        if anchors:
            problems.append("anchors are not allowed for tts")
        text = task.get("prompt")
        if not isinstance(text, str) or not text.strip() or len(text) > 2000:
            problems.append("tts text must be a non-empty string of at most 2000 chars")
        settings = task.get("tts") or {}
        if not isinstance(settings, dict):
            problems.append("tts settings must be an object")
            settings = {}
        allowed_tts_keys = {
            "operation", "speed", "language", "temperature",
            "ref_text", "instruct", "voice",
        }
        unknown_tts_keys = sorted(set(settings) - allowed_tts_keys)
        if unknown_tts_keys:
            problems.append(f"tts contains unsupported keys: {unknown_tts_keys}")
        operation = settings.get("operation", "clone")
        if operation not in ("clone", "voice_design", "custom_voice"):
            problems.append(
                "tts.operation must be clone, voice_design, or custom_voice"
            )
        capability = str(task.get("model_revision") or "")
        supported_capabilities = {
            "cosyvoice3-mlx", "tts.cosyvoice3.clone",
            "tts.qwen3.voice_design", "tts.qwen3.custom_voice",
            "tts.qwen3.clone",
        }
        if capability not in supported_capabilities:
            problems.append(f"unsupported model_revision for tts: {capability!r}")
        expected_operation = {
            "tts.qwen3.voice_design": "voice_design",
            "tts.qwen3.custom_voice": "custom_voice",
            "tts.qwen3.clone": "clone",
            "tts.cosyvoice3.clone": "clone",
            "cosyvoice3-mlx": "clone",
        }.get(capability)
        if expected_operation is not None and operation != expected_operation:
            problems.append(
                f"tts.operation {operation!r} does not match model_revision {capability!r}"
            )
        if operation == "clone":
            if len(references) != 1 or references[0].get("kind") != "audio":
                problems.append("tts clone requires exactly one audio reference")
            ref_text = settings.get("ref_text")
            if (not isinstance(ref_text, str) or not ref_text.strip()) and str(
                task.get("model_revision") or ""
            ).startswith("tts.qwen3"):
                problems.append("tts.ref_text is required for Qwen3 clone")
        elif operation == "voice_design":
            if references:
                problems.append("tts voice_design does not accept references")
            instruct = settings.get("instruct")
            if not isinstance(instruct, str) or not instruct.strip():
                problems.append("tts.instruct is required for voice_design")
        elif operation == "custom_voice":
            if references:
                problems.append("tts custom_voice does not accept references")
            voice = settings.get("voice")
            if not isinstance(voice, str) or not voice.strip():
                problems.append("tts.voice is required for custom_voice")
        if references and references[0].get("content_type") is not None:
            content_type = references[0].get("content_type")
            if not isinstance(content_type, str) or not content_type.startswith("audio/"):
                problems.append("tts reference must have an audio content_type")
        for key in ("ref_text", "instruct", "voice", "language"):
            value = settings.get(key)
            if value is not None and (
                not isinstance(value, str) or len(value) > 1000
            ):
                problems.append(f"tts.{key} must be a string of at most 1000 chars")
        speed = settings.get("speed", 1.0)
        if not isinstance(speed, (int, float)) or isinstance(speed, bool) \
                or not (0.5 <= float(speed) <= 2.0):
            problems.append("tts.speed must be between 0.5 and 2.0")
        temperature = settings.get("temperature", 0.7)
        if not isinstance(temperature, (int, float)) \
                or isinstance(temperature, bool) \
                or not (0.0 <= float(temperature) <= 2.0):
            problems.append("tts.temperature must be between 0.0 and 2.0")
        if references:
            raw_duration = references[0].get("duration_seconds", 0.0)
            try:
                duration = float(raw_duration)
            except (TypeError, ValueError):
                problems.append("tts reference duration_seconds must be numeric")
            else:
                if isinstance(raw_duration, bool) or not math.isfinite(duration) \
                        or duration < 0:
                    problems.append("tts reference duration_seconds must be finite and nonnegative")
                elif duration > 30.0:
                    problems.append("tts reference audio must be at most 30 seconds")
        return problems
    if mode == "align":
        if anchors:
            problems.append("anchors are not allowed for align")
        if len(references) != 1 or references[0].get("kind") != "audio":
            problems.append("align requires exactly one audio reference")
        if task.get("model_revision") != "audio.qwen3.forced_align":
            problems.append(
                f"unsupported model_revision for align: {task.get('model_revision')!r}"
            )
        settings = task.get("align") or {}
        if not isinstance(settings, dict):
            problems.append("align settings must be an object")
            settings = {}
        unknown_align_keys = sorted(set(settings) - {"language"})
        if unknown_align_keys:
            problems.append(f"align contains unsupported keys: {unknown_align_keys}")
        language = settings.get("language", "Chinese")
        if (
            not isinstance(language, str) or not language.strip()
            or len(language) > 64 or not language.isprintable()
        ):
            problems.append("align.language must be a printable string of at most 64 chars")
        if references and references[0].get("content_type") is not None:
            content_type = references[0].get("content_type")
            if not isinstance(content_type, str) or not content_type.startswith("audio/"):
                problems.append("align reference must have an audio content_type")
        text = task.get("prompt")
        if not isinstance(text, str) or not text.strip() or len(text) > 10000:
            problems.append("align text must be a non-empty string of at most 10000 chars")
        if references:
            raw_duration = references[0].get("duration_seconds", 0.0)
            try:
                duration = float(raw_duration)
            except (TypeError, ValueError):
                problems.append("align reference duration_seconds must be numeric")
            else:
                if isinstance(raw_duration, bool) or not math.isfinite(duration) \
                        or duration < 0:
                    problems.append("align reference duration_seconds must be finite and nonnegative")
                elif duration > 3600.0:
                    problems.append("align audio must be at most 3600 seconds")
        return problems
    if mode == "a2va":
        prompt = task.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            problems.append("a2va requires a non-empty prompt")
        if not isinstance(references, list) or len(references) != 1 or not isinstance(references[0], dict):
            problems.append("a2va requires exactly one audio reference")
        else:
            ref = references[0]
            if ref.get("kind") != "audio" or not str(ref.get("content_type") or "").startswith("audio/"):
                problems.append("a2va reference requires kind audio and an audio content_type")
            duration = ref.get("duration_seconds")
            if isinstance(duration, bool) or not isinstance(duration, (int, float)) or not (0 < duration <= 30):
                problems.append("a2va audio duration_seconds must be in (0, 30]")
            if not ref.get("asset_id"):
                problems.append("a2va reference requires an asset_id")
        if not isinstance(anchors, dict) or set(anchors) - {"first"}:
            problems.append("a2va permits only anchors.first")
        elif "first" in anchors:
            first = anchors["first"]
            if not isinstance(first, dict) or not first.get("asset_id"):
                problems.append("a2va anchors.first requires an asset_id")
            elif first.get("content_type") is not None and not str(first["content_type"]).startswith("image/"):
                problems.append("a2va anchors.first must be an image")
        # Target audio is not an ordered Ref2VA reference: its minimum frame
        # count and reference duration budget do not apply.
        generation = dict(task.get("generation") or {})
        seconds = task.get("seconds")
        if seconds is not None:
            if generation.get("frames") is not None:
                problems.append("seconds and frames are mutually exclusive")
            else:
                try:
                    generation["frames"] = frames_from_seconds(float(seconds))
                except (ValueError, TypeError, OverflowError):
                    problems.append("seconds must be finite and numeric")
        problems.extend(validate_generation(generation))
        return problems
    if mode == "ref2va":
        if anchors:
            problems.append("anchors are only allowed for fl2va")
        if not references:
            problems.append("ref2va requires at least one reference")
    elif mode == "fl2va":
        if references:
            problems.append("references are only allowed for ref2va")
        if not anchors.get("first"):
            problems.append("fl2va requires anchors.first")
    else:  # t2va
        if references:
            problems.append("references are only allowed for ref2va")
        if anchors:
            problems.append("anchors are only allowed for fl2va")
    lim = ENGINE_LIMITS
    if len(references) > lim["references_max"]:
        problems.append(f"too many references (max {lim['references_max']})")
    counts = {"image": 0, "video": 0, "audio": 0}
    audio_seconds = 0.0
    for ref in references:
        kind = ref.get("kind")
        if kind not in REFERENCE_KINDS:
            problems.append(f"reference kind must be one of {REFERENCE_KINDS}")
            continue
        if kind == "video_audio":
            counts["video"] += 1
            counts["audio"] += 1
        else:
            counts[kind] += 1
        audio_seconds += float(ref.get("duration_seconds") or 0.0)
    if counts["image"] > lim["reference_images_max"]:
        problems.append(f"too many image references (max {lim['reference_images_max']})")
    if counts["video"] > lim["reference_videos_max"]:
        problems.append(f"too many video references (max {lim['reference_videos_max']})")
    if counts["audio"] > lim["reference_audios_max"]:
        problems.append(f"too many audio references (max {lim['reference_audios_max']})")
    if audio_seconds > lim["reference_audio_seconds_max"]:
        problems.append(
            f"reference audio total exceeds {lim['reference_audio_seconds_max']}s"
        )
    if (counts["audio"] > 0 or counts["video"] > 0) and references:
        gen_frames = (task.get("generation") or {}).get("frames")
        if gen_frames is not None and int(gen_frames) < lim["video_audio_min_frames"]:
            if any(
                r.get("kind") in ("video", "video_audio", "audio") for r in references
            ):
                problems.append(
                    "references with audio require frames >= "
                    f"{lim['video_audio_min_frames']}"
                )
    seconds = task.get("seconds")
    frames = (task.get("generation") or {}).get("frames")
    if seconds is not None and frames is not None:
        problems.append("seconds and frames are mutually exclusive")
    generation = dict(task.get("generation") or {})
    if seconds is not None and frames is None:
        generation["frames"] = frames_from_seconds(float(seconds))
    problems.extend(validate_generation(generation))
    return problems


def validate_alignment_payload(
    payload: Any, expected_text: Optional[str] = None,
    requested_text: Optional[str] = None,
) -> List[str]:
    """Validate persisted Qwen3 word/character timestamp JSON."""
    problems: List[str] = []
    if not isinstance(payload, dict):
        return ["alignment must be a JSON object"]
    text = payload.get("text")
    if not isinstance(text, str) or not text.strip() or len(text) > 10000:
        problems.append("alignment.text must be non-empty and at most 10000 chars")
    if expected_text is not None and text != expected_text:
        problems.append("alignment.text differs from engine result")
    def text_key(value: str) -> str:
        return "".join(
            char for char in value.casefold()
            if not char.isspace() and not unicodedata.category(char).startswith("P")
        )
    if requested_text is not None and isinstance(text, str) \
            and text_key(text) != text_key(requested_text):
        problems.append("alignment.text differs from requested transcript")
    segments = payload.get("segments")
    if not isinstance(segments, list) or not segments or len(segments) > 10000:
        problems.append("alignment.segments must contain 1 to 10000 items")
        return problems
    previous_end = 0.0
    reconstructed: List[str] = []
    for index, segment in enumerate(segments):
        prefix = f"alignment.segments[{index}]"
        if not isinstance(segment, dict):
            problems.append(f"{prefix} must be an object")
            continue
        item_text = segment.get("text")
        if not isinstance(item_text, str) or not item_text or len(item_text) > 1000:
            problems.append(f"{prefix}.text must be non-empty and at most 1000 chars")
        else:
            reconstructed.append(item_text)
        start, end = segment.get("start"), segment.get("end")
        if (
            not isinstance(start, (int, float)) or isinstance(start, bool)
            or not isinstance(end, (int, float)) or isinstance(end, bool)
            or not math.isfinite(float(start)) or not math.isfinite(float(end))
        ):
            problems.append(f"{prefix} timestamps must be finite numbers")
            continue
        start_f, end_f = float(start), float(end)
        if start_f < previous_end or end_f < start_f:
            problems.append(f"{prefix} timestamps must be nonnegative and monotonic")
        previous_end = max(previous_end, end_f)
        duration = segment.get("duration")
        if duration is not None and (
            not isinstance(duration, (int, float)) or isinstance(duration, bool)
            or not math.isfinite(float(duration)) or float(duration) < 0
        ):
            problems.append(f"{prefix}.duration must be a finite nonnegative number")
        elif duration is not None and abs(float(duration) - (end_f - start_f)) > 0.002:
            problems.append(f"{prefix}.duration differs from end-start")
    if isinstance(text, str) and text_key("".join(reconstructed)) != text_key(text):
        problems.append("alignment.text differs from ordered segment text")
    return problems


# ---------------------------------------------------------------------------
# Engine phase -> protocol phase mapping (review item C6)
# ---------------------------------------------------------------------------

# detail_phase strings emitted by the current engine (h3.c / h3_dit.c /
# h3_ffmpeg.c call sites; verified 2026-09-09).  The worker maps these to
# protocol phases; unknown strings fall back per FALLBACK_PHASE.
PHASE_MAPPING = {
    "tokenizer": "loading",
    "text encoder": "encoding",
    "refine text": "encoding",
    "Qwen vision": "encoding",
    "video VAE encoder": "encoding",
    "audio VAE encoder": "encoding",
    "load transformer core": "loading",
    "video VAE load": "loading",
    "preview VAE load": "loading",
    "audio VAE": "loading",
    "denoise enqueue": "denoise",   # GPU (M5 default) sampling path
    "denoise": "denoise",           # CPU sampling path (completed semantics)
    "submit GPU Euler denoise": "denoise",
    "FFmpeg": "mux",
    "decode": "decode",
    "tts model load": "loading",
    "tts generate": "decode",
}

# Progress semantics per engine detail phase (D3): the GPU path reports
# submissions, the CPU path reports completions.
PHASE_SEMANTICS = {
    "denoise enqueue": "submitted",
    "submit GPU Euler denoise": "submitted",
    "denoise": "completed",
}


def map_detail_phase(detail_phase: Optional[str]) -> Optional[str]:
    if not detail_phase:
        return None
    return PHASE_MAPPING.get(detail_phase)


# ---------------------------------------------------------------------------
# Canonical JSON + digests (idempotency content hashing)
# ---------------------------------------------------------------------------

def canonical_json(value: Any) -> str:
    """Stable JSON serialization used for request digests: sorted keys,
    no insignificant whitespace, ensure_ascii=False."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest_obj(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def digest_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def validate_seed_string(seed: Any) -> bool:
    """seed must be a decimal string within uint64 range (protocol section 1)."""
    if not isinstance(seed, str):
        return False
    try:
        value = int(seed, 10)
    except ValueError:
        return False
    return 0 <= value <= 0xFFFFFFFFFFFFFFFF
