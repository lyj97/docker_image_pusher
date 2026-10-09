"""Inference subprocess management: h3 CLI adapter and the controllable fake
runner used for protocol fault testing (client README sections 6 and 10).

Process group: h3 and every FFmpeg it spawns share our process group
(h3_ffmpeg.c uses posix_spawnp without SETPGROUP), so killing the group
covers FFmpeg too.  Cooperative cancel inside denoise is NOT available with
previews off — the process-group kill is the primary stop mechanism, not a
fallback (design review D5).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import signal
import subprocess
import sys
import time
import wave
from array import array
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# When executed as a standalone fake-runner process, make the shared protocol
# package importable: sys.path needs the service/ root (the parent of the
# shared/ package directory).  __file__ may be relative to the launcher's
# cwd, so resolve it first.
_HERE = os.path.dirname(os.path.abspath(__file__))
_SERVICE_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if os.path.isdir(os.path.join(_SERVICE_ROOT, "shared")) and \
        _SERVICE_ROOT not in sys.path:
    sys.path.insert(0, _SERVICE_ROOT)

from shared.h3proto import PHASE_MAPPING, PHASE_SEMANTICS


@dataclass
class EngineEvent:
    """One machine event emitted by the runner on stdout (one JSON/line)."""
    kind: str                 # phase | progress | result | done | error
    detail_phase: Optional[str] = None
    completed: Optional[int] = None
    total: Optional[int] = None
    unit: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    message: str = ""


def parse_engine_line(line: str) -> Optional[EngineEvent]:
    """Parse one stdout line from the runner.  Tolerates non-JSON noise by
    returning None (client README 6: illegal/long lines handled explicitly)."""
    line = line.strip()
    if not line or not line.startswith("{"):
        return None
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict) or "type" not in obj:
        return None
    kind = obj.get("type")
    if kind not in ("phase", "progress", "result", "done", "error"):
        return None
    return EngineEvent(
        kind=kind,
        detail_phase=obj.get("phase") or obj.get("detail_phase"),
        completed=obj.get("completed"),
        total=obj.get("total"),
        unit=obj.get("unit"),
        result=obj.get("result"),
        message=obj.get("message", ""),
    )


def read_engine_failure_detail(stderr_path: str, max_bytes: int = 8192) -> str:
    """Return the last non-empty stderr diagnostic without loading the log."""
    try:
        with open(stderr_path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - max_bytes))
            data = fh.read(max_bytes)
    except OSError:
        return ""
    lines = data.decode("utf-8", "replace").splitlines()
    return next((line.strip() for line in reversed(lines) if line.strip()), "")


def format_engine_failure(
    event_message: str, return_code: int, stderr_path: str,
) -> str:
    """Build the actionable failure text sent in the remote finish payload."""
    message = (event_message or "").strip()
    detail = read_engine_failure_detail(stderr_path)
    if not message:
        prefix = f"engine exited with {return_code}"
        return f"{prefix}: {detail}" if detail else prefix
    if "see stderr" in message.lower():
        return detail or message
    if detail and detail not in message:
        return f"{message}: {detail}"
    return message


def engine_supports_given_audio(config) -> bool:
    """Probe the installed executable, fail closed on old/broken engines.

    Do not cache across registrations: the binary can change after an update.
    Help is weight-free and works before model load; no network inference.
    """
    if config.fake_runner or not getattr(config, "h3_binary", None):
        return False
    try:
        probe = subprocess.run(
            [config.h3_binary, "--help"], cwd=config.h3_working_dir or None,
            env=subprocess_env(config), stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=10, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return probe.returncode == 0 and any(
        line.strip().startswith(b"--given-audio PATH")
        for line in probe.stdout.splitlines()
    )


def engine_optimization_capabilities(config) -> List[str]:
    """Weight-free probe; unknown revisions and unverified hosts fail closed.

    Uses the same executable-help pattern as the given-audio capability.
    Package installations without an audited checkout remain disabled.
    """
    from shared.h3proto import H3_OPTIMIZATION_REVISION, H3_OPTIMIZATION_CAPABILITIES
    if (getattr(config, "fake_runner", False)
            or platform.system() != "Darwin"
            or not getattr(config, "h3_binary", None)):
        return []

    def probe(argv):
        result = subprocess.run(argv, cwd=getattr(config, "h3_working_dir", None) or None,
                                env=subprocess_env(config), stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=10, check=False)
        if result.returncode:
            raise ValueError("capability probe failed")
        return result.stdout.decode("utf-8", errors="replace").strip()

    try:
        if probe(["git", "rev-parse", "HEAD"]) != H3_OPTIMIZATION_REVISION:
            return []
        if probe(["git", "status", "--porcelain", "--untracked-files=no"]):
            return []
        help_text = probe([config.h3_binary, "--help"])
        caps = []
        for key, flag in (("token_reduction", "--token-reduction"),
                          ("render_width", "--render-width")):
            if any(line.strip().startswith(flag + " ") for line in help_text.splitlines()):
                if key != "render_width" or any(
                    line.strip().startswith("--render-height ") for line in help_text.splitlines()
                ):
                    caps.append(H3_OPTIMIZATION_CAPABILITIES[key])
        chip = probe(["sysctl", "-n", "machdep.cpu.brand_string"])
        # Lightweight eligibility only; h3.c enforces Metal 4 at generation.
        if (re.fullmatch(r"Apple M5(?: Pro| Max| Ultra)?", chip)
                and int(platform.mac_ver()[0].split(".")[0]) >= 26
                and any(line.strip().startswith("--use-int8-row-fc2 ")
                        for line in help_text.splitlines())):
            caps.append(H3_OPTIMIZATION_CAPABILITIES["use_int8_row_fc2"])
        return caps
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return []


def _media_probe(path: str, config) -> Dict[str, Any]:
    probe = subprocess.run(
        [config.ffprobe_path or "ffprobe", "-v", "error", "-show_streams",
         "-show_format", "-of", "json", path],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=getattr(config, "media_process_timeout_seconds", 30), check=True,
    )
    return json.loads(probe.stdout)


def validate_a2va_input(path: str, config) -> None:
    """Check actual local audio; uploaded duration metadata is not a decoder."""
    import math
    info = _media_probe(path, config)
    audio = next((s for s in info.get("streams", []) if s.get("codec_type") == "audio"), None)
    if audio is None:
        raise ValueError("a2va input has no audio stream")
    duration = float(audio.get("duration") or info.get("format", {}).get("duration") or 0)
    if not math.isfinite(duration) or not 0 < duration <= 30:
        raise ValueError("a2va input audio duration must be in (0, 30] seconds")


def verify_a2va_mux(path: str, source: str, generation: Dict[str, Any], config) -> Dict[str, Any]:
    """Prove the MP4 contains the original soundtrack's deterministic AAC encode.

    Match compressed audio packets, including padding, against the same local
    FFmpeg encoding policy as the engine. No perceptual threshold can silently
    accept a VAE reconstruction or an unrelated/silent soundtrack. Fail closed
    when tools fail; both processes are bounded and run off the event loop.
    """
    from fractions import Fraction
    import math
    info = _media_probe(path, config)
    streams = info.get("streams", [])
    video = [s for s in streams if s.get("codec_type") == "video"]
    audio = [s for s in streams if s.get("codec_type") == "audio"]
    if len(video) != 1 or len(audio) != 1:
        raise ValueError("a2va MP4 requires exactly one video and one audio stream")
    for key in ("width", "height"):
        if int(video[0].get(key) or 0) != int(generation[key]):
            raise ValueError(f"a2va MP4 {key} differs from request")
    if int(video[0].get("nb_frames") or 0) != int(generation["frames"]) or Fraction(video[0].get("avg_frame_rate") or "0") != 24:
        raise ValueError("a2va MP4 frame count/rate differs from request")
    duration = int(generation["frames"]) / 24.0
    for stream in (video[0], audio[0]):
        actual = float(stream.get("duration") or 0)
        if not math.isfinite(actual) or abs(actual - duration) > 0.05:
            raise ValueError("a2va MP4 stream duration differs from request")
    binary = config.ffmpeg_path or "ffmpeg"
    common = [binary, "-v", "error", "-i"]
    expected = subprocess.run(
        common + [source, "-map", "0:a:0", "-af", "asetpts=PTS-STARTPTS,apad",
                  "-t", f"{duration:.9f}", "-c:a", "aac", "-b:a", "192k",
                  "-f", "adts", "pipe:1"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=getattr(config, "media_process_timeout_seconds", 30), check=True,
    ).stdout
    actual = subprocess.run(
        common + [path, "-map", "0:a:0", "-c:a", "copy", "-f", "adts", "pipe:1"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=getattr(config, "media_process_timeout_seconds", 30), check=True,
    ).stdout
    if not expected or actual != expected:
        raise ValueError("a2va MP4 soundtrack differs from original supplied audio")
    return {
        "policy": "original-aac-192k-pad-truncate-v1",
        "verified": True,
        "aac_sha256": hashlib.sha256(actual).hexdigest(),
        "duration_seconds": duration,
    }


def build_h3_command(
    request: Dict[str, Any],
    output_path: str,
    input_paths: Optional[Dict[str, str]] = None,
    h3_binary: str = "./h3",
    model_dir: Optional[str] = None,
    optimization_capabilities: Optional[List[str]] = None,
) -> List[str]:
    """Translate the normalized task request into an argv array (no shell,
    client README 5).  Only whitelisted flags are emitted; unknown options
    are never passed through.  Mirrors main.c's getopt table:
    -d/--model-dir is REQUIRED (main.c exits 2 without it) and -p/--prompt
    selects one-shot generation (without it h3 enters the interactive REPL).
    """
    generation = request.get("generation") or {}
    from shared.h3proto import validate_generation, required_h3_optimizations
    problems = validate_generation(generation)
    if problems:
        raise ValueError("; ".join(problems))
    missing = set(required_h3_optimizations(generation)) - set(optimization_capabilities or [])
    if missing:
        raise ValueError("unsupported h3 optimizations: " + ", ".join(sorted(missing)))
    resolved_model_dir = model_dir or request.get("model_dir")
    if not resolved_model_dir:
        raise ValueError(
            "model_dir is required for the real h3 binary (client review B1)"
        )
    prompt = str(request.get("prompt") or "")
    if not prompt:
        raise ValueError("task has no prompt; h3 would enter the REPL")
    argv = [
        h3_binary,
        "--machine-events",
        "--model-dir", resolved_model_dir,
        "--prompt", prompt,
        "--width", str(int(generation["width"])),
        "--height", str(int(generation["height"])),
        "--frames", str(int(generation["frames"])),
        "--steps", str(int(generation.get("steps", 20))),
        "--layers", str(int(generation.get("dit_layers", 50))),
        "--seed", str(request.get("seed", "42")),
        "--output", output_path,
    ]
    denoise_reuse = int(generation.get("denoise_reuse", 1) or 1)
    core_reuse = int(generation.get("core_reuse", 1) or 1)
    if denoise_reuse > 1:
        argv += ["--reuse", str(denoise_reuse)]
    if core_reuse > 1:
        argv += ["--core-reuse", str(core_reuse)]
    if generation.get("ssd_streaming"):
        argv += ["--ssd-streaming"]
    for key, flag in (("use_int8_row_fc2", "--use-int8-row-fc2"),
                      ("token_reduction", "--token-reduction")):
        if generation.get(key):
            argv.append(flag)
    if "render_width" in generation:
        argv += ["--render-width", str(generation["render_width"]),
                 "--render-height", str(generation["render_height"])]
    # mode-specific inputs
    mode = request.get("mode")
    input_paths = input_paths or {}
    if mode == "a2va":
        from shared.h3proto import validate_task_request
        problems = validate_task_request(request)
        if problems:
            raise ValueError("; ".join(problems))
        ref = request["references"][0]
        audio = input_paths.get(f"ref:{ref['asset_id']}")
        if not audio:
            raise ValueError("a2va soundtrack has not been resolved")
        argv += ["--given-audio", audio]
        if (request.get("anchors") or {}).get("first"):
            first = input_paths.get("anchors.first")
            if not first:
                raise ValueError("a2va first frame has not been resolved")
            argv += ["--first-frame", first]
    elif mode == "fl2va":
        anchors = request.get("anchors") or {}
        first = input_paths.get("anchors.first") or (anchors.get("first") or {}).get("local_path")
        last = input_paths.get("anchors.last") or (anchors.get("last") or {}).get("local_path")
        if first:
            argv += ["--first-frame", first]
        if last:
            argv += ["--last-frame", last]
    elif mode == "ref2va":
        # reference kind is derived from the asset content type; the option
        # names mirror main.c's OPT_REF_* table (client review B1:
        # --reference does not exist)
        for ref in request.get("references") or []:
            local = input_paths.get(f"ref:{ref.get('asset_id')}")
            if not local:
                continue
            content_type = str(ref.get("content_type") or "").lower()
            if content_type.startswith("image/"):
                argv += ["--ref-image", local]
            elif content_type.startswith("audio/"):
                argv += ["--ref-audio", local]
            elif content_type.startswith("video/"):
                include_audio = ref.get("include_embedded_audio")
                if not isinstance(include_audio, bool):
                    raise ValueError(
                        "video reference include_embedded_audio must be boolean"
                    )
                flag = ("--ref-video" if include_audio
                        else "--ref-silent-video")
                argv += [flag, local]
            else:
                raise ValueError(
                    f"unsupported reference content type {content_type!r}"
                )
    return argv


def tts_python_path() -> str:
    return os.path.join(_SERVICE_ROOT, "var", "tts-venv", "bin", "python")


def qwen_tts_python_path() -> str:
    return os.path.join(_SERVICE_ROOT, "var", "tts-qwen3-venv", "bin", "python")


CAPABILITY_MODEL_ATTR = {
    "cosyvoice3-mlx": "tts_model",
    "tts.cosyvoice3.clone": "tts_model",
    "tts.qwen3.voice_design": "qwen3_voice_design_model",
    "tts.qwen3.custom_voice": "qwen3_custom_voice_model",
    "tts.qwen3.clone": "qwen3_clone_model",
    "audio.qwen3.forced_align": "qwen3_aligner_model",
}


def configured_model(config, capability: str) -> str:
    attr = CAPABILITY_MODEL_ATTR.get(capability)
    return str(getattr(config, attr, "") or "") if attr else ""


def capability_python_path(capability: str) -> str:
    if capability in ("cosyvoice3-mlx", "tts.cosyvoice3.clone"):
        return tts_python_path()
    if capability.startswith("tts.qwen3.") or capability == "audio.qwen3.forced_align":
        return qwen_tts_python_path()
    raise ValueError(f"unknown local audio capability {capability!r}")


def capability_runtime_available(capability: str, model: str) -> bool:
    """Fail closed unless the runtime imports and its exact model is cached."""
    if capability in ("cosyvoice3-mlx", "tts.cosyvoice3.clone"):
        return bool(model) and tts_python_available()
    if not model or capability not in CAPABILITY_MODEL_ATTR:
        return False
    python = qwen_tts_python_path()
    if not (os.path.isfile(python) and os.access(python, os.X_OK)):
        return False
    kind = "align" if capability == "audio.qwen3.forced_align" else "tts"
    expected = {
        "tts.qwen3.voice_design": "voice_design",
        "tts.qwen3.custom_voice": "custom_voice",
        "tts.qwen3.clone": "base",
    }.get(capability, "")
    probe = (
        "import importlib.metadata as m,json,sys;"
        "assert sys.version_info[:2]==(3,12);"
        "v=tuple(int(x) for x in m.version('mlx-audio').split('.')[:3]);"
        "assert v>=(0,5,4);"
        + (
            "from mlx_audio.stt.models.qwen3_asr.qwen3_forced_aligner "
            "import ForcedAlignerConfig;"
            if kind == "align" else
            "from mlx_audio.tts.models.qwen3_tts.qwen3_tts import Model;"
        )
        + "from huggingface_hub import snapshot_download;"
        "p=snapshot_download(repo_id=sys.argv[1],local_files_only=True);"
        "c=json.load(open(p+'/config.json'));"
        "assert c.get('model_type')==sys.argv[2];"
        "assert (not sys.argv[3]) or c.get('tts_model_type')==sys.argv[3];"
        "assert (not sys.argv[4]) or c.get('thinker_config',{}).get('model_type')==sys.argv[4];"
        "print('QWEN3_READY')"
    )
    model_type = "qwen3_asr" if kind == "align" else "qwen3_tts"
    thinker_type = "qwen3_forced_aligner" if kind == "align" else ""
    try:
        result = subprocess.run(
            [python, "-c", probe, model, model_type, expected, thinker_type],
            capture_output=True, timeout=30, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and result.stdout.strip() == b"QWEN3_READY"


def tts_python_available() -> bool:
    python = tts_python_path()
    if not (os.path.isfile(python) and os.access(python, os.X_OK)):
        return False
    probe = (
        "import sys; "
        "assert sys.version_info[:2] == (3, 12); "
        "from mlx_audio.tts.generate import generate_audio; "
        "print('3.12\\nREADY')"
    )
    try:
        result = subprocess.run(
            [python, "-c", probe], capture_output=True, timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and result.stdout.strip() == b"3.12\nREADY"


def build_tts_command(
    request: Dict[str, Any], output_path: str,
    input_paths: Optional[Dict[str, str]] = None,
    model: str = "mlx-community/Fun-CosyVoice3-0.5B-2512-4bit",
    python_path: Optional[str] = None,
) -> List[str]:
    """Build fixed argv for the selected local TTS adapter."""
    references = request.get("references") or []
    settings = request.get("tts") or {}
    operation = str(settings.get("operation") or "clone")
    capability = str(request.get("model_revision") or "")
    backend = "qwen3" if capability.startswith("tts.qwen3.") else "cosyvoice3"
    ref_audio = None
    if references:
        if len(references) != 1:
            raise ValueError("tts clone requires exactly one audio reference")
        asset_id = references[0].get("asset_id")
        ref_audio = (input_paths or {}).get(f"ref:{asset_id}")
        if not ref_audio:
            raise ValueError("tts reference audio was not resolved")
    if operation == "clone" and not ref_audio:
        raise ValueError("tts clone requires exactly one audio reference")
    text = str(request.get("prompt") or "").strip()
    if not text:
        raise ValueError("tts text is required")
    argv = [
        python_path or tts_python_path(),
        "-m", "h3worker.tts_runner",
        "--backend", backend,
        "--operation", operation,
        "--model", model,
        "--text", text,
        "--output", output_path,
        "--seed", str(request.get("seed", "42")),
        "--speed", str(float(settings.get("speed", 1.0))),
        "--language", str(settings.get("language") or "zh"),
        "--temperature", str(float(settings.get("temperature", 0.7))),
    ]
    if ref_audio:
        argv += ["--ref-audio", ref_audio]
    if settings.get("ref_text"):
        argv += ["--ref-text", str(settings["ref_text"])]
    if settings.get("instruct"):
        argv += ["--instruct", str(settings["instruct"])]
    if settings.get("voice"):
        argv += ["--voice", str(settings["voice"])]
    return argv


def build_align_command(
    request: Dict[str, Any], output_path: str,
    input_paths: Optional[Dict[str, str]], model: str,
    python_path: Optional[str] = None,
) -> List[str]:
    """Build fixed argv for Qwen3 ForcedAligner."""
    references = request.get("references") or []
    if len(references) != 1:
        raise ValueError("align requires exactly one audio reference")
    asset_id = references[0].get("asset_id")
    audio = (input_paths or {}).get(f"ref:{asset_id}")
    if not audio:
        raise ValueError("align audio reference was not resolved")
    text = str(request.get("prompt") or "").strip()
    if not text:
        raise ValueError("align text is required")
    language = str((request.get("align") or {}).get("language") or "Chinese")
    return [
        python_path or qwen_tts_python_path(),
        "-m", "h3worker.align_runner",
        "--model", model,
        "--audio", audio,
        "--text", text,
        "--language", language,
        "--output", output_path,
    ]


def subprocess_env(config) -> Dict[str, str]:
    """Fixed, whitelisted environment for the h3 process group (client
    README 5: no arbitrary env injection; engine env vars are ours to set)."""
    env = {}
    for name in config.env_passthrough:
        value = os.environ.get(name)
        if value is not None:
            env[name] = value
    if config.ffmpeg_path:
        env["H3_FFMPEG"] = config.ffmpeg_path
    if config.ffprobe_path:
        env["H3_FFPROBE"] = config.ffprobe_path
    if config.model_dir:
        env.setdefault("H3_MODEL_DIR", config.model_dir)
    return env


def start_process(
    argv: List[str], cwd: str, env: Dict[str, str], stdout_path: str,
    stderr_path: str,
) -> subprocess.Popen:
    """Start the runner in its own process group (session-leader semantics
    via start_new_session so the group covers spawned FFmpeg)."""
    out = open(stdout_path, "ab")
    err = open(stderr_path, "ab")
    try:
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env=env,
            stdout=out,
            stderr=err,
            stdin=subprocess.DEVNULL,
            shell=False,
            start_new_session=True,
        )
    finally:
        out.close()
        err.close()
    return proc


def terminate_group(proc: subprocess.Popen, grace_seconds: float) -> None:
    """SIGTERM the group, wait the grace period, then SIGKILL."""
    import signal

    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        proc.wait(timeout=max(0.1, grace_seconds))
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def process_start_identity(pid: int) -> str:
    """Best-effort OS process start identity to defeat PID reuse (client
    README 7: verify identity via OS start identifier, never trust PID).

    ONLY lstart — deliberately NOT comm: on macOS a venv launcher
    re-execs itself and `comm` flips from the launcher path to the real
    interpreter ~30-50ms after spawn. Recording happens before the flip
    and crash-recovery validation after it, so a comm-based identity
    would misjudge every crashed engine as 'pid reused' and latch the
    worker unhealthy forever (fix-review B-2). lstart is stable for the
    process lifetime; pid reuse within the same second is the accepted
    residual window."""
    try:
        import subprocess as sp

        out = sp.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True, text=True, timeout=5,
        )
        return (out.stdout or "").strip()
    except Exception:
        return f"pid:{pid}"


# ---------------------------------------------------------------------------
# fake runner: a controllable stand-in for h3 used in fault testing
# ---------------------------------------------------------------------------

def fake_runner_main(argv: Optional[List[str]] = None) -> int:
    """Emits machine events on stdout like the planned h3 event mode."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--frames", type=int, default=22)
    parser.add_argument("--steps", type=int, default=20)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("--fail-after", type=float, default=0.0,
                        help="seconds before failing (0 = never)")
    parser.add_argument("--hang", action="store_true",
                        help="never finish (lease expiry test)")
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--ltx-fixture", action="store_true")
    parser.add_argument("--audio", action="store_true")
    parser.add_argument("--alignment", action="store_true")
    parser.add_argument("--text", default="")
    args, _unknown = parser.parse_known_args(argv)

    def emit(obj: Dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(obj) + "\n")
        sys.stdout.flush()

    emit({"type": "phase", "phase": "load transformer core",
          "total": 50, "unit": "layers"})
    for i in range(1, 6):
        emit({"type": "progress", "phase": "load transformer core",
              "completed": i * 10, "total": 50, "unit": "layers"})
        time.sleep(args.sleep)
    emit({"type": "phase", "phase": "text encoder", "total": 1, "unit": "stages"})
    emit({"type": "phase", "phase": "denoise enqueue", "total": args.steps,
          "unit": "steps"})
    start = time.time()
    for step in range(1, args.steps + 1):
        if args.fail_after and (time.time() - start) > args.fail_after:
            emit({"type": "error", "message": "simulated engine failure"})
            return 3
        emit({"type": "progress", "phase": "denoise enqueue", "completed": step,
              "total": args.steps, "unit": "steps"})
        time.sleep(args.sleep)
    emit({"type": "phase", "phase": "denoise", "completed": args.steps,
          "total": args.steps, "unit": "steps"})
    emit({"type": "phase", "phase": "FFmpeg", "total": 1, "unit": "stages"})
    emit({"type": "progress", "phase": "FFmpeg", "completed": 1, "total": 1,
          "unit": "stages"})
    if args.hang:
        while True:
            time.sleep(3600)
    if args.alignment:
        alignment = {
            "text": args.text,
            "segments": [{
                "text": args.text, "start": 0.0, "end": 1.0,
                "duration": 1.0,
            }],
        }
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(alignment, fh, ensure_ascii=False)
        result = {"text": args.text, "segments": 1, "tokens": 1}
    elif args.ltx_fixture:
        subprocess.run(['ffmpeg','-v','error','-y','-f','lavfi','-i','color=c=blue:s=704x480:r=24',
            '-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-frames:v',str(args.frames),
            '-t',str(args.frames/24),'-c:v','libx264','-pix_fmt','yuv420p',
            '-c:a','aac',args.output],check=True,timeout=60)
        result = dict(width=704,height=480,frames=args.frames,fps=24,duration_seconds=args.frames/24)
    elif args.audio:
        sample_rate = 24000
        samples = sample_rate
        with wave.open(args.output, "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(sample_rate)
            wav.writeframes(array("h", [0]) * samples)
        result = {
            "sample_rate": sample_rate, "channels": 1,
            "samples": samples, "duration_seconds": 1.0,
        }
    else:
        # write the fake output: a deterministic payload derived from args
        payload = (
            f"fake-h3-video frames={args.frames} steps={args.steps} "
            f"seed-line={args.output}"
        ).encode()
        with open(args.output, "wb") as fh:
            fh.write(payload)
        result = {
            "width": 512, "height": 512, "frames": args.frames,
            "fps": 24, "duration_seconds": args.frames / 24.0,
        }
    emit({"type": "result", "result": result})
    emit({"type": "done"})
    return 0


def map_engine_event(event: EngineEvent, phase_instance: int) -> Optional[Dict[str, Any]]:
    """Map an EngineEvent to a protocol event body (phase/progress), or None
    when it is not reportable."""
    if event.kind == "phase":
        phase = PHASE_MAPPING.get(event.detail_phase or "")
        if phase is None:
            return None
        return {
            "type": "phase",
            "phase": phase,
            "phase_instance": phase_instance,
            "detail_phase": event.detail_phase,
        }
    if event.kind == "progress":
        phase = PHASE_MAPPING.get(event.detail_phase or "")
        if phase is None:
            return None
        return {
            "type": "progress",
            "phase": phase,
            "phase_instance": phase_instance,
            "detail_phase": event.detail_phase,
            "completed": event.completed,
            "total": event.total,
            "unit": event.unit,
            "semantics": PHASE_SEMANTICS.get(event.detail_phase or "", "completed"),
        }
    return None


if __name__ == "__main__":
    if len(sys.argv) >= 5 and sys.argv[1] == '--bounded-exec':
        # Set limits in the newly spawned child, never preexec_fn in a
        # multithreaded Worker. exec preserves PID/session and cancellation.
        import resource
        bound = int(sys.argv[2])
        if not 0 < bound <= 512 * 1024 * 1024 or sys.argv[3] != '--':
            raise SystemExit('invalid native file bound')
        resource.setrlimit(resource.RLIMIT_FSIZE, (bound, bound))
        os.execvpe(sys.argv[4], sys.argv[4:], os.environ)
    sys.exit(fake_runner_main())
