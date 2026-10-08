#!/usr/bin/env python3
"""Worker-local VACE P0 smoke; stdlib only, existing ComfyUI and ffmpeg required.

No Worker/ComfyUI lifecycle control or media transport. Evidence stays local.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import errno
import fcntl
import os
import tempfile
import uuid
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.error
import urllib.request

# Keep the documented direct-script invocation working without PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.comfy_versions import SUPPORTED

SAVE_NODE = "15"
MAX_JSON = 1024 * 1024
POLL_SECONDS = 30
RUN_SECONDS = 7000
SEED = 654654950714624


class SmokeError(Exception):
    pass


class PromptRejected(SmokeError):
    """ComfyUI explicitly rejected the prompt before queueing it."""


def prompt_rejection(value):
    # Only the known validation envelope proves non-execution. Never expose
    # arbitrary response fields, extra_info (which contains input values), or HTML.
    if not isinstance(value, dict) or not isinstance(value.get('error'), dict):
        return None
    error = value['error']
    if error.get('type') not in {'prompt_outputs_failed_validation', 'prompt_no_outputs', 'invalid_prompt', 'missing_node_type'}:
        return None
    from h3worker.http import sanitize_error_text

    def diagnostic(item):
        return {key: sanitize_error_text(item[key]) for key in ('type', 'message', 'details')
                if isinstance(item.get(key), str)}

    summary = {'error': diagnostic(error)}
    nodes = value.get('node_errors')
    if isinstance(nodes, dict):
        summary['node_errors'] = {
            sanitize_error_text(node, max_bytes=64): {
                'errors': [diagnostic(e) for e in item.get('errors', [])[:8] if isinstance(e, dict)]}
            for node, item in list(nodes.items())[:8]
            if isinstance(item, dict) and isinstance(item.get('errors'), list)}
    return sanitize_error_text(json.dumps(summary, ensure_ascii=False), max_bytes=4096)


def emit(phase, **fields):
    line = json.dumps(dict(phase=phase, **fields), ensure_ascii=True)
    if len(line) > 2048:
        raise SmokeError("progress record too large")
    print(line, flush=True)


def write_json(path, value):
    raw = json.dumps(value, indent=2, ensure_ascii=True) + "\n"
    if len(raw.encode()) > MAX_JSON:
        raise SmokeError("evidence record too large")
    # File fsync precedes same-directory atomic replace. Directory fsync is
    # best effort only for explicitly unsupported filesystems (see below).
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                     prefix=".evidence-", delete=False) as sink:
        temporary = Path(sink.name)
        try:
            sink.write(raw)
            sink.flush()
            os.fsync(sink.fileno())
            os.replace(temporary, path)
            sync_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)


def sync_directory(path):
    """Sync directory metadata where supported; never suppress real I/O errors.

    Some OS/filesystem combinations reject directory fsync. In that case the
    file was still fsynced before atomic replacement, but power-loss durability
    of the rename cannot be promised. Regular-file fsync errors remain fatal.
    """
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            if exc.errno not in {errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP}:
                raise
            return False
        return True
    finally:
        os.close(fd)


@contextmanager
def run_lock(evidence):
    # Never unlink the lock: replacing its inode would defeat flock exclusion.
    with (evidence / ".run.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SmokeError("evidence run is locked; keep drain held") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def failure(exc):
    return dict(error=type(exc).__name__,
                detail=str(exc) if isinstance(exc, SmokeError) else "local operation failed",
                keep_drain=True)


def check_version(api, expected=None):
    version = api.json("/system_stats").get("system", {}).get("comfyui_version")
    if version not in SUPPORTED or (expected is not None and version != expected):
        raise SmokeError("Reviewed ComfyUI release required; version missing or incompatible")
    return version


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, exit_on_error=False)
    parser.add_argument("--comfy-root", type=Path,
                        default=Path("/Users/Shared/h3-comfyui-p0/ComfyUI"))
    parser.add_argument("--base-url", default="http://127.0.0.1:8188")
    parser.add_argument("--width", type=int, default=480)
    parser.add_argument("--height", type=int, default=480)
    parser.add_argument("--frames", type=int, default=17)
    parser.add_argument("--steps", type=int, default=4)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--output-prefix", default="h3_vace_p0/smoke")
    parser.add_argument("--evidence-dir", type=Path, required=True,
                        help="new local directory; never overwrites prior evidence")
    args = parser.parse_args(argv)
    if any(n < 16 or n > 1920 or n % 16 for n in (args.width, args.height)):
        raise SmokeError("dimensions must be multiples of 16 in 16..1920")
    if not 5 <= args.frames <= 241 or (args.frames - 1) % 4:
        raise SmokeError("frames must be 4n+1 in 5..241")
    if not 1 <= args.steps <= 100 or not 0 <= args.seed < 2**64:
        raise SmokeError("invalid steps or seed")
    if (not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", args.output_prefix)
            or len(args.output_prefix) > 120):
        raise SmokeError("invalid output prefix")
    for path in (args.comfy_root, args.evidence_dir):
        if len(str(path.resolve())) > 512:
            raise SmokeError("local path too long")
    return args


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise SmokeError("HTTP redirect refused")


class API:
    def __init__(self, base, timeout=20):
        url = urllib.parse.urlsplit(base)
        if (url.scheme != "http" or url.hostname not in {"127.0.0.1", "localhost", "::1"}
                or url.username is not None or url.password is not None
                or url.query or url.fragment or url.path not in {"", "/"}):
            raise SmokeError("ComfyUI URL must be credential-free loopback HTTP")
        self.timeout = timeout
        self.base = base.rstrip("/")
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def json(self, path, body=None):
        request = urllib.request.Request(self.base + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(MAX_JSON + 1)
        except urllib.error.HTTPError as exc:
            if path == '/prompt' and body is not None and 400 <= exc.code < 500 and exc.code != 408:
                try:
                    with exc:
                        raw = exc.read(MAX_JSON + 1)
                    message = prompt_rejection(json.loads(raw)) if len(raw) <= MAX_JSON else None
                except (OSError, ValueError, RecursionError):
                    message = None
                if message is not None:
                    raise PromptRejected('ComfyUI HTTP ' + str(exc.code) + ': ' + message) from None
            raise
        if len(raw) > MAX_JSON:
            raise SmokeError("HTTP JSON exceeds limit")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise SmokeError("expected HTTP JSON object")
        return value


def build_workflow(args):
    positive = (
        "The girl is dancing in a sea of flowers, slowly moving her hands. "
        "Close-up upper-body shot, dreamy cinematic glass flowers, clear subject, stable motion."
    )
    negative = (
        "overexposed, static, blurry details, subtitles, worst quality, low quality, "
        "jpeg artifacts, deformed hands, deformed face, extra limbs, frozen frame"
    )

    prompt = {
        "1": {"class_type": "UNETLoader", "inputs": {
            "unet_name": "wan2.1_vace_1.3B_fp16.safetensors", "weight_dtype": "default"}},
        "2": {"class_type": "ModelSamplingSD3", "inputs": {"model": ["1", 0], "shift": 8.0}},
        "3": {"class_type": "CLIPLoader", "inputs": {
            "clip_name": "umt5_xxl_fp16.safetensors", "type": "wan", "device": "default"}},
        "4": {"class_type": "CLIPTextEncode", "inputs": {"text": positive, "clip": ["3", 0]}},
        "5": {"class_type": "CLIPTextEncode", "inputs": {"text": negative, "clip": ["3", 0]}},
        "6": {"class_type": "VAELoader", "inputs": {"vae_name": "wan_2.1_vae.safetensors"}},
        "7": {"class_type": "LoadVideo", "inputs": {
            "file": "video_wan_vace_14B_v2v_reference_image_control_video.mp4"}},
        "8": {"class_type": "GetVideoComponents", "inputs": {"video": ["7", 0]}},
        "9": {"class_type": "LoadImage", "inputs": {
            "image": "video_wan_vace_14B_v2v_reference_image.jpg"}},
        "10": {"class_type": "WanVaceToVideo", "inputs": {
            "positive": ["4", 0], "negative": ["5", 0], "vae": ["6", 0],
            "width": args.width, "height": args.height, "length": args.frames, "batch_size": 1, "strength": 1.0,
            "control_video": ["8", 0], "reference_image": ["9", 0]}},
        "11": {"class_type": "KSampler", "inputs": {
            "model": ["2", 0], "seed": args.seed, "steps": args.steps, "cfg": 6.0,
            "sampler_name": "uni_pc", "scheduler": "simple",
            "positive": ["10", 0], "negative": ["10", 1],
            "latent_image": ["10", 2], "denoise": 1.0}},
        "12": {"class_type": "TrimVideoLatent", "inputs": {
            "samples": ["11", 0], "trim_amount": ["10", 3]}},
        "13": {"class_type": "VAEDecode", "inputs": {"samples": ["12", 0], "vae": ["6", 0]}},
        "14": {"class_type": "CreateVideo", "inputs": {"images": ["13", 0], "fps": 16.0}},
        "15": {"class_type": "SaveVideo", "inputs": {
            "video": ["14", 0], "filename_prefix": args.output_prefix, "format": "auto", "codec": "auto"}},
    }
    return prompt


def save_output(item, root, prefix):
    """Only the configured SaveVideo node may identify the output file."""
    node = item.get("outputs", {}).get(SAVE_NODE)
    entries = []

    def collect(value):
        if isinstance(value, dict):
            if "filename" in value:
                entries.append(value)
            for child in value.values():
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(node)
    if len(entries) != 1:
        raise SmokeError("expected exactly one configured SaveVideo output")
    entry = entries[0]
    filename, subfolder = entry.get("filename"), entry.get("subfolder", "")
    if (entry.get("type") != "output" or not isinstance(filename, str)
            or not isinstance(subfolder, str) or len(filename) > 255
            or Path(filename).name != filename or "\\" in filename):
        raise SmokeError("invalid SaveVideo output metadata")
    relative = Path(subfolder) / filename
    expected = Path(prefix)
    if (relative.is_absolute() or ".." in relative.parts
            or relative.parent != expected.parent
            or not re.fullmatch(re.escape(expected.name) + r"_[0-9]+_\.mp4", filename)):
        raise SmokeError("SaveVideo output does not match configured prefix")
    base = (root / "output").resolve()
    output = (base / relative).resolve()
    if not output.is_relative_to(base) or not output.is_file():
        raise SmokeError("SaveVideo output missing or outside output root")
    return output, {key: entry.get(key, "") for key in ("filename", "subfolder", "type")}


def follow(api, prompt_id):
    start = time.monotonic()
    polls = 0
    while time.monotonic() - start < RUN_SECONDS:
        history = api.json("/history/" + prompt_id)
        polls += 1
        item = history.get(prompt_id)
        if item is not None:
            status = item.get("status", {})
            if status.get("status_str") == "error":
                raise SmokeError("ComfyUI execution failed; inspect local ComfyUI logs")
            if status.get("completed") is True:
                if status.get("status_str") != "success":
                    raise SmokeError("ComfyUI completed without success")
                return item
        queue = api.json("/queue")
        # Queue entries include workflows; emit only counts and our membership.
        running = queue.get("queue_running", [])
        pending = queue.get("queue_pending", [])
        if not isinstance(running, list) or not isinstance(pending, list):
            raise SmokeError("invalid ComfyUI queue")
        ours = lambda rows: any(isinstance(row, list) and len(row) > 1
                                and row[1] == prompt_id for row in rows)
        if not ours(running) and not ours(pending):
            # Completion may race history/queue; the next poll resolves it.
            state = "awaiting_history"
        else:
            state = "running" if ours(running) else "pending"
        emit("waiting", elapsed_seconds=round(time.monotonic() - start),
             polls=polls, state=state, queue_running=len(running), queue_pending=len(pending))
        time.sleep(POLL_SECONDS)
    raise SmokeError("history deadline exceeded; prompt may still run; keep drain held")


def executable(name):
    found = shutil.which(name)
    if found:
        return found
    brew = Path("/opt/homebrew/bin") / name
    if brew.is_file():
        return str(brew)
    raise SmokeError("ffprobe and ffmpeg must already be installed")


def capture(argv):
    # Selected ffprobe fields / frame-limited framemd5 bound stdout. Suppress
    # arbitrary tool diagnostics, which can contain embedded media metadata.
    result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            timeout=30, check=True)
    if len(result.stdout) > MAX_JSON:
        raise SmokeError("validation output exceeds limit")
    return result.stdout.decode("utf-8")


def preview_command(ffmpeg, output, frame, destination):
    return [ffmpeg, "-nostdin", "-v", "error", "-n", "-i", str(output),
            "-map", "0:v:0", "-vf", f"select=eq(n\\,{frame})", "-frames:v", "1",
            "-update", "1", str(destination)]


def validate_video(output, args, ffprobe, ffmpeg):
    evidence = args.evidence_dir
    emit("validating", check="ffprobe")
    probe = json.loads(capture([ffprobe, "-v", "error", "-select_streams", "v:0",
        "-count_frames", "-show_entries",
        "stream=codec_name,width,height,r_frame_rate,nb_read_frames,duration",
        "-of", "json", str(output)]))
    write_json(evidence / "ffprobe.json", probe)
    streams = probe.get("streams", [])
    if len(streams) != 1:
        raise SmokeError("expected one probed video stream")
    stream = streams[0]
    if (stream.get("codec_name") != "h264" or stream.get("width") != args.width
            or stream.get("height") != args.height
            or Fraction(stream.get("r_frame_rate", "0")) != 16
            or int(stream.get("nb_read_frames", 0)) != args.frames):
        raise SmokeError("video codec, dimensions, frame rate or frame count mismatch")
    emit("validating", check="framemd5")
    raw = capture([ffmpeg, "-nostdin", "-v", "error", "-i", str(output),
                   "-map", "0:v:0", "-frames:v", str(args.frames + 1), "-f", "framemd5", "-"])
    (evidence / "framemd5.txt").write_text(raw, encoding="utf-8")
    hashes = []
    for line in raw.splitlines():
        if line and not line.startswith("#"):
            fields = [part.strip() for part in line.split(",")]
            if len(fields) != 6 or not re.fullmatch(r"[0-9a-f]{32}", fields[-1]):
                raise SmokeError("invalid framemd5 output")
            hashes.append(fields[-1])
    if len(hashes) != args.frames or len(set(hashes)) < 2:
        raise SmokeError("decoded frames missing, extra or all identical")
    previews = []
    for label, frame in (("first", 0), ("middle", args.frames // 2), ("last", args.frames - 1)):
        emit("validating", check="preview", frame=frame)
        destination = evidence / f"preview-{label}.jpg"
        capture(preview_command(ffmpeg, output, frame, destination))
        if not destination.is_file() or destination.stat().st_size == 0:
            raise SmokeError("preview missing or empty")
        preview = dict(path=str(destination.resolve()), sha256=sha256(destination))
        previews.append(preview)
        emit("preview", **preview)
    result = dict(output=str(output), bytes=output.stat().st_size, sha256=sha256(output),
                  codec="h264", width=args.width, height=args.height, fps=16,
                  decoded_frames=len(hashes), unique_frame_hashes=len(set(hashes)), previews=previews)
    write_json(evidence / "validation.json", result)
    return result


def run(args):
    api = API(args.base_url)
    ffprobe, ffmpeg = executable("ffprobe"), executable("ffmpeg")
    workflow = build_workflow(args)
    inputs = [args.comfy_root / "input" / workflow[node]["inputs"][field]
              for node, field in (("7", "file"), ("9", "image"))]
    if not all(path.is_file() for path in inputs):
        raise SmokeError("fixed P0 inputs are missing")
    args.evidence_dir.mkdir(parents=True, exist_ok=False)
    sync_directory(args.evidence_dir.parent)
    with run_lock(args.evidence_dir):
        try:
            check_version(api)
            submit_and_validate(args, api, workflow, inputs, ffprobe, ffmpeg)
        except (Exception, KeyboardInterrupt) as exc:
            write_json(args.evidence_dir / "failure.json", failure(exc))
            raise


def submit_and_validate(args, api, workflow, inputs, ffprobe, ffmpeg):
    write_json(args.evidence_dir / "api-workflow.json", workflow)
    write_json(args.evidence_dir / "inputs.json",
               [dict(path=str(path.resolve()), bytes=path.stat().st_size, sha256=sha256(path))
                for path in inputs])
    emit("prepared", workflow_sha256=sha256(args.evidence_dir / "api-workflow.json"),
         width=args.width, height=args.height, frames=args.frames, steps=args.steps, seed=args.seed)
    prompt_id, client_id = str(uuid.uuid4()), str(uuid.uuid4())
    write_json(args.evidence_dir / "intent.json", dict(
        schema=1, prompt_id=prompt_id, client_id=client_id,
        base_url=api.base, comfy_root=str(args.comfy_root.resolve()),
        width=args.width, height=args.height, frames=args.frames,
        steps=args.steps, seed=args.seed, output_prefix=args.output_prefix,
        workflow_sha256=sha256(args.evidence_dir / "api-workflow.json"),
        state="submission_intent"))
    # POST is deliberately never retried: a lost response may mean it ran.
    submitted = api.json("/prompt", {"prompt": workflow, "client_id": client_id,
                                     "prompt_id": prompt_id})
    matches = submitted.get("prompt_id") == prompt_id
    write_json(args.evidence_dir / "submission-response.json",
               dict(identity_matches=matches, node_errors=bool(submitted.get("node_errors"))))
    if not matches or submitted.get("node_errors"):
        raise SmokeError("submission identity mismatch or node errors; keep drain held")
    write_json(args.evidence_dir / "submission.json", dict(prompt_id=prompt_id))
    emit("submitted", prompt_id=prompt_id)
    item = follow(api, prompt_id)
    validate_completed(item, prompt_id, args, ffprobe, ffmpeg)


def validate_completed(item, prompt_id, args, ffprobe, ffmpeg):
    output, entry = save_output(item, args.comfy_root, args.output_prefix)
    # No raw history, prompts returned by the server, or other nodes' metadata.
    write_json(args.evidence_dir / "history-output.json",
               dict(prompt_id=prompt_id, save_node=SAVE_NODE, output=entry))
    result = validate_video(output, args, ffprobe, ffmpeg)
    emit("succeeded", prompt_id=prompt_id, evidence=str(args.evidence_dir.resolve()),
         sha256=result["sha256"], decoded_frames=result["decoded_frames"],
         unique_frame_hashes=result["unique_frame_hashes"])


def main(argv=None):
    args = None
    try:
        args = arguments(argv)
        run(args)
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        # Never echo arbitrary HTTP bodies, subprocess stderr or exception text.
        emit("failed", **failure(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
