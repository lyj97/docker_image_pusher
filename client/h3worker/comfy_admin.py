"""Bounded ComfyUI administration; runtime resource maintenance requires drain."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request
from contextlib import closing
from pathlib import Path

from .config import WorkerConfig
from .comfy_runner import (absent, maintenance_preflight, observe,
                           recovery_update_absence_ready)
import vace_smoke as smoke
from shared.comfy_workflow import MAX_BYTES, validate_bytes, workflow_path


NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
SHA256 = re.compile(r"[0-9a-f]{64}")
MODEL_KINDS = {
    "checkpoints", "clip", "clip_vision", "controlnet", "diffusion_models",
    "geometry_estimation", "loras", "model_patches", "text_encoders", "unet",
    "upscale_models", "vae",
}
MAX_ARCHIVE_BYTES = 512 * 1024 * 1024
MAX_EXTRACTED_BYTES = 2 * 1024 * 1024 * 1024
MAX_ARCHIVE_FILES = 20_000
COMFY_JOB = "com.relife.h3comfyui"
RESTART_TIMEOUT = 120
LAUNCHCTL = Path("/bin/launchctl")


def safe_name(value: str) -> str:
    if not NAME.fullmatch(value) or value.casefold() in {
        "comfyui-manager", "comfyui_manager",
    }:
        raise ValueError("invalid or forbidden resource name")
    return value


def digest(value: str) -> str:
    if not SHA256.fullmatch(value):
        raise ValueError("sha256 must contain 64 lowercase hex characters")
    return value


def https_url(value: str) -> str:
    url = urllib.parse.urlsplit(value)
    if (url.scheme != "https" or not url.hostname or url.username is not None
            or url.password is not None or url.fragment):
        raise ValueError("source URL must be credential-free HTTPS")
    return value


def root_for(config: WorkerConfig) -> Path:
    root = Path(config.comfyui_root)
    if (not root.is_absolute() or any(p.is_symlink() for p in (root, *root.parents))
            or any(not (root / name).is_dir() or (root / name).is_symlink()
                   for name in ("custom_nodes", "models"))):
        raise ValueError("safe configured ComfyUI root is required")
    return root.resolve()


def mutation_preflight(config: WorkerConfig) -> Path:
    root = root_for(config)
    maintenance_preflight(config, config.journal_path)
    return root


def download(url: str, expected: str, destination: Path,
             limit: int | None = None) -> int:
    url, expected = https_url(url), digest(expected)
    total = destination.stat().st_size if destination.exists() else 0
    checksum = hashlib.sha256()
    if total:
        with destination.open("rb") as partial:
            while chunk := partial.read(1024 * 1024):
                checksum.update(chunk)
        if checksum.hexdigest() == expected:
            return total
    last_report = 0.0
    headers = {"User-Agent": "h3-comfy-admin/1"}
    if total:
        headers["Range"] = f"bytes={total}-"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.geturl() != url:
            https_url(response.geturl())
        if total and (response.status != 206 or not response.headers.get(
                "Content-Range", "").startswith(f"bytes {total}-")):
            raise ValueError("download server did not honor resume range")
        declared = response.headers.get("Content-Length")
        if declared and limit is not None and total + int(declared) > limit:
            raise ValueError("download exceeds configured size limit")
        expected_total = total + int(declared) if declared else None
        with destination.open("ab" if total else "xb") as output:
            while chunk := response.read(1024 * 1024):
                total += len(chunk)
                if limit is not None and total > limit:
                    raise ValueError("download exceeds configured size limit")
                output.write(chunk)
                checksum.update(chunk)
                now = time.monotonic()
                if now - last_report >= 5:
                    print(json.dumps({"phase": "download", "completed_bytes": total,
                                      "total_bytes": expected_total, "pid": os.getpid(),
                                      "incomplete_files": 1, "timestamp": time.time()}),
                          flush=True)
                    last_report = now
            output.flush()
            os.fsync(output.fileno())
    if checksum.hexdigest() != expected:
        raise ValueError("download sha256 mismatch")
    return total


def extract_node(archive: Path, destination: Path) -> None:
    with tarfile.open(archive, "r:*") as bundle:
        members = bundle.getmembers()
        if not members or len(members) > MAX_ARCHIVE_FILES:
            raise ValueError("node archive file count is invalid")
        size = 0
        roots = set()
        for member in members:
            path = Path(member.name)
            if (path.is_absolute() or ".." in path.parts or not path.parts
                    or member.issym() or member.islnk() or member.isdev()):
                raise ValueError("unsafe node archive member")
            roots.add(path.parts[0])
            size += member.size
        if len(roots) != 1 or size > MAX_EXTRACTED_BYTES:
            raise ValueError("node archive layout or size is invalid")
        bundle.extractall(destination, filter="data")


def archive_existing(root: Path, path: Path) -> Path:
    trash = root / ".h3-trash"
    trash.mkdir(mode=0o700, exist_ok=True)
    if trash.is_symlink() or not trash.is_dir():
        raise ValueError("unsafe resource archive directory")
    target = trash / f"{path.parent.name}-{path.name}-{time.time_ns()}"
    if target.exists():
        raise FileExistsError("archive destination already exists")
    os.replace(path, target)
    return target


def node_install(config: WorkerConfig, args: argparse.Namespace) -> dict:
    root = mutation_preflight(config)
    name = safe_name(args.name)
    destination = root / "custom_nodes" / name
    if destination.exists() or destination.is_symlink():
        if not args.replace or destination.is_symlink() or not destination.is_dir():
            raise FileExistsError("node already exists; use --replace for a regular directory")
    with tempfile.TemporaryDirectory(prefix=".h3-node-", dir=root / "custom_nodes") as temporary:
        temporary = Path(temporary)
        archive = temporary / "node.tar"
        size = download(args.url, args.sha256, archive, MAX_ARCHIVE_BYTES)
        unpacked = temporary / "unpacked"
        unpacked.mkdir()
        extract_node(archive, unpacked)
        source = next(unpacked.iterdir())
        if not source.is_dir() or source.is_symlink():
            raise ValueError("node archive must contain one directory")
        requirements = source / "requirements.txt"
        if requirements.exists():
            if not args.requirements_sha256:
                raise ValueError("requirements.txt requires --requirements-sha256")
            if hashlib.sha256(requirements.read_bytes()).hexdigest() != digest(
                    args.requirements_sha256):
                raise ValueError("requirements.txt sha256 mismatch")
            python = Path(config.comfyui_python)
            if not python.is_absolute() or not python.is_file() or not os.access(python, os.X_OK):
                raise ValueError("comfyui_python executable is required for dependencies")
            subprocess.run([str(python), "-I", "-m", "pip", "install", "-r",
                            str(requirements)], check=True)
        elif args.requirements_sha256:
            raise ValueError("archive has no requirements.txt")
        archived = None
        if destination.exists():
            archived = archive_existing(root, destination)
        try:
            os.replace(source, destination)
        except BaseException:
            if archived is not None and not destination.exists():
                os.replace(archived, destination)
            raise
    return {"operation": "node_install", "name": name, "bytes": size,
            "restart_required": True,
            "archived": str(archived) if archived else None}


def remove(config: WorkerConfig, kind: str, name: str, model_kind: str | None = None) -> dict:
    root = mutation_preflight(config)
    name = safe_name(name)
    if kind == "node":
        target = root / "custom_nodes" / name
    else:
        if model_kind not in MODEL_KINDS:
            raise ValueError("unsupported model kind")
        target = root / "models" / model_kind / name
    if not target.exists() or target.is_symlink() or not (target.is_dir() if kind == "node" else target.is_file()):
        raise FileNotFoundError("resource is missing or unsafe")
    archived = archive_existing(root, target)
    return {"operation": kind + "_remove", "name": name,
            "archived": str(archived), "restart_required": kind == "node"}


def model_install(config: WorkerConfig, args: argparse.Namespace) -> dict:
    root = mutation_preflight(config)
    if args.kind not in MODEL_KINDS:
        raise ValueError("unsupported model kind")
    name = safe_name(args.name)
    folder = root / "models" / args.kind
    if folder.is_symlink():
        raise ValueError("symlinked model folder is forbidden")
    folder.mkdir(parents=True, exist_ok=True)
    destination = folder / name
    if destination.exists() or destination.is_symlink():
        raise FileExistsError("model already exists")
    temporary = folder / (".h3-download-" + name)
    try:
        size = download(args.url, args.sha256, temporary, args.max_bytes)
        os.replace(temporary, destination)
    except ValueError:
        if temporary.exists():
            temporary.unlink()
        raise
    return {"operation": "model_install", "kind": args.kind,
            "name": name, "bytes": size, "restart_required": False}


def status(config: WorkerConfig) -> dict:
    root = root_for(config)
    api = smoke.API(config.comfyui_url, timeout=3)
    stats = api.json("/system_stats")
    queue = api.json("/queue")
    classes = api.json("/object_info")
    nodes = sorted(p.name for p in (root / "custom_nodes").iterdir()
                   if p.is_dir() and not p.is_symlink())
    models = {}
    for kind in sorted(MODEL_KINDS):
        folder = root / "models" / kind
        names = (sorted(p.name for p in folder.iterdir()
                        if p.is_file() and not p.is_symlink())
                 if folder.is_dir() and not folder.is_symlink() else [])
        models[kind] = {"count": len(names), "names": names[:25]}
    return {"operation": "status", "system": stats.get("system", {}),
            "queue_running": len(queue.get("queue_running", [])),
            "queue_pending": len(queue.get("queue_pending", [])),
            "class_count": len(classes), "node_count": len(nodes),
            "nodes": nodes[:100], "models": models}


def free_memory(config: WorkerConfig) -> dict:
    mutation_preflight(config)
    api = smoke.API(config.comfyui_url, timeout=10)
    api.json("/free", {"unload_models": True, "free_memory": True})
    return {"operation": "free", "accepted": True}


def verify(config: WorkerConfig, args: argparse.Namespace) -> dict:
    root = root_for(config)
    classes = smoke.API(config.comfyui_url, timeout=10).json("/object_info")
    missing_classes = sorted(set(args.class_name) - set(classes))
    missing_models = []
    for value in args.model:
        kind, separator, name = value.partition("/")
        if not separator or kind not in MODEL_KINDS:
            raise ValueError("model must be kind/name using a supported kind")
        name = safe_name(name)
        path = root / "models" / kind / name
        if not path.is_file() or path.is_symlink():
            missing_models.append(value)
    if missing_classes or missing_models:
        raise RuntimeError(json.dumps({"missing_classes": missing_classes,
                                       "missing_models": missing_models}))
    return {"operation": "verify", "classes": sorted(set(args.class_name)),
            "models": sorted(set(args.model)), "verified": True}


def _launch_agent(config: WorkerConfig) -> tuple[str, Path]:
    if sys.platform != "darwin":
        raise RuntimeError("ComfyUI LaunchAgent restart requires macOS")
    home = Path.home()
    plist = home / "Library" / "LaunchAgents" / f"{COMFY_JOB}.plist"
    launcher = home / "Library" / "Application Support" / "H3ComfyUI" / "comfy_launch.py"
    for path in (plist, launcher):
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"safe {path.name} is required")
        info = path.stat()
        if info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise RuntimeError(f"unsafe {path.name} ownership or permissions")
    with plist.open("rb") as stream:
        payload = plistlib.load(stream)
    root = root_for(config)
    arguments = payload.get("ProgramArguments")
    if not isinstance(arguments, list) or len(arguments) < 8:
        raise RuntimeError("ComfyUI LaunchAgent arguments are invalid")
    runtime_python = str(arguments[0])
    python = Path(runtime_python)
    if (not python.is_absolute() or ".." in python.parts
            or not python.is_file() or not os.access(python, os.X_OK)
            or (config.comfyui_python and runtime_python != config.comfyui_python)
            or (not config.comfyui_python
                and not python.is_relative_to(root.parent))):
        raise RuntimeError("ComfyUI LaunchAgent Python is unsafe or mismatched")
    expected = [runtime_python, "-I", "-B", str(launcher),
                "--root", str(root), "--python", runtime_python]
    if (payload.get("Label") != COMFY_JOB
            or payload.get("WorkingDirectory") != str(root)
            or payload.get("RunAtLoad") is not True
            or payload.get("KeepAlive") is not True
            or arguments[:len(expected)] != expected):
        raise RuntimeError("ComfyUI LaunchAgent does not match configured runtime")
    launchctl = LAUNCHCTL
    if not launchctl.is_file() or not os.access(launchctl, os.X_OK):
        raise RuntimeError("/bin/launchctl is unavailable")
    return f"gui/{os.getuid()}/{COMFY_JOB}", launchctl


def _job_pid(launchctl: Path, job: str) -> int:
    output = subprocess.run(
        [str(launchctl), "print", job], check=True, capture_output=True, text=True, timeout=5,
    ).stdout
    match = re.search(r"(?m)^\s*pid = ([1-9][0-9]*)\s*$", output)
    if match is None:
        raise RuntimeError("ComfyUI LaunchAgent has no running pid")
    return int(match.group(1))


def preview_launch_agent(config):
    """Stricter automatic-restart contract for the pinned preview instance."""
    from .comfy_launch import absolute_path, arguments as launch_arguments
    job, launchctl = _launch_agent(config)
    home = Path.home()
    plist = home / 'Library/LaunchAgents' / f'{COMFY_JOB}.plist'
    launcher = home / 'Library/Application Support/H3ComfyUI/comfy_launch.py'
    for path in (plist, launcher):
        absolute_path(str(path))
        for parent in (path, *path.parents):
            if parent == home.parent:
                break
            if parent.stat().st_uid != os.getuid() or parent.stat().st_mode & 0o022:
                raise RuntimeError('unsafe LaunchAgent path ownership or permissions')
    if launcher.read_bytes() != Path(__file__).with_name('comfy_launch.py').read_bytes():
        raise RuntimeError('dedicated ComfyUI launcher differs from packaged launcher')
    root = root_for(config)
    python = absolute_path(config.comfyui_python, executable=True)
    if str(root) != config.comfyui_root or str(python) != config.comfyui_python:
        raise RuntimeError('canonical dedicated runtime paths required')
    # Venv Python can point at an operator-provisioned system interpreter.
    # Both the link's parents and resolved interpreter must be trusted.
    for target in (root, root / 'main.py', Path(config.comfyui_frontend_root), python.resolve()):
        absolute_path(str(target))
        info = target.stat()
        if info.st_uid not in (0, os.getuid()) or info.st_mode & 0o022:
            raise RuntimeError('unsafe dedicated runtime ownership or permissions')
    for parent in python.parents:
        if parent == root.parent.parent:
            break
        if parent.stat().st_uid not in (0, os.getuid()) or parent.stat().st_mode & 0o022:
            raise RuntimeError('unsafe dedicated Python ownership or permissions')
    payload = plistlib.loads(plist.read_bytes())
    command = [str(python), '-I', '-B', str(launcher), '--root', str(root),
               '--python', str(python), '--frontend', config.comfyui_frontend_root,
               '--port', '8188']
    if payload.get('ProgramArguments') == command + ['--no-keep-awake']:
        command.append('--no-keep-awake')
    launch_arguments(command[4:])
    logs = home / 'Library/Logs/H3ComfyUI'
    for path in (logs / 'comfyui.stdout.log', logs / 'comfyui.stderr.log'):
        absolute_path(str(path))
        for parent in path.parents:
            if parent == home.parent:
                break
            if parent.stat().st_uid != os.getuid() or parent.stat().st_mode & 0o022:
                raise RuntimeError('unsafe dedicated ComfyUI log path')
        if path.exists() and (not path.is_file() or path.stat().st_uid != os.getuid()
                              or path.stat().st_mode & 0o022):
            raise RuntimeError('unsafe dedicated ComfyUI log file')
    expected = dict(Label=COMFY_JOB, ProgramArguments=command,
        WorkingDirectory=str(root), RunAtLoad=True, KeepAlive=True,
        ThrottleInterval=60, ExitTimeOut=40, ProcessType='Interactive', Umask=0o077,
        EnvironmentVariables={'PATH': '/usr/bin:/bin:/usr/sbin:/sbin'},
        StandardOutPath=str(logs / 'comfyui.stdout.log'),
        StandardErrorPath=str(logs / 'comfyui.stderr.log'))
    if payload != expected:
        raise RuntimeError('dedicated preview plist does not match exact launcher contract')
    return job, launchctl, plist, command


def preview_job_pid(config, launchctl, job):
    """Prove the loaded job matches the reviewed on-disk launch contract."""
    expected_job, expected_launchctl, plist, command = preview_launch_agent(config)
    if (job, launchctl) != (expected_job, expected_launchctl):
        raise RuntimeError('unexpected ComfyUI launchd job')
    output = subprocess.run([str(launchctl), 'print', job], check=True,
        capture_output=True, text=True, timeout=5).stdout
    arguments = re.search(r'(?m)^\s*arguments = \{\s*\n(.*?)^\s*\}', output, re.S)
    loaded = [line.strip() for line in arguments.group(1).splitlines()] if arguments else []
    if (loaded != command or not re.search(r'(?m)^\s*path = ' + re.escape(str(plist)) + r'\s*$', output)
            or not re.search(r'(?m)^\s*program = ' + re.escape(command[0]) + r'\s*$', output)):
        raise RuntimeError('loaded ComfyUI LaunchAgent differs from pinned plist')
    match = re.search(r'(?m)^\s*pid = ([1-9][0-9]*)\s*$', output)
    if not match:
        raise RuntimeError('dedicated ComfyUI LaunchAgent is not running')
    return int(match.group(1))


def restart(config: WorkerConfig, args: argparse.Namespace) -> dict:
    mutation_preflight(config)
    return _restart_runtime(config, args)


def _restart_runtime(config: WorkerConfig, args: argparse.Namespace, *,
                     pid_reader=None, readiness=None, expected_old_pid=None) -> dict:
    job, launchctl = _launch_agent(config)
    pid_reader = _job_pid if pid_reader is None else pid_reader
    old_pid = pid_reader(launchctl, job)
    if expected_old_pid is not None and old_pid != expected_old_pid:
        raise RuntimeError('ComfyUI PID changed before restart; revalidation required')
    subprocess.run([str(launchctl), "kickstart", "-k", job], check=True, timeout=10)
    deadline = time.monotonic() + RESTART_TIMEOUT
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            new_pid = pid_reader(launchctl, job)
            if new_pid == old_pid:
                raise RuntimeError("ComfyUI process has not been replaced")
            api = smoke.API(config.comfyui_url, timeout=3)
            smoke.check_version(api, expected=config.comfyui_version)
            queue = api.json("/queue")
            classes = api.json("/object_info")
            if (not isinstance(queue, dict) or queue.get("queue_running") != []
                    or queue.get("queue_pending") != []
                    or not isinstance(classes, dict)):
                raise RuntimeError("restarted ComfyUI is not idle and ready")
            checked = verify(config, args)
            if readiness is not None:
                readiness()
            return {"operation": "restart", "old_pid": old_pid,
                    "new_pid": new_pid, "class_count": len(classes),
                    "classes": checked["classes"], "models": checked["models"],
                    "verified": True}
        except (OSError, RuntimeError, subprocess.SubprocessError,
                smoke.SmokeError) as exc:
            last_error = exc
            time.sleep(1)
    raise TimeoutError("ComfyUI restart did not become ready") from last_error


def recover(config: WorkerConfig) -> dict:
    """Explicit drained recovery; absence alone never resolves a prompt."""
    root_for(config)
    uri = Path(config.journal_path).absolute().as_uri() + '?mode=rw'
    with closing(sqlite3.connect(uri, uri=True, timeout=3)) as conn:
        if conn.execute(
                'SELECT count(*) FROM attempts WHERE confirmed_terminal IS NOT 1'
                ).fetchone()[0]:
            raise RuntimeError('unconfirmed local H3 attempt blocks recovery')
        rows = conn.execute(
            'SELECT attempt_id, engine_state FROM attempts '
            'WHERE engine_state IS NOT NULL ORDER BY attempt_id').fetchall()
        states = []
        prompts = set()
        for attempt_id, raw in rows:
            state = json.loads(raw)
            if not isinstance(state, dict):
                raise RuntimeError('malformed ComfyUI engine_state')
            if state.get('terminal') is True:
                continue
            prompt = state.get('prompt_id')
            if (not isinstance(prompt, str) or not NAME.fullmatch(prompt)
                    or prompt in prompts
                    or not isinstance(state.get('recovery'), dict)
                    or type(state['recovery'].get('absent_observations')) is not int):
                raise RuntimeError('malformed or conflicting ComfyUI recovery identity/evidence')
            prompts.add(prompt)
            states.append((attempt_id, state))
        if not states:
            raise RuntimeError('no unresolved ComfyUI prompts to recover')
        maintenance_preflight(config, config.journal_path, recovery_update=True)
        if conn.execute(
                'SELECT attempt_id, engine_state FROM attempts '
                'WHERE engine_state IS NOT NULL ORDER BY attempt_id').fetchall() != rows:
            raise RuntimeError('local state changed during recovery admission')
        admitted_at = time.time()
        # Restart readiness can outlast the pre-restart observation freshness.
        # Unchanged admitted evidence plus fresh post-replacement snapshots
        # proves recovery; changed evidence must still be fresh below.
        result = _restart_runtime(config, argparse.Namespace(class_name=[], model=[]))
        api = smoke.API(config.comfyui_url, timeout=3)
        for _, state in states:
            for _ in range(2):
                snapshot, _ = observe(api, state['prompt_id'])
                if not absent(snapshot):
                    raise RuntimeError('old ComfyUI prompt not absent after replacement')
        queue = api.json('/queue')
        if (not isinstance(queue, dict) or queue.get('queue_running') != []
                or queue.get('queue_pending') != []):
            raise RuntimeError('ComfyUI queue not empty after replacement')
        # Commit exactly the admitted rows, preserving identity and counters.
        with conn:
            conn.execute('BEGIN IMMEDIATE')
            current = conn.execute(
                'SELECT attempt_id, engine_state FROM attempts '
                'WHERE engine_state IS NOT NULL ORDER BY attempt_id').fetchall()
            if ([a for a, _ in current] != [a for a, _ in rows] or conn.execute(
                    'SELECT count(*) FROM attempts WHERE confirmed_terminal IS NOT 1'
                    ).fetchone()[0] or conn.execute(
                    'SELECT count(*) FROM processes').fetchone()[0]):
                raise RuntimeError('local state changed during ComfyUI recovery')
            latest = []
            identity = ('prompt_id', 'root', 'url', 'request_digest', 'graph_digest')
            now = time.time()
            for (attempt_id, raw), (_, original_raw) in zip(current, rows):
                state, original = json.loads(raw), json.loads(original_raw)
                if (not isinstance(state, dict) or state.get('identity_conflict')
                        or state.get('terminal') is not original.get('terminal')
                        or any((key in state, state.get(key)) !=
                               (key in original, original.get(key)) for key in identity)):
                    raise RuntimeError('ComfyUI recovery identity or terminal state changed')
                if state.get('terminal') is True:
                    continue
                if (state.get('terminal') is not False
                        or not isinstance(state.get('recovery'), dict)
                        or type(state['recovery'].get('absent_observations')) is not int
                        or not (recovery_update_absence_ready(state, now) or (
                            all(state['recovery'].get(key) == original['recovery'].get(key)
                                for key in ('absent_since', 'last_observed_at', 'absent_observations'))
                            and state.get('cancel_sent') is original.get('cancel_sent')
                            and recovery_update_absence_ready(original, admitted_at)))):
                    raise RuntimeError('ComfyUI recovery absence evidence no longer ready')
                latest.append((attempt_id, state))
            for attempt_id, state in latest:
                state.update(terminal=True, status='cancelled')
                state['recovery'].update(
                    resolved_at=time.time(), proof='runtime_replaced_after_stable_absence',
                    old_pid=result['old_pid'], new_pid=result['new_pid'])
                conn.execute('UPDATE attempts SET engine_state = ? WHERE attempt_id = ?',
                             (json.dumps(state), attempt_id))
    return dict(result, operation='recover', recovered_attempts=[a for a, _ in states])


def workflow_save(config: WorkerConfig, args) -> dict:
    """Store only the named UI document; never inspect queue or submit a prompt."""
    return save_workflow_bytes(config, args.name, args.replace, sys.stdin.buffer.read(MAX_BYTES + 1))


def save_workflow_bytes(config, name, replace, raw):
    """Explicit byte-oriented save, shared by CLI and authenticated bridge."""
    path = workflow_path(name)
    raw = validate_bytes(raw)
    api = smoke.API(config.comfyui_url, timeout=10)  # loopback, no proxy/redirect
    endpoint = api.base + '/userdata/' + urllib.parse.quote(path, safe='')

    def request(method, url, body=None, limit=MAX_BYTES):
        req = urllib.request.Request(url, data=body, method=method,
                                     headers={'Content-Type': 'application/json'})
        try:
            with api.opener.open(req, timeout=api.timeout) as response:
                if response.status != 200 or response.geturl() != url:
                    raise RuntimeError('unexpected workflow userdata response')
                data = response.read(limit + 1)
        except urllib.error.HTTPError as exc:
            code = exc.code
            exc.close()
            raise RuntimeError(f'workflow userdata HTTP {code}; no automatic retry') from None
        if len(data) > limit:
            raise RuntimeError('workflow userdata response exceeds limit')
        return data

    url = endpoint + '?overwrite=' + ('true' if replace else 'false') + '&full_info=false'
    try:
        saved_path = json.loads(request('POST', url, raw, 256))
    except (ValueError, UnicodeError):
        raise RuntimeError('invalid workflow save response; file may already be saved') from None
    if saved_path != path:
        raise RuntimeError('workflow save response path mismatch; file may already be saved')
    expected = hashlib.sha256(raw).hexdigest()
    if hashlib.sha256(request('GET', endpoint)).hexdigest() != expected:
        raise RuntimeError('workflow readback digest mismatch; file may already be saved')
    return {'operation': 'workflow-save', 'path': path, 'sha256': expected,
            'bytes': len(raw), 'verified': True, 'replace': replace}


def arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    commands.add_parser("status")
    commands.add_parser("free")
    commands.add_parser("recover")
    save = commands.add_parser("workflow-save")
    save.add_argument("--name", required=True)
    save.add_argument("--input-stdin", action="store_true", required=True)
    save.add_argument("--replace", action="store_true")
    check = commands.add_parser("verify")
    check.add_argument("--class-name", action="append", default=[])
    check.add_argument("--model", action="append", default=[])
    install = commands.add_parser("node-install")
    install.add_argument("--name", required=True)
    install.add_argument("--url", required=True)
    install.add_argument("--sha256", required=True)
    install.add_argument("--requirements-sha256")
    install.add_argument("--replace", action="store_true")
    remove_node = commands.add_parser("node-remove")
    remove_node.add_argument("--name", required=True)
    model = commands.add_parser("model-install")
    model.add_argument("--kind", required=True, choices=sorted(MODEL_KINDS))
    model.add_argument("--name", required=True)
    model.add_argument("--url", required=True)
    model.add_argument("--sha256", required=True)
    model.add_argument("--max-bytes", type=int, default=1024 * 1024 * 1024 * 1024)
    remove_model = commands.add_parser("model-remove")
    remove_model.add_argument("--kind", required=True, choices=sorted(MODEL_KINDS))
    remove_model.add_argument("--name", required=True)
    restart_parser = commands.add_parser("restart")
    restart_parser.add_argument("--class-name", action="append", default=[])
    restart_parser.add_argument("--model", action="append", default=[])
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = arguments(argv)
    config = WorkerConfig.from_env()
    if args.operation == "workflow-save":
        try:
            result = workflow_save(config, args)
        except Exception:
            print('workflow save failed; file may already be saved; no automatic retry', file=sys.stderr)
            return 1
    elif args.operation == "status":
        result = status(config)
    elif args.operation == "verify":
        result = verify(config, args)
    elif args.operation == "free":
        result = free_memory(config)
    elif args.operation == "node-install":
        result = node_install(config, args)
    elif args.operation == "node-remove":
        result = remove(config, "node", args.name)
    elif args.operation == "model-install":
        result = model_install(config, args)
    elif args.operation == "recover":
        result = recover(config)
    elif args.operation == "restart":
        result = restart(config, args)
    else:
        result = remove(config, "model", args.name, args.kind)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
