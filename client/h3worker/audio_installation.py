"""Local, offline audio readiness checks and startup repair (never Worker commands)."""
from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from h3worker.enable_qwen3 import CAPABILITY_MODEL_FIELDS, CAPABILITY_MODELS
from h3worker.enable_tts import CAPABILITY, DEFAULT_MODEL

CLIENT = Path(__file__).resolve().parents[1]
ROOT = CLIENT.parent


def selected_models(
    config: dict, backend: str, all_models: bool = False,
) -> dict[str, str]:
    capabilities = config.get("capability_models", [])
    if backend == "qwen3":
        return {
            cap: (
                os.environ.get("H3WORKER_" + field.upper())
                or config.get(field)
                or CAPABILITY_MODELS[cap]
            )
            for cap, field in CAPABILITY_MODEL_FIELDS.items()
            if all_models or cap in capabilities
        }
    if all_models or any(
        cap in capabilities for cap in (CAPABILITY, "tts.cosyvoice3.clone")
    ):
        return {
            CAPABILITY: (
                os.environ.get("H3WORKER_TTS_MODEL")
                or config.get("tts_model")
                or DEFAULT_MODEL
            ),
        }
    return {}


def validate_snapshot(model: str, snapshot: str) -> None:
    """Sanity-check the local cache without inventing another readiness marker."""
    path = Path(snapshot).resolve(strict=True)
    json.loads((path / "config.json").read_text(encoding="utf-8"))
    weights = list(path.rglob("*.safetensors"))
    if not weights or any(not p.is_file() or p.stat().st_size == 0 for p in weights):
        raise RuntimeError(f"missing or empty audio weights: {model}")
    if any(p.is_symlink() and not p.exists() for p in path.rglob("*")):
        raise RuntimeError(f"broken audio cache link: {model}")
    # Hub writes partial downloads into the repository's blobs directory.
    cache = path.parent.parent if path.parent.name == "snapshots" else path
    if next(cache.rglob("*.incomplete"), None) is not None:
        raise RuntimeError(f"incomplete audio download: {model}")


def check_models(models: dict[str, str]) -> None:
    from huggingface_hub import snapshot_download

    for model in models.values():
        snapshot = snapshot_download(repo_id=model, local_files_only=True)
        validate_snapshot(model, snapshot)


def check_runtime(backend: str) -> None:
    import pip  # noqa: F401 -- missing pip must trigger repair
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError('audio runtime requires Python 3.12')
    requirements = CLIENT / (
        "qwen3-requirements.txt" if backend == "qwen3" else "requirements-tts.txt"
    )
    pins = {}
    for line in requirements.read_text().splitlines():
        line = line.split('#', 1)[0].strip()
        if line:
            package, version = line.split('==', 1)
            pins[package.split('[', 1)[0]] = version
    for package, version in pins.items():
        if importlib.metadata.version(package) != version:
            raise RuntimeError(f'audio dependency version mismatch: {package}')
    importlib.import_module('mlx_audio.tts.generate')
    if backend == 'qwen3':
        for module in ('mlx_audio.stt.generate', 'nagisa', 'soynlp'):
            importlib.import_module(module)
    subprocess.run(
        [sys.executable, "-m", "pip", "check"],
        check=True, stdout=subprocess.DEVNULL,
    )


def ready(backend: str, config_path: Path) -> bool:
    venv = 'tts-qwen3-venv' if backend == 'qwen3' else 'tts-venv'
    python = ROOT / 'var' / venv / 'bin/python'
    try:
        result = subprocess.run(
            [str(python), "-m", "h3worker.audio_installation", "--check", backend],
            cwd=CLIENT,
            env={**os.environ, "H3WORKER_CONFIG": str(config_path)},
            timeout=120,
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def run_installer(script: Path, env: dict[str, str], models: dict[str, str]) -> None:
    """Bound startup preparation and cancel its entire process group on shutdown."""
    timeout = float(env.get('H3WORKER_AUDIO_INSTALL_TIMEOUT_SECONDS', '14400'))
    stall_timeout = float(env.get('H3WORKER_AUDIO_INSTALL_STALL_SECONDS', '600'))
    if timeout <= 0 or stall_timeout <= 0:
        raise ValueError('audio installation timeouts must be positive')
    cache_home = Path(env.get("XDG_CACHE_HOME", str(Path.home() / ".cache")))
    hf_home = Path(env.get("HF_HOME", str(cache_home / "huggingface")))
    hub = Path(env.get(
        "HF_HUB_CACHE", env.get("HUGGINGFACE_HUB_CACHE", str(hf_home / "hub")),
    ))
    watched = [ROOT / "var"] + [
        hub / ("models--" + model.replace("/", "--"))
        for model in models.values()
    ]

    def progress() -> tuple[int, int]:
        count = size = 0
        for directory in watched:
            for file in directory.rglob('*'):
                try:
                    if file.is_file() and not file.is_symlink():
                        count += 1
                        size += file.stat().st_size
                except FileNotFoundError:
                    pass
        return count, size

    def interrupted(signum, _frame):
        raise RuntimeError(f'audio installation interrupted by signal {signum}')

    (ROOT / 'var').mkdir(parents=True, exist_ok=True)
    scratch = tempfile.TemporaryDirectory(prefix='.audio-install-', dir=ROOT / 'var')
    previous = {
        sig: signal.signal(sig, interrupted)
        for sig in (signal.SIGTERM, signal.SIGINT)
    }
    process = None
    try:
        started = changed = time.monotonic()
        last_progress = progress()
        process = subprocess.Popen(
            [str(script)], cwd=CLIENT,
            env={**env, "TMPDIR": scratch.name}, start_new_session=True,
        )
        while True:
            try:
                code = process.wait(timeout=30)
                if code:
                    raise subprocess.CalledProcessError(code, str(script))
                return
            except subprocess.TimeoutExpired:
                now = time.monotonic()
                current = progress()
                if current != last_progress:
                    changed = now
                    last_progress = current
                print(
                    f"[start] {script.name}: elapsed={int(now - started)}s "
                    f"files={current[0]} bytes={current[1]}",
                    flush=True,
                )
                if now - started >= timeout or now - changed >= stall_timeout:
                    raise RuntimeError(
                        f"{script.name}: installation timeout or no filesystem progress"
                    )
    finally:
        try:
            # poll() returning None means the child has not exited or been reaped.
            # Never signal its old process-group ID after wait() has completed.
            if process is not None and process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    if process.poll() is None:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait(timeout=5)
        finally:
            scratch.cleanup()
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def repair(config_path: Path) -> None:
    config = json.loads(config_path.read_text())
    for backend, script, bootstrap in (
        ('cosyvoice3', 'enable-tts.sh', 'H3WORKER_TTS_BOOTSTRAP_PYTHON'),
        ('qwen3', 'enable-qwen3.sh', 'H3WORKER_QWEN3_BOOTSTRAP_PYTHON'),
    ):
        if selected_models(config, backend) and not ready(backend, config_path):
            print(
                f"[start] Repairing configured {backend} installation; "
                "progress follows",
                flush=True,
            )
            run_installer(
                CLIENT / script,
                {
                    **os.environ, bootstrap: sys.executable,
                    "H3WORKER_NO_RESTART": "1", "H3WORKER_AUDIO_REPAIR": "1",
                    "H3WORKER_CONFIG": str(config_path),
                },
                selected_models(config, backend),
            )
            if not ready(backend, config_path):
                raise RuntimeError(
                    f"{backend} repair did not pass readiness validation"
                )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', choices=('qwen3', 'cosyvoice3'))
    parser.add_argument('--all-enabled', action='store_true')
    parser.add_argument('--has-audio', action='store_true')
    parser.add_argument('--runtime', choices=('qwen3', 'cosyvoice3'))
    args = parser.parse_args()
    if args.runtime:
        check_runtime(args.runtime)
        return
    config_path = Path(os.environ.get(
        "H3WORKER_CONFIG", str(CLIENT / "worker.json"),
    ))
    config = json.loads(config_path.read_text())
    if args.has_audio:
        raise SystemExit(not any(
            selected_models(config, b) for b in ("qwen3", "cosyvoice3")
        ))
    if args.all_enabled:
        capabilities = set(config.get("capability_models", []))
        raise SystemExit(not all(
            set(selected_models(config, b, True)) <= capabilities
            for b in ("qwen3", "cosyvoice3")
        ))
    if args.check:
        check_runtime(args.check)
        check_models(selected_models(config, args.check))
    else:
        repair(config_path)


if __name__ == '__main__':
    main()
