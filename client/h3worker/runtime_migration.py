"""Migrate a legacy interpreter only during a verified controlled Git update."""
from __future__ import annotations

import argparse
import os
import re
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path
import json

from .config import WorkerConfig
from .upgrade import _installation_path


def migrate(config, repo: Path, python: str, *, run=subprocess.run) -> Path:
    repo = _installation_path(repo, "runtime migration root")
    marker_path = _installation_path(config.update_marker_path, "update marker")
    info = marker_path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise RuntimeError("controlled update marker must be private and user-owned")
    marker = json.loads(marker_path.read_text())
    revision = marker.get("revision", "")
    if (marker.get("source") != "git" or marker.get("startup_failed")
            or not re.fullmatch(r"[0-9a-f]{40}", revision)
            or not re.fullmatch(r"upd_[0-9a-f]{24}", marker.get("request_id", ""))):
        raise RuntimeError("verified controlled Git update marker required")

    def checked(args, **kwargs):
        return run(args, check=True, timeout=180, **kwargs)

    def git(*args):
        return checked(["git", "-C", str(repo), *args],
                       capture_output=True, text=True).stdout.strip()

    if git("rev-parse", "HEAD") != revision or git("status", "--porcelain", "--untracked-files=no"):
        raise RuntimeError("runtime migration requires a clean exact update revision")
    git("merge-base", "--is-ancestor", revision, "origin/main")
    base = Path(python)
    if not base.is_absolute() or not base.is_file():
        raise RuntimeError("absolute Python 3.12 executable required")
    version = checked([str(base), "-c", "import sys; print('.'.join(map(str, sys.version_info[:2])))"],
                      capture_output=True, text=True).stdout.strip()
    if version != "3.12":
        raise RuntimeError("migration interpreter must be Python 3.12")
    if shutil.disk_usage(repo).free < config.min_free_disk_bytes + 512 * 1024**2:
        raise RuntimeError("insufficient disk reserve for runtime migration")

    runtime = repo / "var" / "runtime"
    _installation_path(runtime, "runtime directory")
    runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
    stage = Path(tempfile.mkdtemp(prefix="worker-py312-", dir=runtime))
    venv = repo / ".venv"
    backup = runtime / ("legacy-venv-" + stage.name)
    pointer = repo / (".venv-switch-" + stage.name)
    published = False
    try:
        checked([str(base), "-m", "venv", str(stage)])
        interpreter = stage / "bin" / "python"
        checked([str(interpreter), "-m", "pip", "install", "--only-binary=:all:",
                 "--disable-pip-version-check", "--no-cache-dir", "--timeout", "20",
                 "--retries", "1", "-r", str(repo / "requirements.txt")])
        checked([str(interpreter), "-B", "-c",
                 "import starlette, uvicorn, asyncpg, cryptography, websockets; "
                 "from h3worker.config import WorkerConfig; WorkerConfig.from_env()"],
                cwd=repo / "client")
        if shutil.disk_usage(repo).free < config.min_free_disk_bytes:
            raise RuntimeError("runtime preparation exceeded disk reserve")
        pointer.symlink_to(stage.relative_to(repo))
        os.replace(venv, backup)
        try:
            os.replace(pointer, venv)
        except BaseException:
            os.replace(backup, venv)
            raise
        published = True
        return stage
    finally:
        pointer.unlink(missing_ok=True)
        if not published:
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", required=True)
    parser.add_argument("--repo", required=True)
    args = parser.parse_args()
    migrate(WorkerConfig.from_env(), Path(args.repo), args.python)
    print("[start] Controlled update runtime migrated to Python 3.12; original environment preserved", flush=True)


if __name__ == "__main__":
    main()
