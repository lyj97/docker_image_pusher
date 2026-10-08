"""Write the per-user macOS LaunchAgent used by restart/upgrade."""

from __future__ import annotations

import os
import plistlib
import sys
import tempfile
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 5:
        raise SystemExit(
            "usage: setup_launchd.py <plist> <start-script> <config> <log-dir>"
        )
    plist, start, config, log_dir = map(Path, sys.argv[1:])
    log_dir.mkdir(parents=True, exist_ok=True)
    plist.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "Label": "com.relife.h3worker",
        "ProgramArguments": [str(start)],
        "WorkingDirectory": str(start.parent),
        "EnvironmentVariables": {
            "H3WORKER_CONFIG": str(config),
            "H3WORKER_LAUNCHED_BY_LAUNCHD": "1",
        },
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Interactive",
        "ExitTimeOut": 40,
        "StandardOutPath": str(log_dir / "worker.stdout.log"),
        "StandardErrorPath": str(log_dir / "worker.stderr.log"),
    }
    fd, tmp = tempfile.mkstemp(prefix=plist.name + ".", dir=plist.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            plistlib.dump(payload, stream, sort_keys=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, plist)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


if __name__ == "__main__":
    main()
