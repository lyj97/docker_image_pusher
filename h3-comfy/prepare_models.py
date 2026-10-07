#!/usr/bin/env python3
"""Prepare only the five pinned H3 Turbo files; never load them into RAM."""
import argparse
import atexit
import contextlib
import uuid
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import quote
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
EVENTS = []
SKIP_READBACK = False


@contextlib.contextmanager
def measured(phase, target, byte_count=0):
    start = time.monotonic()
    event = {"phase": phase, "target": str(target), "bytes": byte_count,
             "started_at_unix": time.time(), "success": False}
    try:
        yield event
        event["success"] = True
    finally:
        event["seconds"] = time.monotonic() - start
        event["MB_per_second"] = event["bytes"] / 1e6 / event["seconds"] if event["seconds"] else None
        EVENTS.append(event)
        print(json.dumps({"measurement": event}), flush=True)



def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def valid(path, model):
    if not path.is_file() or path.stat().st_size != model["bytes"]:
        return False
    if SKIP_READBACK:
        return True
    with measured("sha256_read", path, model["bytes"]):
        return sha256(path) == model["sha256"]


def check_path(root, relative):
    path = root / relative
    if not path.resolve().is_relative_to(root.resolve()):
        raise RuntimeError(f"Unsafe model path: {relative}")
    return path


def main():
    global SKIP_READBACK
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--local", action="store_true", help="Download to Pod local disk; default /workspace, direct streaming, no full readback")
    p.add_argument("--readback", action="store_true", help="Opt in to full readback SHA-256, including local mode")
    p.add_argument("--manifest", type=Path, default=HERE / "models.lock.json")
    p.add_argument("--storage-root", type=Path, default=None)
    p.add_argument("--staging", type=Path, default=Path("/workspace/h3-staging"))
    p.add_argument("--skip-readback", action="store_true", help="Keep streaming download SHA-256; reuse existing files by size only")
    p.add_argument("--metrics-dir", type=Path, default=Path("/tmp/h3-download-metrics"))
    p.add_argument("--plan", action="store_true", help="Print inventory, do not download or write")
    p.add_argument("--verify-only", action="store_true")
    p.add_argument("--direct", action="store_true", help="Sequential Global Volume write; no local staging or resume")
    p.add_argument("--allow-unmounted", action="store_true", help="Local validation only")
    args = p.parse_args()
    args.storage_root = args.storage_root or Path("/workspace" if args.local else "/workspace-global")
    if args.local:
        args.direct = True
        args.skip_readback = not args.readback
    SKIP_READBACK = args.skip_readback
    manifest = json.loads(args.manifest.read_text())
    models = manifest["models"]
    print(json.dumps({"files": len(models), "bytes": sum(m["bytes"] for m in models),
                      "largest_file_bytes": max(m["bytes"] for m in models),
                      "global_volume_id": manifest["global_volume_id"]}, indent=2), flush=True)
    root = args.storage_root / "h3" / "models"
    for m in models:
        check_path(root, m["target"])
        if len(m["sha256"]) != 64 or m["bytes"] <= 0:
            raise RuntimeError("Invalid manifest")
    if args.plan:
        for m in models:
            print(f'{m["bytes"] / 1e9:.3f} GB  {m["target"]}')
        return
    if not args.local and not args.allow_unmounted and not os.path.ismount(args.storage_root):
        raise RuntimeError(f"{args.storage_root} is not mounted; refusing ephemeral-only writes")
    if args.local:
        args.storage_root.mkdir(parents=True, exist_ok=True)
        # Refuse object-backed mounts even if the user accidentally chose the old Global path.
        for line in Path("/proc/mounts").read_text().splitlines():
            fields = line.split()
            if len(fields) >= 3 and args.storage_root.resolve().is_relative_to(Path(fields[1])) and "fuse" in fields[2]:
                raise RuntimeError("Local mode refuses a FUSE/object-backed mount")
        needed = sum(m["bytes"] for m in models
                     if not (check_path(root, m["target"]).is_file()
                             and check_path(root, m["target"]).stat().st_size == m["bytes"]))
        if shutil.disk_usage(args.storage_root).free < needed + 2 * 1024**3:
            raise RuntimeError("Not enough local model space; need remaining models + 2GiB")
    EVENTS.clear()
    run_start = time.monotonic()
    args.metrics_dir.mkdir(parents=True, exist_ok=True)
    report = args.metrics_dir / ("download-" + uuid.uuid4().hex + ".json")
    outcome = {"completed": False}
    def save_report():
        report.write_text(json.dumps({"schema_version": 1, "storage_root": str(args.storage_root), "storage_mode": "local" if args.local else "global",
            "readback_skipped": args.skip_readback, "existing_files_checked_by_size_only": args.skip_readback,
            "mode": "verify" if args.verify_only else "direct" if args.direct else "staged",
            "manifest_sha256": sha256(args.manifest), "planned_bytes": sum(m["bytes"] for m in models),
            "elapsed_seconds": time.monotonic() - run_start, "events": EVENTS, **outcome}, indent=2) + "\n")
    atexit.register(save_report)
    print(f"METRICS {report}", flush=True)
    root.mkdir(parents=True, exist_ok=True)
    marker = root.parent / "models-ready.json"
    # A stale marker must never advertise partially overwritten files as ready.
    marker.unlink(missing_ok=True)
    if not args.verify_only and not args.direct:
        if not shutil.which("curl"):
            raise RuntimeError("curl is required on the preparation Pod")
        args.staging.mkdir(parents=True, exist_ok=True)
        if args.staging.resolve().is_relative_to(args.storage_root.resolve()):
            raise RuntimeError("Use local disk for staging, not object-backed storage")
    started = time.monotonic()
    for m in models:
        target = check_path(root, m["target"])
        if valid(target, m):
            print(f'{"SIZE_ONLY" if args.skip_readback else "VERIFIED"} {m["target"]}', flush=True)
            continue
        if args.verify_only:
            raise RuntimeError(f'Missing or corrupt model: {target}')
        url = (f'https://huggingface.co/{m["repo"]}/resolve/{m["revision"]}/'
               f'{quote(m["source"], safe="/")}')
        if args.direct:
            target.parent.mkdir(parents=True, exist_ok=True)
            print(f'DIRECT DOWNLOAD {m["target"]}', flush=True)
            digest = hashlib.sha256()
            size = 0
            deadline = time.monotonic() + 3600
            # Single writer only. The file is incomplete until the readiness marker exists.
            # A failed direct download is redownloaded in full on the next invocation.
            with measured("download_and_write", target) as event, urlopen(url, timeout=60) as source, target.open("wb") as dest:
                while chunk := source.read(8 * 1024 * 1024):
                    if time.monotonic() > deadline:
                        raise RuntimeError("Direct download exceeded one-hour per-file limit")
                    size += len(chunk)
                    event["bytes"] = size
                    if size > m["bytes"]:
                        raise RuntimeError("Download exceeded pinned file size")
                    digest.update(chunk)
                    dest.write(chunk)
            if size != m["bytes"] or digest.hexdigest() != m["sha256"] or not valid(target, m):
                raise RuntimeError(f"Direct download/read-back failed verification: {target}")
            print(f'PERSISTED {m["target"]}', flush=True)
            continue
        stage = args.staging / (m["sha256"] + ".part")
        if stage.exists() and stage.stat().st_size > m["bytes"]:
            raise RuntimeError(f"Oversized staging file: {stage}; inspect before removing")
        missing = m["bytes"] - (stage.stat().st_size if stage.exists() else 0)
        if shutil.disk_usage(args.staging).free < missing + 2 * 1024**3:
            raise RuntimeError("Not enough local staging space; need largest remaining file + 2GiB")
        if not valid(stage, m):
            if stage.exists() and stage.stat().st_size == m["bytes"]:
                raise RuntimeError(f"Staging checksum mismatch: {stage}; inspect before removing")
            print(f'DOWNLOADING {m["target"]}', flush=True)
            before = stage.stat().st_size if stage.exists() else 0
            with measured("download_to_local", stage) as event:
                try:
                    subprocess.run(["curl", "--fail", "--location", "--retry", "4",
                            "--connect-timeout", "30", "--max-time", "3600",
                            "--continue-at", "-", "--output", str(stage), url], check=True)
                finally:
                    event["bytes"] = max(0, (stage.stat().st_size if stage.exists() else 0) - before)
        if not valid(stage, m):
            raise RuntimeError(f"Downloaded file failed size/SHA-256 check: {stage}")
        target.parent.mkdir(parents=True, exist_ok=True)
        # Object-backed storage: sequential copy, no flock/hardlink/atomic-rename assumptions.
        with measured("copy_to_storage", target, m["bytes"]), stage.open("rb") as source, target.open("wb") as dest:
            shutil.copyfileobj(source, dest, length=8 * 1024 * 1024)
        if not valid(target, m):
            raise RuntimeError(f"Global Volume read-back failed: {target}; staging preserved")
        stage.unlink()
        print(f'PERSISTED {m["target"]}', flush=True)
    ready = {"manifest_sha256": sha256(args.manifest), "verified_at_unix": time.time(),
             "elapsed_seconds": round(time.monotonic() - started, 3),
             "storage_mode": "local" if args.local else "global",
             "readback_verified": not args.skip_readback,
             "models": models, "global_volume_id": manifest["global_volume_id"]}
    marker.write_text(json.dumps(ready, indent=2) + "\n")
    outcome["completed"] = True
    save_report()
    atexit.unregister(save_report)
    print(f"READY {marker}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
