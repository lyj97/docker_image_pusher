"""Write first-run Worker configuration from NUL-delimited stdin."""

from __future__ import annotations

import json
import os
import sys


KEYS = (
    "server_url",
    "worker_token",
    "worker_id",
    "cf_access_client_id",
    "cf_access_client_secret",
    "data_dir",
    "h3_binary",
    "h3_working_dir",
    "model_dir",
    "ffmpeg_path",
    "ffprobe_path",
)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: setup_config.py <output-path>")
    values = sys.stdin.buffer.read().split(b"\0")
    if values and values[-1] == b"":
        values.pop()
    if len(values) != len(KEYS):
        raise SystemExit("invalid setup input")
    config = dict(zip(KEYS, (item.decode() for item in values)))
    config["fake_runner"] = False
    config["capability_models"] = ["installed-model-revision"]
    with open(sys.argv[1], "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.chmod(sys.argv[1], 0o600)


if __name__ == "__main__":
    main()
