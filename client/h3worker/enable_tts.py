"""Enable CosyVoice3 only after its configured model is locally available."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Callable


CAPABILITY = "cosyvoice3-mlx"
DEFAULT_MODEL = "mlx-community/Fun-CosyVoice3-0.5B-2512-4bit"
_MODEL_ID = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")


def enable(
    config_path: Path,
    download: Callable[..., object],
    *,
    validate: Callable[[str, object], None] | None = None,
    repair: bool = False,
) -> str:
    if not config_path.is_file():
        raise RuntimeError(f"worker config missing: {config_path}")
    raw = config_path.read_text(encoding="utf-8")
    config = json.loads(raw)
    if not isinstance(config, dict):
        raise ValueError("worker config must be a JSON object")
    model = (
        os.environ.get("H3WORKER_TTS_MODEL") or config.get("tts_model") or DEFAULT_MODEL
    )
    if not isinstance(model, str) or not _MODEL_ID.fullmatch(model):
        raise ValueError("tts_model must be a Hugging Face owner/repository id")

    # Download first.  A failed or incomplete download must not advertise a
    # capability that would make the scheduler assign TTS work to this node.
    snapshot = download(repo_id=model)
    if validate is not None:
        validate(model, snapshot)

    models = config.get("capability_models") or []
    if not isinstance(models, list) or not all(isinstance(x, str) for x in models):
        raise ValueError("capability_models must be a list of strings")
    if not repair and CAPABILITY not in models:
        models.append(CAPABILITY)
    config["capability_models"] = models
    config["tts_model"] = model

    mode = stat.S_IMODE(config_path.stat().st_mode)
    fd, tmp_name = tempfile.mkstemp(
        prefix=config_path.name + ".", suffix=".tmp", dir=config_path.parent,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(config, fh, ensure_ascii=False, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, config_path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
    return model


def main() -> None:
    from huggingface_hub import snapshot_download
    from h3worker.audio_installation import check_runtime, validate_snapshot

    check_runtime("cosyvoice3")

    client_dir = Path(__file__).resolve().parents[1]
    config_path = Path(
        os.environ.get("H3WORKER_CONFIG", str(client_dir / "worker.json"))
    ).expanduser().resolve()
    model = enable(
        config_path, snapshot_download, validate=validate_snapshot,
        repair=os.environ.get("H3WORKER_AUDIO_REPAIR") == "1",
    )
    print(f"[enable-tts] cached {model} and enabled {CAPABILITY}", flush=True)


if __name__ == "__main__":
    main()
