"""Enable Qwen3 audio capabilities after every configured model is cached."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Callable

CAPABILITY_MODEL_FIELDS = {
    "tts.qwen3.voice_design": "qwen3_voice_design_model",
    "tts.qwen3.custom_voice": "qwen3_custom_voice_model",
    "tts.qwen3.clone": "qwen3_clone_model",
    "audio.qwen3.forced_align": "qwen3_aligner_model",
}
CAPABILITY_MODELS = {
    "tts.qwen3.voice_design": "mlx-community/Qwen3-TTS-12Hz-1.7B-VoiceDesign-bf16",
    "tts.qwen3.custom_voice": "mlx-community/Qwen3-TTS-12Hz-0.6B-CustomVoice-bf16",
    "tts.qwen3.clone": "mlx-community/Qwen3-TTS-12Hz-0.6B-Base-bf16",
    "audio.qwen3.forced_align": "mlx-community/Qwen3-ForcedAligner-0.6B-8bit",
}
_MODEL_ID = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")


def enable(
    config_path: Path,
    download: Callable[..., object],
    *,
    validate: Callable[[str, object], None] | None = None,
    repair: bool = False,
) -> dict[str, str]:
    if not config_path.is_file():
        raise RuntimeError(f"worker config missing: {config_path}")
    raw = config_path.read_text(encoding="utf-8")
    config = json.loads(raw)
    if not isinstance(config, dict):
        raise ValueError("worker config must be a JSON object")

    selected = {}
    for capability, field in CAPABILITY_MODEL_FIELDS.items():
        if repair and capability not in config.get("capability_models", []):
            continue
        model = (
            os.environ.get("H3WORKER_" + field.upper())
            or config.get(field)
            or CAPABILITY_MODELS[capability]
        )
        if not isinstance(model, str) or not _MODEL_ID.fullmatch(model):
            raise ValueError(f"{field} must be a Hugging Face owner/repository id")
        selected[capability] = model

    # Complete every download before changing capability advertisement.
    for model in selected.values():
        snapshot = download(repo_id=model)
        if validate is not None:
            validate(model, snapshot)

    models = config.get("capability_models") or []
    if not isinstance(models, list) or not all(isinstance(x, str) for x in models):
        raise ValueError("capability_models must be a list of strings")
    for capability in selected:
        field = CAPABILITY_MODEL_FIELDS[capability]
        if capability not in models:
            models.append(capability)
        config[field] = selected[capability]
    config["capability_models"] = models

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
    return selected


def main() -> None:
    from huggingface_hub import snapshot_download
    from h3worker.audio_installation import check_runtime, validate_snapshot

    check_runtime("qwen3")

    client_dir = Path(__file__).resolve().parents[1]
    config_path = Path(
        os.environ.get("H3WORKER_CONFIG", str(client_dir / "worker.json"))
    ).expanduser().resolve()
    enabled = enable(
        config_path, snapshot_download, validate=validate_snapshot,
        repair=os.environ.get("H3WORKER_AUDIO_REPAIR") == "1",
    )
    for capability, model in enabled.items():
        print(f"[enable-qwen3] cached {model} and enabled {capability}", flush=True)


if __name__ == "__main__":
    main()
