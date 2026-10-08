"""Worker configuration from environment / config file."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Optional
from shared.comfy_versions import SUPPORTED


def _load_config_file(path: Optional[str]) -> dict:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


@dataclass
class WorkerConfig:
    # server
    server_url: str = "http://127.0.0.1:8730"
    # Optional verified address for this HTTPS origin; Host/SNI stay unchanged.
    server_connect_ip: str = ""
    worker_token: str = "worker-token"
    worker_id: str = "mac_01"
    cf_access_client_id: str = field(default="", repr=False)
    cf_access_client_secret: str = field(default="", repr=False)

    # paths
    data_dir: str = "worker-data"
    h3_binary: str = "./h3"
    h3_working_dir: str = "."
    ffmpeg_path: str = ""
    ffprobe_path: str = ""
    model_dir: str = ""
    tts_model: str = "mlx-community/Fun-CosyVoice3-0.5B-2512-4bit"
    qwen3_voice_design_model: str = (
        "mlx-community/Qwen3-TTS-12Hz-1.7B-VoiceDesign-bf16"
    )
    qwen3_custom_voice_model: str = (
        "mlx-community/Qwen3-TTS-12Hz-0.6B-CustomVoice-bf16"
    )
    qwen3_clone_model: str = "mlx-community/Qwen3-TTS-12Hz-0.6B-Base-bf16"
    qwen3_aligner_model: str = "mlx-community/Qwen3-ForcedAligner-0.6B-8bit"

    # Dedicated ComfyUI instance. Tasks never install; drained operators may.
    comfyui_url: str = ""
    comfyui_root: str = ""
    comfyui_python: str = ""
    comfyui_version: str = ""
    comfyui_ready: bool = False
    comfyui_denied_classes: tuple = ()
    comfyui_preview_enabled: bool = False
    comfyui_frontend_root: str = ""
    comfyui_frontend_sha256: str = ""

    # engine env whitelist: fixed environment for the h3 subprocess
    # (client README section 5: the worker launches with a fixed env; the
    # task cannot inject arbitrary variables)
    env_passthrough: tuple = (
        "PATH", "HOME", "TMPDIR", "METAL_DEVICE_WRAPPER_TYPE",
        "METAL_DEBUG_ERROR_MODE", "OS_ACTIVITY_MODE", "HF_ENDPOINT",
    )

    # lease / scheduling
    heartbeat_seconds: float = 10.0
    lease_safety_margin_seconds: float = 15.0
    claim_wait_seconds: int = 25
    progress_merge_seconds: float = 1.5
    cancel_grace_seconds: float = 5.0
    durability_timeout_seconds: float = 900.0  # large-media filesystem flush bound

    # http
    http_connect_timeout_seconds: float = 10.0
    http_timeout_seconds: float = 35.0   # > heartbeat + claim margins
    upload_timeout_seconds: float = 600.0
    download_timeout_seconds: float = 300.0
    max_retries: int = 8

    # Observational enabling slice; does not grant another execution slot.
    cpu_tail_overlap: bool = False
    cpu_tail_pilot: bool = False
    cpu_tail_max_bytes: int = 512 * 1024 * 1024

    # resources
    min_free_disk_bytes: int = 20 * 1024 * 1024 * 1024
    max_asset_bytes: int = 512 * 1024 * 1024

    # fake runner for protocol tests (client README section 10: fault tests
    # use a controllable fake inference subprocess)
    fake_runner: bool = False
    fake_runner_sleep_seconds: float = 0.2
    # fault injection for protocol tests: seconds before the fake runner
    # fails (0 = never)
    fake_runner_fail_after: float = 0.0
    # model revisions this node can serve (registration capabilities and
    # the local-incapacity precheck both use this list)
    capability_models: tuple = ("installed-model-revision",)

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        file_values = _load_config_file(os.environ.get("H3WORKER_CONFIG"))
        values = {}
        defaults = cls()
        for f in cls.__dataclass_fields__.values():
            env_name = "H3WORKER_" + f.name.upper()
            if env_name in os.environ:
                raw = os.environ[env_name]
                if f.type == "bool" or f.type is bool:
                    values[f.name] = raw.lower() in ("1", "true", "yes", "on")
                elif f.type in ("int", "float") or f.type in (int, float):
                    values[f.name] = (
                        float(raw) if "float" in str(f.type) else int(raw)
                    )
                elif f.type == "tuple":
                    values[f.name] = tuple(x for x in raw.split(",") if x)
                else:
                    values[f.name] = raw
            elif f.name in file_values:
                values[f.name] = file_values[f.name]
        cfg = cls(**values)
        if bool(cfg.cf_access_client_id) != bool(cfg.cf_access_client_secret):
            raise ValueError(
                "Cloudflare Access client ID and client secret must be set together"
            )
        return cfg

    def __post_init__(self) -> None:
        if self.server_connect_ip:
            from .http import validate_server_connect_ip
            validate_server_connect_ip(self.server_url, self.server_connect_ip)
        if type(self.comfyui_preview_enabled) is not bool:
            raise ValueError("comfyui_preview_enabled must be a boolean")
        if self.comfyui_preview_enabled and (self.comfyui_url != "http://127.0.0.1:8188" or self.comfyui_version not in SUPPORTED or not self.comfyui_ready):
            raise ValueError("preview requires dedicated pinned loopback ComfyUI")
        if self.comfyui_preview_enabled:
            from .comfy_launch import absolute_path
            absolute_path(self.comfyui_frontend_root, directory=True)
            if len(self.comfyui_frontend_sha256) != 64 or any(c not in '0123456789abcdef' for c in self.comfyui_frontend_sha256):
                raise ValueError('preview requires an operator-pinned frontend SHA256')
        if not isinstance(self.cpu_tail_overlap, bool):
            raise ValueError("cpu_tail_overlap must be a boolean")
        if self.cpu_tail_overlap:
            self.cpu_tail_pilot = True
        if (not isinstance(self.cpu_tail_max_bytes, int)
                or isinstance(self.cpu_tail_max_bytes, bool)
                or self.cpu_tail_max_bytes <= 0):
            raise ValueError("cpu_tail_max_bytes must be a positive integer")
        if self.cpu_tail_overlap and self.max_asset_bytes > 512 * 1024 * 1024:
            raise ValueError("overlap input asset bound exceeds disk admission allowance")
        if self.cpu_tail_overlap and self.min_free_disk_bytes < 20 * 1024**3:
            raise ValueError("overlap requires at least 20 GiB disk headroom")
        if self.cpu_tail_overlap and self.cpu_tail_max_bytes > 512 * 1024 * 1024:
            raise ValueError("overlap tail bound exceeds server limit")
        if not isinstance(self.cpu_tail_pilot, bool):
            raise ValueError("cpu_tail_pilot must be a boolean")

    # ------------------------------------------------------------------
    def ensure_dirs(self) -> None:
        for sub in ("cache/sha256", "attempts", "tmp"):
            os.makedirs(os.path.join(self.data_dir, sub), exist_ok=True)

    def attempt_dir(self, attempt_id: str) -> str:
        # external ids are validated before they become path components
        # (client README 4; review C6)
        import re

        if not re.match(r"^att_[A-Za-z0-9_-]{8,64}$", attempt_id):
            raise ValueError(f"invalid attempt id {attempt_id!r}")
        path = os.path.join(self.data_dir, "attempts", attempt_id)
        os.makedirs(path, exist_ok=True)
        os.makedirs(os.path.join(path, "inputs"), exist_ok=True)
        return path

    @property
    def journal_path(self) -> str:
        return os.path.join(self.data_dir, "journal.sqlite3")

    @property
    def lock_path(self) -> str:
        return os.path.join(self.data_dir, "worker.lock")

    @property
    def update_marker_path(self) -> str:
        return os.path.join(self.data_dir, "update-complete.json")
