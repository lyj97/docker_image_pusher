#!/usr/bin/env bash
# Image-build only. Upgrade the official baked bundle, never a running Pod.
set -euo pipefail
commit="${1:?exact ComfyUI commit required}"
[[ "$commit" == b0b743566f65daafc423b4fea8a2fbda94b3384a ]]
baked=/opt/comfyui-baked
test -d "$baked"
build_root=$(mktemp -d)
trap 'rm -rf "$build_root"' EXIT
git init "$build_root/core"
git -C "$build_root/core" remote add origin https://github.com/Comfy-Org/ComfyUI.git
git -C "$build_root/core" fetch --depth 1 origin "$commit"
git -C "$build_root/core" checkout --detach "$commit"
# Keep the tested base-image CUDA/PyTorch packages while updating core deps.
python3 - "$build_root/torch-constraints.txt" <<'PY'
from importlib.metadata import version
from pathlib import Path
import sys
Path(sys.argv[1]).write_text(''.join(f'{p}=={version(p)}\n' for p in ('torch', 'torchvision', 'torchaudio')))
PY
PIP_CONSTRAINT="$build_root/torch-constraints.txt" python3 -m pip install --no-cache-dir \
  -r "$build_root/core/requirements.txt"
python3 -m pip check
python3 - "$build_root/core" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from comfyui_version import __version__
assert __version__ == '0.39.0', __version__
PY
python3 "$build_root/core/main.py" --help > "$build_root/cli.txt"
grep -q -- '--disable-partner-nodes' "$build_root/cli.txt"
rsync -a --delete --exclude=/.git --exclude=/models --exclude=/input \
  --exclude=/output --exclude=/user --exclude=/custom_nodes --exclude=/.venv\* \
  "$build_root/core/" "$baked/"
printf 'h3-comfy-0.39.0-%s\n' "$commit" > "$baked/.runpod-bundle-version"
# Official startup exports this file during workspace initialization/upgrades.
python3 -m pip freeze > /opt/comfyui-runtime-constraints.txt
python3 -m pip freeze > /opt/h3/requirements-resolved.txt
