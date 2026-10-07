#!/usr/bin/env bash
# Default: each new Pod downloads models to local disk; no persistent volume required.
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
storage_root="${H3_STORAGE_ROOT:-/workspace}"
comfy_dir="${H3_COMFY_DIR:-/workspace/runpod-slim/ComfyUI}"
# Inspect paths before downloading; refuse to overwrite independent local models.
if [[ -d "$comfy_dir/models" ]]; then
  python3 - "$script_dir/models.lock.json" "$storage_root" "$comfy_dir" <<'PY'
import json, sys
from pathlib import Path
manifest, storage, comfy = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
planned = []
for model in json.loads(manifest.read_text())['models']:
    link = comfy / 'models' / model['target']
    destination = storage / 'h3/models' / model['target']
    old_global = Path('/workspace-global/h3/models') / model['target']
    if link.is_symlink():
        actual = link.readlink()
        if actual not in (destination, old_global):
            raise SystemExit('Unexpected existing model link: ' + str(link))
    elif link.exists():
        raise SystemExit('Refusing to overwrite existing model: ' + str(link))
    planned.append((link, destination))
for link, destination in planned:
    link.parent.mkdir(parents=True, exist_ok=True)
    if link.is_symlink():
        link.unlink()
    link.symlink_to(destination)
print('Official ComfyUI model paths configured for local downloads')
PY
fi
python3 "$script_dir/prepare_models.py" --local --storage-root "$storage_root" \
  --metrics-dir "${H3_METRICS_DIR:-/tmp/h3-download-metrics}"
echo 'Local model download complete. Preserve results/logs before deleting the Pod.'
