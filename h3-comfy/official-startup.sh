#!/usr/bin/env bash
# Run inside the official ComfyUI image. An external controller stops the Pod.
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
storage_root="${H3_STORAGE_ROOT:-/workspace}"
comfy_dir="${H3_COMFY_DIR:-/workspace/runpod-slim/ComfyUI}"
run_root="${H3_RUN_ROOT:-/workspace/h3-runs}"
run_dir="$run_root/$(date -u +%Y%m%dT%H%M%SZ)-$$"
base_pid=""
# Reject Global/FUSE before the official script tries to install onto it.
python3 - "$storage_root" "$comfy_dir" "$run_root" <<'PY'
from pathlib import Path
import sys
for line in Path('/proc/mounts').read_text().splitlines():
    fields = line.split()
    if len(fields) >= 3 and 'fuse' in fields[2]:
        if any(Path(p).resolve().is_relative_to(Path(fields[1])) for p in sys.argv[1:]):
            raise SystemExit('This startup requires local paths, not Global/FUSE')
PY
mkdir -p "$run_dir"
export H3_STORAGE_ROOT="$storage_root" H3_COMFY_DIR="$comfy_dir"
export H3_METRICS_DIR="$run_dir/download-metrics"
printf '%s\n' "$run_dir" > "$run_root/current-run.txt"
finish() {
  result=$?
  trap - EXIT
  if [[ -f "$comfy_dir/user/comfyui_8188.log" ]]; then
    cp "$comfy_dir/user/comfyui_8188.log" "$run_dir/comfyui.log" || true
  fi
  if [[ $result -eq 0 || $result -eq 130 || $result -eq 143 ]]; then
    printf 'STOPPED\n' > "$run_dir/status.txt"
    echo "H3_STOPPED $run_dir exit=$result"
  else
    printf 'FAILED exit=%s\n' "$result" > "$run_dir/status.txt"
    printf 'STOP_REQUIRED\n' > "$run_dir/stop-required.txt"
    echo "H3_FAILED $run_dir exit=$result STOP_REQUIRED"
  fi
  # Stop workload processes; this does not stop RunPod billing.
  if [[ -n "$base_pid" ]]; then kill "$base_pid" 2>/dev/null || true; fi
  exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'RUNNING\n' > "$run_dir/status.txt"
test -n "${PUBLIC_KEY:-}" || { echo 'Set the RunPod account SSH public key.' >&2; exit 1; }
test -x /start.sh
# Native H3 graph uses core nodes. Avoid unrelated custom-node startup/installers.
mkdir -p /workspace/runpod-slim
args_file=/workspace/runpod-slim/comfyui_args.txt
if [[ -f "$args_file" ]]; then cp "$args_file" "$run_dir/comfyui_args.before.txt"; fi
printf '%s\n' '--disable-all-custom-nodes' '--disable-partner-nodes' '--disable-auto-launch' '--cache-none' > "$args_file"
/start.sh > "$run_dir/base-start.log" 2>&1 &
base_pid=$!
export H3_READY_TIMEOUT="${H3_READY_TIMEOUT:-600}"
python3 - "$base_pid" <<'PY'
import json, os, sys, time
from urllib.request import urlopen
deadline = time.monotonic() + float(os.environ['H3_READY_TIMEOUT'])
while time.monotonic() < deadline:
    os.kill(int(sys.argv[1]), 0)
    try:
        with urlopen('http://127.0.0.1:8188/system_stats', timeout=5) as r:
            if r.status == 200:
                version = json.load(r).get('system', {}).get('comfyui_version')
                expected = os.environ.get('H3_COMFY_VERSION', '0.39.0')
                if version != expected:
                    raise SystemExit(f'ComfyUI version mismatch: expected {expected}, got {version}')
                break
    except OSError:
        pass
    time.sleep(2)
else:
    raise SystemExit('ComfyUI readiness timed out')
PY
bash "$script_dir/prepare_pod.sh" 2>&1 | tee "$run_dir/download.log"
# Read-only node/model checks; never enqueue a generation during startup.
python3 -B "$script_dir/benchmark.py" --check-only \
  2>&1 | tee "$run_dir/readiness.log"
printf 'READY\n' > "$run_dir/status.txt"
echo "H3_READY $run_dir; waiting for user generation requests"
# The official process owns ComfyUI. Stay alive until it ends or the Pod is stopped.
wait "$base_pid"
echo 'Official ComfyUI startup process exited unexpectedly.' >&2
exit 1
