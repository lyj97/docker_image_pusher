"""Boot the exact copied Comfy tree on CPU, without model downloads or tasks."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen

from h3burst.engine import copy_baked_core
from shared.execution import profiles

with tempfile.TemporaryDirectory() as folder:
    root = Path(folder) / 'ComfyUI'
    copy_baked_core('/opt/comfyui-baked', root)
    for name in ('models', 'custom_nodes', 'input', 'output', 'user', 'temp'):
        (root/name).mkdir(exist_ok=True)
    nodes = {n['name'] for p in profiles() for n in p['nodes']['nodes']}
    for name in nodes:
        shutil.copytree(Path('/opt/comfyui-baked/custom_nodes')/name, root/'custom_nodes'/name)
    args = [sys.executable, '-B', str(root/'main.py'), '--cpu', '--listen', '127.0.0.1',
            '--port', '8188', '--disable-auto-launch', '--disable-partner-nodes', '--cache-none',
            '--disable-all-custom-nodes']
    if nodes:
        args += ['--whitelist-custom-nodes', *sorted(nodes)]
    with (Path(folder)/'boot.log').open('wb') as log:
        process = subprocess.Popen(args, cwd=root, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError('copied Comfy core exited before readiness')
                try:
                    with urlopen('http://127.0.0.1:8188/system_stats', timeout=2) as response:
                        stats = json.load(response)
                    with urlopen('http://127.0.0.1:8188/object_info', timeout=2) as response:
                        classes = json.load(response)
                    break
                except OSError:
                    time.sleep(.2)
            else:
                raise RuntimeError('copied Comfy core readiness timed out')
            assert stats['system']['comfyui_version'] == profiles()[0]['comfy_version']
            required = {n['class_type'] for p in profiles() for n in p['workflow_template']['graph'].values()}
            assert not required - classes.keys(), 'required task node classes missing'
            assert not list((root/'models').rglob('*.safetensors'))
            print('Copied Comfy core CPU boot and required task nodes passed; no models or generation')
        except Exception:
            print((Path(folder)/'boot.log').read_text(errors='replace'))
            raise
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait()
