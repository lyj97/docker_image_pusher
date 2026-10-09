"""Boot the exact copied Comfy tree on CPU, without model downloads or tasks."""
import copy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from h3burst.engine import copy_baked_core
from shared.execution import profiles
from h3worker.comfy_runner import bind_graph, cleanup_bound_inputs

def check_lanpaint_inputs(root, folder):
    profile = next(p for p in profiles() if 'lanpaint' in p['profile_id'])
    workflow = copy.deepcopy(profile['workflow_template'])
    task = {'workflow': workflow, 'references': []}
    video = Path(folder) / 'synthetic.mp4'
    image = Path(folder) / 'synthetic.png'
    subprocess.run(['ffmpeg', '-v', 'error', '-f', 'lavfi', '-i', 'color=size=256x256:rate=24',
        '-frames:v', '39', '-c:v', 'libx264', '-threads', '1', str(video)], check=True, timeout=30)
    subprocess.run(['ffmpeg', '-v', 'error', '-i', str(video), '-frames:v', '1',
        '-threads', '1', str(image)], check=True, timeout=30)
    inputs = {}
    for i, (_, content_type) in enumerate(profile['input_slots']):
        asset = 'as_ci_' + str(i)
        task['references'].append({'asset_id': asset, 'content_type': content_type})
        workflow['bindings'][i]['asset_id'] = asset
        inputs['ref:' + asset] = str(video if content_type.startswith('video/') else image)
    namespace = 'h3_' + 'c' * 32
    graph = bind_graph(task, inputs, root, namespace)
    try:
        with urlopen('http://127.0.0.1:8188/object_info/LanPaint_VideoMaskEditor', timeout=10) as response:
            options = json.load(response)['LanPaint_VideoMaskEditor']['input']['required']['video'][0]
        video_nodes = [key for key, node in graph.items() if node['class_type'] == 'LanPaint_VideoMaskEditor']
        assert len(video_nodes) == 3
        assert all(graph[key]['inputs']['video'] in options for key in video_nodes)
        def validate(candidate):
            request = Request('http://127.0.0.1:8188/prompt', data=json.dumps({'prompt': candidate}).encode(),
                headers={'Content-Type': 'application/json'})
            try:
                with urlopen(request, timeout=30) as response:
                    raise AssertionError('weight-free validation unexpectedly queued inference')
            except HTTPError as error:
                assert error.code == 400
                return json.load(error)['node_errors']
        errors = validate(graph)
        assert not set(video_nodes) & errors.keys(), errors
        # Only absent model dropdowns may fail; every real material node must validate.
        assert errors and all(graph[key]['class_type'] in
            {'UNETLoader', 'CLIPLoader', 'VAELoader', 'ModelPatchLoader'} for key in errors), errors
        old = copy.deepcopy(graph)
        for key in video_nodes:
            old[key]['inputs']['video'] = namespace + '/' + old[key]['inputs']['video'].removeprefix(namespace + '_')
        assert set(video_nodes) <= validate(old).keys(), 'negative control failed to detect old subdirectory bug'
        with urlopen('http://127.0.0.1:8188/queue', timeout=10) as response:
            queue = json.load(response)
        assert queue['queue_running'] == [] and queue['queue_pending'] == []
        print('Real pinned LanPaint node and full prompt validation passed for three bound videos; old paths rejected; no inference')
    finally:
        cleanup_bound_inputs(root, namespace)
    assert list((root / 'input').iterdir()) == []


with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
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
            check_lanpaint_inputs(root, folder)
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
