"""Static pinned-source contracts only: no CPU media/model/encoding/inference."""
import ast
import hashlib
import json
from pathlib import Path
import subprocess
from shared.execution import profiles, execution_workflow

root = Path('/opt/comfyui-baked')
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip() == profiles()[0]['comfy_commit']
for p in profiles():
    for n in p['nodes']['nodes']:
        sources = (root / 'custom_nodes' / n['name']).rglob('*.py')
        classes = {v.name for source in sources for v in ast.walk(ast.parse(source.read_text())) if isinstance(v, ast.ClassDef)}
        assert set(n['classes']) <= classes
adapter = root / 'custom_nodes/H3AVContract/__init__.py'
assert hashlib.sha256(adapter.read_bytes()).hexdigest() == profiles()[1]['runtime_adapter']['source_sha256']
compile(adapter.read_text(),str(adapter),'exec')
p = profiles()[1]
business = {'mode':'comfyui_video', 'workflow':p['workflow_template']}
graph = execution_workflow(business,p)['graph']
assert graph['h3avmask'] == {'class_type':'H3AVMaskPrepare','inputs':{'latent':['30',0]}}
assert graph['10']['class_type'] == 'SamplerCustomAdvanced'
assert graph['10']['inputs']['latent_image'] == ['h3avmask',0]
assert business['workflow']['graph']['10']['inputs']['latent_image'] == ['30',0]
reference_source = root / 'comfy_extras/nodes_minimax_h3.py'
tree = ast.parse(reference_source.read_text())
reference_class = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'MiniMaxH3ReferenceToVideo')
prefixes = {}
for call in ast.walk(reference_class):
    if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == 'TemplatePrefix':
        fields = {k.arg: k.value for k in call.keywords}
        prefixes[ast.literal_eval(fields['prefix'])] = (ast.literal_eval(fields['min']), ast.literal_eval(fields['max']))
assert prefixes == {'ref_image_': (0, 9), 'ref_video_': (0, 3),
                    'ref_video_audio_': (0, 3), 'ref_audio_': (0, 3)}
# Prompt dynamic keys are flattened by the pinned V3 IO layer, not execute() dicts.
io_source = (root / 'comfy_api/latest/_io.py').read_text()
assert 'def finalize_prefix' in io_source and 'build_nested_inputs' in io_source
for p in profiles():
    if not p.get('parameterized_native'):
        continue
    task = {'mode': p['native_modes'][0], 'prompt': 'static CI contract', 'seed': '42',
            'generation': {'width':512,'height':512,'frames':56,'steps':20}, 'anchors':{}, 'references':[]}
    if task['mode'] == 'ref2va':
        task['references'] = [{'asset_id':'as_static','kind':'image'}]
    graph = execution_workflow(task,p)['graph']
    assert graph['7']['inputs']['width'] == 512 and graph['14']['inputs']['steps'] == 20
    if task['mode'] == 'ref2va':
        assert graph['7']['inputs']['ref_images.ref_image_0'] == ['input_0',0]
print(json.dumps({'static_contracts':'passed','cpu_encoding':False,'cpu_decoding':False,
    'cpu_sampling':False,'model_download':False,'gpu_inference':False,
    'unverified':['real VAE latent shapes','mask edit semantics','GPU generation']}))
