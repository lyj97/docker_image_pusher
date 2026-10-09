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
        source = root / 'custom_nodes' / n['name'] / 'src/LanPaint/nodes.py'
        classes = {v.name for v in ast.walk(ast.parse(source.read_text())) if isinstance(v, ast.ClassDef)}
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
print(json.dumps({'static_contracts':'passed','cpu_encoding':False,'cpu_decoding':False,
    'cpu_sampling':False,'model_download':False,'gpu_inference':False,
    'unverified':['real VAE latent shapes','mask edit semantics','GPU generation']}))
