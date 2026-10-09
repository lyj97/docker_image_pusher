"""CI-only node checkout: keep pinned runtime code, omit LanPaint demos."""
import json
import shutil
from pathlib import Path
import subprocess

from shared.execution import profiles
from h3burst.prepare import manifest_nodes

comfy_root=Path('/opt/comfyui-baked')
commits={p['comfy_commit'] for p in profiles()}
if len(commits) != 1:raise ValueError('profiles require different ComfyUI commits')
commit=next(iter(commits))
if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=comfy_root,text=True).strip():
    raise ValueError('base ComfyUI contains tracked modifications')
subprocess.run(['git','fetch','--depth','1','origin',commit],cwd=comfy_root,check=True,timeout=180)
subprocess.run(['git','checkout','--detach',commit],cwd=comfy_root,check=True,timeout=180)

nodes = {n['name']:n for p in profiles() for n in p['nodes']['nodes']}
manifest_nodes({'schema_version':1, 'nodes':list(nodes.values())})
for name, node in nodes.items():
    root = Path('/opt/comfyui-baked/custom_nodes') / name
    root.parent.mkdir(exist_ok=True)
    root.mkdir()
    def git(*args):
        subprocess.run(['git', *args],cwd=root,check=True,timeout=180)
    git('init')
    git('remote','add','origin',node['repository'])
    sparse = (node['repository'] == 'https://github.com/scraed/LanPaint.git'
              and node['commit'] == '2d7912f9a5efe5ece8de334c7ca18317b8288c39')
    fetch = ['fetch','--depth','1']
    if sparse:
        fetch.append('--filter=blob:none')
    git(*fetch,'origin',node['commit'])
    if sparse:
        git('config','core.sparseCheckout','true')
        (root/'.git/info/sparse-checkout').write_text('/*\n!/examples/\n')
    git('checkout','--detach',node['commit'])
    (root/'.h3-managed-node.json').write_text(json.dumps(node,sort_keys=True))
print(json.dumps({'baked_nodes':list(nodes),'models_downloaded':False}))

shutil.copytree('/opt/h3-service/client/comfy_h3_contract_node', '/opt/comfyui-baked/custom_nodes/H3AVContract')
