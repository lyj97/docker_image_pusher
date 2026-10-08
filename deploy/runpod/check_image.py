"""Offline image checks: no model download, server boot, or GPU allocation."""
import json
import os
from pathlib import Path
import subprocess

from shared.execution import profiles, profile_by_id
import torch

for name in ('RUNPOD_API_KEY','GITHUB_TOKEN','GH_TOKEN','H3POD_TOKEN',
             'H3WORKER_WORKER_TOKEN','AWS_SECRET_ACCESS_KEY','ALIYUN_REGISTRY_PASSWORD'):
    assert not os.environ.get(name), 'credential baked into image environment'

for directory in ('/opt/h3-service','/root/.ssh','/root/.aws','/root/.docker'):
    root=Path(directory)
    if not root.exists():
        continue
    for path in root.rglob('*'):
        assert not (path.is_file() and (path.name.startswith('.env')
            or path.suffix in ('.pem','.key','.sqlite','.sqlite3')
            or path.name in ('id_rsa','id_ed25519','credentials','config.json','executor.env','controller.env'))), 'credential or runtime data file in image'

assert torch.__version__ == profiles()[0]['torch_version']
assert os.environ.get('H3POD_BAKED_NODES_REQUIRED') == '1'
nodes = {n['name']:n for p in profiles() for n in p['nodes']['nodes']}
for name, node in nodes.items():
    root = Path('/opt/comfyui-baked/custom_nodes') / name
    assert json.loads((root / '.h3-managed-node.json').read_text()) == node
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip() == node['commit']
    assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=root,text=True).strip()
    if node['repository'] == 'https://github.com/scraed/LanPaint.git':
        assert not (root/'examples').exists(), 'demo assets in runtime image'
        assert subprocess.check_output(['git','config','--get','remote.origin.partialclonefilter'],cwd=root,text=True).strip() == 'blob:none'
profile = profile_by_id(os.environ.get('H3POD_PROFILE_ID', profiles()[0]['profile_id']))
assert profile['models']['models'] and profile['workflow_template']['graph']
print(json.dumps({'profile_id':profile['profile_id'], 'baked_nodes':list(nodes),
                  'torch_version':torch.__version__, 'gpu_generation_tested':False}))
