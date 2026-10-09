"""Offline image checks: no model download, server boot, or GPU allocation."""
import json
import os
from pathlib import Path
import subprocess
import shutil

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

from importlib.metadata import version, PackageNotFoundError
from packaging.requirements import Requirement

# pip check does not validate a checked-out source tree's requirements.
for line in Path('/opt/comfyui-baked/requirements.txt').read_text().splitlines():
    line=line.split('#',1)[0].strip()
    if not line:continue
    requirement=Requirement(line)
    if requirement.marker is not None and not requirement.marker.evaluate():continue
    try:installed=version(requirement.name)
    except PackageNotFoundError:raise AssertionError('missing core requirement: '+requirement.name) from None
    assert installed in requirement.specifier, 'incompatible core requirement: '+str(requirement)+'; installed '+installed

assert torch.__version__ == profiles()[0]['torch_version']
assert os.environ.get('H3POD_BAKED_NODES_REQUIRED') == '1'
assert Path('/usr/sbin/sshd').is_file() and shutil.which('ssh-keygen')
assert not list(Path('/etc/ssh').glob('ssh_host_*')), 'SSH host keys baked into image'
subprocess.run(['/bin/sh', '-n', '/opt/h3-service/start-ssh.sh'], check=True)
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

import hashlib
adapter=Path('/opt/comfyui-baked/custom_nodes/H3AVContract/__init__.py')
assert hashlib.sha256(adapter.read_bytes()).hexdigest() == profiles()[1]['runtime_adapter']['source_sha256']
import runpy
assert 'H3AVMaskPrepare' in runpy.run_path(str(adapter))['NODE_CLASS_MAPPINGS']
