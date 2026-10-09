"""Bake isolated official audio runtimes in CI. Never called on a rented Pod."""
import json
from importlib.metadata import version
from pathlib import Path
import subprocess
import sys

from shared.cuda_audio import UPSTREAM

ROOT = Path('/opt/h3-audio')
REQUIREMENTS = {
    'qwen_tts': ['transformers==4.57.3','accelerate==1.12.0'],
    'qwen_align': ['transformers==4.57.6','accelerate==1.12.0'],
    # A minimal inference import chain, not the upstream training/TRT requirements.
    # Matcha's fixed decoder imports diffusers and its logger imports lightning.
    'cosyvoice3': ['transformers==4.51.3','onnxruntime-gpu==1.30.0','conformer==0.3.2',
        'diffusers==0.35.1','HyperPyYAML==1.2.3','inflect==7.3.1','librosa==0.10.2',
        'lightning==2.2.4','modelscope==1.40.2','omegaconf==2.3.0','openai-whisper==20250625',
        'soundfile==0.12.1','x-transformers==2.11.24','wetext==0.1.8'],
}
MATCHA = 'dd9105b34bf2be2230f4aa1e4769fb586a3c824e'


def command(argv, **kwargs):
    subprocess.run([str(a) for a in argv], check=True, **kwargs)


def checkout(repo, commit, target, paths=None):
    target.mkdir(parents=True)
    command(['git','init',target]);command(['git','remote','add','origin',repo],cwd=target)
    command(['git','fetch','--depth','1','--filter=blob:none','origin',commit],cwd=target)
    if paths:
        command(['git','sparse-checkout','init','--cone'],cwd=target)
        command(['git','sparse-checkout','set',*paths],cwd=target)
    command(['git','checkout','--detach','FETCH_HEAD'],cwd=target)
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=target,text=True).strip()==commit


def main():
    ROOT.mkdir(parents=True,exist_ok=True)
    constraints = ROOT/'shared-torch.txt'
    constraints.write_text(''.join(name+'=='+version(name)+'\n' for name in ('torch','torchaudio','torchvision')))
    for runtime,(repo,commit) in UPSTREAM.items():
        target = ROOT/runtime
        command([sys.executable,'-m','venv','--system-site-packages',target])
        source = ROOT/(runtime+'-source')
        checkout(repo,commit,source,['cosyvoice'] if runtime=='cosyvoice3' else None)
        python = target/'bin/python'
        command([python,'-m','pip','install','--no-cache-dir','-c',constraints,*REQUIREMENTS[runtime]])
        if runtime=='cosyvoice3':
            matcha=source/'third_party/Matcha-TTS'
            checkout('https://github.com/shivammehta25/Matcha-TTS.git',MATCHA,matcha,['matcha'])
            site = subprocess.check_output([python,'-c','import sysconfig; print(sysconfig.get_path("purelib"))'],text=True).strip()
            (Path(site)/'h3-cosyvoice.pth').write_text(str(source)+'\n'+str(matcha)+'\n')
        else:
            # Keep official metadata so pip check checks their complete dependency sets.
            command([python,'-m','pip','install','--no-cache-dir','-c',constraints,source])
        command([python,'-m','pip','check'])
        (target/'provenance.json').write_text(json.dumps({'repository':repo,'commit':commit,
            'torch_version':version('torch'),'matcha_commit':MATCHA if runtime=='cosyvoice3' else None}))


if __name__=='__main__':main()
