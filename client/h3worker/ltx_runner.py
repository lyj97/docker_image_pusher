"""Dependency-free parent boundary; upstream imports occur only in dedicated child."""
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path('/Users/Shared/h3-ltx2')
RUNTIME = ROOT / 'runtime'
PYTHON = RUNTIME / 'venv/bin/python'
MODEL = ROOT / 'models/ltx-2.5-mlx-q8'
MARKER = ROOT / 'acceptance.json'
SERVICE_ROOT = Path(__file__).resolve().parents[2]
if str(SERVICE_ROOT) not in sys.path: sys.path.insert(0, str(SERVICE_ROOT))
from shared import ltx_policy as policy


def environment():
    return {'PATH':str(RUNTIME/'venv/bin') + ':/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin', 'HOME':str(ROOT),
            'HF_HUB_OFFLINE':'1', 'TRANSFORMERS_OFFLINE':'1', 'PYTHONNOUSERSITE':'1'}


def cli_args(request, inputs, output):
    problems = policy.validate(request)
    if problems: raise ValueError('; '.join(problems))
    paths = {r['role']: inputs['ref:' + r['asset_id']] for r in request['references']}
    for path in [*paths.values(), output]:
        if not os.path.isabs(path) or '\x00' in path: raise ValueError('LTX paths must be absolute')
    return ['keyframe','--model',str(MODEL),'--prompt=' + request['prompt'],
            '--start',paths['start'],'--end',paths['end'],'--height','480','--width','704',
            '--frames',str(request['generation']['frames']),'--frame-rate','24',
            '--dev-transformer','transformer-dev.safetensors','--low-ram',
            '--seed',request['seed'],'--output',output]


def build_command(request, inputs, output, request_path):
    # Worker-owned paths may use the existing relative data_dir convention.
    inputs = {key: os.path.abspath(path) for key,path in inputs.items()}
    output = os.path.abspath(output)
    request_path = os.path.abspath(request_path)
    cli_args(request, inputs, output)
    Path(request_path).write_text(json.dumps(dict(request=request,inputs=inputs,output=output)))
    return [str(PYTHON), '-I', '-B', str(Path(__file__).resolve()), '--request', request_path]


def sha256(path):
    h = hashlib.sha256()
    with open(path,'rb') as f:
        for block in iter(lambda:f.read(8*1024*1024), b''): h.update(block)
    return h.hexdigest()


def file_identity(path):
    s = path.stat()
    return [s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns]


def hardware():
    if platform.system() != 'Darwin' or platform.machine() != 'arm64': return None
    chip = subprocess.check_output(['/usr/sbin/sysctl','-n','machdep.cpu.brand_string'], timeout=5).decode().strip()
    memory = int(subprocess.check_output(['/usr/sbin/sysctl','-n','hw.memsize'], timeout=5))
    version = subprocess.check_output(['/usr/bin/sw_vers','-productVersion'], timeout=5).decode().strip()
    if chip != 'Apple M5 Pro' or memory < 64 * 1024**3 or version != '26.5.1': return None
    return dict(chip=chip, memory_bytes=memory, macos=version)


class Readiness:
    """Hash once per boot/identity change, then stat; call only in background thread."""
    def __init__(self): self.cached = None

    def check(self, *, hash_files=True):
        try:
            if any(p.is_symlink() for p in (ROOT, RUNTIME, MODEL, MARKER, PYTHON)): return False
            hw = hardware()
            if not hw or MARKER.stat().st_size > 16*1024*1024: return False
            marker = json.loads(MARKER.read_text())
            if marker['provenance'] != policy.PROVENANCE or marker['hardware'] != hw or marker['accepted'] is not True: return False
            smoke = marker['smoke']
            if (smoke['generation'] != policy.DEFAULT_GENERATION or smoke['has_audio'] is not True
                    or type(smoke['elapsed_seconds']) not in (int,float)
                    or not math.isfinite(smoke['elapsed_seconds']) or not 0 < smoke['elapsed_seconds'] <= 86400): return False
            media = smoke['media']
            if (any(media.get(k) != v for k,v in policy.DEFAULT_GENERATION.items())
                    or media.get('has_audio') is not True
                    or not math.isfinite(media['duration_seconds'])
                    or abs(media['duration_seconds'] - 49/24) > .15): return False
            files = marker['files']
            if not isinstance(files,dict) or not 10 <= len(files) <= 100000: return False
            inventory = marker['model_inventory']
            if not isinstance(inventory, dict) or len(inventory) < 2: return False
            for name, spec in inventory.items():
                entry = files.get('models/ltx-2.5-mlx-q8/' + name, {})
                if any(entry.get(k) != spec.get(k) for k in ('sha256','size_bytes')): return False
            names = set(files)
            if not {'models/ltx-2.5-mlx-q8/transformer-dev.safetensors',
                    'models/ltx-2.5-mlx-q8/transformer-distilled.safetensors',
                    'runtime/venv/bin/python','runtime/source/.git/HEAD'} <= names: return False
            # A newly inserted .pth/module would bypass checking only old files.
            runtime_names = {str(p.relative_to(ROOT)) for p in RUNTIME.rglob('*')
                             if p.is_file()}
            if runtime_names != {name for name in files if name.startswith('runtime/')}: return False
            identities = {}
            for name,spec in files.items():
                path = ROOT / name
                if Path(name).is_absolute() or '..' in Path(name).parts or not path.resolve().is_relative_to(ROOT.resolve()): return False
                identity = file_identity(path)
                if identity != spec['identity'] or identity[2] != spec['size_bytes']: return False
                identities[name] = identity
            key = [file_identity(MARKER), identities]
            if hash_files and key != self.cached:
                source = RUNTIME / 'source'
                commit = subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],timeout=10).decode().strip()
                if commit != policy.RUNTIME_COMMIT: return False
                # --no-optional-locks avoids mutating Git index stat metadata.
                dirty = subprocess.check_output(['git','--no-optional-locks','-C',str(source),'status','--porcelain'],timeout=10)
                if dirty: return False
                versions = subprocess.check_output([str(PYTHON),'-I','-B','-c',
                    'from importlib.metadata import version; print(version("ltx-core-mlx"), version("ltx-pipelines-mlx"))'],
                    timeout=30,env=environment()).decode().strip()
                if versions != '0.15.12 0.15.12': return False
                for name,spec in files.items():
                    if sha256(ROOT/name) != spec['sha256']: return False
                self.cached = key
            if file_identity(MARKER) != key[0]: return False
            if any(file_identity(ROOT/name) != identity for name,identity in identities.items()): return False
            return True
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            self.cached = None
            return False


def probe(path):
    from fractions import Fraction
    data = json.loads(subprocess.check_output(['/usr/bin/env','ffprobe','-v','error','-count_frames',
        '-show_streams','-show_format','-of','json',path], timeout=30, env=environment()))
    streams = data['streams']; v = next(s for s in streams if s['codec_type']=='video')
    return dict(width=int(v['width']),height=int(v['height']),frames=int(v['nb_read_frames']),
                fps=float(Fraction(v['avg_frame_rate'])),has_audio=any(s['codec_type']=='audio' for s in streams),
                duration_seconds=float(data['format']['duration']))


def child_main():
    import argparse
    parser = argparse.ArgumentParser(); parser.add_argument('--request',required=True)
    args = parser.parse_args()
    try:
        payload = json.loads(Path(args.request).read_text())
        if not Readiness().check(hash_files=False): raise RuntimeError('MODEL_UNAVAILABLE: LTX acceptance evidence invalid')
        argv = cli_args(payload['request'],payload['inputs'],payload['output'])
        # The pinned CLI runs in this process; existing Worker owns its process group.
        from ltx_pipelines_mlx.cli import main
        sys.argv = ['ltx-2-mlx', *argv]
        print(json.dumps(dict(type='phase',phase='loading')),flush=True)
        main()
        result = probe(payload['output'])
        g = payload['request']['generation']
        if any(result[k] != g[k] for k in g) or not result['has_audio']: raise RuntimeError('LTX output structural mismatch')
        print(json.dumps(dict(type='result',result={**g, 'duration_seconds':result['duration_seconds']})),flush=True)
        return 0
    except Exception as exc:
        print(json.dumps(dict(type='error',message=str(exc)[:1024])),flush=True)
        return 1

if __name__ == '__main__': sys.exit(child_main())
