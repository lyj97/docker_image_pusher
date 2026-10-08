"""Pod-local trusted node installer, independent of the H3 Worker control loop.

The manifest is an operator-owned deployment artifact, never a task payload.
Prepared does not mean ComfyUI-ready: the supervisor must still probe classes.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time


def manifest_nodes(manifest):
    if not isinstance(manifest, dict) or set(manifest) != {'schema_version', 'nodes'} \
            or type(manifest['schema_version']) is not int or manifest['schema_version'] != 1:
        raise ValueError('invalid node manifest')
    if not isinstance(manifest['nodes'], list) or len(manifest['nodes']) > 32:
        raise ValueError('invalid nodes')
    names = set()
    for node in manifest['nodes']:
        if not isinstance(node, dict) or set(node) != {'name', 'repository', 'commit', 'requirements', 'classes'}:
            raise ValueError('invalid node fields')
        if not isinstance(node['name'], str) or not re.fullmatch('[A-Za-z0-9_-]{1,64}', node['name']) \
                or node['name'] in names:
            raise ValueError('invalid/duplicate node name')
        names.add(node['name'])
        if not isinstance(node['repository'], str) or not re.fullmatch(
                r'https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:\.git)?', node['repository']):
            raise ValueError('only public, credential-free GitHub repository URLs are supported')
        if not isinstance(node['commit'], str) or not re.fullmatch('[0-9a-f]{40}', node['commit']):
            raise ValueError('node commit must be exact')
        if node['requirements'] not in (None, 'requirements.txt'):
            raise ValueError('only requirements.txt is supported')
        if not isinstance(node['classes'], list) or not node['classes'] \
                or any(not isinstance(c, str) or not re.fullmatch('[A-Za-z0-9_]{1,128}', c)
                       for c in node['classes']):
            raise ValueError('required ComfyUI classes must be declared')
    return manifest['nodes']


def atomic_json(path, value):
    temp = path.with_name(path.name + '.pending')
    with temp.open('w') as file:
        json.dump(value, file, sort_keys=True)
        file.flush()
        os.fsync(file.fileno())
    temp.replace(path)


def safe_root(path):
    if not path.is_absolute() or any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('installation paths must be absolute without symlinks')


def terminate(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    except ProcessLookupError:
        process.wait()


def command(argv, cwd, log, progress, node, *, timeout=900, stall=300, cancel_event=None):
    started = last = time.monotonic()
    env = dict(os.environ, GIT_TERMINAL_PROMPT='0', PYTHONUNBUFFERED='1',
               PIP_DISABLE_PIP_VERSION_CHECK='1', PIP_NO_INPUT='1')
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                for key, _ in selector.select(timeout=1):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if chunk:
                        last = time.monotonic()
                        log.write(chunk)
                        log.flush()
                        atomic_json(progress, {'state': 'preparing', 'node': node,
                            'stage': argv[0], 'last_progress_at': time.time()})
                    else:
                        selector.unregister(key.fileobj)
                if not selector.get_map() and process.poll() is not None:
                    break
                if cancel_event is not None and cancel_event.is_set():
                    raise RuntimeError('preparation cancelled by drain')
                now = time.monotonic()
                if now - started > timeout or now - last > stall:
                    raise RuntimeError('node preparation timed out or stalled')
            if process.wait() != 0:
                raise RuntimeError('node preparation command failed; see raw log')
    except BaseException:
        terminate(process)
        raise
    finally:
        process.stdout.close()


def prepare(manifest, root, state):
    nodes = manifest_nodes(manifest)
    safe_root(root)
    safe_root(state)
    if not (root / 'main.py').is_file():
        raise ValueError('ComfyUI root does not contain main.py')
    destination = root / 'custom_nodes'
    safe_root(destination)
    destination.mkdir(exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    # Serialize preparations, including independent supervisors.
    import fcntl
    with (state / 'prepare.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stamp = state / 'nodes-prepared.json'
        # Existing readiness cannot survive a newly requested preparation.
        if stamp.exists():
            stamp.replace(state / 'nodes-prepared.previous.json')
        progress = state / 'progress.json'
        atomic_json(progress, {'state': 'preparing', 'started_at': time.time()})
        with (state / 'node-install.log').open('ab') as log:
            try:
                # Preserve the image's CUDA-compatible torch packages.
                constraints = state / 'torch-constraints.txt'
                from importlib.metadata import version
                constraints.write_text(''.join(name + '==' + version(name) + '\n'
                                       for name in ('torch', 'torchvision', 'torchaudio')))
                for node in nodes:
                    target = destination / node['name']
                    safe_root(target)
                    if target.exists():
                        # Never overwrite an operator checkout or unknown directory.
                        marker = target / '.h3-managed-node.json'
                        if not marker.is_file() or marker.is_symlink() \
                                or json.loads(marker.read_text()) != node:
                            raise ValueError('existing node has no matching managed manifest')
                        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=target,
                                                       timeout=10, text=True).strip()
                        dirty = subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'],
                                                        cwd=target, timeout=10, text=True).strip()
                        if head != node['commit'] or dirty:
                            raise ValueError('managed node checkout changed')
                    else:
                        # A failed staging directory is retained for diagnosis;
                        # each retry gets a unique directory rather than deleting it.
                        import tempfile
                        staging_root = root / '.h3-node-staging'
                        safe_root(staging_root)
                        staging_root.mkdir(exist_ok=True)
                        staging = Path(tempfile.mkdtemp(prefix=node['name'] + '-', dir=staging_root))
                        run = lambda argv: command(argv, staging, log, progress, node['name'])
                        run(['git', 'init'])
                        run(['git', 'remote', 'add', 'origin', node['repository']])
                        run(['git', 'fetch', '--depth', '1', 'origin', node['commit']])
                        run(['git', 'checkout', '--detach', node['commit']])
                        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=staging,
                                                       timeout=10, text=True).strip()
                        if head != node['commit']:
                            raise ValueError('downloaded commit mismatch')
                        atomic_json(staging / '.h3-managed-node.json', node)
                        staging.rename(target)
                    if node['requirements']:
                        requirement = target / 'requirements.txt'
                        if not requirement.is_file() or requirement.is_symlink():
                            raise ValueError('requirements.txt missing or symlinked')
                        command([sys.executable, '-m', 'pip', 'install', '--constraint',
                                 str(constraints), '-r', str(requirement)], target,
                                log, progress, node['name'])
                command([sys.executable, '-m', 'pip', 'check'], root, log, progress, 'all')
                atomic_json(stamp, {'state': 'nodes_prepared_not_ready', 'manifest_sha256':
                    hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
                    'required_classes': sorted({c for n in nodes for c in n['classes']}),
                    'prepared_at': time.time()})
                atomic_json(progress, {'state': 'nodes_prepared_not_ready'})
            except BaseException:
                atomic_json(progress, {'state': 'failed', 'stop_required': True})
                raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--comfy-root', type=Path, required=True)
    parser.add_argument('--state', type=Path, required=True)
    args = parser.parse_args()
    try:
        prepare(json.loads(args.manifest.read_text()), args.comfy_root, args.state)
    except Exception:
        print('Node preparation failed; do not advertise readiness. Inspect progress/raw log.', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
