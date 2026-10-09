"""Private pinned ComfyUI runtime and supervised, streaming model preparation."""
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
from urllib.parse import quote
from urllib.request import urlopen

from shared.execution import profiles, profile_by_id, digest, execution_workflow
from .pod import Refused
from .prepare import atomic_json, safe_root, terminate


def file_digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b''):
            h.update(chunk)
    return Path(path).stat().st_size, h.hexdigest()


def copy_baked_core(source, destination):
    source = Path(source)
    def ignore(directory, names):
        ignored = {'__pycache__'} & set(names)
        if Path(directory) == source:
            ignored.update(set(names) & {'custom_nodes', 'models', 'input', 'output', 'user', 'temp'})
        return ignored
    shutil.copytree(source, destination, ignore=ignore)


class Engine:
    def __init__(self, comfy_root, state_root):
        import vace_smoke
        self.comfy_root, self.state_root = Path(comfy_root), Path(state_root)
        safe_root(self.comfy_root)
        safe_root(self.state_root)
        self.api = vace_smoke.API('http://127.0.0.1:8188', timeout=3)
        self.process = None
        self.profile = profile_by_id(os.environ.get('H3POD_PROFILE_ID', profiles()[0]['profile_id']))
        self.proof = {'prepared': False, 'profile_digest': self.profile['profile_digest']}
        self.proofs = {}
        self.verified_models = {}
        self.resource_lock = threading.Lock()
        self.resource_queue = []
        self.resource_current = None
        self.resource_thread = None
        self.background_preparing = False
        self.phase = 'preparing'
        self.error = False
        self.stop_preparation = threading.Event()
        self.started_at = time.time()
        from .audio_process import AudioProcesses
        self.audio = AudioProcesses(self.state_root/'audio',self.comfy_root/'models',self.comfy_root/'output')

    def evidence(self):
        return dict(self.proof)

    def all_evidence(self):
        with self.resource_lock:
            return [dict(v) for v in self.proofs.values()]

    def request_resources(self, identities):
        known = {p['profile_digest']:p for p in profiles()}
        if (not isinstance(identities, list) or len(identities) > len(known)
                or any(identity not in known for identity in identities)):
            raise Refused('unknown_resource_profile', 400)
        if not self.proof.get('prepared') or self.stop_preparation.is_set():
            raise Refused('preparation_not_available')
        with self.resource_lock:
            for identity in identities:
                if identity not in self.proofs and identity not in self.resource_queue and identity != self.resource_current:
                    self.resource_queue.append(identity)
            if self.resource_queue and not self.background_preparing:
                self.background_preparing = True
                self.resource_thread = threading.Thread(target=self.complete_resources, daemon=True,
                    name='h3-resource-completion')
                self.resource_thread.start()
        return {'requested_profiles':identities}

    def complete_resources(self):
        known = {p['profile_digest']:p for p in profiles()}
        try:
            while not self.stop_preparation.is_set():
                with self.resource_lock:
                    if not self.resource_queue:
                        self.background_preparing = False
                        return
                    self.resource_current = self.resource_queue.pop(0)
                    profile = known[self.resource_current]
                with (self.state_root / 'preparation.log').open('ab', buffering=0) as log:
                    self.download_models(profile, log, self.state_root / 'resource-progress.json',
                        time.monotonic())
                proof = {k:profile[k] for k in ('profile_digest', 'comfy_version',
                    'comfy_commit', 'torch_version', 'models_digest', 'nodes_digest')}
                proof.update(prepared=True, gpu_vendor='nvidia', vram_bytes=self.proof['vram_bytes'])
                with self.resource_lock:
                    self.proofs[profile['profile_digest']] = proof
                    self.resource_current = None
        except Exception:
            self.error = True
            atomic_json(self.state_root / 'resource-progress.json',
                {'state':'failed','stop_required':True,'last_progress_at':time.time()})
        finally:
            with self.resource_lock:
                if self.resource_thread is threading.current_thread():
                    self.background_preparing = False
                    self.resource_current = None

    def download_models(self, profile, log, progress, started):
        models = profile['models']['models']
        missing = [m for m in models if self.verified_models.get(m['target']) != (m['sha256'],m['bytes'])]
        progress_lock = threading.Lock()
        active = {}
        def download(model):
            path = self.comfy_root / 'models' / model['target']
            safe_root(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            part = path.with_name(path.name + '.pending')
            size, h, at = 0, hashlib.sha256(), time.monotonic()
            url = 'https://huggingface.co/' + model['repo'] + '/resolve/' + model['revision'] + '/' + quote(model['source'], safe='/')
            with urlopen(url, timeout=60) as source, part.open('wb') as dest:
                while chunk := source.read(8 * 1024**2):
                    if self.stop_preparation.is_set():
                        raise RuntimeError('preparation cancelled')
                    size += len(chunk)
                    if size > model['bytes']:raise RuntimeError('model exceeds pinned size')
                    h.update(chunk);dest.write(chunk)
                    with progress_lock:
                        active[model['target']] = {'bytes':size,'total_bytes':model['bytes']}
                        atomic_json(progress, {'state':'preparing','profile_digest':profile['profile_digest'],
                            'model':model['target'],'bytes':size,'total_bytes':model['bytes'],
                            'models':dict(active),'last_progress_at':time.time()})
                dest.flush();os.fsync(dest.fileno())
            if size != model['bytes'] or h.hexdigest() != model['sha256']:
                raise RuntimeError('model download integrity failure')
            os.replace(part,path)
            with progress_lock:
                self.verified_models[model['target']] = (model['sha256'],model['bytes'])
                log.write((json.dumps({'profile_digest':profile['profile_digest'], 'model':model['target'],
                    'bytes':size,'seconds':time.monotonic()-at,'streaming_sha256':h.hexdigest()})+'\n').encode())
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix='h3-model-download') as pool:
            futures = [pool.submit(download, model) for model in missing]
            try:
                for future in as_completed(futures):future.result()
            except Exception:
                self.error = True
                self.stop_preparation.set()
                for future in futures:future.cancel()
                raise

    def request_drain(self):
        self.stop_preparation.set()

    def last_progress(self):
        return max([self.started_at]+[json.loads(p.read_text()).get('last_progress_at',self.started_at)
            for p in (self.state_root/'progress.json',self.state_root/'resource-progress.json') if p.is_file()])

    def preparing(self):
        return self.phase == 'preparing' or self.background_preparing

    def failed(self):
        return self.error or (self.process is not None and self.process.poll() is not None)

    def empty(self):
        if not self.audio.empty():return False
        if self.process is None:
            return self.phase == 'failed'
        if self.process.poll() is not None:
            terminate(self.process)
            try:
                os.killpg(self.process.pid, 0)
                return False
            except ProcessLookupError:
                return True
        q = self.api.json('/queue')
        return q.get('queue_running') == [] and q.get('queue_pending') == []

    def submit(self, prompt, graph):
        import vace_smoke
        if set(graph) == {'_audio'}:
            # Exclusive ownership was committed before this call. Unload idle Comfy
            # models so a completed video task cannot occupy the audio job's VRAM.
            self.api.json('/free', {'unload_models':True,'free_memory':True})
            self.audio.submit(prompt,graph['_audio'])
            return
        try:
            value = self.api.json('/prompt', {'prompt': graph, 'prompt_id': prompt, 'client_id': prompt})
        except vace_smoke.PromptRejected:
            raise Refused('prompt_rejected') from None
        if value.get('node_errors') or value.get('prompt_id') != prompt:
            raise Refused('prompt_identity_uncertain')

    def observe(self, prompt, task):
        if task.get('mode') in ('tts','align'):return self.audio.observe(prompt)
        from vace_reconcile import observe
        return_state, item = observe(self.api, prompt)
        return return_state['state'], item

    def cancel(self, prompt):
        if prompt in self.audio.jobs:
            self.audio.cancel(prompt)
            return
        # v0.39 scoped job API. Never global /interrupt or delete arbitrary queue entries.
        self.api.json('/api/jobs/' + prompt + '/cancel', {})

    def artifact(self, item, record):
        from h3worker.comfy_runner import output_path
        from .media import video_result
        if record['task'].get('mode') in ('tts','align'):
            from .media import audio_result
            path = Path(item)
            expected = self.audio.output_root / ('h3_'+record['prompt_id'].replace('-','')) / (
                'alignment.json' if record['task']['mode']=='align' else 'result.wav')
            if path != expected:raise Refused('unsafe_artifact_path')
            safe_root(path)
            result = audio_result(path,record['task'])
        else:
            path = output_path(item, self.comfy_root,
                'h3_' + record['prompt_id'].replace('-', '') + '/video', execution_workflow(record['task'])['output_node'])
            result = video_result(path, 'ffprobe', record['task'])
        size, sha = file_digest(path)
        if size <= 0:
            raise Refused('output_size_exceeds_bound')
        return {'size_bytes': size, 'sha256': sha, 'result': result,
                'filename': str(path.relative_to(self.comfy_root / 'output'))}

    def output_path(self, record):
        filename = record['artifact']['filename']
        path = self.comfy_root / 'output' / filename
        if Path(filename).is_absolute() or '..' in Path(filename).parts:
            raise Refused('unsafe_artifact_path')
        prefix = 'h3_' + record['prompt_id'].replace('-', '')
        if (not path.is_relative_to(self.comfy_root / 'output' / prefix)
                or any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file()):
            raise Refused('unsafe_artifact_path')
        return path

    def preparation(self):
        """Called in a supervised thread, independently of the serving control loop."""
        profile = self.profile
        progress = self.state_root / 'progress.json'
        started = time.monotonic()
        log_path = self.state_root / 'preparation.log'
        try:
            # No reinstallation or checkout at task execution time.
            if (self.comfy_root / 'main.py').is_file():
                raise RuntimeError('dedicated Comfy root already exists; use fresh ephemeral storage')
            copy_baked_core('/opt/comfyui-baked', self.comfy_root)
            for name in ('models', 'custom_nodes', 'input', 'output', 'user', 'temp'):
                (self.comfy_root / name).mkdir(mode=0o700, exist_ok=True)
            with log_path.open('ab', buffering=0) as log:
                # Exact nodes/dependencies/provenance were verified when publishing this image.
                nodes = {n['name']:n for p in profiles() for n in p['nodes']['nodes']}
                for name in (*nodes, 'H3AVContract', 'H3Reference'):
                    shutil.copytree(Path('/opt/comfyui-baked/custom_nodes') / name,
                                    self.comfy_root / 'custom_nodes' / name)
                if self.stop_preparation.is_set():
                    raise RuntimeError('preparation cancelled by drain')
                # Only reviewed nodes enabled; no Manager, partner API or public Comfy.
                argv = [sys.executable, '-B', str(self.comfy_root / 'main.py'), '--listen', '127.0.0.1',
                    '--port', '8188', '--disable-auto-launch', '--disable-partner-nodes', '--cache-none',
                    '--disable-all-custom-nodes']
                if nodes:
                    argv += ['--whitelist-custom-nodes', *nodes, 'H3AVContract', 'H3Reference']
                self.comfy_log = (self.state_root / 'comfy.log').open('ab', buffering=0)
                self.process = subprocess.Popen(argv, cwd=self.comfy_root,
                    stdout=self.comfy_log, stderr=subprocess.STDOUT, start_new_session=True)
                while not self.stop_preparation.is_set():
                    if self.process.poll() is not None:
                        raise RuntimeError('Comfy startup failed')
                    try:
                        stats = self.api.json('/system_stats')
                        break
                    except OSError:
                        time.sleep(2)
                else:
                    raise RuntimeError('preparation cancelled')
                devices = stats.get('devices', [])
                gpu = next((d for d in devices if d.get('type') == 'cuda'), {})
                import torch
                if not torch.cuda.is_available():
                    raise RuntimeError('CUDA unavailable')
                self.download_models(profile, log, progress, started)
                if self.stop_preparation.is_set():
                    raise RuntimeError('preparation cancelled by drain')
                if self.process.poll() is not None:
                    raise RuntimeError('Comfy exited during model preparation')
                self.proof = {key: profile[key] for key in ('profile_digest', 'comfy_version',
                    'comfy_commit', 'torch_version', 'models_digest', 'nodes_digest')}
                self.proof.update(prepared=True, gpu_vendor='nvidia', vram_bytes=int(gpu['vram_total']))
                with self.resource_lock:self.proofs[profile['profile_digest']] = dict(self.proof)
                self.phase = 'prepared_not_validated'
                atomic_json(progress, {'state': self.phase, 'last_progress_at': time.time()})
        except Exception as exc:
            self.error, self.phase = True, 'failed'
            # Fixed diagnostic category avoids leaking redirected model URLs/credentials.
            from h3worker.http import sanitize_error_text
            diagnostic = sanitize_error_text(str(exc), secrets=(os.environ.get('H3POD_TOKEN', ''),))
            with log_path.open('a') as log:
                log.write('PREPARATION_FAILED ' + type(exc).__name__ + ': ' + diagnostic + '\n')
            atomic_json(progress, {'state': 'failed', 'stop_required': True,
                                   'error_category': type(exc).__name__, 'error': diagnostic, 'last_progress_at': time.time()})
            if self.process:
                terminate(self.process)


def main():
    import uvicorn
    from .pod import Pod, create_app
    root = Path(os.environ.get('H3POD_STATE_ROOT', '/workspace/h3-pod'))
    generation = os.environ['H3POD_GENERATION']
    token = os.environ['H3POD_TOKEN']
    expiry = float(os.environ['H3POD_TOKEN_EXPIRES_AT'])
    if not time.time() < expiry <= time.time() + 86400:
        raise ValueError('Pod token lifetime must be at most 24 hours')
    engine = Engine(os.environ.get('H3POD_COMFY_ROOT', '/workspace/h3-service-comfy'), root)
    pod = Pod(root, generation, engine, engine.evidence,
              acceptance_enabled=os.environ.get('H3POD_ACCEPTANCE_ENABLED') == '1',
              stop_on_task_failure=os.environ.get('H3POD_STOP_ON_TASK_FAILURE') == '1')
    # A generation represents one Comfy boot. Reusing it after Pod restart is refused.
    with pod.db:
        old = pod.db.execute('SELECT value FROM meta WHERE key=?', ('boot:' + generation,)).fetchone()
        if old:
            raise ValueError('generation already booted; issue a fresh binding and retain old journal')
        pod.db.execute('INSERT INTO meta VALUES (?,?)', ('boot:' + generation, str(time.time())))
    @asynccontextmanager
    async def lifespan(app):
        threading.Thread(target=engine.preparation, daemon=True, name='h3-pod-prepare').start()
        async with protocol_lifespan(app):
            yield
        engine.audio.close()
        if engine.process and engine.process.poll() is None:
            terminate(engine.process)
        pod.close()
    app = create_app(pod, token, expiry)
    protocol_lifespan = app.router.lifespan_context
    app.router.lifespan_context = lifespan
    # Only authenticated protocol exposed. Comfy remains loopback, never /start.sh.
    uvicorn.run(app, host='127.0.0.1', port=8190, access_log=False, log_level='warning')


if __name__ == '__main__':
    main()
