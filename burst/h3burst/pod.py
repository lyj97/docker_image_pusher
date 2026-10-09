"""Thin Pod execution protocol. Durable intents fence duplicate Comfy submission."""
import asyncio
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
import uuid

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Route
from shared.execution import admitted_profiles, digest, execution_workflow, task_references
from .transport import MAX_JSON

TERMINAL = {'completed', 'failed', 'cancelled'}
ID_RE = r'att_[A-Za-z0-9_-]{8,64}'


class Refused(RuntimeError):
    def __init__(self, code, status=409):
        super().__init__(code)
        self.code, self.status = code, status


class Pod:
    def __init__(self, root, generation, engine, evidence, *, acceptance_enabled=False,
                 stop_on_task_failure=False):
        self.root = Path(root)
        if not self.root.is_absolute() or any(p.is_symlink() for p in (self.root, *self.root.parents)):
            raise ValueError('Pod journal root must be absolute without symlinks')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.lock_file = (self.root / 'pod.lock').open('a')
        fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.db = sqlite3.connect(self.root / 'executions.sqlite3', check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS execution (id TEXT PRIMARY KEY, generation TEXT, '
                        'request_digest TEXT, record TEXT NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)')
        self.generation, self.engine, self.evidence = generation, engine, evidence
        self.uploads = 0
        self.acceptance_enabled = acceptance_enabled
        self.stop_on_task_failure = stop_on_task_failure
        self.mutex = threading.RLock()
        self.observation_lock = threading.Lock()
        self.draining = self.db.execute("SELECT value FROM meta WHERE key='draining'").fetchone() is not None

    def close(self):
        self.db.close()
        self.lock_file.close()

    def load(self, execution):
        row = self.db.execute('SELECT record FROM execution WHERE id=?', (execution,)).fetchone()
        if not row:
            raise Refused('execution_not_found', 404)
        value = json.loads(row[0])
        if value['generation'] != self.generation:
            raise Refused('old_generation')
        return value

    def save(self, record):
        with self.db:
            self.db.execute('INSERT INTO execution VALUES (?,?,?,?) ON CONFLICT(id) DO UPDATE SET record=excluded.record',
                (record['execution_id'], record['generation'], record['request_digest'], json.dumps(record)))
        # Original protocol journal has no prompts, URLs, tokens or outputs.
        with (self.root / 'protocol.log').open('a') as log:
            log.write(json.dumps({key: record.get(key) for key in
                ('execution_id', 'generation', 'state', 'prompt_id', 'cancel_requested', 'observation_error')}) + '\n')
            log.flush()
            os.fsync(log.fileno())

    def active(self):
        return [json.loads(row[0]) for row in self.db.execute('SELECT record FROM execution')
                if json.loads(row[0])['state'] not in TERMINAL]

    def ready(self, proof=None):
        proof = dict(proof if proof is not None else self.evidence())
        accepted = self.db.execute('SELECT value FROM meta WHERE key=?',
            ('acceptance:' + proof.get('profile_digest',''),)).fetchone()
        acceptance = json.loads(accepted[0]) if accepted else {}
        if (not self.engine.failed() and proof.get('prepared') is True and acceptance.get('generation') == self.generation
                and acceptance.get('runtime_digest') == digest(proof)):
            proof = dict(proof, ready=True, validation_artifact_sha256=acceptance['artifact_sha256'])
        else:
            proof = dict(proof, ready=False)
        return proof

    def stop_required(self):
        return self.engine.failed() or self.db.execute("SELECT 1 FROM meta WHERE key='input_cleanup_failed'").fetchone() is not None or (self.stop_on_task_failure and any(
            json.loads(r[0]).get('generation') == self.generation and json.loads(r[0]).get('state') == 'failed'
            for r in self.db.execute('SELECT record FROM execution')))

    def status(self):
        with self.mutex:
            pending = self.active()
            proofs = self.engine.all_evidence() if hasattr(self.engine, 'all_evidence') else [self.evidence()]
            return {'generation': self.generation, 'profile': self.ready(),
                'acceptance_enabled': self.acceptance_enabled,
                'profiles':[self.ready(p) for p in proofs],
                'draining': self.draining, 'active_executions': len(pending),
                'queue_empty': not pending and self.engine.empty(), 'preparing': self.engine.preparing() or self.uploads > 0,
                'stop_required': self.stop_required(),
                'last_progress_at': self.engine.last_progress(),
                'observed_at': time.time()}

    def execute(self, body):
        with self.mutex:
            if set(body) not in ({'execution_id', 'task', 'acceptance'}, {'execution_id', 'task', 'acceptance', 'inputs'}) or type(body['acceptance']) is not bool:
                raise Refused('invalid_execution', 400)
            execution, task = body['execution_id'], body['task']
            if not isinstance(execution, str) or not re.fullmatch(ID_RE, execution) or not isinstance(task, dict):
                raise Refused('invalid_execution', 400)
            request_digest = digest(body)
            row = self.db.execute('SELECT record FROM execution WHERE id=?', (execution,)).fetchone()
            if row:
                record = self.load(execution)
                if record['request_digest'] != request_digest:
                    raise Refused('execution_identity_conflict')
                return self.observe(execution)  # NEVER resubmit, even after restart or lost reply.
            matches = admitted_profiles(task)
            proofs = self.engine.all_evidence() if hasattr(self.engine, 'all_evidence') else [self.evidence()]
            proof = next((self.ready(p) for p in proofs if len(matches)==1 and p.get('profile_digest')==matches[0]['profile_digest']), {})
            if (len(matches) != 1 or proof.get('profile_digest') != matches[0]['profile_digest']
                    or proof.get('prepared') is not True or self.draining or self.stop_required()):
                raise Refused('profile_not_prepared')
            acceptance = body['acceptance']
            if self.uploads or self.active() or not self.engine.empty():
                raise Refused('exclusive_slot_busy')
            prompt = str(uuid.uuid4())
            execution_task = dict(task, workflow=execution_workflow(task, matches[0]), references=task_references(task))
            record = {'execution_id': execution, 'generation': self.generation,
                'request_digest': request_digest, 'prompt_id': prompt, 'state': 'submitting',
                'terminal': False, 'task': task, 'graph_digest': digest(execution_task['workflow']['graph']),
                'acceptance': acceptance, 'runtime_digest': digest({k:v for k,v in proof.items() if k not in ('ready','validation_artifact_sha256')}),
                'runtime_proof':{k:v for k,v in proof.items() if k not in ('ready','validation_artifact_sha256')},
                'cancel_requested': False, 'started_at': time.time(), 'submission_started': False}
            if execution_task.get('references'):
                record['input_root'] = str(self.engine.comfy_root.resolve())
            self.save(record)  # Intent owns private inputs before any copy or Comfy submission.
            try:
                if execution_task.get('references'):
                    from .inputs import paths
                    from h3worker.comfy_runner import bind_graph
                    graph = bind_graph(execution_task, paths(self, task, body.get('inputs', {})),
                        self.engine.comfy_root, 'h3_' + prompt.replace('-', ''))
                else:
                    if body.get('inputs'):
                        raise Refused('input_identity_conflict', 400)
                    graph = copy.deepcopy(execution_task['workflow']['graph'])
                    graph[execution_task['workflow']['output_node']]['inputs']['filename_prefix'] = 'h3_' + prompt.replace('-', '') + '/video'
            except BaseException:
                record.update(state='failed', terminal=True, error_code='input_binding_failed')
                self.save(record)
                self.cleanup_inputs(record)
                raise
            record.update(graph_digest=digest(graph), submission_started=True)
            self.save(record)  # FULL commit before the only Comfy POST; uncertainty never rebinds.
            try:
                self.engine.submit(prompt, graph)
            except Refused as exc:
                if exc.code == 'prompt_rejected':
                    record.update(state='failed', terminal=True)
                else:
                    record['state'] = 'uncertain'
            except Exception:
                record['state'] = 'uncertain'
            else:
                record['state'] = 'submitted'
            self.save(record)
            self.cleanup_inputs(record)
            return self.public(record)

    def cleanup_inputs(self, record):
        if not record.get("terminal") or "input_root" not in record:
            return
        from h3worker.comfy_runner import cleanup_bound_inputs
        root = self.engine.comfy_root
        if str(root.resolve()) != record["input_root"]:
            raise ValueError("ComfyUI input ownership conflict")
        try:
            cleanup_bound_inputs(root, "h3_" + record["prompt_id"].replace("-", ""))
        except (OSError, ValueError):
            record["error_code"] = "input_cleanup_failed"
            self.save(record)
            self.draining = True
            with self.db:
                self.db.execute("INSERT OR REPLACE INTO meta VALUES ('input_cleanup_failed','true')")

    def public(self, record):
        return {key: record[key] for key in ('execution_id', 'generation', 'prompt_id', 'state',
            'terminal', 'graph_digest', 'request_digest', 'cancel_requested', 'artifact', 'error_code') if key in record}

    def observe(self, execution):
        # Concurrent readers use durable state while one observer does slow Comfy I/O.
        if not self.observation_lock.acquire(blocking=False):
            with self.mutex:
                record = self.load(execution)
                if record.get('observation_error'):
                    raise Refused('pod_observation_unavailable', 503)
                return self.public(record)
        try:
            with self.mutex:
                record = self.load(execution)
                if record.get('submission_started') is False and not record['terminal']:
                    record.update(state='failed', terminal=True, error_code='input_binding_interrupted')
                    self.save(record)
                if record['state'] in TERMINAL:
                    self.cleanup_inputs(record)
                    return self.public(record)
            try:
                state, output = self.engine.observe(record['prompt_id'], record['task'])
            except Exception:
                with self.mutex:
                    record = self.load(execution)
                    record['observation_error'] = 'comfy_observation_unavailable'
                    self.save(record)
                raise Refused('pod_observation_unavailable', 503) from None
            artifact, artifact_error = None, False
            if state == 'completed' and record.get('last_observation') == state:
                try:artifact = self.engine.artifact(output, record)
                except Exception:artifact_error = True
            with self.mutex:
                # Cancellation can update this record while Comfy/media I/O is running.
                record = self.load(execution)
                if record['state'] in TERMINAL:return self.public(record)
                return self._apply_observation(record, state, artifact, artifact_error)
        finally:
            self.observation_lock.release()

    def _apply_observation(self, record, state, artifact, artifact_error):
        with self.mutex:
            record.pop('observation_error', None)
            if record['state'] not in TERMINAL:
                if state in TERMINAL and record.get('last_observation') == state:
                    if state == 'completed':
                        if artifact_error:
                            # Completion is confirmed; invalid media is a terminal failure, not lost submission.
                            record.update(state='failed', terminal=True, error_code='artifact_validation_failed')
                            self.save(record)
                            self.cleanup_inputs(record)
                            return self.public(record)
                        record['artifact'] = artifact
                        if record['acceptance']:
                            receipt = {'generation': self.generation,
                                'runtime_digest': record['runtime_digest'], 'artifact_sha256': digest({
                                    'artifact': artifact, 'profile': record.get('runtime_proof',self.evidence()), 'prompt_id': record['prompt_id']})}
                            with self.db:
                                self.db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',
                                    ('acceptance:' + record.get('runtime_proof',self.evidence())['profile_digest'], json.dumps(receipt)))
                    record.update(state=state, terminal=True)
                elif state in ('pending', 'running', *TERMINAL):
                    record['state'] = state if state not in TERMINAL else 'observing_terminal'
                else:
                    record['state'] = 'uncertain'  # Absence is not submission/cancellation proof.
                record['last_observation'] = state
                self.save(record)
            self.cleanup_inputs(record)
            return self.public(record)

    def cancel(self, execution):
        with self.mutex:
            record = self.load(execution)
            if not record['terminal']:
                record['cancel_requested'] = True
                self.save(record)
                self.engine.cancel(record['prompt_id'])  # Prompt-scoped job cancellation only.
            return self.observe(execution)

    def drain(self):
        with self.mutex:
            self.draining = True
            if self.engine.preparing():
                self.engine.request_drain()
            with self.db:
                self.db.execute("INSERT OR REPLACE INTO meta VALUES ('draining','true')")
            return self.status()

    def artifact_path(self, execution):
        record = self.load(execution)
        if not record['terminal'] or record['state'] != 'completed':
            raise Refused('artifact_not_ready')
        return self.engine.output_path(record)

    def diagnostics(self):
        # Fixed allowlist; never export SQLite (contains business prompts) or env.
        files = ('protocol.log', 'preparation.log', 'comfy.log', 'progress.json', 'resource-progress.json')
        return {'generation': self.generation, 'files': {name:
            (self.root / name).read_bytes()[-256 * 1024:].decode('utf-8', 'replace')
            for name in files if (self.root / name).is_file() and not (self.root / name).is_symlink()}}


def create_app(pod, token, expires_at):
    if not re.fullmatch('[A-Za-z0-9_-]{32,256}', token) or expires_at <= time.time():
        raise ValueError('valid expiring Pod token required')

    upload_lock = asyncio.Lock()

    async def dispatch(request: Request):
        if (time.time() >= expires_at or not secrets.compare_digest(
                request.headers.get('authorization', ''), 'Bearer ' + token)):
            return JSONResponse({'error': 'unauthorized'}, status_code=401)
        if request.headers.get('x-h3-generation') != pod.generation:
            return JSONResponse({'error': 'generation_conflict'}, status_code=409)
        try:
            if request.method == 'PUT' and 'sha' in request.path_params:
                from .inputs import receive
                async with upload_lock:
                    value = await receive(pod, request, request.path_params['sha'], expires_at)
                return JSONResponse(value, headers={'Cache-Control': 'no-store'})
            route = request.path_params.get('operation', '')
            execution = request.path_params.get('execution', '')
            if execution and not re.fullmatch(ID_RE, execution):
                raise Refused('invalid_execution', 400)
            if request.method == 'POST':
                raw = bytearray()
                async for chunk in request.stream():
                    raw.extend(chunk)
                    if len(raw) > MAX_JSON:
                        raise Refused('request_too_large', 413)
                body = json.loads(raw or b'{}')
                if not isinstance(body, dict):
                    raise Refused('invalid_body', 400)
            if route == 'status' and request.method == 'GET':
                value = await asyncio.to_thread(pod.status)
            elif route == 'diagnostics' and request.method == 'GET':
                value = await asyncio.to_thread(pod.diagnostics)
            elif route == 'prepare' and request.method == 'POST':
                if set(body) != {'profile_digests'} or pod.draining:raise Refused('invalid_preparation',400)
                value = await asyncio.to_thread(pod.engine.request_resources,body['profile_digests'])
            elif route == 'drain' and request.method == 'POST':
                value = await asyncio.to_thread(pod.drain)
            elif route == 'executions' and request.method == 'POST':
                value = await asyncio.to_thread(pod.execute, body)
            elif execution and request.method == 'GET' and route == 'artifact':
                path = await asyncio.to_thread(pod.artifact_path, execution)
                return FileResponse(path, media_type='video/mp4', headers={'Cache-Control': 'no-store'})
            elif execution and request.method == 'GET' and not route:
                value = await asyncio.to_thread(pod.observe, execution)
            elif execution and request.method == 'POST' and route == 'cancel':
                value = await asyncio.to_thread(pod.cancel, execution)
            else:
                raise Refused('not_found', 404)
            return JSONResponse(dict(value, generation=pod.generation), headers={'Cache-Control': 'no-store'})
        except Refused as exc:
            return JSONResponse({'error': exc.code, 'generation': pod.generation}, status_code=exc.status)
        except (ValueError, TypeError, KeyError):
            return JSONResponse({'error': 'invalid_request', 'generation': pod.generation}, status_code=400)
        except Exception:
            return JSONResponse({'error': 'pod_observation_unavailable', 'generation': pod.generation}, status_code=503)

    @asynccontextmanager
    async def lifespan(app):
        async def supervise():
            while True:
                try:
                    if time.time() >= expires_at:
                        await asyncio.to_thread(pod.drain)
                        for record in pod.active():
                            if record['generation'] == pod.generation:
                                await asyncio.to_thread(pod.cancel, record['execution_id'])
                    with pod.mutex:active = pod.active()
                    for record in active:
                        if record['generation'] == pod.generation:
                            await asyncio.to_thread(pod.observe, record['execution_id'])
                except Exception:
                    pass  # Durable uncertainty stays fenced and visible to the controller.
                await asyncio.sleep(2)
        watcher = asyncio.create_task(supervise())
        try:
            yield
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    return Starlette(lifespan=lifespan, routes=[Route('/v1/inputs/{sha}', dispatch, methods=['PUT']), Route('/v1/{operation}', dispatch, methods=['GET', 'POST']),
        Route('/v1/executions/{execution}', dispatch, methods=['GET']),
        Route('/v1/executions/{execution}/{operation}', dispatch, methods=['GET', 'POST'])])
