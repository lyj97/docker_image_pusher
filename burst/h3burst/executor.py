"""Service-side logical Worker. No H3 credentials or journal reside on the Pod."""
import asyncio
import argparse
import json
import importlib
import os
from pathlib import Path
import subprocess
import tempfile
import time

from h3worker.config import WorkerConfig
from h3worker.worker import Worker
from shared.execution import profiles, claimable_profiles
from . import remote_runner
from .transport import Transport, RemoteError
from .failure import SCHEMA, read_failure


class CloudExecutor(Worker):
    def __init__(self, config, remote, approved_profiles, *, first_task_validation=False, stop_on_error=False):
        if type(first_task_validation) is not bool:
            raise ValueError('first task validation must be an explicit boolean')
        if type(stop_on_error) is not bool:
            raise ValueError("stop_on_error must be an explicit boolean")
        self.stop_on_error = stop_on_error
        self.first_task_validation = first_task_validation
        if (config.fake_runner or config.cpu_tail_overlap or config.cpu_tail_pilot
                or config.comfyui_preview_enabled):
            raise ValueError('cloud executor requires exclusive remote Comfy mode')
        config.durability_timeout_seconds = None
        config.min_free_disk_bytes = 0
        config.media_process_timeout_seconds = None
        config.capability_models = ()
        config.comfyui_ready = False
        config.comfyui_version = profiles()[0]['comfy_version']
        super().__init__(config)
        self.remote, self.approved_profiles = remote, tuple(approved_profiles)
        self._comfy_adapter = remote_runner
        self.remote_ready, self.remote_status = False, {}
        with self.journal.conn:self.journal.conn.execute(SCHEMA)
        self.test_failure = read_failure(self.journal, remote.generation) if stop_on_error else None
        if self.test_failure:self._drain = True
        from h3worker.monitor import Monitor
        owner = self
        class CloudMonitor(Monitor):
            def report_payload(self):
                result = super().report_payload()
                rows = owner.journal.conn.execute('SELECT engine_state FROM attempts WHERE engine_state IS NOT NULL').fetchall()
                unresolved = sum(not json.loads(r['engine_state']).get('terminal') for r in rows)
                result['status_report']['cloud_safety'] = {
                    'generation': owner.remote.generation, 'test_failure': owner.test_failure,
                    'unresolved_executions': unresolved,
                    'local_idle': not owner.journal.active_attempts() and unresolved == 0
                        and not owner.journal.conn.execute('SELECT 1 FROM claim_requests WHERE resolved_attempt_id IS NULL LIMIT 1').fetchone()}
                return result
        self.monitor = CloudMonitor(self.journal, config.worker_id, self.boot_id)

    def _latch_test_failure(self, attempt_id, category):
        if not self.stop_on_error:return
        self._drain, self.remote_ready = True, False
        self.test_failure = {'attempt_id': attempt_id, 'category': category}
        with self.journal.conn:
            self.journal.conn.execute('INSERT OR IGNORE INTO cloud_test_failures VALUES (?,?,?)',
                (self.remote.generation, attempt_id, category))
        self.test_failure = read_failure(self.journal, self.remote.generation)

    async def _finish_failed(self, attempt_id, *args, **kwargs):
        self._latch_test_failure(attempt_id, 'attempt_failed')
        await super()._finish_failed(attempt_id, *args, **kwargs)

    def _defer_recovery(self, attempt_id, reason='contact unavailable'):
        self._latch_test_failure(attempt_id, 'recovery_deferred')
        super()._defer_recovery(attempt_id, reason)

    def _quarantine_attempt(self, attempt_id, reason):
        self._latch_test_failure(attempt_id, 'attempt_quarantined')
        super()._quarantine_attempt(attempt_id, reason)

    async def _release_or_fail(self, attempt_id, *args, **kwargs):
        self._latch_test_failure(attempt_id, 'local_incapacity')
        await super()._release_or_fail(attempt_id, *args, **kwargs)

    async def _capability_snapshot(self, comfy_ready):
        if self.test_failure:self.remote_ready = False
        proofs = self.remote_status.get('profiles', [self.remote_status.get('profile', {})])
        proofs = [dict(p, generation=self.remote.generation) for p in proofs
                  if isinstance(p, dict) and (p.get('ready') is True or
                      p.get('prepared') is True)]
        capabilities = {'models':[remote_runner.CAPABILITY] if self.remote_ready else [],
            'modes':[remote_runner.MODE] if self.remote_ready else [], 'update_sources':[],
            'execution_backend':'runpod','executor_generation':self.remote.generation,
            'first_task_validation_enabled':getattr(self, 'first_task_validation', False) and self.remote_status.get('acceptance_enabled') is True,
            'execution_profiles':proofs if self.remote_ready else [],
            'cloud_state':{'ready':any(p.get('ready') is True for p in proofs),
                'can_execute':self.remote_ready, 'preparing':self.remote_status.get('preparing',False),
                'stop_required':bool(self.test_failure) or self.remote_status.get('stop_required',False),
                'test_failure':self.test_failure}}
        from types import SimpleNamespace
        config = SimpleNamespace(runpod_execution_enabled=True,
            runpod_first_task_validation=getattr(self, 'first_task_validation', False),
            runpod_worker_ids=(self.config.worker_id,),runpod_validated_profiles=self.approved_profiles)
        accepted = claimable_profiles(capabilities,config,self.config.worker_id)
        capabilities['execution_profiles'] = [p for p in capabilities['execution_profiles']
            if p.get('profile_digest') in accepted]
        capabilities['cloud_state']['ready'] = any(p.get('ready') is True
            for p in capabilities['execution_profiles'])
        capabilities['cloud_state']['can_execute'] = bool(accepted)
        native_modes = {mode for p in profiles() if p['profile_digest'] in accepted
                        if p['backend']=='comfyui'
                        for mode in (p.get('native_modes') or (['a2va'] if p.get('native_generation') else []))}
        if native_modes:
            capabilities['models'].append('installed-model-revision')
            capabilities['modes'].extend(sorted(native_modes))
        for profile in profiles():
            if profile['profile_digest'] in accepted and profile['backend']=='audio_cuda':
                capabilities['models'].extend(profile['model_revisions'])
                capabilities['modes'].extend(profile['native_modes'])
        capabilities['models'] = sorted(set(capabilities['models']))
        capabilities['modes'] = sorted(set(capabilities['modes']))
        if not accepted:
            capabilities.update(models=[],modes=[],execution_profiles=[])
            self.remote_ready = False
        return capabilities

    async def _prepare_queue_resources(self):
        # Only Service reads business queue metadata; Pod receives reviewed digests.
        from .control import H3Control
        try:
            monitor = H3Control(self.config.server_url,self.config.worker_id)
            known = {p['profile_digest'] for p in profiles()}
            requested = []
            page = 1
            while True:
                view = await asyncio.to_thread(monitor.request,
                    f'/v1/monitor?status=QUEUED&task_page={page}&task_page_size=200')
                for task in view.get('tasks', []):
                    advice = task.get('execution_advice') or {}
                    if (task.get('status') != 'QUEUED' or not advice.get('admission_snapshot')
                            or advice.get('execution_target') == 'local'):
                        continue
                    for identity in advice.get('profile_digests', []):
                        if identity in known and identity not in requested:
                            requested.append(identity)
                pagination = view.get('task_pagination')
                if (len(requested) == len(known) or not pagination
                        or pagination['page'] != page
                        or page * pagination['page_size'] >= pagination['total']):
                    break
                page += 1
            if requested:
                await asyncio.to_thread(self.remote.json,'/v1/prepare',{'profile_digests':requested})
        except Exception:
            pass  # Prefetch failure never grants capability or changes task eligibility.

    async def _register(self, comfy_ready=None):
        if comfy_ready is None:
            await self._refresh_comfy(initial=True)
            return
        from h3worker import __version__
        caps = await self._capability_snapshot(comfy_ready)
        result = await asyncio.to_thread(self.http.register, self.config.worker_id, self.boot_id, caps,
            {'worker_version': __version__, 'engine_version': 'remote-comfy-' + self.config.comfyui_version})
        self._registered_current_boot = True
        self.heartbeat_interval = float(result.get('heartbeat_interval_seconds') or self.config.heartbeat_seconds)
        self.lease_seconds = float(result.get('lease_seconds') or 90)
        if self.lease_seconds <= self.config.lease_safety_margin_seconds:
            raise ValueError('cloud lease is shorter than safety margin')

    async def _refresh_comfy(self, *, initial=False):
        try:
            self.remote_status = await asyncio.to_thread(self.remote.json, '/v1/status')
            if not self._drain:await self._prepare_queue_resources()
            self.remote_ready = (any(p.get('ready') is True or
                    p.get('prepared') is True
                    for p in self.remote_status.get('profiles', [self.remote_status.get('profile', {})]))
                and not self.remote_status.get('draining') and not self.remote_status.get('stop_required')
                and not self._unhealthy_reason and not self.test_failure)
        except (RemoteError, AttributeError, TypeError, ValueError):
            self.remote_ready, self.remote_status = False, {}
        # Always re-register the current evidence, preventing stale readiness.
        await self._register(self.remote_ready)

    async def _claim_once_unlocked(self):
        # Do not hide a replayable prior claim when readiness is withdrawn.
        # Normal recovery handles journal attempts before this path.
        pending_claim = self.journal.conn.execute('SELECT 1 FROM claim_requests WHERE resolved_attempt_id IS NULL LIMIT 1').fetchone()
        if (not self.remote_ready or self._comfy_unhealthy_reason) and not pending_claim:
            await self._node_heartbeat('unhealthy' if self._unhealthy_reason else 'idle')
            await asyncio.sleep(5)
            return
        try:
            await remote_runner.release_finished(self)
        except RemoteError:
            await asyncio.sleep(2)
            return  # Retry the same release before uploading the next task.
        await super()._claim_once_unlocked()

    async def _node_heartbeat(self, *args, **kwargs):
        payload = await super()._node_heartbeat(*args, **kwargs)
        pending = self.journal.conn.execute('SELECT 1 FROM claim_requests WHERE resolved_attempt_id IS NULL LIMIT 1').fetchone()
        if pending and self._drain and not self.journal.active_attempts():
            # Replay only the durable old claim while draining; server row remains fenced.
            self._drain = False
            try:
                await Worker._claim_once_unlocked(self)
            finally:
                self._drain = True
        return payload

    def _remote_profile_metadata(self, task, result):
        from shared.execution import admitted_profiles
        profile = admitted_profiles(task)[0]
        metadata = {'engine':'comfyui', 'engine_version':profile['comfy_version'],
            'execution_backend':'runpod', 'execution_profile':profile['profile_id'],
            'execution_profile_digest':profile['profile_digest'],
            'precision':profile.get('precision', 'H3 INT8 / Qwen NVFP4 AWQ / Turbo BF16'),
            'engine_prompt_id':result['prompt_id'], 'remote_graph_digest':result['workflow_digest']}
        if task.get('mode') in ('t2va','fl2va','ref2va','a2va'):
            # CUDA profiles do not implement the Mac optimization contract,
            # even when an explicit render size equals the output dimensions.
            metadata.update(optimization_capabilities=[], optimization_contract_revision=None)
        if profile['backend']=='audio_cuda':
            from shared.cuda_audio import differences
            metadata.update(engine=profile['audio_runtime']+'-cuda', engine_version=profile['upstream']['commit'],
                audio_model_directory=profile['model_directory'], backend_differences=differences(task,profile))
        return metadata

    async def _run_engine(self, attempt_id, lease_token, task_request, local_inputs):
        return await remote_runner.run(self, attempt_id, lease_token, task_request, local_inputs)

    async def _maybe_run_command(self, payload):
        if payload.get('command'):
            self._unhealthy_reason = 'cloud executor does not support arbitrary Worker commands'
            self._drain = True

    def _apply_command_control(self, payload):
        pass

    async def _command_control_loop(self):
        await self.stop_event.wait()

    async def _maybe_apply_update(self, payload):
        if payload.get('update'):
            self._unhealthy_reason = 'cloud executor upgrades require service-side deployment'
            self._drain = True

    async def _complete_pending_update(self):
        pass


def check_config(worker):
    """Explicit preboot deployment check; never registers, claims or contacts a Pod."""
    # Material transfer imports are lazy during execution; check them before boot.
    importlib.import_module("h3burst.inputs")
    importlib.import_module("h3burst.media")
    cfg = worker.config
    if cfg.worker_token == 'worker-token':
        raise ValueError('existing H3 Worker credential required')
    if not worker.approved_profiles or not set(worker.approved_profiles) <= {
            p['profile_digest'] for p in profiles()}:
        raise ValueError('binding uses unknown execution profiles')
    cfg.ensure_dirs()
    worker._acquire_singleton_lock()
    try:
        node = worker.journal.get_node()
        if node and node['worker_id'] != cfg.worker_id:
            raise ValueError('journal belongs to another Worker')
        with tempfile.TemporaryFile(dir=cfg.data_dir) as probe:
            probe.write(b'preboot');probe.flush();os.fsync(probe.fileno())
        worker.journal.conn.execute('BEGIN IMMEDIATE')
        worker.journal.conn.rollback()
        for binary in (cfg.ffprobe_path or 'ffprobe', cfg.ffmpeg_path or 'ffmpeg'):
            subprocess.run([binary, '-version'], check=True, capture_output=True)
        return {'check_config': 'passed', 'worker_id': cfg.worker_id,
                'uid': os.geteuid(), 'pod_contacted': False, 'task_claimed': False}
    finally:
        worker._lock_fh.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding', type=Path, required=True)
    parser.add_argument('--check-config', action='store_true',
                        help='check deployment as the service user before Pod startup, then exit')
    args = parser.parse_args()
    binding = json.loads(args.binding.read_text())
    # Binding is operator-owned nonsecret config; no task-supplied URLs/repos.
    cfg = WorkerConfig.from_env()
    if cfg.worker_id != binding['worker_id'] or not Path(cfg.data_dir).is_absolute():
        raise ValueError('binding identity and absolute persistent data dir required')
    remote = Transport(binding['pod_url'], os.environ['H3BURST_POD_TOKEN'], binding['generation'])
    worker = CloudExecutor(cfg, remote, binding['approved_profiles'],
                           first_task_validation=binding.get('first_task_validation', False),
                           stop_on_error=binding.get('stop_on_error', False))
    if args.check_config:
        try:
            print(json.dumps(check_config(worker)))
        finally:
            worker.journal.close()
        return
    asyncio.run(worker.run())


if __name__ == '__main__':
    main()
