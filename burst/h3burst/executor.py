"""Service-side logical Worker. No H3 credentials or journal reside on the Pod."""
import asyncio
import argparse
import json
import os
from pathlib import Path
import time

from h3worker.config import WorkerConfig
from h3worker.worker import Worker
from shared.execution import profiles, claimable_profiles
from . import remote_runner
from .transport import Transport, RemoteError


class CloudExecutor(Worker):
    def __init__(self, config, remote, approved_profiles, *, first_task_validation=False):
        if type(first_task_validation) is not bool:
            raise ValueError('first task validation must be an explicit boolean')
        self.first_task_validation = first_task_validation
        if (config.fake_runner or config.cpu_tail_overlap or config.cpu_tail_pilot
                or config.comfyui_preview_enabled):
            raise ValueError('cloud executor requires exclusive remote Comfy mode')
        config.capability_models = ()
        config.comfyui_ready = False
        config.comfyui_version = profiles()[0]['comfy_version']
        super().__init__(config)
        self.remote, self.approved_profiles = remote, tuple(approved_profiles)
        self._comfy_adapter = remote_runner
        self.remote_ready, self.remote_status = False, {}
        from h3worker.monitor import Monitor
        owner = self
        class CloudMonitor(Monitor):
            def report_payload(self):
                result = super().report_payload()
                rows = owner.journal.conn.execute('SELECT engine_state FROM attempts WHERE engine_state IS NOT NULL').fetchall()
                unresolved = sum(not json.loads(r['engine_state']).get('terminal') for r in rows)
                result['status_report']['cloud_safety'] = {
                    'generation': owner.remote.generation, 'unresolved_executions': unresolved,
                    'local_idle': not owner.journal.active_attempts() and unresolved == 0
                        and not owner.journal.conn.execute('SELECT 1 FROM claim_requests WHERE resolved_attempt_id IS NULL LIMIT 1').fetchone()}
                return result
        self.monitor = CloudMonitor(self.journal, config.worker_id, self.boot_id)

    async def _capability_snapshot(self, comfy_ready):
        proofs = self.remote_status.get('profiles', [self.remote_status.get('profile', {})])
        proofs = [dict(p, generation=self.remote.generation) for p in proofs
                  if isinstance(p, dict) and (p.get('ready') is True or
                      (getattr(self, 'first_task_validation', False) and self.remote_status.get('acceptance_enabled') is True
                       and p.get('prepared') is True and p.get('ready') is False))]
        capabilities = {'models':[remote_runner.CAPABILITY] if self.remote_ready else [],
            'modes':[remote_runner.MODE] if self.remote_ready else [], 'update_sources':[],
            'execution_backend':'runpod','executor_generation':self.remote.generation,
            'first_task_validation_enabled':getattr(self, 'first_task_validation', False) and self.remote_status.get('acceptance_enabled') is True,
            'execution_profiles':proofs if self.remote_ready else [],
            'cloud_state':{'ready':any(p.get('ready') is True for p in proofs),
                'can_execute':self.remote_ready, 'preparing':self.remote_status.get('preparing',False),
                'stop_required':self.remote_status.get('stop_required',False)}}
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
        if any(p.get('native_generation') and p['profile_digest'] in accepted for p in profiles()):
            capabilities['models'].append('installed-model-revision');capabilities['modes'].append('a2va')
        if not accepted:
            capabilities.update(models=[],modes=[],execution_profiles=[])
            self.remote_ready = False
        return capabilities

    async def _prepare_queue_resources(self):
        # Only Service reads business queue metadata; Pod receives reviewed digests.
        from .control import H3Control
        try:
            monitor = H3Control(self.config.server_url,self.config.worker_id)
            view = await asyncio.to_thread(monitor.request,'/v1/monitor')
            known = {p['profile_digest'] for p in profiles()}
            requested = []
            for task in view.get('tasks',[]):
                advice = task.get('execution_advice') or {}
                if task.get('status') != 'QUEUED' or not advice.get('admission_snapshot') or advice.get('execution_target') == 'local':continue
                for identity in advice.get('profile_digests',[]):
                    if identity in known and identity not in requested:requested.append(identity)
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
                    (self.first_task_validation and self.remote_status.get('acceptance_enabled') is True
                     and p.get('prepared') is True and p.get('ready') is False)
                    for p in self.remote_status.get('profiles', [self.remote_status.get('profile', {})]))
                and not self.remote_status.get('draining') and not self.remote_status.get('stop_required')
                and not self._unhealthy_reason)
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
        return {'engine':'comfyui', 'engine_version':profile['comfy_version'],
            'execution_backend':'runpod', 'execution_profile':profile['profile_id'],
            'execution_profile_digest':profile['profile_digest'],
            'precision':profile.get('precision', 'H3 INT8 / Qwen NVFP4 AWQ / Turbo BF16'),
            'engine_prompt_id':result['prompt_id'], 'remote_graph_digest':result['workflow_digest']}

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding', type=Path, required=True)
    args = parser.parse_args()
    binding = json.loads(args.binding.read_text())
    # Binding is operator-owned nonsecret config; no task-supplied URLs/repos.
    cfg = WorkerConfig.from_env()
    if cfg.worker_id != binding['worker_id'] or not Path(cfg.data_dir).is_absolute():
        raise ValueError('binding identity and absolute persistent data dir required')
    remote = Transport(binding['pod_url'], os.environ['H3BURST_POD_TOKEN'], binding['generation'])
    worker = CloudExecutor(cfg, remote, binding['approved_profiles'],
                           first_task_validation=binding.get('first_task_validation', False))
    asyncio.run(worker.run())


if __name__ == '__main__':
    main()
