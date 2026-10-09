"""Remote adapter for the existing H3 attempt, lease and artifact pipeline."""
import asyncio
import json
import re
from pathlib import Path

from shared.execution import admitted_profiles, digest, task_references
from h3worker import comfy_runner as local
from h3worker.worker import NativeGroupUnproven, CancelRequested, LeaseLost, _shutdown_requested
from .transport import RemoteError

CAPABILITY, MODE = local.CAPABILITY, local.MODE
state_for, persist, quarantine = local.state_for, local.persist, local.quarantine


def unresolved(worker, message):
    reasons = getattr(worker, '_remote_recovery_reasons', set())
    reasons.add(message)
    worker._remote_recovery_reasons = reasons
    quarantine(worker, message)
    raise NativeGroupUnproven(message)


def clear_quarantine(worker):
    local.clear_quarantine(worker)
    reasons = getattr(worker, '_remote_recovery_reasons', set())
    if getattr(worker, '_other_unhealthy_reason', None) in reasons:
        worker._other_unhealthy_reason = None
    reasons.clear()


def confirmed_stopped(worker, state):
    proof_path = Path(worker.config.data_dir).parent / 'controller' / 'provider-stopped.json'
    if not proof_path.is_file() or any(p.is_symlink() for p in (proof_path, *proof_path.parents)):
        return False
    try:
        proof = json.loads(proof_path.read_text())
        return (proof.get('worker_id') == worker.config.worker_id and proof.get('provider_confirmed') is True
                and proof.get('generation') == state.get('generation') and proof.get('pod_url') == state.get('url')
                and proof.get('status') in ('STOPPED', 'EXITED'))
    except (OSError, ValueError):
        return False


def apply_observation(worker, attempt_id, state, view):
    if (not isinstance(view.get('prompt_id'), str)
            or not re.fullmatch('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', view['prompt_id'])
            or not isinstance(view.get('graph_digest'), str)
            or not re.fullmatch('[0-9a-f]{64}', view['graph_digest'])
            or type(view.get('terminal')) is not bool
            or view.get('state') not in {'submitting', 'submitted', 'pending', 'running',
                'observing_terminal', 'uncertain', 'completed', 'failed', 'cancelled'}):
        unresolved(worker, 'malformed remote observation; terminal proof unavailable')
    if (view.get('generation') != state['generation'] or view.get('execution_id') != attempt_id
            or view.get('request_digest') != state['remote_request_digest']):
        unresolved(worker, 'remote execution identity conflict')
    state['prompt_id'] = view['prompt_id']
    state['graph_digest'] = view['graph_digest']
    if view.get('terminal') is True and view.get('state') in ('completed', 'failed', 'cancelled'):
        state.update(terminal=True, status=view['state'])
    persist(worker, attempt_id, state)


async def run(worker, attempt_id, lease_token, task, local_inputs):
    if len(admitted_profiles(task)) != 1:
        raise ValueError('cloud execution requires an admitted profile')
    state = state_for(worker, attempt_id)
    remote = worker.remote
    # A real leased business attempt can produce the first validation artifact.
    # Preserve normal journal/GET-only recovery; this never creates a sample task.
    profile = admitted_profiles(task)[0]
    validated = any(p.get('profile_digest') == profile['profile_digest'] and p.get('ready') is True
        for p in worker.remote_status.get('profiles', [worker.remote_status.get('profile', {})]))
    body = {'execution_id': attempt_id, 'task': task,
            'acceptance': not validated}
    if state and not state.get('terminal') and confirmed_stopped(worker, state):
        state.update(terminal=True, status='failed')
        persist(worker, attempt_id, state)
    if state and state.get('terminal') and state.get('status') != 'completed':
        return False, {'code': 'ENGINE_FAILED', 'message': 'remote execution ended or provider stop confirmed'}
    if state and (state.get('request_digest') != digest(task) or state.get('generation') != remote.generation
                  or state.get('url') != remote.base):
        unresolved(worker, 'remote binding changed while execution is unresolved')
    if task_references(task):
        from .inputs import descriptors
        body['inputs'] = descriptors(task)
    if not state:
        resolved = dict(local_inputs)
        for name, anchor in (task.get('anchors') or {}).items():
            resolved['ref:' + anchor['asset_id']] = resolved.pop('anchors.' + name, None)
        if set(resolved) != {'ref:' + r['asset_id'] for r in task_references(task)} or any(v is None for v in resolved.values()):
            raise ValueError('cloud input resolution incomplete')
        state = {'remote': True, 'terminal': False, 'generation': remote.generation,
                 'url': remote.base, 'request_digest': digest(task),
                 'remote_request_digest': digest(body), 'submission_started': False}
        persist(worker, attempt_id, state)  # Own uploads before any byte transfer.
        try:
            for asset, descriptor in body.get('inputs', {}).items():
                worker._check_lease_alive()
                await asyncio.to_thread(remote.upload, resolved['ref:' + asset], descriptor,
                    worker._transfer_should_abort, execution_id=attempt_id, request_digest=digest(body))
            worker._check_lease_alive()
            if _shutdown_requested(worker) or worker.cancel_requested.is_set():raise CancelRequested()
        except BaseException:
            state.update(terminal=True, status='failed')
            persist(worker, attempt_id, state)
            raise
        state['submission_started'] = True
        persist(worker, attempt_id, state)
        try:
            view = await asyncio.to_thread(remote.json, '/v1/executions', body)
        except RemoteError as exc:
            # 403/409 prove refusal only when no uncertain prior intent existed.
            if exc.code in ('profile_not_prepared', 'paid_acceptance_disabled', 'functional_acceptance_required',
                            'exclusive_slot_busy', 'invalid_execution', 'request_too_large', 'invalid_body'):
                state.update(terminal=True, status='failed')
                persist(worker, attempt_id, state)
                return False, {'code': 'ENGINE_FAILED', 'message': exc.code}
            view = None  # Lost response: GET only, never repeat POST.
        if view is not None:
            apply_observation(worker, attempt_id, state, view)
    phase, observation_failures = None, 0
    try:
        while True:
            worker._check_lease_alive()
            if _shutdown_requested(worker) or worker.cancel_requested.is_set():
                raise CancelRequested()
            try:
                view = await asyncio.to_thread(remote.json, '/v1/executions/' + attempt_id)
                apply_observation(worker, attempt_id, state, view)
                observation_failures = 0
                if state.pop('observation_error', None) is not None:
                    persist(worker, attempt_id, state)
            except RemoteError as exc:
                if exc.code == 'execution_not_found':
                    # This boot never committed an intent, so it could not POST to Comfy.
                    state.update(terminal=True, status='failed')
                    persist(worker, attempt_id, state)
                    return False, {'code': 'ENGINE_FAILED', 'message': 'remote intent was not accepted'}
                code = exc.code if isinstance(exc.code, str) and re.fullmatch('[a-z][a-z0-9_]{0,63}', exc.code) else 'remote_error'
                status = exc.status if type(exc.status) is int and 0 <= exc.status <= 599 else 0
                state['observation_error'] = {'operation': 'get_execution', 'code': code, 'status': status}
                persist(worker, attempt_id, state)
                observation_failures += 1
                print(f'[cloud] {attempt_id} GET observation: {code}, HTTP {status}, failure {observation_failures}', flush=True)
                if (code in ('pod_unreachable', 'pod_observation_unavailable')
                        or status in (502, 503, 504)) and observation_failures < 3:
                    # Re-read the same intent; a missing observation is not a failed inference.
                    await asyncio.sleep(2 * observation_failures)
                    continue
                unresolved(worker, f'remote observation unavailable ({code}, HTTP {status}); retaining recovery intent')
            if state.get('terminal'):
                if state['status'] != 'completed':
                    return False, {'code': 'ENGINE_FAILED', 'message': 'remote ' + state['status']}
                artifact = view['artifact']
                name = 'result.wav' if task.get('mode')=='tts' else 'alignment.json' if task.get('mode')=='align' else 'result.mp4'
                dest = Path(worker.config.attempt_dir(attempt_id)) / name
                await asyncio.to_thread(remote.download, '/v1/executions/' + attempt_id + '/artifact',
                    dest, artifact['size_bytes'], artifact['sha256'], worker._transfer_should_abort)
                if task.get('mode') == 'a2va':
                    from .media import original_soundtrack
                    await asyncio.to_thread(original_soundtrack, dest, local_inputs['ref:' + task['references'][0]['asset_id']], task, worker.config)
                from .media import video_result, audio_result
                result = (await asyncio.to_thread(audio_result,dest,task) if task.get('mode') in ('tts','align')
                    else await asyncio.to_thread(video_result, dest, worker.config.ffprobe_path, task))
                result.update(prompt_id=state['prompt_id'], workflow_digest=state['graph_digest'])
                clear_quarantine(worker)
                return True, result
            if view['state'] == 'uncertain':
                unresolved(worker, 'Comfy submission or prompt status uncertain; no repeat generation')
            next_phase = 'denoise' if view['state'] == 'running' else 'loading'
            if phase != next_phase:
                await worker._emit_event(attempt_id, {'type': 'phase', 'phase': next_phase,
                    'detail_phase': 'RunPod ComfyUI ' + view['state']})
                await worker._flush_events_safe(attempt_id, lease_token)
                phase = next_phase
            await asyncio.sleep(2)
    except (CancelRequested, LeaseLost):
        if not state.get('terminal'):
            state['cancel_requested'] = True
            persist(worker, attempt_id, state)
            try:
                view = await asyncio.to_thread(remote.json, '/v1/executions/' + attempt_id + '/cancel', {})
                apply_observation(worker, attempt_id, state, view)
            except (RemoteError, NativeGroupUnproven):
                quarantine(worker, 'remote cancellation unresolved; reconciliation required')
        raise


async def recover_orphans(worker):
    resolved, pending = False, False
    for identity in worker.journal.conn.execute('SELECT attempt_id FROM attempts WHERE engine_state IS NOT NULL').fetchall():
        row = worker.journal.get_attempt(identity['attempt_id'])
        state = json.loads(row['engine_state']) if row.get('engine_state') else None
        if not state or not state.get('remote') or state.get('terminal'):
            continue
        if state.get('submission_started') is False:
            state.update(terminal=True,status='failed')
            persist(worker,row['attempt_id'],state)
            resolved=True
            continue
        if confirmed_stopped(worker, state):
            state.update(terminal=True, status='failed')
            persist(worker, row['attempt_id'], state)
            resolved = True
            continue
        try:
            if state['generation'] != worker.remote.generation or state['url'] != worker.remote.base:
                raise RemoteError('generation_conflict')
            route = '/v1/executions/' + row['attempt_id']
            cancel = row.get('confirmed_terminal') or state.get('cancel_requested') or row.get('cancel_intent')
            view = await asyncio.to_thread(worker.remote.json, route + '/cancel' if cancel else route,
                                           {} if cancel else None)
            apply_observation(worker, row['attempt_id'], state, view)
            resolved |= state.get('terminal', False)
            pending |= not state.get('terminal', False)
        except (RemoteError, NativeGroupUnproven):
            pending = True
    if pending:
        quarantine(worker, 'unresolved remote prompt; cloud slot fenced')
    else:
        clear_quarantine(worker)
    return resolved


async def release_finished(worker):
    # Confirmed H3 publication/finish is the ownership handoff, not Pod completion.
    for identity in worker.journal.conn.execute('SELECT attempt_id FROM attempts WHERE engine_state IS NOT NULL').fetchall():
        row = worker.journal.get_attempt(identity['attempt_id'])
        state = json.loads(row['engine_state']) if row.get('engine_state') else None
        if (not state or not state.get('remote') or not state.get('terminal') or state.get('status') not in ('completed','failed','cancelled') or state.get('released')
                or not row.get('confirmed_terminal')
                or (state.get('status') == 'completed' and not row.get('finish_accepted'))):
            continue
        if state['generation'] != worker.remote.generation or state['url'] != worker.remote.base:
            continue  # Another boot's data cannot be deleted via this binding.
        try:
            view = await asyncio.to_thread(worker.remote.json, '/v1/executions/' + row['attempt_id'] + '/release',
                {'request_digest': state['remote_request_digest']})
        except RemoteError as exc:
            if exc.code != 'execution_not_found' or state.get('status') == 'completed':raise
            view = {'generation':state['generation'],'execution_id':row['attempt_id'],
                'request_digest':state['remote_request_digest'],'released':True}
        if (view.get('generation') != state['generation'] or view.get('execution_id') != row['attempt_id']
                or view.get('request_digest') != state['remote_request_digest']):
            raise RemoteError('release_identity_conflict')
        if view.get('released') is not True:
            raise RemoteError('release_not_confirmed')
        state['released'] = True
        persist(worker, row['attempt_id'], state)


async def recovery_loop(worker):
    while not worker.stop_event.is_set():
        if worker.current_attempt is None:
            try:
                await release_finished(worker)
                if await recover_orphans(worker):
                    worker._comfy_reconcile_pending = True
            except Exception:
                quarantine(worker, 'remote recovery deferred')
        try:
            await asyncio.wait_for(worker.stop_event.wait(), 5)
        except asyncio.TimeoutError:
            pass
