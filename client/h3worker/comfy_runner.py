"""Dedicated local ComfyUI adapter. H3 owns leases and terminal decisions.

Reuse the pinned VACE transport/observation rules without modifying smoke tools.
No POST retry, installation, global interrupt, or queue mutation.
"""
import asyncio
import copy
import fcntl
import json
import math
import os
import shutil
import sqlite3
import threading
import time
import uuid
import re
import urllib.request
import urllib.error
from contextlib import closing
from pathlib import Path

from shared.comfy_versions import SUPPORTED
from shared.comfy_policy import CAPABILITY, MODE, validate
from shared.h3proto import digest_obj
import vace_smoke as smoke
from vace_reconcile import observe
from . import runner
from .config import WorkerConfig
from .http import sanitize_error_text
from .comfy_progress import ProgressStream

POLL_SECONDS = 2
TERMINAL = {'completed', 'failed', 'cancelled'}
ABSENCE_OBSERVATIONS = 6
ABSENCE_GRACE_SECONDS = 60
_RUNTIME_LOCK = threading.Lock()


class PromptAbsenceConflict(RuntimeError):
    """Fresh maintenance evidence contradicts an earlier absence window."""


def configured(config: WorkerConfig) -> bool:
    if (getattr(config, 'fake_runner', False)
            or not getattr(config, 'comfyui_ready', False)):
        return False
    root = Path(config.comfyui_root)
    return (
        config.comfyui_version in SUPPORTED
        and root.is_absolute()
        and not any(p.is_symlink() for p in (root, *root.parents))
        and all((root / name).is_dir() and not (root / name).is_symlink()
                for name in ('input', 'output'))
        and bool(config.comfyui_url)
        and bool(shutil.which(config.ffprobe_path or 'ffprobe'))
    )


def runtime(worker):
    config = worker.config
    root = Path(config.comfyui_root)
    if (not root.is_absolute()
            or any(p.is_symlink() for p in (root, *root.parents))
            or any((root / name).is_symlink() for name in ('input', 'output'))):
        raise ValueError('ComfyUI root must be absolute without symlinks')
    if config.comfyui_version not in SUPPORTED:
        raise ValueError('unsupported configured ComfyUI version')
    root = root.resolve()
    with _RUNTIME_LOCK:
        _lock_instance(worker, root)
    api = smoke.API(config.comfyui_url, timeout=3)
    smoke.check_version(api, expected=config.comfyui_version)
    return api


def _lock_instance(worker, root):
    if not getattr(worker, '_comfy_lock', None):
        lock = (root / '.h3-instance.lock').open('a')
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            lock.close()
            raise
        worker._comfy_lock = lock  # held for Worker lifetime, across attempts


def available(worker):
    if not configured(worker.config):
        return False
    try:
        runtime(worker)
        return True
    except (OSError, ValueError, RuntimeError, smoke.SmokeError):
        return False


def state_for(worker, attempt_id):
    raw = worker.journal.get_attempt(attempt_id).get('engine_state')
    return json.loads(raw) if raw else None


def persist(worker, attempt_id, state):
    worker.journal.update_attempt(attempt_id, engine_state=state)


def bind_graph(task, local_inputs, root, namespace):
    if not re.fullmatch(r'h3_[0-9a-f]{32}', namespace):
        raise ValueError('invalid ComfyUI input namespace')
    root = Path(root)
    if any(p.is_symlink() for p in (root, *root.parents, root / 'input')):
        raise ValueError('unsafe ComfyUI input binding')
    graph = copy.deepcopy(task['workflow']['graph'])
    folder = root / 'input' / namespace
    folder.mkdir(mode=0o700, exist_ok=False)
    types = {'image/png': '.png', 'image/jpeg': '.jpg', 'image/webp': '.webp',
             'video/mp4': '.mp4', 'video/webm': '.webm', 'audio/wav': '.wav',
             'audio/mpeg': '.mp3', 'audio/flac': '.flac', 'audio/ogg': '.ogg'}
    refs = {r['asset_id']: r for r in task['references']}
    try:
        for index, binding in enumerate(task['workflow']['bindings']):
            asset = binding['asset_id']
            source = local_inputs['ref:' + asset]
            suffix = types.get(refs[asset].get('content_type'))
            if suffix is None:
                import mimetypes
                content_type = str(refs[asset].get('content_type') or '')
                if not content_type.startswith(('image/', 'video/', 'audio/')):
                    raise ValueError('unsupported bound asset media type')
                suffix = mimetypes.guess_extension(content_type) or '.bin'
                if not re.fullmatch(r'\.[A-Za-z0-9]{1,12}', suffix):
                    suffix = '.bin'
            name = f'asset_{index}{suffix}'
            with (folder / name).open('xb') as output, open(source, 'rb') as incoming:
                shutil.copyfileobj(incoming, output)
            node = graph[binding['node_id']]
            if node['class_type'] == 'LanPaint_VideoMaskEditor' and binding['input'] == 'video':
                # The pinned node lists only root files. A hard link retains ownership proof.
                visible = namespace + '_' + name
                os.link(folder / name, root / 'input' / visible)
            else:
                visible = namespace + '/' + name
            node['inputs'][binding['input']] = visible
        graph[task['workflow']['output_node']]['inputs']['filename_prefix'] = namespace + '/video'
        return graph
    except BaseException:
        cleanup_bound_inputs(root, namespace)
        raise


def cleanup_bound_inputs(root, namespace):
    """Remove owned input aliases only when they still share the private file's inode."""
    root = Path(root)
    if not re.fullmatch(r'h3_[0-9a-f]{32}', namespace):
        raise ValueError('invalid ComfyUI input namespace')
    folder = root / 'input' / namespace
    if any(p.is_symlink() for p in (root, *root.parents, root / 'input', folder)):
        raise ValueError('unsafe ComfyUI input cleanup')
    if folder.exists():
        if not folder.is_dir() or any(p.is_symlink() for p in folder.rglob('*')):
            raise ValueError('unsafe ComfyUI input cleanup')
        aliases = []
        for source in folder.iterdir():
            if not re.fullmatch(r'asset_[0-9]+\.[a-z0-9]+', source.name) or not source.is_file():
                continue
            alias = root / 'input' / (namespace + '_' + source.name)
            if alias.is_symlink():
                raise ValueError('unsafe ComfyUI input cleanup')
            if alias.is_file() and os.path.samefile(source, alias):
                aliases.append(alias)
        for alias in aliases:
            alias.unlink()
        shutil.rmtree(folder)


def cleanup_inputs(worker, state):
    """Delete only this journal-owned generated input directory after proof."""
    if not state or not state.get('terminal') or state.get('native'):
        return
    root = Path(worker.config.comfyui_root)
    namespace = str(state.get('prefix', '')).removesuffix('/video')
    if (not re.fullmatch(r'h3_[0-9a-f]{32}', namespace)
            or namespace != 'h3_' + str(state.get('prompt_id', '')).replace('-', '')
            or str(root.resolve()) != state.get('root')):
        return  # Legacy/unowned paths require local operator recovery.
    cleanup_bound_inputs(root, namespace)


def native_request(api, path, body, token):
    raw = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode()
    req = urllib.request.Request(api.base + path, data=raw, method='POST',
        headers={'Content-Type': 'application/json', 'Origin': api.base, 'X-H3-Execution': token})
    try:
        with api.opener.open(req, timeout=api.timeout) as response:
            if response.status != 200 or response.geturl() != req.full_url:
                raise smoke.SmokeError('native engine response refused')
            reply = response.read(smoke.MAX_JSON + 1)
            if len(reply) > smoke.MAX_JSON:
                raise ValueError()
            value = json.loads(reply)
            if not isinstance(value, dict):
                raise ValueError('invalid native engine response')
            return value
    except urllib.error.HTTPError as exc:
        raw = exc.read(smoke.MAX_JSON + 1)
        exc.close()
        try:
            value = json.loads(raw) if len(raw) <= smoke.MAX_JSON else {}
            if not isinstance(value, dict):
                value = {}
            if (path == '/prompt' and
                    (isinstance(value.get('error'), dict) and value['error'].get('type') == 'h3_native_refused'
                     or smoke.prompt_rejection(value) is not None)):
                raise smoke.PromptRejected('native prompt rejected before execution')
        except (ValueError, TypeError):
            pass
        raise smoke.SmokeError('native engine response uncertain; no automatic retry') from None
    except Exception:
        raise smoke.SmokeError('native engine response uncertain; no automatic retry') from None


def output_path(item, root, prefix, node_id):
    # Re-key only the declared output; the existing strict prefix/root validator
    # ignores all other nodes and rejects multiple files or symlink escapes.
    output = smoke.save_output({'outputs': {smoke.SAVE_NODE:
        item.get('outputs', {}).get(node_id)}}, root, prefix)[0]
    expected = root.resolve() / 'output' / Path(prefix).parent / output.name
    if output != expected or expected.is_symlink():
        raise smoke.SmokeError('SaveVideo output aliases another path')
    return output


def video_result(path, config):
    info = runner._media_probe(str(path), config)
    videos = [s for s in info.get('streams', []) if s.get('codec_type') == 'video']
    if len(videos) != 1:
        raise ValueError('output requires exactly one video stream')
    v = videos[0]
    duration = float(info.get('format', {}).get('duration') or 0)
    width, height = int(v.get('width', 0)), int(v.get('height', 0))
    if not math.isfinite(duration) or not 0 < duration <= 3600 or not 0 < width <= 8192 or not 0 < height <= 8192:
        raise ValueError('invalid or excessive video dimensions/duration')
    return {'width': width, 'height': height, 'duration_seconds': duration,
            'output': str(path)}


async def cancel(worker, attempt_id, api, state):
    if state.get('terminal'):
        return
    # Persist intent before every idempotent, prompt-scoped attempt. Legacy
    # cancel_sent records also remain retryable after a lost response.
    state['cancel_sent'] = True
    state['cancel_attempts'] = min(state.get('cancel_attempts', 0) + 1, 2147483647)
    persist(worker, attempt_id, state)
    try:
        reply = await asyncio.to_thread(api.json, '/api/jobs/' + state['prompt_id'] + '/cancel', {})
        if not isinstance(reply, dict) or type(reply.get('cancelled')) is not bool:
            raise smoke.SmokeError('invalid prompt cancellation response')
        state['cancel_result'] = reply['cancelled']
        if reply['cancelled']:
            if 'cancel_accepted_at' not in state:
                reset_absence(state)
                state['cancel_accepted_at'] = time.time()
        persist(worker, attempt_id, state)
    except (OSError, ValueError, smoke.SmokeError):
        pass  # cancellation response is never terminal proof


def absent(snapshot):
    # observe's unknown means no job, no history and no prompt membership.
    return (snapshot.get('state') == 'unknown'
            and snapshot.get('queue_running') == 0
            and snapshot.get('queue_pending') == 0)


def _observation_fresh(last, now):
    return (type(last) in (int, float) and math.isfinite(last)
            and last <= now
            and now - last <= RECOVERY_MAX_SECONDS + 3 * POLL_SECONDS + 10)


def recovery_update_absence_ready(state, now):
    """Absence window for controlled update admission, not terminal proof."""
    evidence = state.get('recovery', {})
    since = evidence.get('absent_since')
    last = evidence.get('last_observed_at')
    return (state.get('cancel_sent') is True
            and type(since) in (int, float) and math.isfinite(since)
            and _observation_fresh(last, now) and since <= last
            and evidence.get('absent_observations', 0) >= ABSENCE_OBSERVATIONS
            and now - since >= ABSENCE_GRACE_SECONDS)


def absence_ready(state, now):
    """Terminal absence proof requires acceptance before the evidence window."""
    accepted = state.get('cancel_accepted_at')
    return (recovery_update_absence_ready(state, now)
            and type(accepted) in (int, float) and math.isfinite(accepted)
            and accepted <= state['recovery']['absent_since'] <= now)


def reset_absence(state):
    recovery = state.setdefault('recovery', {})
    recovery.pop('absent_since', None)
    recovery.pop('absent_observations', None)


async def run(worker, attempt_id, lease_token, task, local_inputs):
    from .worker import CancelRequested, _shutdown_requested

    problems = validate(task, worker.config.comfyui_denied_classes)
    if problems:
        raise ValueError('; '.join(problems))
    state = state_for(worker, attempt_id)
    api = None
    progress = None
    submission_error = None
    native = task.get('_native')
    grants = getattr(getattr(worker, '_preview_bridge', None), 'native', None)
    native_token = None
    try:
        api = await asyncio.to_thread(runtime, worker)
        root = Path(worker.config.comfyui_root).resolve()
        if state:
            if (state['request_digest'] != digest_obj(task) or state['root'] != str(root)
                    or state['url'] != api.base or state.get('identity_conflict')):
                raise RuntimeError('ComfyUI persisted identity/configuration conflict')
        else:
            worker._check_lease_alive()
            if _shutdown_requested(worker) or worker.cancel_requested.is_set():
                raise CancelRequested()
            prompt_id = str(uuid.uuid4())
            namespace = 'h3_' + prompt_id.replace('-', '')
            state = dict(prompt_id=prompt_id, client_id=native['client_id'] if native else str(uuid.uuid4()),
                         root=str(root), url=api.base, prefix=namespace + '/video',
                         request_digest=digest_obj(task), terminal=False, cancel_sent=False,
                         submission_started=False, native=bool(native))
            persist(worker, attempt_id, state)  # Intent BEFORE any private copy.
            try:
                if native:
                    if grants is None:
                        raise ValueError('native bridge unavailable')
                    graph, native_token = await grants.prepare(attempt_id, lease_token, task, prompt_id, state['client_id'])
                else:
                    graph = await asyncio.to_thread(bind_graph, task, local_inputs, root, namespace)
            except BaseException:
                state.update(terminal=True, status='failed')  # No engine submission.
                persist(worker, attempt_id, state)
                raise
            worker._check_lease_alive()
            if _shutdown_requested(worker) or worker.cancel_requested.is_set():
                raise CancelRequested()
            state['graph_digest'] = digest_obj(graph)
            persist(worker, attempt_id, state)  # FULL SQLite commit BEFORE POST
            progress = ProgressStream(api.base, state, task['workflow']['graph'])
            await progress.start()  # subscribe before submission, bounded wait
            worker._check_lease_alive()
            if _shutdown_requested(worker) or worker.cancel_requested.is_set():
                raise CancelRequested()
            try:
                state['submission_started'] = True
                persist(worker, attempt_id, state)
                body = {'prompt': graph, 'prompt_id': prompt_id, 'client_id': state['client_id']}
                reply = (await asyncio.to_thread(native_request, api, '/prompt', body, native_token)
                         if native else await asyncio.to_thread(api.json, '/prompt', body))
            except smoke.PromptRejected as exc:
                state.update(terminal=True, status='failed')
                persist(worker, attempt_id, state)
                return False, {'code': 'ENGINE_FAILED', 'message': str(exc)}
            except (OSError, ValueError, smoke.SmokeError) as exc:
                submission_error = exc  # may have executed: observe same identity, NEVER resubmit
            else:
                returned_id = reply.get('prompt_id')
                if returned_id != prompt_id and not (returned_id is None and reply.get('node_errors')):
                    state['identity_conflict'] = True
                    persist(worker, attempt_id, state)
                    raise RuntimeError('ComfyUI submission identity conflict or node errors')
                if reply.get('node_errors'):
                    # A real engine rejection is a terminal runner failure.
                    state.update(terminal=True, status='failed')
                    persist(worker, attempt_id, state)
                    return False, {'code': 'ENGINE_FAILED',
                                   'message': json.dumps(reply['node_errors'], ensure_ascii=False)}
                state['submission_accepted'] = True
                persist(worker, attempt_id, state)
        if progress is None:
            progress = ProgressStream(api.base, state, task['workflow']['graph'])
            await progress.start()
        prior = None
        uncertain = 0
        phase = None
        last_counter = None
        while True:
            worker._check_lease_alive()
            if native:
                if grants is None:
                    raise CancelRequested()
                try:
                    await grants.check(state['prompt_id'])
                except Exception:
                    raise CancelRequested() from None
            if (_shutdown_requested(worker) or worker.cancel_requested.is_set()
                    or state.get('cancel_requested')):
                raise CancelRequested()
            snapshot, item = await asyncio.to_thread(observe, api, state['prompt_id'])
            status = snapshot['state']
            if status in {'queued', 'running', *TERMINAL} and not state.get('submission_accepted'):
                state['submission_accepted'] = True  # Observe, never resubmit.
                persist(worker, attempt_id, state)
            uncertain = uncertain + 1 if status in {'unknown', 'ambiguous'} else 0
            if uncertain >= 3:
                if submission_error is not None:
                    raise submission_error
                raise RuntimeError('ComfyUI prompt disappeared or is ambiguous; automatic reconciliation pending')
            if status in TERMINAL and prior == status:
                state['terminal'] = True
                state['status'] = status
                persist(worker, attempt_id, state)
                if status != 'completed':
                    message = 'ComfyUI ' + status
                    for kind, detail in (item or {}).get('status', {}).get('messages', []):
                        if kind == 'execution_error' and isinstance(detail, dict):
                            message = detail.get('exception_message') or json.dumps(detail, ensure_ascii=False)
                    return False, {'code': 'ENGINE_FAILED', 'message': message}
                await worker._emit_event(attempt_id, {'type': 'phase', 'phase': 'validating',
                    'detail_phase': 'ComfyUI output validation'})
                source = output_path(item, root, state['prefix'], task['workflow']['output_node'])
                dest = Path(worker.config.attempt_dir(attempt_id)) / 'result.mp4'
                await asyncio.to_thread(shutil.copyfile, source, dest)
                result = await asyncio.to_thread(video_result, dest, worker.config)
                result.update(prompt_id=state['prompt_id'], workflow_digest=state['graph_digest'])
                if native:
                    result['input_digests'] = dict(grants.entries[state['prompt_id']]['input_digests'])
                return True, result
            if state.get('cancel_sent') and status not in TERMINAL:
                await cancel(worker, attempt_id, api, state)
            next_phase = 'denoise' if status == 'running' else 'loading'
            detail, counter = ('ComfyUI ' + status + ' (HTTP observation)', None)
            observed = progress.detail() if status == 'running' and progress.connected else None
            if observed is not None:
                detail, counter = observed
            elif status == 'running':
                detail = 'ComfyUI running; node telemetry unavailable (HTTP observation)'
            context = (next_phase, detail)
            changed = False
            if phase != context:
                await worker._emit_event(attempt_id, {'type': 'phase', 'phase': next_phase,
                    'detail_phase': detail, 'new_phase_instance': True})
                phase = context
                last_counter = None
                changed = True
            if counter is not None and counter != last_counter:
                await worker._emit_event(attempt_id, {'type': 'progress', 'phase': next_phase,
                    'detail_phase': detail, 'completed': counter[0], 'total': counter[1],
                    'unit': 'items', 'semantics': 'submitted'})
                last_counter = list(counter)
                state['progress_counters'] = dict(progress.counters)
                persist(worker, attempt_id, state)
                changed = True
            if changed:
                await worker._flush_events_safe(attempt_id, lease_token)
            # Coalesce WebSocket bursts to at most one phase + counter per HTTP
            # observation. Neither callbacks nor previews enter the H3 outbox.
            prior = status
            await asyncio.sleep(POLL_SECONDS)
    finally:
        if progress is not None:
            await progress.close()
        if state and not state.get('terminal'):
            if state.get('submission_started') is False:
                state.update(terminal=True, status='cancelled')
                persist(worker, attempt_id, state)
            if (not state.get('terminal') and api is not None and state.get('url') == api.base
                    and state.get('root') == str(Path(worker.config.comfyui_root).resolve())
                    and not state.get('identity_conflict')):
                try:
                    await asyncio.wait_for(stop_prompt(worker, attempt_id, api, state), 4)
                except (asyncio.TimeoutError, OSError, ValueError, smoke.SmokeError):
                    pass
            if not state.get('terminal'):
                quarantine(worker, 'ComfyUI prompt unresolved; automatic reconciliation pending')
        if native and state and grants is not None:
            if native_token and api is not None:
                try:
                    await asyncio.to_thread(native_request, api, '/h3/native-release',
                        {'execution': state['prompt_id']}, native_token)
                except Exception:
                    pass  # Volatile Worker grant is still revoked below.
            grants.revoke(state['prompt_id'])
        if state and state.get('terminal'):
            await asyncio.to_thread(cleanup_inputs, worker, state)


async def stop_prompt(worker, attempt_id, api, state):
    await cancel(worker, attempt_id, api, state)
    first, _ = await asyncio.to_thread(observe, api, state['prompt_id'])
    second, _ = await asyncio.to_thread(observe, api, state['prompt_id'])
    if first['state'] in TERMINAL and first['state'] == second['state'] and not state.get('identity_conflict'):
        state['terminal'] = True
        state['status'] = second['state']
        persist(worker, attempt_id, state)


def quarantine(worker, reason):
    """Record diagnostic telemetry without blocking ordinary execution."""
    reason = sanitize_error_text(reason, secrets=(
        getattr(worker.config, "worker_token", ""),
        getattr(worker.config, "cf_access_client_id", ""),
        getattr(worker.config, "cf_access_client_secret", "")))
    worker._comfy_unhealthy_reason = reason
    # Lightweight adapter fixtures/embedders without Worker's property.
    if not isinstance(getattr(type(worker), '_unhealthy_reason', None), property):
        previous = getattr(worker, '_comfy_owned_reason', None)
        if worker._unhealthy_reason is None or worker._unhealthy_reason == previous:
            worker._unhealthy_reason = reason
            worker._comfy_owned_reason = reason


def clear_quarantine(worker):
    worker._comfy_unhealthy_reason = None
    if not isinstance(getattr(type(worker), '_unhealthy_reason', None), property):
        if worker._unhealthy_reason == getattr(worker, '_comfy_owned_reason', None):
            worker._unhealthy_reason = None
        worker._comfy_owned_reason = None


async def recover_orphans(worker):
    lock = getattr(worker, '_comfy_maintenance_lock', None)
    if lock is not None:
        async with lock:
            return await _recover_orphans(worker)
    return await _recover_orphans(worker)


async def _recover_orphans(worker):
    """One bounded pass, retaining original identity and durable terminal proof.

    The live loop calls this only outside attempt/command execution. Startup
    also uses it before registration. No submission or lifecycle operations.
    """
    rows = worker.journal.conn.execute(
        'SELECT attempt_id, engine_state FROM attempts WHERE engine_state IS NOT NULL').fetchall()
    unresolved = False
    for row in rows:
        state = json.loads(row['engine_state'])
        if state.get('terminal'):
            await asyncio.to_thread(cleanup_inputs, worker, state)
            continue
        if state.get('submission_started') is False:
            state.update(terminal=True, status='cancelled')
            persist(worker, row['attempt_id'], state)
            await asyncio.to_thread(cleanup_inputs, worker, state)
            continue
        unresolved = True
        quarantine(worker, 'ComfyUI prompt unresolved; automatic reconciliation pending')
        recovery = state.setdefault('recovery', {})
        recovery.update(passes=min(recovery.get('passes', 0) + 1, 2147483647),
                        last_started_at=time.time(), state='observing', error=None)
        persist(worker, row['attempt_id'], state)
        try:
            api = await asyncio.to_thread(runtime, worker)
            if (state['url'] != api.base
                    or state['root'] != str(Path(worker.config.comfyui_root).resolve())
                    or state.get('identity_conflict')):
                raise RuntimeError('instance identity changed or submission conflict')
            prior = None
            for index in range(3):
                if worker.stop_event.is_set():
                    return False
                snapshot, _ = await asyncio.to_thread(observe, api, state['prompt_id'])
                status = snapshot['state']
                now = time.time()
                # Validate the prior timestamp before replacing it. A fresh
                # snapshot cannot bridge an outage or a backwards clock jump.
                if not _observation_fresh(recovery.get('last_observed_at'), now):
                    reset_absence(state)
                recovery.update(state=status, observations=min(
                    recovery.get('observations', 0) + 1, 2147483647),
                    last_observed_at=now)
                if absent(snapshot) and state.get('cancel_sent') is True:
                    recovery.setdefault('absent_since', now)
                    recovery['absent_observations'] = min(
                        recovery.get('absent_observations', 0) + 1, ABSENCE_OBSERVATIONS)
                else:
                    reset_absence(state)
                    # Reappearance invalidates the earlier accepted cancel.
                    state.pop('cancel_accepted_at', None)
                if absent(snapshot) and absence_ready(state, now):
                    state.update(terminal=True, status='cancelled')
                    recovery.update(resolved_at=time.time(), proof='accepted_cancel_stable_absence')
                    persist(worker, row['attempt_id'], state)
                    break
                if status == prior and status in TERMINAL:
                    state.update(terminal=True, status=status)
                    recovery['resolved_at'] = time.time()
                    persist(worker, row['attempt_id'], state)
                    break
                persist(worker, row['attempt_id'], state)
                if status not in TERMINAL:
                    await cancel(worker, row['attempt_id'], api, state)
                prior = status
                if index < 2:
                    await asyncio.sleep(POLL_SECONDS)
        except (OSError, ValueError, RuntimeError, smoke.SmokeError) as exc:
            reset_absence(state)
            recovery.update(state='blocked', error=sanitize_error_text(exc, secrets=(
                getattr(worker.config, 'worker_token', ''),
                getattr(worker.config, 'cf_access_client_id', ''),
                getattr(worker.config, 'cf_access_client_secret', '')))[:200])
            persist(worker, row['attempt_id'], state)
        if not state.get('terminal'):
            quarantine(worker, 'ComfyUI prompt ' + state['prompt_id']
                       + ' unresolved: ' + recovery['state']
                       + '; passes=' + str(recovery['passes'])
                       + ('; ' + recovery['error'] if recovery.get('error') else ''))
        if state.get('terminal'):
            await asyncio.to_thread(cleanup_inputs, worker, state)
    # Re-read durable state; clearing never depends on an empty runtime queue.
    remaining = any(not json.loads(row['engine_state']).get('terminal') for row in
                    worker.journal.conn.execute(
                        'SELECT engine_state FROM attempts WHERE engine_state IS NOT NULL'))
    if not remaining:
        if unresolved:
            worker._comfy_reconcile_pending = True
        clear_quarantine(worker)
    return unresolved and not remaining


RECOVERY_MIN_SECONDS = 5
RECOVERY_MAX_SECONDS = 60


async def recovery_loop(worker):
    """Worker-lifetime task, independent of heartbeats, claims and readiness."""
    delay = RECOVERY_MIN_SECONDS
    while not worker.stop_event.is_set():
        # Never race the normal runner's state writes or a maintenance command.
        command = getattr(worker, '_command_task', None)
        if (getattr(worker, 'current_attempt', None) is None
                and (command is None or command.done())):
            try:
                resolved = await recover_orphans(worker)
                if resolved:
                    worker._comfy_reconcile_pending = True
                delay = (min(delay * 2, RECOVERY_MAX_SECONDS)
                         if getattr(worker, '_comfy_unhealthy_reason', None)
                         else RECOVERY_MIN_SECONDS)
            except Exception as exc:
                # Journal/monitor faults belong to their own slot and cannot
                # disappear when a later ComfyUI observation succeeds.
                if getattr(worker, '_other_unhealthy_reason', None) is None:
                    worker._unhealthy_reason = ('ComfyUI recovery journal/state error: '
                                                + sanitize_error_text(exc)[:200])
                delay = RECOVERY_MAX_SECONDS
        try:
            await asyncio.wait_for(worker.stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass


def maintenance_preflight(config: WorkerConfig, journal_path: str, *,
                          recovery_update=False) -> None:
    """Read-only gate. Caller must already hold drain and prevent other producers.

    Runtime maintenance requires terminal proof. Only a controlled recovery
    Git update may preserve unresolved identity with durable and fresh absence
    evidence; it cannot resolve an orphan or authorize claims.
    """
    uri = Path(journal_path).absolute().as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True, timeout=3)) as conn:
        active = conn.execute(
            'SELECT count(*) FROM attempts WHERE confirmed_terminal = 0'
        ).fetchone()[0]
        processes = conn.execute('SELECT count(*) FROM processes').fetchone()[0]
        if active or processes:
            raise RuntimeError('active local attempt/process blocks maintenance')
        rows = conn.execute(
            'SELECT engine_state FROM attempts WHERE engine_state IS NOT NULL'
        ).fetchall()
    states = [json.loads(row[0]) for row in rows]
    absent_states = []
    for state in states:
        if (not isinstance(state, dict) or state.get('identity_conflict')
                or not isinstance(state.get('prompt_id'), str)
                or not state['prompt_id']):
            raise RuntimeError('unresolved ComfyUI engine_state blocks maintenance')
        if state.get('terminal') is not True:
            if (not recovery_update or state.get('terminal') is not False
                    or state.get('url') != config.comfyui_url
                    or state.get('root') != str(Path(config.comfyui_root).resolve())
                    or not recovery_update_absence_ready(state, time.time())):
                raise RuntimeError('unresolved ComfyUI engine_state blocks maintenance')
            absent_states.append(state)
    if not config.comfyui_url and not states:
        return
    if not configured(config):
        raise RuntimeError('cannot prove configured ComfyUI instance safe')
    api = smoke.API(config.comfyui_url, timeout=3)
    smoke.check_version(api, expected=config.comfyui_version)
    for state in absent_states:
        for _ in range(2):
            snapshot, _ = observe(api, state['prompt_id'])
            if not absent(snapshot):
                raise PromptAbsenceConflict('ComfyUI prompt not provably absent; update refused')
    queue = api.json('/queue')
    if (not isinstance(queue, dict) or queue.get('queue_running') != []
            or queue.get('queue_pending') != []):
        raise PromptAbsenceConflict('ComfyUI queue not provably empty; maintenance refused')
