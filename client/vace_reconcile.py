#!/usr/bin/env python3
"""Observe one VACE run; optionally cancel its prompt or resume local validation.

Stdlib only. Never submits work or changes H3 drain. Run separately from smoke.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
import urllib.error
import uuid

import vace_smoke as smoke


def read_json(path):
    with path.open('rb') as source:
        raw = source.read(smoke.MAX_JSON + 1)
    if len(raw) > smoke.MAX_JSON:
        raise smoke.SmokeError('evidence record exceeds limit')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise smoke.SmokeError('expected evidence object')
    return value


def recover(evidence):
    intent = read_json(evidence / 'intent.json')
    if intent.get('schema') != 1:
        raise smoke.SmokeError('unsupported intent schema')
    for field in ('prompt_id', 'client_id'):
        value = intent.get(field)
        if not isinstance(value, str) or str(uuid.UUID(value)) != value:
            raise smoke.SmokeError('invalid intent identity')
    response = evidence / 'submission-response.json'
    if response.exists() and read_json(response).get('identity_matches') is not True:
        raise smoke.SmokeError('submission identity conflict requires operator investigation')
    submission = evidence / 'submission.json'
    if submission.exists() and read_json(submission).get('prompt_id') != intent['prompt_id']:
        raise smoke.SmokeError('submission and intent disagree')
    if smoke.sha256(evidence / 'api-workflow.json') != intent.get('workflow_sha256'):
        raise smoke.SmokeError('workflow differs from durable intent')
    # Reuse the smoke argument constraints, also bounding media validation work.
    argv = ['--evidence-dir', str(evidence)]
    for name in ('comfy_root', 'base_url', 'width', 'height', 'frames', 'steps', 'seed', 'output_prefix'):
        argv.extend(['--' + name.replace('_', '-'), str(intent[name])])
    args = smoke.arguments(argv)
    if smoke.build_workflow(args) != read_json(evidence / 'api-workflow.json'):
        raise smoke.SmokeError('workflow does not match validation configuration')
    return intent['prompt_id'], args


def observe(api, prompt_id):
    """Independent snapshots can race: contradictions stay unresolved until re-read."""
    history = api.json('/history/' + prompt_id)
    queue = api.json('/queue')
    if (not isinstance(history, dict) or not isinstance(queue, dict)
            or (prompt_id in history and not isinstance(history[prompt_id], dict))):
        raise smoke.SmokeError('invalid ComfyUI history/queue')
    membership = []
    counts = {}
    for key, state in (('queue_running', 'running'), ('queue_pending', 'pending')):
        rows = queue.get(key)
        if not isinstance(rows, list) or any(
                not isinstance(row, list) or len(row) < 2 or not isinstance(row[1], str)
                for row in rows):
            raise smoke.SmokeError('invalid ComfyUI queue')
        counts[key] = len(rows)
        membership.extend(state for row in rows if row[1] == prompt_id)
    try:
        job = api.json('/api/jobs/' + prompt_id)
        if not isinstance(job, dict):
            raise smoke.SmokeError('invalid ComfyUI job response')
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
        exc.close()
        job = None
    item = history.get(prompt_id)
    job_status = job.get('status') if job else None
    if job is not None and (job.get('id') != prompt_id or job_status not in {
            'pending', 'in_progress', 'completed', 'failed', 'cancelled'}):
        raise smoke.SmokeError('invalid ComfyUI job identity or status')
    state = 'ambiguous'
    if len(membership) == 1 and item is None:
        expected = {'running': 'in_progress', 'pending': 'pending'}[membership[0]]
        if job_status == expected:
            state = membership[0]
    elif not membership:
        if item is None and job is None:
            state = 'unknown'
        elif isinstance(item, dict):
            status = item.get('status', {})
            if (status.get('completed') is True and status.get('status_str') == 'success'
                    and job_status == 'completed'):
                state = 'completed'
            elif status.get('status_str') == 'error' and job_status in {'failed', 'cancelled'}:
                state = job_status
    return dict(state=state, **counts), item


def reconcile(api, prompt_id, args, evidence, cancel=False, validate=False, timeout=300):
    start = time.monotonic()
    prior = None
    cancel_attempted = False
    polls = 0
    while True:
        snapshot, item = observe(api, prompt_id)
        polls += 1
        state = snapshot['state']
        # v0.37.0's boolean cancel result cannot distinguish dequeue from a
        # pending -> running race followed by interrupt. Even repeated absence
        # is therefore unknown, never proof of dequeue or completed interrupt.
        result = dict(prompt_id=prompt_id, **snapshot, polls=polls,
                      elapsed_seconds=round(time.monotonic() - start), keep_drain=True,
                      resolved=False, validated=False)
        smoke.write_json(evidence / 'state.json', result)
        smoke.emit('reconciling', **result)
        if state in {'completed', 'failed', 'cancelled'} and prior == state:
            result['resolved'] = True
            result['evidence'] = str(evidence.resolve())
            if validate:
                if state != 'completed':
                    raise smoke.SmokeError('only completed successful output can be validated')
                args.evidence_dir = evidence
                smoke.validate_completed(item, prompt_id, args,
                                         smoke.executable('ffprobe'), smoke.executable('ffmpeg'))
                result['validated'] = True
            smoke.write_json(evidence / 'result.json', result)
            smoke.emit('reconciled', **result)
            return result
        if cancel and not cancel_attempted and state in {'pending', 'running'}:
            cancel_attempted = True
            smoke.write_json(evidence / 'cancel-intent.json', dict(prompt_id=prompt_id, observed_state=state))
            try:
                response = api.json('/api/jobs/' + prompt_id + '/cancel', {})
                if type(response.get('cancelled')) is not bool:
                    raise smoke.SmokeError('invalid cancel response')
                smoke.write_json(evidence / 'cancel-response.json',
                                 dict(cancelled=response['cancelled']))
            except (OSError, ValueError) as exc:
                # Response loss is not proof of cancellation. Continue observing,
                # without retrying; terminal history can still resolve the run.
                smoke.write_json(evidence / 'cancel-error.json', smoke.failure(exc))
        prior = state
        remaining = timeout - (time.monotonic() - start)
        if remaining <= 0:
            raise smoke.SmokeError('reconciliation unresolved at deadline; keep drain held')
        time.sleep(min(smoke.POLL_SECONDS, remaining))


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--cancel', action='store_true', help='targeted cancellation; never global interrupt')
    parser.add_argument('--validate', action='store_true', help='resume validation of successful output')
    parser.add_argument('--timeout', type=int, default=300, help='observation budget, 1..7000 seconds')
    args = parser.parse_args(argv)
    if not 1 <= args.timeout <= 7000:
        raise smoke.SmokeError('timeout must be in 1..7000')
    return args


def run(options):
    evidence = options.evidence_dir.resolve(strict=True)
    with smoke.run_lock(evidence):
        invocations = evidence / 'reconciliation'
        invocations.mkdir(exist_ok=True)
        smoke.sync_directory(evidence)
        invocation = invocations / str(uuid.uuid4())
        invocation.mkdir()
        smoke.sync_directory(invocations)
        try:
            smoke.write_json(invocation / 'invocation.json', dict(
                cancel=options.cancel, validate=options.validate, timeout=options.timeout))
            prompt_id, args = recover(evidence)
            smoke.emit('reconciliation_started', prompt_id=prompt_id,
                       evidence=str(invocation), keep_drain=True)
            api = smoke.API(args.base_url)
            smoke.check_version(api)
            return reconcile(api, prompt_id, args, invocation,
                             options.cancel, options.validate, options.timeout)
        except (Exception, KeyboardInterrupt) as exc:
            smoke.write_json(invocation / 'failure.json', smoke.failure(exc))
            raise


def main(argv=None):
    try:
        run(arguments(argv))
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        smoke.emit('failed', **smoke.failure(exc))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
