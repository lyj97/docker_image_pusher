"""Bounded publication scheduler helpers; all task authority stays in Worker."""
import asyncio

NATIVE_MODES = {'t2va', 'fl2va', 'ref2va', 'a2va'}
MIN_HEADROOM = 20 * 1024**3
MAX_BYTES = 512 * 1024**2


def enabled(worker):
    return bool(getattr(worker.config, 'cpu_tail_overlap', False) and
                getattr(worker, '_cpu_overlap_negotiated', False))


def can_claim(worker):
    if worker._drain or worker.stop_event.is_set():
        return False
    if enabled(worker) and worker._disk_free_bytes() < max(worker.config.min_free_disk_bytes, MIN_HEADROOM) + MAX_BYTES:
        return False
    active = worker.journal.active_attempts()
    if not active:
        return True
    if not enabled(worker) or len(active) != 1:
        return False
    record = active[0]
    handoff = worker.journal.cpu_handoff(record['attempt_id'])
    runtime = worker._attempt_runtimes.get(record['attempt_id'])
    return bool(handoff and handoff['acknowledgement'] and
        handoff['acknowledgement']['gpu_occupancy_released'] is True and
        handoff['confirmed_boot'] == worker.boot_id and record.get('recovery_state') == 'online' and
        not record.get('cancel_intent') and runtime and not runtime.values['lease_lost'].is_set() and
        not runtime.values['cancel_requested'].is_set() and
        record['attempt_id'] in getattr(worker, '_publication_jobs', {}) and
        worker._disk_free_bytes() >= max(worker.config.min_free_disk_bytes, MIN_HEADROOM) + MAX_BYTES)


async def settle_jobs(worker):
    jobs = getattr(worker, '_publication_jobs', {})
    for aid, job in list(jobs.items()):
        if job.done():
            await asyncio.gather(job, return_exceptions=True)
            del jobs[aid]


async def tick(worker):
    await settle_jobs(worker)
    jobs = getattr(worker, '_publication_jobs', {})
    unresolved = [r for r in worker.journal.active_attempts() if r['attempt_id'] not in jobs]
    if unresolved:
        await worker._recover_on_boot()
    elif can_claim(worker):
        await worker._claim_and_execute_once()
    status = 'draining' if worker._drain else 'busy' if worker.journal.active_attempts() else 'idle'
    await worker._node_heartbeat(status)
    await asyncio.sleep(.1)


def paging_counters():
    """System-wide macOS cumulative I/O counters; never label faults as bytes."""
    import sys
    import subprocess
    import re
    if sys.platform != 'darwin':
        return dict(page_in_bytes=None, page_out_bytes=None, paging_counter_status='unavailable_platform')
    try:
        result = subprocess.run(['/usr/bin/vm_stat'], capture_output=True, text=True, timeout=1, check=True)
        page_size = int(re.search(r'page size of (\d+) bytes', result.stdout)[1])
        def counter(label):
            return int(re.search(r'^' + label + r':\s*(\d+)\.', result.stdout, re.M)[1]) * page_size
        return dict(page_in_bytes=counter('Pageins'), page_out_bytes=counter('Pageouts'),
                    paging_counter_status='available_system_wide_cumulative_bytes')
    except (OSError, subprocess.SubprocessError, ValueError, TypeError):
        return dict(page_in_bytes=None, page_out_bytes=None, paging_counter_status='unavailable_sampler_error')


def revisions(config):
    """Separate code tree and native executable identities; failures stay null."""
    import hashlib
    import subprocess
    from pathlib import Path
    from .upgrade import _repo_root
    worker = engine = None
    try:
        result = subprocess.run(['git', '-C', str(_repo_root()), 'rev-parse', 'HEAD'],
                                capture_output=True, text=True, timeout=2, check=True)
        worker = result.stdout.strip()
        if len(worker) != 40:
            worker = None
        dirty = subprocess.run(['git', '-C', str(_repo_root()), 'status', '--porcelain',
                                '--untracked-files=normal', '--', 'client/h3worker', 'shared'],
                               capture_output=True, text=True, timeout=2, check=True)
        if dirty.stdout.strip():
            worker = None  # HEAD alone cannot identify an edited Worker build
    except (OSError, subprocess.SubprocessError):
        pass
    path = Path(config.h3_binary)
    if not path.is_absolute():
        path = Path(config.h3_working_dir) / path
    try:
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
        engine = digest.hexdigest()
    except OSError:
        pass
    if config.fake_runner:
        engine = 'fake_runner_not_hardware_evidence'
    return dict(worker_revision=worker, engine_revision=engine)
