"""Content-addressed bounded asset transfer; never fetch task URLs on the Pod."""
import asyncio
import hashlib
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
from shared.execution import task_references
from .pod import Refused

MAX_ASSET = 512 * 1024**2
MAX_CACHE = 4 * 1024**3


def descriptors(task):
    return {r['asset_id']: {k: r[k] for k in ('sha256', 'size_bytes', 'content_type')}
            for r in task_references(task)}


def verify_descriptor(item):
    if (set(item) != {'sha256', 'size_bytes', 'content_type'}
            or not isinstance(item['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', item['sha256'])
            or type(item['size_bytes']) is not int or not 0 < item['size_bytes'] <= MAX_ASSET):
        raise Refused('invalid_input', 400)


def paths(pod, task, manifest):
    if manifest != descriptors(task):
        raise Refused('input_identity_conflict', 400)
    result = {}
    for asset, item in manifest.items():
        verify_descriptor(item)
        path = pod.root / 'inputs' / item['sha256']
        if not path.is_file() or path.is_symlink() or path.stat().st_size != item['size_bytes']:
            raise Refused('input_not_ready')
        result['ref:' + asset] = str(path)
    return result


async def receive(pod, request, sha, expires):
    if not re.fullmatch('[0-9a-f]{64}', sha):
        raise Refused('invalid_input', 400)
    length = int(request.headers.get('content-length', '0'))
    if not 0 < length <= MAX_ASSET:
        raise Refused('input_size_exceeds_bound', 413)
    if pod.draining or pod.active():
        raise Refused('exclusive_slot_busy')
    folder = pod.root / 'inputs';folder.mkdir(mode=0o700, exist_ok=True)
    if folder.is_symlink():
        raise Refused('unsafe_input_path')
    used = sum(p.stat().st_size for p in folder.iterdir() if p.is_file())
    if used + length > MAX_CACHE or shutil.disk_usage(folder).free < length + 1024**3:
        raise Refused('input_disk_limit', 413)
    pod.uploads += 1
    temporary = None
    try:
        fd, name = tempfile.mkstemp(prefix='.pending-', dir=folder);temporary = Path(name)
        h = hashlib.sha256();size = 0
        with os.fdopen(fd, 'wb') as output:
            stream = request.stream().__aiter__();deadline = time.monotonic() + 1800
            while True:
                try:chunk = await asyncio.wait_for(anext(stream), timeout=60)
                except StopAsyncIteration:break
                if time.monotonic() > deadline:raise Refused('input_transfer_timeout')
                if time.time() >= expires or pod.draining:
                    raise Refused('input_transfer_cancelled')
                size += len(chunk)
                if size > length:
                    raise Refused('input_size_exceeds_bound', 413)
                h.update(chunk);output.write(chunk)
            output.flush();os.fsync(output.fileno())
        if size != length or h.hexdigest() != sha:
            raise Refused('input_integrity_failure', 400)
        target = folder / sha
        if target.is_symlink():raise Refused('unsafe_input_path')
        os.replace(temporary, target)
        directory = os.open(folder, os.O_RDONLY);os.fsync(directory);os.close(directory)
        return {'generation': pod.generation, 'sha256': sha, 'size_bytes': size}
    finally:
        pod.uploads -= 1
        if temporary is not None:temporary.unlink(missing_ok=True)
