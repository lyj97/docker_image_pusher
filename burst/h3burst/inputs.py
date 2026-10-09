"""Content-addressed bounded asset transfer; never fetch task URLs on the Pod."""
import asyncio
import hashlib
import os
from pathlib import Path
import re
import time
from shared.execution import task_references
from .pod import Refused, ID_RE

def descriptors(task):
    return {r['asset_id']: {k: r[k] for k in ('sha256', 'size_bytes', 'content_type')}
            for r in task_references(task)}


def verify_descriptor(item):
    if (set(item) != {'sha256', 'size_bytes', 'content_type'}
            or not isinstance(item['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', item['sha256'])
            or type(item['size_bytes']) is not int or item['size_bytes'] <= 0):
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
    owner = request.headers.get('x-h3-execution','')
    request_digest = request.headers.get('x-h3-request-digest','')
    if not re.fullmatch(ID_RE,owner) or not re.fullmatch('[0-9a-f]{64}',request_digest):
        raise Refused('invalid_upload_owner',400)
    length = int(request.headers.get('content-length', '0'))
    if length <= 0:
        raise Refused('input_size_exceeds_bound', 413)
    with pod.mutex:
        if pod.draining or pod.active():
            raise Refused('exclusive_slot_busy')
    folder = pod.root / 'inputs';folder.mkdir(mode=0o700, exist_ok=True)
    if folder.is_symlink():
        raise Refused('unsafe_input_path')
    with pod.mutex:
        if pod.draining or pod.active():raise Refused('exclusive_slot_busy')
        intent=pod.db.execute('SELECT request_digest,released FROM upload_intent WHERE id=?',(owner,)).fetchone()
        if intent and (intent['released'] or intent['request_digest'] != request_digest):
            raise Refused('upload_identity_conflict')
        if pod.db.execute('SELECT 1 FROM execution WHERE id=?',(owner,)).fetchone():
            raise Refused('upload_identity_conflict')
        pending = folder / ('.pending-' + owner + '-' + sha)
        if pending.exists() or pending.is_symlink():
            raise Refused('pending_upload_requires_release')
        # Reserve ownership durably before creating or publishing any bytes.
        with pod.db:
            pod.db.execute('INSERT OR IGNORE INTO upload_intent(id,request_digest) VALUES (?,?)',(owner,request_digest))
            pod.db.execute('INSERT OR IGNORE INTO pending_input VALUES (?,?)',(owner,sha))
        pod.uploads += 1
    temporary = None
    try:
        fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        temporary = pending
        h = hashlib.sha256();size = 0
        with os.fdopen(fd, 'wb') as output:
            stream = request.stream().__aiter__()
            while True:
                try:chunk = await asyncio.wait_for(anext(stream), timeout=60)
                except StopAsyncIteration:break
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
        with pod.mutex:
            os.replace(temporary, target)
            directory = os.open(folder, os.O_RDONLY);os.fsync(directory);os.close(directory)
        return {'generation': pod.generation, 'sha256': sha, 'size_bytes': size}
    finally:
        with pod.mutex:pod.uploads -= 1
        if temporary is not None:temporary.unlink(missing_ok=True)
