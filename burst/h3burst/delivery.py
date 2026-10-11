"""Fixed owner-only SSH delivery of an already verified Pod result; no inference."""
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import sqlite3
import stat
import subprocess
import sys
import time
import traceback

if __package__:
    from .transport import RemoteError
else:
    # sshd does not inherit Docker's PYTHONPATH; this is the fixed published entry file.
    sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
    from h3burst.transport import RemoteError

MAX_HEADER = 8192
MAX_ARTIFACT = 1024 ** 3
COMMAND = 'python3 -B /opt/h3-service/burst/h3burst/delivery.py'


def open_regular(path):
    """Walk with directory FDs so a parent replacement cannot redirect the read."""
    directory=os.open('/',os.O_RDONLY|os.O_DIRECTORY)
    try:
        for component in path.parts[1:-1]:
            child=os.open(component,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory)
            os.close(directory);directory=child
        return os.open(path.name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
    finally:os.close(directory)


def checked_request(value):
    if set(value) != {'execution_id', 'generation', 'request_digest', 'prompt_id', 'graph_digest', 'runtime_digest', 'pod_id'}:
        raise ValueError('invalid delivery identity')
    patterns = {'execution_id': r'att_[A-Za-z0-9_-]{8,64}', 'generation': r'[A-Za-z0-9_.:-]{1,128}',
        'request_digest': '[0-9a-f]{64}', 'graph_digest': '[0-9a-f]{64}',
        'runtime_digest': '[0-9a-f]{64}',
        'prompt_id': r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
        'pod_id': '[a-z0-9]{8,32}'}
    if any(not isinstance(value[k], str) or not re.fullmatch(p, value[k]) for k,p in patterns.items()):
        raise ValueError('invalid delivery identity')
    return value


def serve(request, output, *, root=Path('/workspace/h3-pod'), output_root=Path('/workspace/h3-service-comfy/output')):
    checked_request(request)
    for path in (root, output_root):
        if not path.is_absolute() or any(p.is_symlink() for p in (path,*path.parents)):
            raise ValueError('unsafe delivery root')
    db = root / 'executions.sqlite3'
    if db.is_symlink() or not db.is_file():raise ValueError('unsafe delivery journal')
    with sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=1) as conn:
        row = conn.execute('SELECT record FROM execution WHERE id=? AND generation=?',
            (request['execution_id'], request['generation'])).fetchone()
    if not row:raise ValueError('delivery identity absent')
    record = json.loads(row[0])
    if any(record.get(k) != request[k] for k in request):
        raise ValueError('delivery identity mismatch')
    if record.get('terminal') is not True or record.get('state') != 'completed' or record.get('released'):
        raise ValueError('delivery not verified complete')
    artifact = record['artifact']
    size, sha = artifact.get('size_bytes'), artifact.get('sha256')
    if type(size) is not int or not 0 < size <= MAX_ARTIFACT or not isinstance(sha,str) or not re.fullmatch('[0-9a-f]{64}',sha):
        raise ValueError('invalid delivery descriptor')
    filename = Path(artifact['filename'])
    prefix = 'h3_' + record['prompt_id'].replace('-', '')
    path = output_root / filename
    if filename.is_absolute() or '..' in filename.parts or not path.is_relative_to(output_root/prefix) or any(p.is_symlink() for p in (path,*path.parents)):
        raise ValueError('unsafe delivery path')
    # Keep this exact opened inode throughout streaming, never reopen a caller path.
    fd = open_regular(path)
    with os.fdopen(fd,'rb') as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size != size:raise ValueError('delivery size changed')
        header = dict(request, terminal=True, state='completed', artifact={k:artifact[k] for k in ('size_bytes','sha256')})
        raw = json.dumps(header,separators=(',',':')).encode() + b'\n'
        if len(raw) > MAX_HEADER:raise ValueError('delivery header too large')
        output.write(raw);output.flush()
        sent = 0
        while chunk := source.read(min(1024*1024, size-sent+1)):
            sent += len(chunk)
            if sent > size:raise ValueError('delivery size changed')
            output.write(chunk)
        after = os.fstat(source.fileno())
        if sent != size or (before.st_size,before.st_mtime_ns,before.st_ino) != (after.st_size,after.st_mtime_ns,after.st_ino):
            raise ValueError('delivery file changed')
        output.flush()


class SSHDelivery:
    def __init__(self, binding, generation, pod_id):
        if set(binding) != {'host','port','identity_file','known_hosts','generation','pod_id'}:
            raise ValueError('invalid SSH delivery binding')
        if binding['generation'] != generation or binding['pod_id'] != pod_id:
            raise ValueError('SSH delivery binding mismatch')
        host,port = binding['host'],binding['port']
        if not isinstance(host,str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9.-]{0,252}',host) or type(port) is not int or not 1<=port<=65535:
            raise ValueError('invalid SSH delivery endpoint')
        for key in ('identity_file','known_hosts'):
            path = Path(binding[key])
            if not path.is_absolute() or not path.is_file() or any(p.is_symlink() for p in (path,*path.parents)):
                raise ValueError('unsafe SSH trust file')
            if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o022:
                raise ValueError('unsafe SSH trust ownership')
            if key=='identity_file' and path.stat().st_mode & 0o077:
                raise ValueError('SSH private key must be private')
        self.binding = dict(binding)
        self.argv = ['ssh','-F','/dev/null','-T','-i',binding['identity_file'],'-p',str(port),
            '-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','ConnectTimeout=2',
            '-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+binding['known_hosts'],
            '-o','GlobalKnownHostsFile=/dev/null','root@'+host,COMMAND]

    def download(self, request, destination, abort=lambda:False, *, budget=30):
        checked_request(request)
        if request['generation'] != self.binding['generation'] or request['pod_id'] != self.binding['pod_id']:
            raise RemoteError('delivery_binding_conflict')
        destination = Path(destination); partial = destination.with_name(destination.name+'.pending')
        if not destination.is_absolute() or any(p.is_symlink() for p in (destination,*destination.parents)):
            raise RemoteError('unsafe_delivery_destination')
        if destination.exists() or partial.exists():raise RemoteError('delivery_destination_exists')
        if type(budget) not in (int,float) or not 0<budget<=30:raise RemoteError('invalid_delivery_budget')
        started=time.monotonic();limit=started+budget;deadline=min(limit,started+5)
        process = selector = None
        header, buffered, size, digest = None, bytearray(), 0, hashlib.sha256()
        stderr=bytearray();stderr_truncated=False
        try:
            process = subprocess.Popen(self.argv,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            selector = selectors.DefaultSelector()
            selector.register(process.stdout,selectors.EVENT_READ)
            selector.register(process.stderr,selectors.EVENT_READ)
            process.stdin.write(json.dumps(request).encode()+b'\n');process.stdin.close()
            with partial.open('xb') as target:
                os.chmod(partial,0o600)
                while True:
                    if abort():raise RemoteError('transfer_aborted')
                    if time.monotonic() >= deadline:raise RemoteError('delivery_deadline_exceeded')
                    if not selector.get_map():break
                    events=selector.select(.05)
                    if not events:continue
                    stream=events[0][0].fileobj
                    chunk = os.read(stream.fileno(),1024*1024)
                    if stream is process.stderr:
                        if not chunk:selector.unregister(stream)
                        else:
                            stderr_truncated |= len(stderr)+len(chunk)>65536
                            stderr.extend(chunk[:max(0,65536-len(stderr))])
                        continue
                    if not chunk:
                        selector.unregister(stream);continue
                    if header is None:
                        buffered.extend(chunk)
                        newline = buffered.find(b'\n')
                        if newline < 0:
                            if len(buffered)>MAX_HEADER:raise RemoteError('delivery_header_exceeds_bound')
                            continue
                        if newline>MAX_HEADER:raise RemoteError('delivery_header_exceeds_bound')
                        header=json.loads(buffered[:newline]);chunk=bytes(buffered[newline+1:]);buffered.clear()
                        if not isinstance(header,dict) or set(header)!=set(request)|{'terminal','state','artifact'}:
                            raise RemoteError('delivery_identity_conflict')
                        if any(header.get(k)!=v for k,v in request.items()) or header.get('terminal') is not True or header.get('state')!='completed':
                            raise RemoteError('delivery_identity_conflict')
                        artifact=header.get('artifact',{}); expected=artifact.get('size_bytes');sha=artifact.get('sha256')
                        if type(expected) is not int or not 0<expected<=MAX_ARTIFACT or not isinstance(sha,str) or not re.fullmatch('[0-9a-f]{64}',sha):
                            raise RemoteError('invalid_artifact_descriptor')
                        # Connection/header stays short; declared-size transfer remains capped by ownership.
                        deadline=min(limit,started+max(5,3+expected/(512*1024)))
                    size+=len(chunk)
                    if size>expected:raise RemoteError('artifact_exceeds_bound')
                    digest.update(chunk);target.write(chunk)
                exit_code=process.wait(timeout=max(.01,deadline-time.monotonic()))
                if exit_code!=0:
                    raise RemoteError('ssh_delivery_failed') from RuntimeError(
                        'SSH exit '+str(exit_code)+'; captured stderr'+(' truncated' if stderr_truncated else '')+': '+stderr.decode('utf-8','replace'))
                if header is None or size!=expected or digest.hexdigest()!=sha or abort():
                    raise RemoteError('artifact_verification_failed')
                target.flush();os.fsync(target.fileno())
            if abort():raise RemoteError('transfer_aborted')
            # Persist the authenticated descriptor before publishing bytes. A crash after
            # rename can be recovered without another network transfer.
            receipt=destination.with_name(destination.name+'.receipt.json')
            receipt_pending=receipt.with_name(receipt.name+'.pending')
            with receipt_pending.open('x',encoding='utf-8') as output:
                os.chmod(receipt_pending,0o600)
                json.dump(header,output);output.flush();os.fsync(output.fileno())
            os.replace(receipt_pending,receipt)
            os.replace(partial,destination)
            directory=os.open(destination.parent,os.O_RDONLY|os.O_DIRECTORY)
            try:os.fsync(directory)
            finally:os.close(directory)
            return header
        except (ValueError,TypeError,KeyError,AttributeError,OSError,subprocess.TimeoutExpired) as exc:
            raise RemoteError('ssh_delivery_failed') from exc
        finally:
            if selector is not None:
                try:selector.close()
                except OSError:pass
            if process is not None:
                try:
                    if process.poll() is None:process.kill();process.wait()
                except OSError:pass
                for stream in (process.stdin,process.stdout,process.stderr):
                    if stream is not None:
                        try:stream.close()
                        except OSError:pass
            try:partial.unlink(missing_ok=True)
            except OSError:pass


def main():
    try:
        raw=sys.stdin.buffer.readline(MAX_HEADER+1)
        if len(raw)>MAX_HEADER:raise ValueError('delivery request exceeds bound')
        serve(json.loads(raw),sys.stdout.buffer)
    except Exception:
        # This authenticated peer stores the exception only in its restricted diagnostic sink.
        traceback.print_exc(file=sys.stderr)
        raise SystemExit(1)


if __name__=='__main__':main()
