"""Renew one stopped Pod session. No start, inference, retry or task submission."""
import argparse
from contextlib import contextmanager, nullcontext
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import re
import secrets
import select
import subprocess
import sys
import time
import termios
from uuid import uuid4

from .control import Provider, H3Control
from .prepare import safe_root
from .transport import MAX_JSON


@contextmanager
def private_stdio():
    """Native child-only PTY protection when the tool cannot keep pipe stdin open."""
    saved = None
    fd = sys.stdin.fileno()
    try:
        if sys.stdin.isatty():
            saved = termios.tcgetattr(fd)
            private = list(saved)
            private[6] = list(saved[6])
            private[3] &= ~(termios.ECHO | termios.ECHONL | termios.ICANON)
            private[6][termios.VMIN], private[6][termios.VTIME] = 1, 0
            termios.tcsetattr(fd, termios.TCSANOW, private)
            if termios.tcgetattr(fd)[3] & (termios.ECHO | termios.ECHONL | termios.ICANON):
                raise ValueError('private terminal mode not established')
        yield
    finally:
        if saved is not None:
            termios.tcsetattr(fd, termios.TCSANOW, saved)


class MCPProvider(Provider):
    """Private stdio transport for three normal MCP operations, not shell commands."""
    def __init__(self, pod_id, *, input_fd=None, output=None, timeout=120):
        if not re.fullmatch('[A-Za-z0-9]{8,64}', pod_id):
            raise ValueError('invalid bound Pod id')
        self.pod_id, self.sequence = pod_id, 0
        self.input_fd = sys.stdin.fileno() if input_fd is None else input_fd
        self.output = sys.stdout if output is None else output
        self.timeout, self.buffer = timeout, b''

    def exchange(self, operation, params):
        if operation not in ('list_pods', 'get_pod', 'update_env'):
            raise ValueError('unsupported provider operation')
        self.sequence += 1
        message = json.dumps({'type':'provider_request', 'id':self.sequence,
                              'operation':operation, 'params':params})
        if len(message.encode()) > MAX_JSON:
            raise ValueError('provider frame too large')
        self.output.write(message + '\n'); self.output.flush()
        deadline = time.monotonic() + self.timeout
        while b'\n' not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([self.input_fd], [], [], remaining)[0]:
                raise ValueError('provider reply deadline')
            chunk = os.read(self.input_fd, 65536)
            if not chunk:
                raise ValueError('provider reply EOF')
            self.buffer += chunk
            if len(self.buffer) > MAX_JSON:
                raise ValueError('provider reply too large')
        raw, self.buffer = self.buffer.split(b'\n', 1)
        try:
            reply = json.loads(raw)
            if set(reply) != {'id', 'payload'} or type(reply['id']) is not int \
                    or reply['id'] != self.sequence or not isinstance(reply['payload'], dict):
                raise ValueError()
            return reply['payload']
        except Exception:
            raise ValueError('invalid provider reply') from None

    def request_url(self, url, stop=False):
        # Reuse Provider.inventory's complete pagination; reject every other endpoint.
        from urllib.parse import urlsplit, parse_qs
        parsed = urlsplit(url)
        query = parse_qs(parsed.query)
        if stop or parsed.scheme != 'https' or parsed.netloc != 'api.runpod.io' \
                or parsed.path != '/v2/pods' or parsed.fragment \
                or set(query) - {'includeClusterPods', 'limit', 'cursor'} \
                or query.get('includeClusterPods') != ['true'] or query.get('limit') != ['100'] \
                or any(len(values) != 1 for values in query.values()):
            raise ValueError('unsupported provider operation')
        params = {'includeClusterPods':True, 'limit':100}
        if 'cursor' in query:
            params['cursor'] = query['cursor'][0]
        return self.exchange('list_pods', params)

    def get(self, pod_id):
        if pod_id != self.pod_id:
            raise ValueError('provider Pod identity mismatch')
        return self.exchange('get_pod', {'id':pod_id})

    def update_env(self, pod_id, env):
        if pod_id != self.pod_id or not isinstance(env, dict) \
                or any(not isinstance(k, str) or not isinstance(v, str) for k,v in env.items()):
            raise ValueError('provider environment identity mismatch')
        return self.exchange('update_env', {'id':pod_id, 'body':{'env':env}})

    def stop(self, pod_id):
        raise ValueError('session bridge cannot start or stop resources')


def private_write(path, data):
    temp = path.with_name(path.name + '.pending')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as file:
        file.write(data)
        file.flush()
        os.fsync(file.fileno())
    temp.replace(path)


def safe_pod(pod, binding):
    return (pod.get('id') == binding['pod_id'] and pod.get('name') == binding['pod_name']
        and pod.get('status') == 'EXITED' and pod.get('cloud') == 'SECURE'
        and pod.get('gpu', {}).get('id') == 'NVIDIA RTX A5000'
        and pod.get('gpu', {}).get('count') == 1)


def private_config(path):
    safe_root(path)
    if not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError('private regular configuration required')


def inactive_services():
    for unit in ('h3-cloud-executor.service', 'h3-cloud-controller.service'):
        result = subprocess.run(['/usr/bin/systemctl', 'is-active', unit],
            capture_output=True, text=True, timeout=10, check=False)
        if result.stdout.strip() != 'inactive' or result.returncode != 3:
            raise ValueError('cloud services must be inactive before renewal')


def renew(binding_path, env_path, operation, provider, h3, *, ttl=86400, now=None):
    """A pending marker blocks executor startup on uncertain or interrupted updates."""
    now = time.time() if now is None else now
    if not re.fullmatch('[A-Za-z0-9_-]{1,64}', operation) or not 300 <= ttl <= 86400:
        raise ValueError('invalid session operation or lifetime')
    binding_path, env_path = Path(binding_path), Path(env_path)
    for path in (binding_path, env_path):
        private_config(path)
    root = binding_path.parent
    marker = root / 'session-renewal.pending'
    for path in (marker, root / 'session-renewal.lock'):
        safe_root(path)
    with (root / 'session-renewal.lock').open('a') as lock:
        os.chmod(lock.name, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if marker.exists():
            raise ValueError('unfinished session renewal; reconcile before boot')
        old_binding = binding_path.read_text()
        binding = json.loads(old_binding)
        history = root / 'session-history'
        safe_root(history)
        history.mkdir(mode=0o700, exist_ok=True)
        os.chmod(history, 0o700)
        record = history / operation
        safe_root(record)
        inventory = provider.inventory()
        if len(inventory) != 1 or inventory[0]['id'] != binding['pod_id']:
            raise ValueError('exactly one bound account Pod required')
        pod = provider.get(binding['pod_id'])
        workers = h3.request('/v1/admin/workers').get('workers', [])
        worker = next((w for w in workers if w.get('worker_id') == binding['worker_id']), {})
        if not safe_pod(pod, binding) or not h3.renewable() \
                or worker.get('operator_draining') is not True \
                or not isinstance(worker.get('capacity'), int) or worker['capacity'] <= 0:
            raise ValueError('stopped A5000 and empty H3 slot required')
        if record.exists():
            receipt = json.loads((record / 'receipt.json').read_text())
            expected = json.loads((record / 'new-session.json').read_text())
            if receipt.get('completed') is not True or binding['generation'] != expected['generation'] \
                    or receipt.get('expires_at', 0) <= now \
                    or any(pod.get('env', {}).get(k) != v for k, v in expected['pod_fields'].items()) \
                    or env_path.read_text().splitlines().count(
                        'H3BURST_POD_TOKEN=' + expected['pod_fields']['H3POD_TOKEN']) != 1:
                raise ValueError('session receipt does not match current binding')
            return {**{k: receipt[k] for k in ('operation', 'pod_id', 'status', 'generation',
                                               'expires_at', 'completed', 'started')}, 'cached': True}
        old_env = env_path.read_text()
        lines = old_env.splitlines()
        token_lines = [i for i, line in enumerate(lines) if line.startswith('H3BURST_POD_TOKEN=')]
        if len(token_lines) != 1:
            raise ValueError('one existing Pod credential slot required')
        generation, token, expiry = uuid4().hex, secrets.token_urlsafe(32), int(now + ttl)
        fields = {'H3POD_GENERATION': generation, 'H3POD_TOKEN': token,
                  'H3POD_TOKEN_EXPIRES_AT': str(expiry)}
        new_env = dict(pod.get('env') or {}, **fields)
        lines[token_lines[0]] = 'H3BURST_POD_TOKEN=' + token
        new_binding = dict(binding, generation=generation)
        record.mkdir(mode=0o700)
        private_write(record / 'binding-before.json', old_binding)
        private_write(record / 'executor-before.env', old_env)
        private_write(record / 'pod-env-before.json', json.dumps(pod.get('env') or {}))
        private_write(record / 'new-session.json', json.dumps({'generation': generation, 'pod_fields': fields}))
        private_write(marker, json.dumps({'operation': operation}))
        try:
            provider.update_env(binding['pod_id'], new_env)
            check = provider.get(binding['pod_id'])
            if not safe_pod(check, binding) or check.get('env') != new_env:
                raise ValueError('provider session readback mismatch')
            private_write(env_path, '\n'.join(lines) + '\n')
            private_write(binding_path, json.dumps(new_binding, indent=2) + '\n')
            receipt = {'operation': operation, 'pod_id': binding['pod_id'], 'status': 'EXITED',
                       'generation': generation, 'expires_at': expiry, 'completed': True,
                       'started': False, 'cached': False}
            private_write(record / 'receipt.json', json.dumps(receipt))
            marker.unlink()
            return receipt
        except Exception:
            # Never expose provider errors or private configuration in tracebacks.
            rollback = False
            try:
                check = provider.get(binding['pod_id'])
                if safe_pod(check, binding):
                    provider.update_env(binding['pod_id'], pod.get('env') or {})
                    check = provider.get(binding['pod_id'])
                    if safe_pod(check, binding) and check.get('env') == (pod.get('env') or {}):
                        private_write(env_path, old_env)
                        private_write(binding_path, old_binding)
                        rollback = True
            except Exception:
                pass
            private_write(record / 'receipt.json', json.dumps({'operation': operation,
                'completed': False, 'rollback_confirmed': rollback, 'started': False}))
            # Keep marker even after rollback: failed operation is never an implicit retry.
            raise ValueError('session renewal failed; startup blocked; rollback=' + str(rollback)) from None


def adopt(binding_path, env_path, operation, pod_id, proof_path, renewal_id, image, provider, h3):
    """Publish a replacement binding; never create, start or change provider env."""
    now=time.time()
    for value in (operation,renewal_id):
        if not re.fullmatch('[A-Za-z0-9_-]{1,64}',value):raise ValueError('invalid operation')
    if not re.fullmatch('[A-Za-z0-9]{8,64}',pod_id):raise ValueError('invalid Pod')
    binding_path,env_path,proof_path=map(Path,(binding_path,env_path,proof_path))
    for path in (binding_path,env_path,proof_path):private_config(path)
    root=binding_path.parent;marker=root/'session-renewal.pending'
    safe_root(marker);safe_root(root/'session-renewal.lock')
    with (root/'session-renewal.lock').open('a') as lock:
        os.chmod(lock.name,0o600);fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if marker.exists():raise ValueError('unfinished maintenance')
        old_binding=binding_path.read_text();binding=json.loads(old_binding)
        origin_path=root/'session-history'/renewal_id/'receipt.json';private_config(origin_path)
        origin=json.loads(origin_path.read_text());proof=json.loads(proof_path.read_text())
        record=root/'session-history'/operation;safe_root(record)
        cached=record.exists()
        if origin.get('completed') is not True or origin.get('started') is not False \
                or origin.get('generation')!=binding['generation'] \
                or origin.get('expires_at',0)<=now \
                or proof.get('id')!=origin.get('pod_id') or proof.get('status')!='EXITED' \
                or (not cached and binding['pod_id']!=proof['id']) \
                or proof.get('image')!=image:
            raise ValueError('unverified replacement source')
        parse=lambda value:datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
        # Default renewal was 24h. Provider start remained before that renewal;
        # both rejected starts left EXITED. No reused Comfy boot is authorized.
        renewed_at=origin['expires_at']-86400
        if not parse(proof['startedAt'])<renewed_at<=proof['observed_at']<=now:
            raise ValueError('source generation may have booted')
        inventory=provider.inventory()
        if len(inventory)!=1 or inventory[0]['id']!=pod_id:raise ValueError('unique replacement required')
        pod=provider.get(pod_id);candidate=dict(binding,pod_id=pod_id)
        stopped=dict(pod,status='EXITED')
        if not safe_pod(stopped,candidate) or pod.get('status') not in ('PROVISIONING','STARTING','RUNNING','EXITED') \
                or pod.get('image')!=image or pod.get('disk')!=150 or pod.get('mounts') \
                or parse(pod['createdAt'])<=proof['observed_at'] or not h3.renewable():
            raise ValueError('replacement identity or drain mismatch')
        lines=env_path.read_text().splitlines()
        tokens=[line.split('=',1)[1] for line in lines if line.startswith('H3BURST_POD_TOKEN=')]
        fields=pod.get('env') or {}
        if len(tokens)!=1 or fields.get('H3POD_TOKEN')!=tokens[0] \
                or fields.get('H3POD_GENERATION')!=binding['generation'] \
                or fields.get('H3POD_TOKEN_EXPIRES_AT')!=str(origin['expires_at']):
            raise ValueError('replacement session readback mismatch')
        result={'operation':operation,'pod_id':pod_id,'generation':binding['generation'],
            'status':pod['status'],'completed':True,'provider_mutated':False,'cached':cached}
        if cached:
            receipt_path=record/'receipt.json';private_config(receipt_path)
            receipt=json.loads(receipt_path.read_text())
            if binding['pod_id']!=pod_id or receipt.get('pod_id')!=pod_id \
                    or receipt.get('generation')!=binding['generation'] or receipt.get('completed') is not True:
                raise ValueError('cached adoption mismatch')
            return result
        record.mkdir(mode=0o700)
        private_write(record/'binding-before.json',old_binding)
        private_write(record/'source-proof.json',json.dumps(proof))
        private_write(marker,json.dumps({'operation':operation}))
        # Any interrupted publication retains the startup latch.
        private_write(binding_path,json.dumps(candidate,indent=2)+'\n')
        private_write(record/'receipt.json',json.dumps(result))
        marker.unlink()
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding', type=Path, required=True)
    parser.add_argument('--executor-env', type=Path, required=True)
    parser.add_argument('--id', required=True)
    parser.add_argument('--ttl', type=int, default=86400)
    parser.add_argument('--mcp-stdio', action='store_true',
                        help='private JSON bridge to existing MCP tools; child disables PTY echo if needed')
    parser.add_argument('--adopt-pod')
    parser.add_argument('--old-pod-proof',type=Path)
    parser.add_argument('--renewal-id')
    parser.add_argument('--image')
    args = parser.parse_args()
    try:
        with private_stdio() if args.mcp_stdio else nullcontext():
            private_config(args.binding)
            private_config(args.executor_env)
            inactive_services()
            binding = json.loads(args.binding.read_text())
            provider = MCPProvider(args.adopt_pod or binding['pod_id']) if args.mcp_stdio else Provider(os.environ.get('RUNPOD_API_KEY'))
            h3=H3Control(binding.get('h3_admin_url','http://127.0.0.1:8730'),binding['worker_id'])
            if args.adopt_pod:
                if not args.old_pod_proof or not args.renewal_id or not args.image:raise ValueError('adoption inputs required')
                result=adopt(args.binding,args.executor_env,args.id,args.adopt_pod,
                    args.old_pod_proof,args.renewal_id,args.image,provider,h3)
            else:
                if args.old_pod_proof or args.renewal_id or args.image:raise ValueError('unexpected adoption inputs')
                result = renew(args.binding, args.executor_env, args.id,
                provider,
                h3, ttl=args.ttl)
        print(json.dumps({'type':'result', 'payload':result} if args.mcp_stdio else result))
    except Exception:
        result = {'completed': False, 'error': 'session_renewal_refused_or_failed', 'started': False}
        print(json.dumps({'type':'result', 'payload':result} if args.mcp_stdio else result))
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
