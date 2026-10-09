"""Durable stop-only provider controller for explicitly bound, manually started Pods.

No create/start/delete methods exist. Budget withdrawal precedes safe drain;
unknown execution or provider state never authorizes a blind stop.
"""
import argparse
import asyncio
from decimal import Decimal
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
import urllib.request
import urllib.parse

from .transport import Transport, NoRedirect, RemoteError, MAX_JSON
from .prepare import atomic_json


class Provider:
    def __init__(self, key):
        if not key:
            raise ValueError('RunPod provider credential required')
        self.key = key
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, pod_id, stop=False):
        if not re.fullmatch('[A-Za-z0-9]{8,64}', pod_id):
            raise ValueError('invalid bound Pod id')
        url = 'https://api.runpod.io/v2/pods/' + pod_id + ('/action' if stop else '')
        return self.request_url(url, stop)

    def request_url(self, url, stop=False):
        req = urllib.request.Request(url, data=b'{"action":"stop"}' if stop else None,
            headers={'Authorization': 'Bearer ' + self.key, 'Content-Type': 'application/json'})
        try:
            with self.opener.open(req, timeout=10) as response:
                raw = response.read(MAX_JSON + 1)
                if len(raw) > MAX_JSON:
                    raise RemoteError('provider_response_too_large')
                return json.loads(raw) if raw else {}
        except Exception:
            raise RemoteError('provider_operation_uncertain') from None

    def inventory(self):
        """Account-wide, including stopped and cluster Pods; reject partial lists."""
        pods, cursors = {}, set()
        cursor = None
        for _ in range(100):
            query = {'includeClusterPods': 'true', 'limit': '100'}
            if cursor:
                query['cursor'] = cursor
            page = self.request_url('https://api.runpod.io/v2/pods?' + urllib.parse.urlencode(query))
            if not isinstance(page, dict) or not isinstance(page.get('pods'), list):
                raise RemoteError('invalid_pod_inventory')
            for pod in page['pods']:
                if not isinstance(pod, dict) or not isinstance(pod.get('id'), str):
                    raise RemoteError('invalid_pod_inventory')
                pods[pod['id']] = {'id': pod['id'], 'name': pod.get('name'), 'status': pod.get('status')}
            pagination = page.get('pagination')
            if not isinstance(pagination, dict) or type(pagination.get('hasNextPage')) is not bool:
                raise RemoteError('incomplete_pod_inventory')
            if not pagination['hasNextPage']:
                return list(pods.values())
            cursor = pagination.get('nextCursor')
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise RemoteError('incomplete_pod_inventory')
            cursors.add(cursor)
        raise RemoteError('incomplete_pod_inventory')

    def get(self, pod_id):
        return self.request(pod_id)

    def stop(self, pod_id):
        return self.request(pod_id, True)


class H3Control:
    """Existing loopback-only admin API; no DB credentials or direct task mutation."""
    def __init__(self, url, worker_id):
        if not re.fullmatch('[A-Za-z0-9_-]{1,128}', worker_id):
            raise ValueError('invalid Worker identity')
        if url not in ('http://127.0.0.1:8730', 'http://localhost:8730'):
            raise ValueError('H3 stop controller uses the existing private loopback admin boundary')
        self.base, self.worker = url, worker_id
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, path, body=None):
        req = urllib.request.Request(self.base + path,
            data=None if body is None else json.dumps(body).encode(),
            method='PATCH' if body is not None else 'GET', headers={'Content-Type': 'application/json'})
        with self.opener.open(req, timeout=5) as response:
            raw = response.read(MAX_JSON + 1)
        if len(raw) > MAX_JSON:
            raise ValueError('H3 control response too large')
        return json.loads(raw)

    def drain(self):
        self.request('/v1/admin/workers/' + self.worker, {'operator_draining': True})

    def idle(self):
        proof = self.request('/v1/admin/workers/' + self.worker + '/cloud-stop-proof')
        return proof.get('safe_to_stop') is True



class Controller:
    def __init__(self, root, binding, provider, remote, h3, journal):
        self.root = Path(root)
        if not self.root.is_absolute() or any(p.is_symlink() for p in (self.root, *self.root.parents)):
            raise ValueError('controller state requires a private absolute directory')
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.lock = (self.root / 'controller.lock').open('a')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.db = sqlite3.connect(self.root / 'controller.sqlite3')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT)')
        self.binding, self.provider, self.remote, self.h3, self.journal = binding, provider, remote, h3, journal
        identity = {k: binding[k] for k in ('owner', 'pod_id', 'pod_name', 'worker_id', 'generation')}
        prior = self.read('identity')
        if prior and prior != identity:
            raise ValueError('controller slot already bound; reconcile before changing identity')
        self.save('identity', identity)
        self.rate = Decimal(str(binding['hourly_usd']))
        self.cap = Decimal(str(binding['max_hourly_usd']))
        self.budget = Decimal(str(binding['session_budget_usd']))
        if not all(x.is_finite() and x > 0 for x in (self.rate, self.cap, self.budget)):
            raise ValueError('finite positive budget and rates required')
        if not 30 <= binding['idle_seconds'] <= 3600:
            raise ValueError('idle timeout out of bounds')
        if not self.read('accounting_start'):
            # Explicit provider/operator start time, not first controller observation.
            start = binding['billing_started_at']
            if not isinstance(start, (int, float)) or not 0 < start <= time.time():
                raise ValueError('validated billing start time required')
            self.save('accounting_start', start)

    def close(self):
        self.db.close()
        self.lock.close()

    def read(self, key):
        row = self.db.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def save(self, key, value):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO state VALUES (?,?)', (key, json.dumps(value)))

    def local_idle(self):
        active = self.journal.active_attempts()
        command = self.journal.get_command()
        rows = self.journal.conn.execute('SELECT engine_state FROM attempts WHERE engine_state IS NOT NULL').fetchall()
        return (not active and not (command and not command.get('confirmed'))
            and not self.journal.conn.execute('SELECT 1 FROM processes LIMIT 1').fetchone()
            and not self.journal.conn.execute('SELECT 1 FROM claim_requests WHERE resolved_attempt_id IS NULL LIMIT 1').fetchone()
            and not any(not json.loads(r['engine_state']).get('terminal') for r in rows))

    def request_stop(self, now):
        # Persist before the request so a lost reply cannot cause a tight retry loop.
        try:
            self.save('stop_requested_at', now)
        finally:
            self.provider.stop(self.binding['pod_id'])

    def abort_test(self, now=None):
        """Explicit test policy: bounded evidence then stop even during preparation."""
        try:
            try:
                self.save('phase', 'STOPPING')
            except Exception:
                pass
            try:
                self.h3.drain()
            except Exception:
                pass
            try:
                diagnostics = self.remote.json('/v1/diagnostics')
                atomic_json(self.root / ('diagnostics-' + self.binding['generation'] + '.json'), diagnostics)
            except Exception:
                try:
                    self.save('diagnostics_unavailable', True)
                except Exception:
                    pass
        finally:
            self.request_stop(time.time() if now is None else now)
        return {'state':'STOPPING', 'reason':'test_error', 'provider_confirmed':False}

    def tick(self, now=None):
        now = time.time() if now is None else now
        if self.read('phase') == 'OFF':
            return {'state': 'OFF', 'automatic_start': False}
        pod = self.provider.get(self.binding['pod_id'])
        if pod.get('id') != self.binding['pod_id'] or pod.get('name') != self.binding['pod_name']:
            raise ValueError('provider ownership binding conflict')
        # REST v2 state is status; legacy desiredStatus is not stop confirmation.
        provider_status = pod.get('status')
        spent = max(self.rate, self.cap) * Decimal(str(max(0, now - self.read('accounting_start')))) / Decimal(3600)
        self.save('estimated_compute_usd', str(spent))
        phase = self.read('phase') or 'OBSERVING'
        if provider_status in ('STOPPED', 'EXITED'):
            self.h3.drain()
            # Retain unresolved journals for subsequent H3 recovery; never fake a finish.
            atomic_json(self.root / 'provider-stopped.json', {
                'provider_confirmed': True, 'status': provider_status, 'worker_id': self.binding['worker_id'],
                'generation': self.binding['generation'], 'pod_url': self.binding['pod_url'],
                'pod_id': self.binding['pod_id'], 'observed_at': now})
            self.save('phase', 'OFF')
            return {'state': 'OFF', 'provider_confirmed': True, 'estimated_compute_usd': str(spent)}
        if provider_status != 'RUNNING':
            self.h3.drain()
            self.save('phase', 'RECONCILING')
            return {'state': 'RECONCILING', 'automatic_start': False}
        if self.binding.get('stop_on_error') is True:
            if phase == 'STOPPING':
                if now - (self.read('stop_requested_at') or 0) >= 60:
                    self.request_stop(now)
                return {'state':'STOPPING', 'provider_confirmed':False}
            try:
                view = self.remote.json('/v1/status')
            except RemoteError:
                return self.abort_test(now)
            if view.get('stop_required') is True:
                return self.abort_test(now)
        if phase == 'STOPPING':
            if now - (self.read('stop_requested_at') or 0) < 60:
                return {'state': 'STOPPING', 'provider_confirmed': False}
            # Lost reply: provider GET and fresh drain proof before another stop.
            self.h3.drain()
            view = self.remote.json('/v1/status')
            if not (self.local_idle() and self.h3.idle() and view.get('draining') is True
                    and view.get('active_executions') == 0 and view.get('queue_empty') is True
                    and view.get('preparing') is False):
                return {'state': 'STOPPING', 'reason': 'renewed_stop_proof_required'}
            self.request_stop(now)
            return {'state': 'STOPPING'}
        if self.binding.get('stop_on_error') is not True:
            view = self.remote.json('/v1/status')
        idle = (self.local_idle() and view.get('active_executions') == 0
                and view.get('queue_empty') is True and view.get('preparing') is False)
        if idle:
            idle_since = self.read('idle_since')
            if idle_since is None:
                idle_since = now
                self.save('idle_since', now)
        else:
            idle_since = now
            self.save('idle_since', None)
        stop = (phase in ('DRAINING', 'RECONCILING') or self.rate > self.cap or spent >= self.budget
                or view.get('stop_required') is True
                or now - idle_since >= self.binding['idle_seconds'])
        if not stop:
            self.save('phase', 'READY' if view.get('profile', {}).get('ready') else 'PREPARING')
            return {'state': self.read('phase'), 'estimated_compute_usd': str(spent)}
        self.save('phase', 'DRAINING')
        self.h3.drain()  # Existing Worker-row lock atomically fences new claims.
        view = self.remote.json('/v1/drain', {})
        if not (idle and self.local_idle() and self.h3.idle() and view.get('draining') is True
                and view.get('active_executions') == 0 and view.get('queue_empty') is True
                and view.get('preparing') is False):
            return {'state': 'DRAINING', 'reason': 'terminal_and_upload_proof_required'}
        diagnostics = self.remote.json('/v1/diagnostics')
        atomic_json(self.root / ('diagnostics-' + self.binding['generation'] + '.json'), diagnostics)
        self.save('diagnostics_exported', hashlib.sha256(json.dumps(diagnostics, sort_keys=True).encode()).hexdigest())
        self.save('phase', 'STOPPING')  # Commit stop intent before provider mutation.
        self.request_stop(now)
        return {'state': 'STOPPING', 'estimated_compute_usd': str(spent)}


def main():
    from h3worker.journal import Journal
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding', required=True, type=Path)
    parser.add_argument('--state', required=True, type=Path)
    parser.add_argument('--journal', required=True, type=Path)
    parser.add_argument('--interval', default=10, type=int)
    args = parser.parse_args()
    binding = json.loads(args.binding.read_text())
    journal = Journal.open_readonly(str(args.journal))
    remote = Transport(binding['pod_url'], os.environ['H3BURST_POD_TOKEN'], binding['generation'])
    control = Controller(args.state, binding, Provider(os.environ['RUNPOD_API_KEY']), remote,
                         H3Control(binding.get('h3_admin_url', 'http://127.0.0.1:8730'), binding['worker_id']), journal)
    try:
        while True:
            try:
                print(json.dumps(control.tick()), flush=True)
            except Exception:
                # Preserve intent; stop claims on control-plane loss. Never guess success.
                try:
                    control.h3.drain()
                except Exception:
                    pass
                print(json.dumps({'state': 'RECONCILING', 'reason': 'control_operation_unavailable'}), flush=True)
            time.sleep(max(5, args.interval))
    finally:
        control.close()
        journal.close()


if __name__ == '__main__':
    main()
