"""Volatile exact-prompt grants; only authenticated attempt ownership resolves media.

Tokens stay in Worker/ComfyUI IPC and HTTP headers, never graphs or journals.
"""
import asyncio
import base64
import hashlib
import secrets
from shared import comfy_bridge as bridge
from shared.h3proto import digest_obj


class NativeGrants:
    def __init__(self, worker):
        self.worker = worker
        self.entries = {}

    async def authorize(self, entry, asset_id=None):
        from .comfy_preview import transport
        self.worker._check_lease_alive()
        if self.worker._local_lease_expired():
            raise ValueError('native lease unavailable')
        native = entry['task']['_native']
        value = dict(op='native_check' if asset_id is None else 'native_media',
            browser_id=native['browser_id'], task_id=entry['task_id'],
            attempt_id=entry['attempt_id'], lease_token=entry['lease_token'])
        if asset_id is not None:
            value['asset_id'] = asset_id
        result = await asyncio.to_thread(transport, self.worker, native['session_id'], value, asset_id is not None)
        if asset_id is None and result.get('authorized') is not True:
            raise ValueError('native authority unavailable')
        return result

    async def prepare(self, attempt_id, lease_token, task, execution, client_id):
        record = self.worker.journal.get_attempt(attempt_id)
        native = task['_native']
        if (record is None or record['request_snapshot'] != task or record['lease_token'] != lease_token
                or native['protocol'] != bridge.NATIVE_PROTOCOL
                or native['worker_id'] != self.worker.config.worker_id
                or native['boot_id'] != self.worker.boot_id or self.entries
                or client_id != native['client_id']):
            raise ValueError('native ownership unavailable')
        graph = bridge.native_graph(task['workflow'], task['references'], execution)
        entry = dict(task=task, task_id=record['task_id'], attempt_id=attempt_id,
            lease_token=lease_token, token=secrets.token_hex(32), graph=graph,
            digest=digest_obj(graph), execution=execution, client_id=client_id,
            submitted=False, input_digests={})
        await self.authorize(entry)
        self.entries[execution] = entry
        return graph, entry['token']

    async def dispatch(self, value):
        op = value.get('op')
        schemas = {'native_submit': {'op', 'execution', 'token', 'graph', 'client_id'},
                   'native_resolve': {'op', 'execution', 'token', 'node'},
                   'native_release': {'op', 'execution', 'token'}}
        if op not in schemas or set(value) != schemas[op]:
            raise ValueError('native operation refused')
        entry = self.entries.get(value['execution'])
        if (entry is None or not isinstance(value['token'], str)
                or not secrets.compare_digest(entry['token'], value['token'])):
            raise ValueError('native grant unavailable')
        if op == 'native_release':
            self.entries.pop(value['execution'], None)
            return {'released': True}
        await self.authorize(entry)
        if op == 'native_submit':
            if (entry['submitted'] or value['client_id'] != entry['client_id']
                    or digest_obj(value['graph']) != entry['digest']):
                raise ValueError('native submission refused')
            entry['submitted'] = True  # Consume before engine handler; never retry.
            return {'authorized': True}
        if not entry['submitted']:
            raise ValueError('native execution not submitted')
        binding = next((b for b in entry['task']['workflow']['bindings'] if b['node_id'] == value['node']), None)
        if binding is None:
            raise ValueError('native node unavailable')
        asset_id = binding['asset_id']
        raw, mime = await self.authorize(entry, asset_id)
        ref = next(r for r in entry['task']['references'] if r['asset_id'] == asset_id)
        digest = hashlib.sha256(raw).hexdigest()
        if (not 1 <= len(raw) <= bridge.MAX_MEDIA or len(raw) != ref['size_bytes']
                or mime != ref['content_type'] or digest != ref['sha256']):
            raise ValueError('native media refused')
        entry['input_digests'][asset_id] = digest
        return dict(media=base64.b64encode(raw).decode(), content_type=mime)

    async def check(self, execution):
        entry = self.entries.get(execution)
        if entry is None:
            raise ValueError('native grant unavailable after restart')
        await self.authorize(entry)

    def revoke(self, execution):
        entry = self.entries.pop(execution, None)
        return dict(entry['input_digests']) if entry else {}
