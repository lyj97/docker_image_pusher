"""Opt-in Worker Unix RPC, authenticated control-plane relay and private staging.

ComfyUI has no Worker credentials. Every action is authorized by H3; IPC is
restricted to a 0700 node-local directory. No shell, public listener or URL input.
"""
import asyncio
import base64
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import stat
import struct
import time
import urllib.error
import urllib.request
from shared import comfy_bridge as bridge
from .http import _USER_AGENT

MAX_WIRE = 12 * 1024 * 1024


class PreviewTransportError(ValueError):
    """A retryable failure with an uncertain control-plane outcome."""


class PreviewRequestRefused(ValueError):
    """A definitive control-plane refusal that must not be retried."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('bridge redirect refused')


def transport(worker, sid, value, media=False):
    if not bridge.SESSION.fullmatch(sid):
        raise ValueError('invalid preview identity')
    http = worker.http
    url = http.base_url + f'/v1/workers/{worker.config.worker_id}/preview-sessions/{sid}/rpc'
    value = dict(value, boot_id=worker.boot_id)
    req = urllib.request.Request(url, method='POST',
        data=json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode(),
        headers={'Authorization': 'Bearer ' + http.token, 'Content-Type': 'application/json',
                 'Accept': '*/*' if media else 'application/json', 'User-Agent': _USER_AGENT})
    http._add_access_headers(req, url)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(req, timeout=15) as response:
            if response.status != 200 or response.geturl() != url:
                raise ValueError()
            raw = response.read((bridge.MAX_MEDIA if media else bridge.MAX_JSON) + 1)
            if media:
                size, digest = response.headers.get('X-H3-Size'), response.headers.get('X-H3-SHA256')
                mime = response.headers.get('Content-Type', '').split(';')[0]
                if (mime not in bridge.MEDIA_TYPES or not size or not size.isdigit()
                        or not 1 <= int(size) <= bridge.MAX_MEDIA or len(raw) != int(size)
                        or hashlib.sha256(raw).hexdigest() != digest):
                    raise ValueError()
                return raw, mime
            return bridge.decode(raw)
    except urllib.error.HTTPError as exc:
        # Bounded numeric status only: never log URL, headers, body or caller
        # identity. This distinguishes an upstream refusal from a lost reply.
        logging.getLogger('h3worker').warning('preview control-plane HTTP status=%d', exc.code)
        # Only read-only sync retries these transient failures. Mutations
        # may have succeeded and must never be automatically retried.
        if exc.code in (408, 425, 429) or exc.code >= 500:
            raise PreviewTransportError('preview control-plane request unavailable') from None
        raise PreviewRequestRefused('preview control-plane request refused') from None
    except Exception:
        # Do not propagate urllib's URL, response body or caller graph to logs.
        raise PreviewTransportError('preview control-plane request unavailable') from None


def private_directory(path):
    if not path.is_absolute() or any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('unsafe preview directory')
    path.mkdir(mode=0o700, exist_ok=True)
    st = path.stat()
    if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o700:
        raise ValueError('preview directory must be owned and private')
    return path


class PreviewBridge:
    def __init__(self, worker):
        self.worker = worker
        self.root = Path(worker.config.data_dir).absolute() / 'preview'
        self.socket = self.root / 'rpc.sock'
        self.server = None
        self.sweeper = None
        self.sessions = {}
        self.lock = asyncio.Lock()
        self.connections = set()
        from .comfy_native import NativeGrants
        self.native = NativeGrants(worker)

    async def start(self):
        private_directory(self.root)
        self.cleanup(all_sessions=True)
        if self.socket.exists() or self.socket.is_symlink():
            if not stat.S_ISSOCK(self.socket.lstat().st_mode) or self.socket.lstat().st_uid != os.getuid():
                raise ValueError('unsafe existing preview socket')
            self.socket.unlink()  # Worker singleton lock is already held.
        self.server = await asyncio.start_unix_server(self.handle, str(self.socket), limit=bridge.MAX_JSON+4)
        os.chmod(self.socket, 0o600)
        self.sweeper = asyncio.create_task(self.sweep())

    def cleanup(self, all_sessions=False):
        now = time.time()
        for path in self.root.iterdir():
            if bridge.SESSION.fullmatch(path.name) and not path.is_symlink() and path.is_dir():
                state = self.sessions.get(path.name)
                if all_sessions or not state or state['expires'] <= now:
                    shutil.rmtree(path)
                    self.sessions.pop(path.name, None)
        for sid, state in list(self.sessions.items()):
            if state['expires'] <= now or all_sessions:
                self.sessions.pop(sid, None)

    async def sweep(self):
        while True:
            await asyncio.sleep(5)
            async with self.lock:
                self.cleanup()

    async def close(self):
        self.native.entries.clear()
        if self.sweeper:
            self.sweeper.cancel()
            await asyncio.gather(self.sweeper, return_exceptions=True)
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in tuple(self.connections):
            task.cancel()
        await asyncio.gather(*tuple(self.connections), return_exceptions=True)
        self.cleanup(all_sessions=True)
        if self.socket.exists() and stat.S_ISSOCK(self.socket.lstat().st_mode):
            self.socket.unlink()

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        if len(self.connections) >= 4:
            writer.close()
            await writer.wait_closed()
            return
        self.connections.add(task)
        try:
            await self._handle(reader, writer)
        finally:
            self.connections.discard(task)
            writer.close()
            await writer.wait_closed()

    async def _handle(self, reader, writer):
        try:
            size = struct.unpack('!I', await asyncio.wait_for(reader.readexactly(4), 3))[0]
            if not 1 <= size <= bridge.MAX_JSON:
                raise ValueError()
            value = bridge.decode(await asyncio.wait_for(reader.readexactly(size), 3))
            async with self.lock:
                result = await self.dispatch(value)
            raw = json.dumps({'ok': True, 'result': result}, ensure_ascii=False,
                             separators=(',', ':')).encode()
        except Exception:
            raw = b'{"ok":false,"error":"preview action refused; refresh H3 session status"}'
        writer.write(struct.pack('!I', len(raw)) + raw)
        await asyncio.wait_for(writer.drain(), 3)

    async def dispatch(self, value):
        self.cleanup()
        # Recheck the loaded bundle on every action. Cached Server capability
        # metadata is never authority to use a changed/unavailable frontend.
        if not await asyncio.to_thread(available, self.worker):
            raise ValueError('preview runtime unavailable')
        if value.get('op') in {'native_submit', 'native_resolve', 'native_release'}:
            return await self.native.dispatch(value)
        sid = value.pop('session_id', None)
        if not isinstance(sid, str) or not bridge.SESSION.fullmatch(sid):
            raise ValueError('invalid preview identity')
        if value.get('op') not in {'sync', 'ack', 'media', 'execute', 'save', 'native_admit', 'native_status'}:
            raise ValueError('invalid preview action')
        op = value['op']
        if op == 'save':
            value['document'] = bridge.saved_document(value['document'])
        state = self.sessions.get(sid)
        if op == 'sync' and not state:
            if len(self.sessions) >= bridge.MAX_SESSIONS:
                # DELETE is a user-to-Server operation, so no browser request
                # necessarily arrives to release its Worker-local slot. Probe
                # all local sessions concurrently and retain only those still
                # authorized by the Server before enforcing the local bound.
                async def stale(local_sid):
                    try:
                        await asyncio.to_thread(transport, self.worker, local_sid,
                            dict(op='sync', browser_id='0'*64))
                        return None
                    except PreviewRequestRefused:
                        return local_sid
                    except Exception:
                        return None  # An outage is not proof of revocation.
                expired = await asyncio.gather(*(
                    stale(local_sid)
                    for local_sid in tuple(self.sessions)
                ))
                for local_sid in expired:
                    if local_sid:
                        self.sessions.pop(local_sid, None)
                self.cleanup()
                if len(self.sessions) >= bridge.MAX_SESSIONS:
                    raise ValueError('preview session refused')
        elif not state:
            raise ValueError('preview session unavailable')
        try:
            if op == 'media':
                # Authorize every read even if staged; cancellation and owner
                # revocation therefore cannot serve a cached asset.
                raw, mime = await asyncio.to_thread(transport, self.worker, sid, value, True)
                if state['bytes'] + len(raw) > bridge.MAX_SESSION_MEDIA:
                    raise ValueError('preview staging budget exceeded')
                root = private_directory(self.root / sid)
                name = os.urandom(16).hex()  # no caller filenames or extensions
                path = root / name
                try:
                    with path.open('xb') as sink:
                        os.chmod(path, 0o600)
                        sink.write(raw)
                    state['bytes'] += len(raw)
                    return dict(media=base64.b64encode(raw).decode(), content_type=mime)
                finally:
                    # Browser receives bytes, never a filesystem name; temporary
                    # staging is removed even on errors. TTL also clears dirs.
                    path.unlink(missing_ok=True)
                    state['bytes'] = 0
            attempts = 2 if op == 'sync' else 1
            for attempt in range(attempts):
                try:
                    result = await asyncio.to_thread(transport, self.worker, sid, value)
                    break
                except PreviewTransportError:
                    if attempt + 1 >= attempts:
                        raise
                    await asyncio.sleep(0.25)
            if op == 'sync':
                from datetime import datetime
                expires = datetime.fromisoformat(result['expires_at'].replace('Z', '+00:00')).timestamp()
                if not expires > time.time():
                    raise ValueError('invalid preview expiry')
                bridge.preview(result['preview'])
                from shared.comfy_policy import validate
                if validate(bridge.task(result['preview']['workflow'], result['preview']['references']),
                            getattr(self.worker.config, 'comfyui_denied_classes', ())):
                    raise ValueError('operator preview policy refused')
                if not state:
                    state = self.sessions[sid] = dict(expires=min(expires, time.time()+bridge.TTL), bytes=0)
                else:
                    state['expires'] = min(state['expires'], expires)
            if op == 'native_status':
                result = dict(result)
                record = self.worker.journal.get_attempt(result['attempt_id']) if result.get('attempt_id') else None
                engine = json.loads(record['engine_state']) if record and record.get('engine_state') else None
                if engine and engine.get('native') and engine.get('submission_started'):
                    result['prompt_id'] = engine['prompt_id']
                    result['submitted'] = bool(engine.get('submission_accepted'))
                return result
            if op == 'save':
                if result.get('save_authorized') is not True:
                    raise ValueError('save not authorized')
                from .comfy_admin import save_workflow_bytes
                try:
                    await asyncio.to_thread(save_workflow_bytes, self.worker.config,
                        value['name'], value['replace'], json.dumps(value['document'],
                            ensure_ascii=False, separators=(',', ':')).encode())
                    saved = 'saved'
                except Exception:
                    saved = 'save_uncertain'
                await asyncio.to_thread(transport, self.worker, sid, dict(op='ack', browser_id=value['browser_id'], revision=value['revision'], state=saved))
                return dict(state=saved, sequence=result['sequence'])
            return result
        except Exception:
            # Fail closed; a lost response can mean execute/save already happened.
            # Never retry those mutations. The user reads authoritative status.
            if op in {'sync', 'media'}:
                self.sessions.pop(sid, None)
                self.cleanup()
            raise ValueError('preview action refused') from None


def available(worker):
    """Advertise only a running IPC bridge and an exact loaded extension bundle."""
    if not getattr(worker, '_preview_bridge', None) or not worker._preview_bridge.server:
        return False
    try:
        import vace_smoke as smoke
        source = Path(__file__).resolve().parents[1] / 'comfy_bridge_node'
        expected = hashlib.sha256((source / '__init__.py').read_bytes() + (source / 'web/bridge.js').read_bytes()).hexdigest()
        result = smoke.API(worker.config.comfyui_url, timeout=3).json('/h3/bridge-status')
        return result == dict(preview_protocol=bridge.PROTOCOL, preview_frontend=bridge.FRONTENDS.get(worker.config.comfyui_version), bundle_sha256=expected)
    except Exception:
        return False


def native_available(worker):
    if not available(worker):
        return False
    try:
        import vace_smoke as smoke
        return smoke.API(worker.config.comfyui_url, timeout=3).json('/h3/native-status') == {'native_protocol': bridge.NATIVE_PROTOCOL}
    except Exception:
        return False
