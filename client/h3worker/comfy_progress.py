"""Best-effort, identity-scoped ComfyUI 0.37.0 WebSocket telemetry.

Only the runner's HTTP reconciliation can establish a terminal outcome. Keep
one coalesced snapshot, never enqueue raw events, previews, inputs or outputs.
"""
import asyncio
import json
from urllib.parse import quote, urlsplit, urlunsplit

MAX_MESSAGE = 64 * 1024
CONNECT_SECONDS = 3
RETRY_SECONDS = 5
MAX_COUNTER = 2**31 - 1


def websocket_url(base, client_id):
    parsed = urlsplit(base)
    if parsed.scheme not in ('http', 'https') or parsed.query or parsed.fragment:
        raise ValueError('invalid ComfyUI base URL')
    return urlunsplit(('wss' if parsed.scheme == 'https' else 'ws',
                      parsed.netloc, parsed.path.rstrip('/') + '/ws',
                      'clientId=' + quote(client_id, safe=''), ''))


class ProgressStream:
    def __init__(self, base, state, graph):
        self.url = websocket_url(base, state['client_id'])
        self.prompt_id = state['prompt_id']
        self.graph = graph
        # Bounded by the validated installed graph. Preserve high-water marks
        # across reconnects and attempt recovery; a reset isn't new completion.
        self.counters = {
            node: list(counter) for node, counter in state.get('progress_counters', {}).items()
            if node in graph and isinstance(counter, list) and len(counter) == 2
            and all(type(v) is int for v in counter)
            and 0 <= counter[0] <= counter[1] <= MAX_COUNTER and counter[1] > 0
        }
        self.node = None
        self.snapshot = None
        self.connected = False
        self.ready = asyncio.Event()
        self.task = None

    def ingest(self, raw):
        if not isinstance(raw, str) or len(raw.encode('utf-8')) > MAX_MESSAGE:
            return
        try:
            message = json.loads(raw)
        except (ValueError, RecursionError):
            return
        if not isinstance(message, dict):
            return
        data = message.get('data')
        if not isinstance(data, dict) or data.get('prompt_id') != self.prompt_id:
            # The pinned server's reconnect "executing" event has no prompt_id.
            # Client routing alone is insufficient evidence of prompt identity.
            return
        kind = message.get('type')
        node = data.get('node')
        if not isinstance(node, str) or node not in self.graph:
            return
        if kind == 'executing':
            if self.node != node:
                self.snapshot = (node, None)
            self.node = node
        elif kind == 'progress':
            value, total = data.get('value'), data.get('max')
            if (type(value) is not int or type(total) is not int
                    or not 0 <= value <= total <= MAX_COUNTER or total == 0):
                return
            old = self.counters.get(node)
            if old is not None and (total != old[1] or value < old[0]):
                return
            # A progress event carries both identities and can recover a missed
            # executing event. Reject delayed counters from an earlier node.
            if self.node is not None and self.node != node:
                return
            self.node = node
            self.counters[node] = [value, total]
            self.snapshot = (node, [value, total])

    async def start(self):
        self.task = asyncio.create_task(self._receive())
        try:
            await asyncio.wait_for(self.ready.wait(), CONNECT_SECONDS + .1)
        except asyncio.TimeoutError:
            pass  # never make telemetry availability a submission requirement

    async def close(self):
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def _receive(self):
        try:
            from websockets.asyncio.client import connect
            from websockets.exceptions import WebSocketException
        except ImportError:
            self.ready.set()
            return  # older stdlib-only installations retain HTTP observation
        while True:
            try:
                connection = connect(self.url, proxy=None, open_timeout=CONNECT_SECONDS,
                                     close_timeout=1, ping_interval=10, ping_timeout=10,
                                     max_size=MAX_MESSAGE, max_queue=4,
                                     compression=None)
                # Pin the observation to the same instance as HTTP. The pinned
                # websockets connector otherwise follows handshake redirects.
                connection.process_redirect = lambda exc: exc
                async with connection as socket:
                    self.connected = True
                    self.ready.set()
                    async for raw in socket:
                        self.ingest(raw)
            except (OSError, TimeoutError, ValueError, WebSocketException):
                pass
            finally:
                self.connected = False
                self.snapshot = None
                self.node = None
                self.ready.set()
            await asyncio.sleep(RETRY_SECONDS)

    def detail(self):
        if self.snapshot is None:
            return None
        node, counter = self.snapshot
        # No UI titles or arbitrary custom-node payloads enter monitoring.
        label = self.graph[node]['class_type'][:96]
        return (f'ComfyUI node {node}: {label}', counter)
