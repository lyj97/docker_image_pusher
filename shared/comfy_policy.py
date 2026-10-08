"""Baseline ComfyUI policy, not a sandbox for operator-installed Python nodes."""
import json
import math
import re

MODE = 'comfyui_video'
CAPABILITY = 'comfyui.video.v1'
# Additional operator restrictions may extend, never replace, this baseline.
DENIED = ('manager', 'install', 'download', 'http', 'api', 'shell', 'command',
          'script', 'code', 'eval', 'exec', 'python', 'network', 'socket', 'upload')
# 'decode' and 'encode' are ordinary diffusion operations, not code execution.
DANGEROUS_KEYS = {'url', 'uri', 'path', 'directory', 'folder', 'filename',
                  'filename_prefix', 'output_path', 'output_dir', 'save_path',
                  'command', 'cmd', 'script', 'code', 'expression', 'python',
                  'eval', 'exec', 'download_url', 'api_key', 'endpoint', 'prefix',
                  'destination', 'file_name', 'filepath', 'output_directory'}
ASSET_KEYS = {'image', 'audio', 'video', 'file', 'mask', 'images', 'audio_file', 'video_file', 'image_file'}
# Only explicit bindings may fill these filesystem input slots.
LOCAL_ASSET_KEYS = {'path', 'file_path', 'filepath', 'filename', 'file_name',
                    'video_path', 'audio_path', 'image_path', 'mask_path',
                    'source_file', 'source_path', 'input_file', 'input_path'}
URI_FORM = re.compile(
    r'(?i)\b(?:[a-z][a-z0-9+.-]*:\s*/|'
    r'(?:https?|file|ftps?|sftp|ssh|s3|gs|az|wss?|tcp|udp|git|'
    r'data|javascript|mailto|urn):)')
ID = re.compile(r'^[A-Za-z0-9_-]{1,64}$')


def dangerous_key(key):
    key = key.lower()
    return (key in DANGEROUS_KEYS
            or re.search(r'(^|_)(path|dir|folder|code|exec|eval|cmd)(_|$)', key)
            or any(x in key for x in ('output_path', 'directory', 'filename', 'command', 'script', 'endpoint', 'url', 'prefix')))


def validate(task, extra_denied=()):
    """Return bounded diagnostics; validate before copying/hashing/injecting graphs."""
    try:
        _validate(task, extra_denied)
        return []
    except (ValueError, TypeError, RecursionError, OverflowError) as exc:
        return ['ComfyUI policy: ' + str(exc)[:300]]


def _validate(task, extra_denied):
    workflow = task.get('workflow')
    count = 0

    def bounded(value, depth=0):
        nonlocal count
        count += 1
        if count > 20000 or depth > 16:
            raise ValueError('JSON complexity exceeds limit')
        if isinstance(value, str):
            if len(value.encode('utf-8')) > 16384:
                raise ValueError('string exceeds limit')
        elif isinstance(value, (list, dict)):
            if len(value) > 1024:
                raise ValueError('collection exceeds limit')
            for key, child in (value.items() if isinstance(value, dict) else enumerate(value)):
                if isinstance(value, dict):
                    if not isinstance(key, str):
                        raise ValueError('object keys must be strings')
                    bounded(key, depth + 1)
                bounded(child, depth + 1)
        elif value is not None and type(value) not in (bool, int, float):
            raise ValueError('not JSON')
        elif isinstance(value, (int, float)) and (not math.isfinite(value) or abs(value) > 2**64):
            raise ValueError('invalid number')
    bounded(workflow)
    if len(json.dumps(workflow, ensure_ascii=False, allow_nan=False).encode()) > 262144:
        raise ValueError('workflow exceeds 256 KiB')
    if not isinstance(workflow, dict) or set(workflow) != {'graph', 'bindings', 'output_node'}:
        raise ValueError('workflow requires graph, bindings, output_node only')
    graph, bindings, output = workflow['graph'], workflow['bindings'], workflow['output_node']
    if not isinstance(graph, dict) or not 1 <= len(graph) <= 256:
        raise ValueError('graph requires 1..256 nodes')
    if not isinstance(output, str) or output not in graph:
        raise ValueError('output_node must identify SaveVideo')
    if not isinstance(bindings, list) or len(bindings) > 64:
        raise ValueError('bindings must be a list of at most 64 entries')
    refs = task.get('references', [])
    if not isinstance(refs, list) or len(refs) > 64 or any(
            not isinstance(r, dict) or not isinstance(r.get('asset_id'), str)
            or not ID.fullmatch(r['asset_id']) for r in refs):
        raise ValueError('invalid asset references')
    assets = {r['asset_id'] for r in refs}
    bound = set()
    used = set()
    for b in bindings:
        if not isinstance(b, dict) or set(b) != {'node_id', 'input', 'asset_id'}:
            raise ValueError('invalid binding')
        node, key, asset = b['node_id'], b['input'], b['asset_id']
        if not all(isinstance(x, str) for x in (node, key, asset)) or asset not in assets:
            raise ValueError('binding requires a referenced asset')
        if node not in graph or not isinstance(graph[node], dict) or not isinstance(graph[node].get('inputs'), dict):
            raise ValueError('binding node missing')
        if key not in graph[node]['inputs'] or graph[node]['inputs'][key] is not None or (node, key) in bound:
            raise ValueError('binding input must be a unique null placeholder')
        if (not ID.fullmatch(key)
                or any(token in key.lower() for token in (
                    'output', 'destination', 'prefix', 'url', 'uri', 'endpoint',
                    'command', 'cmd', 'code', 'script', 'expression', 'eval', 'exec', 'python', 'save'))
                or (dangerous_key(key) and key.lower() not in LOCAL_ASSET_KEYS)):
            raise ValueError('unsafe binding input')
        bound.add((node, key))
        used.add(asset)
    if used != assets or len(assets) != len(refs) or task.get('anchors'):
        raise ValueError('references must exactly match bound assets; no anchors')

    def safe(value):
        if isinstance(value, str):
            if ('\\' in value or re.search(r'(^|[\s/])\.\.([/\s]|$)', value)
                    or value.startswith(('/', '~')) or URI_FORM.search(value)
                    or '\x00' in value or re.search(r'(?i)\.(png|jpe?g|webp|gif|mp4|webm|mov|wav|mp3|flac|ogg|txt|json|py|sh)$', value)):
                raise ValueError('unsafe path or scheme')
        elif isinstance(value, dict):
            for k, v in value.items():
                if dangerous_key(k):
                    raise ValueError('dangerous nested input key')
                safe(v)
        elif isinstance(value, list):
            for v in value:
                safe(v)

    for node_id, node in graph.items():
        if isinstance(node, dict) and str(node.get('class_type', '')).startswith('H3Native'):
            raise ValueError('execution-only native node')
        if not ID.fullmatch(node_id) or not isinstance(node, dict) or set(node) != {'class_type', 'inputs'}:
            raise ValueError('nodes require string ID, class_type and inputs only')
        cls, inputs = node['class_type'], node['inputs']
        if not isinstance(cls, str) or not cls or len(cls) > 128 or not isinstance(inputs, dict):
            raise ValueError('malformed node')
        safe(cls)
        # Avoid rejecting VAEDecode/CLIPTextEncode while blocking Code execution classes.
        name = cls.lower().replace('decode', '').replace('encode', '')
        if (any(token in name for token in DENIED)
                or any(token.lower() in cls.lower() for token in extra_denied)):
            raise ValueError('denied node class')
        if ('save' in name or 'preview' in name) and not (node_id == output and cls == 'SaveVideo'):
            raise ValueError('only declared SaveVideo may write outputs')
        for key, value in inputs.items():
            safe(key)
            lower = key.lower()
            if (node_id, key) in bound:
                continue  # binding-specific policy already checked above
            if dangerous_key(key) or lower in LOCAL_ASSET_KEYS:
                raise ValueError('dangerous input key or unbound asset path')
            if lower in ASSET_KEYS and not (isinstance(value, list) and len(value) == 2
                    and isinstance(value[0], str) and value[0] in graph and type(value[1]) is int and 0 <= value[1] <= 255):
                raise ValueError('asset input requires binding or node link')
            if isinstance(value, list) and len(value) == 2 and isinstance(value[0], str) and type(value[1]) is int:
                if value[0] not in graph or not 0 <= value[1] <= 255:
                    raise ValueError('invalid node link')
            safe(value)
    if graph[output]['class_type'] != 'SaveVideo':
        raise ValueError('output_node must be SaveVideo')
    if graph[output]['inputs'].get('format') != 'mp4':
        raise ValueError('SaveVideo format must be mp4')
    if task.get('model_revision') != CAPABILITY:
        raise ValueError('model_revision must be ' + CAPABILITY)
