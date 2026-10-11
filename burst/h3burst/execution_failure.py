"""Closed, non-textual error evidence from an already fetched Comfy history."""
import re

NODE_TYPES = frozenset(('KSampler', 'SamplerCustomAdvanced', 'UNETLoader', 'CLIPLoader',
    'CLIPTextEncode', 'VAELoader', 'VAEEncode', 'VAEDecode', 'WanVaceToVideo',
    'TrimVideoLatent', 'LanPaint_AVEncode', 'LanPaint_AVDecode', 'LoadAudio',
    'LoadVideo', 'GetVideoComponents', 'CreateVideo', 'SaveVideo'))
EXCEPTIONS = frozenset(('RuntimeError', 'ValueError', 'TypeError', 'KeyError',
    'IndexError', 'AssertionError', 'MemoryError', 'NotImplementedError',
    'OSError', 'FileNotFoundError', 'OutOfMemoryError'))


def safe_failure(value):
    if not isinstance(value, dict):
        return None
    node = value.get('node_id')
    node = str(node) if type(node) is int else node
    return {'node_id': node if isinstance(node, str) and re.fullmatch(r'[0-9]{1,10}', node) else 'unknown',
        'node_type': value.get('node_type') if isinstance(value.get('node_type'), str) and value['node_type'] in NODE_TYPES else 'unknown',
        'exception_type': value.get('exception_type') if isinstance(value.get('exception_type'), str) and value['exception_type'] in EXCEPTIONS else 'unknown'}


def history_failure(item):
    if not isinstance(item, dict) or not isinstance(item.get('status'), dict):
        return None
    messages = item['status'].get('messages')
    if not isinstance(messages, list):
        return None
    # Bounded examination; arbitrary exception text and inputs never cross this boundary.
    for entry in reversed(messages[-128:]):
        if isinstance(entry, (list, tuple)) and len(entry) == 2 and entry[0] == 'execution_error':
            return safe_failure(entry[1])
    return None


def failure_message(state, fallback):
    evidence = safe_failure(state.get('execution_error'))
    if evidence is None:
        return fallback
    return 'remote failed: node {node_id} {node_type}, {exception_type}'.format(**evidence)
