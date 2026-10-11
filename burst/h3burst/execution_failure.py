"""Closed, non-textual error evidence from an already fetched Comfy history."""
import re
import hashlib
import json
import os
from pathlib import Path
import tempfile
import traceback

NODE_TYPES = frozenset(('KSampler', 'SamplerCustomAdvanced', 'UNETLoader', 'CLIPLoader',
    'CLIPTextEncode', 'VAELoader', 'VAEEncode', 'VAEDecode', 'WanVaceToVideo',
    'TrimVideoLatent', 'LanPaint_AVEncode', 'LanPaint_AVDecode', 'LoadAudio',
    'LoadVideo', 'GetVideoComponents', 'CreateVideo', 'SaveVideo', 'LoadImage',
    'ImageCompositeMasked', 'ImageFromBatch', 'ImageBatch', 'ModelSamplingSD3'))
EXCEPTIONS = frozenset(('RuntimeError', 'ValueError', 'TypeError', 'KeyError',
    'IndexError', 'AssertionError', 'MemoryError', 'NotImplementedError',
    'OSError', 'FileNotFoundError', 'OutOfMemoryError', 'PromptRejected', 'HTTPError'))


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
        code = state.get('error_code')
        if code in {'prompt_rejected', 'prompt_identity_uncertain', 'input_binding_failed', 'input_binding_interrupted',
                    'artifact_validation_failed', 'comfy_observation_unavailable'}:
            return 'remote failed: ' + code
        return fallback
    return 'remote failed: node {node_id} {node_type}, {exception_type}'.format(**evidence)


def bounded_raw(value, limit=65536):
    raw = value.encode('utf-8', 'replace') if isinstance(value, str) else b''
    return {'text': raw[:limit].decode('utf-8', 'replace'), 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest(), 'truncated': len(raw) > limit}


def private_exception(origin, error):
    """Error text is operator-only evidence, never a public task message."""
    value = {'origin': origin, 'exception_type': type(error).__name__,
             'exception_message': bounded_raw(str(error), 16384),
             'traceback': bounded_raw(''.join(traceback.format_exception(error)), 32768)}
    cause = error
    for _ in range(8):
        detail = getattr(cause, 'diagnostic', None)
        if isinstance(detail, dict):
            if type(detail.get('http_status')) is int:value['http_status'] = detail['http_status']
            if detail.get('response_capture_truncated') is True:value['response_capture_truncated'] = True
            if isinstance(detail.get('validation_response'), str):
                value['validation_response'] = bounded_raw(detail['validation_response'])
            break
        cause = cause.__cause__
        if cause is None:break
    return value


def private_history(item):
    messages = item.get('status', {}).get('messages') if isinstance(item, dict) and isinstance(item.get('status'), dict) else None
    if not isinstance(messages, list):return None
    for entry in reversed(messages[-128:]):
        if isinstance(entry, (list, tuple)) and len(entry) == 2 and entry[0] in {'execution_error', 'execution_interrupted'} and isinstance(entry[1], dict):
            fields = entry[1]
            value = {'origin': entry[0], **safe_failure(fields)}
            for key in ('exception_type', 'exception_message'):
                if isinstance(fields.get(key), str):value['original_' + key] = bounded_raw(fields[key])
            trace = fields.get('traceback')
            if isinstance(trace, list):trace = ''.join(x for x in trace if isinstance(x, str))
            if isinstance(trace, str):value['traceback'] = bounded_raw(trace)
            return value
    return None


def write_private(path, value):
    path = Path(path)
    if any(p.is_symlink() for p in (path, *path.parents)):raise ValueError('diagnostic symlink refused')
    raw = json.dumps(value, ensure_ascii=False).encode()
    if len(raw) > 1024 * 1024:raise ValueError('diagnostic exceeds bound')
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            os.chmod(temporary, 0o600);output.write(raw);output.flush();os.fsync(output.fileno())
            os.replace(temporary, path)
        finally:temporary.unlink(missing_ok=True)


def validation_failure(value):
    try:
        body = json.loads(value['validation_response']['text'])
        nodes = body.get('node_errors', {})
        for node, error in list(nodes.items())[:32]:
            if isinstance(error, dict):
                return safe_failure({'node_id':node, 'node_type':error.get('class_type'),
                                     'exception_type':'PromptRejected'})
    except (ValueError, TypeError, KeyError, AttributeError):pass
    return None


def protocol_diagnostic(value, execution, generation, prompt):
    if not isinstance(value, dict) or set(value) != {'execution_id', 'generation', 'prompt_id', 'errors'} \
            or (value['execution_id'], value['generation'], value['prompt_id']) != (execution, generation, prompt):
        raise ValueError('diagnostic identity mismatch')
    if not isinstance(value['errors'], list) or not 1 <= len(value['errors']) <= 4:
        raise ValueError('invalid diagnostic count')
    fields = {'origin','exception_type','exception_message','traceback','http_status','validation_response','response_capture_truncated',
              'node_id','node_type','original_exception_type','original_exception_message'}
    for error in value['errors']:
        if not isinstance(error, dict) or set(error) - fields \
                or error.get('origin') not in {'submission','input_binding','observation','artifact_validation','execution_error','execution_interrupted'}:
            raise ValueError('invalid diagnostic fields')
        for key, item in error.items():
            if key == 'response_capture_truncated':
                if item is not True:raise ValueError('invalid response capture marker')
            elif key == 'http_status':
                if type(item) is not int or not 100 <= item <= 599:raise ValueError('invalid diagnostic HTTP status')
            elif key in {'exception_message','traceback','validation_response','original_exception_type','original_exception_message'}:
                if not isinstance(item, dict):raise ValueError('invalid bounded diagnostic type')
                if set(item) != {'text','bytes','sha256','truncated'} or not isinstance(item['text'], str) \
                        or len(item['text'].encode()) > 65540 or type(item['bytes']) is not int \
                        or item['bytes'] < 0 or type(item['truncated']) is not bool \
                        or not isinstance(item['sha256'], str) or not re.fullmatch('[0-9a-f]{64}',item['sha256']):
                    raise ValueError('invalid bounded diagnostic')
            elif not isinstance(item, str) or len(item) > 128:
                raise ValueError('invalid diagnostic scalar')
    return value
