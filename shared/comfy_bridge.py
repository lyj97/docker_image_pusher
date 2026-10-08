"""Comfy preview v1 contract. Diagnostics never contain caller values."""
import copy
import json
import re
import hashlib
from . import comfy_policy
from .comfy_versions import FRONTENDS

PROTOCOL = 1
FRONTEND = '1.52.7'  # legacy default; readiness uses the exact release pair
MAX_JSON = 1200 * 1024
MAX_MEDIA = 8 * 1024 * 1024
MAX_SESSION_MEDIA = 32 * 1024 * 1024
TTL = 900
NATIVE_PROTOCOL = 1
NATIVE_LOADERS = {'LoadImage': ('image', 'image', 'H3NativeImage'),
                  'LoadAudio': ('audio', 'audio', 'H3NativeAudio'),
                  'LoadVideo': ('file', 'video', 'H3NativeVideo')}
MAX_SESSIONS = 4
OPAQUE = re.compile(r'^[0-9a-f]{64}$')
SESSION = re.compile(r'^prv_[0-9a-f]{32}$')
MEDIA_TYPES = {'image/png', 'image/jpeg', 'image/webp', 'video/mp4', 'video/webm',
               'audio/wav', 'audio/mpeg', 'audio/flac', 'audio/ogg'}


def native_target(worker_id, boot_id):
    return 'h3.native.' + hashlib.sha256((worker_id + '\0' + boot_id).encode()).hexdigest()


def native_workflow(workflow, references):
    task(workflow, references)
    kinds = {ref['asset_id']: ref.get('kind') or ref.get('content_type', '').split('/')[0]
             for ref in references}
    if (len(references) > 3 or len(set(kinds.values())) != len(references)
            or len(workflow['bindings']) != len(references)):
        raise ValueError('native media budget requires at most one input per kind')
    for ref in references:
        if 'content_type' in ref and (ref['content_type'] not in MEDIA_TYPES
                or ref['content_type'].split('/')[0] != kinds[ref['asset_id']]
                or not 1 <= ref['size_bytes'] <= MAX_MEDIA):
            raise ValueError('unsupported authoritative native media')
    reachable = set()
    def visit(node_id):
        if node_id in reachable:
            return
        reachable.add(node_id)
        for value in workflow['graph'][node_id]['inputs'].values():
            if isinstance(value, list) and len(value) == 2 and isinstance(value[0], str) and value[0] in workflow['graph']:
                visit(value[0])
    visit(workflow['output_node'])
    for binding in workflow['bindings']:
        if binding['node_id'] not in reachable:
            raise ValueError('native bound input must reach output')
        node = workflow['graph'][binding['node_id']]
        spec = NATIVE_LOADERS.get(node['class_type'])
        if (not spec or binding['input'] != spec[0] or kinds[binding['asset_id']] != spec[1]
                or set(node['inputs']) != {spec[0]}):
            raise ValueError('unsupported native binding')
    return workflow


def native_graph(workflow, references, execution):
    native_workflow(workflow, references)
    graph = copy.deepcopy(workflow['graph'])
    for binding in workflow['bindings']:
        node = graph[binding['node_id']]
        graph[binding['node_id']] = dict(class_type=NATIVE_LOADERS[node['class_type']][2],
            inputs=dict(execution=execution, node=binding['node_id']))
    graph[workflow['output_node']]['inputs']['filename_prefix'] = 'h3_' + execution.replace('-', '') + '/video'
    return graph


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('invalid bridge payload')
            result[key] = value
        return result
    try:
        if not 1 <= len(raw) <= MAX_JSON:
            raise ValueError()
        value = json.loads(raw, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if not isinstance(value, dict):
            raise ValueError()
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError('invalid bridge payload') from None


def task(workflow, references):
    value = dict(mode='comfyui_video', model_revision=comfy_policy.CAPABILITY,
                 workflow=workflow, references=references, anchors={})
    if comfy_policy.validate(value):
        raise ValueError('invalid bridge graph or bindings')
    return value


def preview(value):
    if set(value) != {'workflow', 'references'}:
        raise ValueError('invalid preview fields')
    task(value['workflow'], value['references'])
    if any(set(ref) != {'asset_id', 'kind'} or ref['kind'] not in ('image', 'video', 'audio')
           for ref in value['references']):
        raise ValueError('invalid preview references')
    return value


def reconcile(graph, original, references):
    """Reject removed/retargeted bindings; only server-owned bindings can null media."""
    if not isinstance(graph, dict):
        raise ValueError('invalid exported graph')
    graph = copy.deepcopy(graph)
    for binding in original['bindings']:
        node_id, key = binding['node_id'], binding['input']
        node = graph.get(node_id)
        source = original['graph'][node_id]
        if (not isinstance(node, dict) or node.get('class_type') != source['class_type']
                or not isinstance(node.get('inputs'), dict) or key not in node['inputs']
                or node['inputs'][key] is not None):
            # v1 media is displayed in an adjacent adapter, never a path widget.
            # Changing a bound widget/link requires a new explicit preview.
            raise ValueError('binding changed in editor')
        node['inputs'][key] = None
    workflow = dict(graph=graph, bindings=original['bindings'], output_node=original['output_node'])
    task(workflow, references)
    return workflow


def saved_document(value):
    """Validate limits before stripping execution metadata; never save loaders."""
    from .comfy_workflow import validate_bytes
    validate_bytes(json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode())
    value = copy.deepcopy(value)
    def strip(item):
        if isinstance(item, dict):
            for key in tuple(item):
                if key in {'_native', 'h3_native', 'h3_execution'}:
                    del item[key]
                else:
                    strip(item[key])
        elif isinstance(item, list):
            for child in item:
                strip(child)
    strip(value)
    if any(node['type'].startswith('H3Native') for node in value['nodes']):
        raise ValueError('execution-only workflow refused')
    validate_bytes(json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode())
    return value
