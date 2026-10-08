"""Bounded transport for editable ComfyUI 0.4/1 workflow documents, not prompts."""
import json
import math
import re
import shlex

MAX_BYTES = 1024 * 1024
MEDIA_DATA_URL = re.compile(r'data\s*:\s*(?:image|video|audio)/', re.IGNORECASE)


def save_command(name, replace=False):
    workflow_path(name)
    return shlex.join(['.venv/bin/python', 'client/comfy_admin.py', 'workflow-save',
                       '--name', name, '--input-stdin'] + (['--replace'] if replace else []))


def workflow_path(name):
    if (not isinstance(name, str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}\.json', name)
            or name[:-5].upper() in {'CON', 'PRN', 'AUX', 'NUL',
                                    *(f'COM{i}' for i in range(1, 10)),
                                    *(f'LPT{i}' for i in range(1, 10))}):
        raise ValueError('workflow name must be a safe ASCII basename ending in .json')
    return 'workflows/' + name


def validate_bytes(raw):
    if not isinstance(raw, bytes) or not 1 <= len(raw) <= MAX_BYTES:
        raise ValueError('workflow must contain 1..1048576 UTF-8 bytes')

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs)
    except (UnicodeError, ValueError, RecursionError):
        raise ValueError('invalid workflow JSON') from None
    count = 0

    def bounded(item, depth=0):
        nonlocal count
        count += 1
        if count > 20000 or depth > 16:
            raise ValueError('workflow complexity exceeds limit')
        if isinstance(item, str):
            if MEDIA_DATA_URL.search(item):
                raise ValueError('embedded media data URLs are forbidden')
            if len(item.encode('utf-8')) > 16384:
                raise ValueError('workflow string exceeds limit')
        elif isinstance(item, (list, dict)):
            if len(item) > 1024:
                raise ValueError('workflow collection exceeds limit')
            for key, child in (item.items() if isinstance(item, dict) else enumerate(item)):
                if isinstance(item, dict):
                    bounded(key, depth + 1)
                bounded(child, depth + 1)
        elif type(item) in (int, float):
            if abs(item) > 2**64 or not math.isfinite(item):
                raise ValueError('invalid workflow number')
    bounded(value)

    def node_id(item):
        return type(item) is int or isinstance(item, str) and bool(item)

    def vector(item):
        return (isinstance(item, list) and len(item) == 2
                or isinstance(item, dict) and '0' in item and '1' in item) and all(
                    type(x) in (int, float) for x in
                    (item if isinstance(item, list) else [item['0'], item['1']]))

    if (not isinstance(value, dict) or type(value.get('version')) not in (int, float)
            or value['version'] not in (0.4, 1) or not isinstance(value.get('nodes'), list)):
        raise ValueError('editable UI workflow 0.4 or 1 required; API graphs are not UI documents')
    version = value['version']
    if version == 0.4:
        if (not node_id(value.get('last_node_id')) or type(value.get('last_link_id')) is not int
                or not isinstance(value.get('links'), list)):
            raise ValueError('invalid UI 0.4 graph state')
    else:
        state = value.get('state')
        if (not isinstance(state, dict) or any(type(state.get(k)) not in (int, float)
                for k in ('lastGroupId', 'lastNodeId', 'lastLinkId', 'lastRerouteId'))):
            raise ValueError('invalid UI 1 graph state')
    for key in ('groups', 'links', 'floatingLinks', 'reroutes', 'models', 'subgraphs'):
        if key in value and not isinstance(value[key], list):
            raise ValueError('invalid UI graph collection')
    for key in ('config', 'extra'):
        if value.get(key) is not None and not isinstance(value[key], dict):
            raise ValueError('invalid UI graph metadata')
    ids = set()
    for node in value['nodes']:
        if (not isinstance(node, dict) or not node_id(node.get('id'))
                or str(node['id']) in ids or not isinstance(node.get('type'), str)
                or not node['type'] or not vector(node.get('pos')) or not vector(node.get('size'))
                or not isinstance(node.get('flags'), dict)
                or not isinstance(node.get('properties'), dict)
                or any(type(node.get(k)) not in (int, float) for k in ('order', 'mode'))
                or any(k in node and not isinstance(node[k], list) for k in ('inputs', 'outputs'))
                or 'widgets_values' in node and not isinstance(node['widgets_values'], (list, dict))):
            raise ValueError('invalid UI node')
        ids.add(str(node['id']))
    link_ids = set()
    for link in value.get('links', []):
        if version == 1:
            if not isinstance(link, dict) or not all(k in link for k in
                    ('id', 'origin_id', 'origin_slot', 'target_id', 'target_slot', 'type')):
                raise ValueError('invalid UI 1 link')
            link = [link[k] for k in ('id', 'origin_id', 'origin_slot', 'target_id', 'target_slot', 'type')]
        if (not isinstance(link, list) or len(link) != 6 or type(link[0]) is not int
                or link[0] in link_ids or not node_id(link[1]) or not node_id(link[3])
                or str(link[1]) not in ids or str(link[3]) not in ids
                or any(type(link[i]) is not int or link[i] < 0 for i in (2, 4))
                or type(link[5]) not in (str, list, int, float)
                or isinstance(link[5], list) and not all(isinstance(x, str) for x in link[5])):
            raise ValueError('invalid UI link')
        link_ids.add(link[0])
    return raw
