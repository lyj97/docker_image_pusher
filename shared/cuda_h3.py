"""Build native H3 business requests with pinned Comfy nodes and weights."""
import copy
import re


def references(task):
    result = list(task.get('references') or [])
    for name in ('first', 'last'):
        anchor = (task.get('anchors') or {}).get(name)
        if anchor:
            result.append(dict(anchor, kind='image'))
    return result


def compatible(task, profile):
    from .h3proto import validate_task_request, effective_h3_generation
    try:
        if (task.get('mode') not in profile['native_modes'] or task.get('_native')
                or task.get('model_revision') != 'installed-model-revision'
                or validate_task_request(task)):
            return False
        g = effective_h3_generation(task['generation'])
        # These alter the requested algorithm; CUDA does not implement them.
        if (g['dit_layers'] != 50 or g['core_reuse'] != 1 or g['denoise_reuse'] != 1
                or g['token_reduction'] or g['use_int8_row_fc2']
                or g['render_width'] != g['width'] or g['render_height'] != g['height']):
            return False
        if not re.fullmatch(r'[0-9]{1,20}', str(task.get('seed'))) or type(task.get('seed')) is bool:
            return False
        if not 0 <= int(task['seed']) <= 2**64-1:
            return False
        refs = references(task)
        if len({r['asset_id'] for r in refs}) != len(refs):
            return False
        from .comfy_policy import validate
        candidate = dict(task, mode='comfyui_video', model_revision='comfyui.video.v1',
                         anchors={}, references=refs, workflow=workflow(task, profile))
        # Native business validation owns text length. A fixed prompt socket
        # cannot turn URLs or filenames mentioned in prose into filesystem IO.
        candidate['workflow']['graph']['7']['inputs']['prompt'] = 'native prompt contract'
        return not validate(candidate)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def workflow(task, profile):
    result = copy.deepcopy(profile['workflow_template'])
    graph = result['graph']
    graph.pop('17', None)
    graph.pop('18', None)
    graph.pop('19', None)
    result['bindings'] = []
    generation = task['generation']
    conditioning = graph['7']['inputs']
    conditioning.pop('first_frame', None)
    conditioning.update(prompt=task['prompt'], width=generation['width'],
                        height=generation['height'], length=generation['frames'])
    graph['8']['inputs']['noise_seed'] = int(task['seed'])
    graph['14']['inputs']['steps'] = generation.get('steps', 20)
    graph['9']['inputs']['conditioning'] = ['7', 0]
    graph['15']['inputs']['latent_image'] = ['7', 1]

    def load(identity, cls, key):
        node = 'input_' + str(len(result['bindings']))
        graph[node] = {'class_type': cls, 'inputs': {key: None}}
        result['bindings'].append({'node_id': node, 'input': key, 'asset_id': identity})
        return node

    if task['mode'] == 'ref2va':
        graph['7']['class_type'] = 'MiniMaxH3ReferenceToVideo'
        conditioning.update(audio_vae=['6', 0], ref_image_size='match')
        counts = {'image': 0, 'video': 0, 'audio': 0}
        for ref in task['references']:
            kind = 'video' if ref['kind'] == 'video_audio' else ref['kind']
            number = counts[kind]
            counts[kind] += 1
            if kind == 'image':
                node = load(ref['asset_id'], 'LoadImage', 'image')
                conditioning['ref_images.ref_image_' + str(number)] = [node, 0]
            elif kind == 'audio':
                node = load(ref['asset_id'], 'LoadAudio', 'audio')
                conditioning['ref_audios.ref_audio_' + str(number)] = [node, 0]
            else:
                node = load(ref['asset_id'], 'LoadVideo', 'file')
                components, frames = node + '_components', node + '_frames'
                graph[components] = {'class_type': 'GetVideoComponents', 'inputs': {'video': [node, 0]}}
                graph[frames] = {'class_type': 'H3ReferenceFrames', 'inputs':
                                 {'images': [components, 0], 'fps': [components, 2]}}
                conditioning['ref_videos.ref_video_' + str(number)] = [frames, 0]
                if ref.get('include_embedded_audio', ref['kind'] == 'video_audio'):
                    conditioning['ref_video_audios.ref_video_audio_' + str(number)] = [components, 1]
    else:
        for name in ('first', 'last'):
            anchor = (task.get('anchors') or {}).get(name)
            if anchor:
                node = load(anchor['asset_id'], 'LoadImage', 'image')
                conditioning[name + '_frame'] = [node, 0]
        if task['mode'] == 'a2va':
            node = load(task['references'][0]['asset_id'], 'LoadAudio', 'audio')
            graph['19'] = {'class_type': 'MiniMaxH3AddGuide', 'inputs':
                           {'positive': ['7', 0], 'latent': ['7', 1], 'frame_idx': 0,
                            'audio_vae': ['6', 0], 'audio': [node, 0]}}
            graph['9']['inputs']['conditioning'] = ['19', 0]
            graph['15']['inputs']['latent_image'] = ['19', 1]
    return result
