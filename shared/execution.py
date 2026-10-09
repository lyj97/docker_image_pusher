"""Reviewed execution requirements shared by scheduling, UI and burst planning.

Backend compatibility is separate from current readiness and permission to spend.
Unknown workflows fail closed. No task is converted between model precisions.
"""
from functools import lru_cache
import copy
import hashlib
import json
from pathlib import Path
import re


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
        ensure_ascii=False, allow_nan=False).encode()).hexdigest()


@lru_cache(maxsize=1)
def profiles():
    root = Path(__file__).with_name('execution_profiles')
    template = json.loads((root / 'h3-turbo-768p.workflow.json').read_text())
    models = json.loads((root / 'h3-turbo.models.json').read_text())
    profile = {'profile_id': 'h3-turbo-int8-768p-cu130-v1',
        'label': 'H3 INT8 + Turbo · 1344×768 · 124 帧',
        'backend': 'comfyui', 'gpu_vendor': 'nvidia', 'comfy_version': '0.39.0',
        'comfy_commit': 'b0b743566f65daafc423b4fea8a2fbda94b3384a',
        'torch_version': '2.10.0+cu130',
        'workflow_template': template,
        'models': models, 'nodes': {'schema_version': 1, 'nodes': []},
        'variable_inputs': [['7', 'prompt', 'text'], ['8', 'noise_seed', 'seed']],
        'validation': 'A40/Comfy 0.30 已生成；0.39 须完成实例功能验证'}
    profile['models_digest'] = digest(models)
    profile['nodes_digest'] = digest(profile['nodes'])
    profile['profile_digest'] = digest({key: profile[key] for key in (
        'profile_id', 'backend', 'gpu_vendor', 'comfy_version', 'comfy_commit',
        'torch_version', 'workflow_template', 'models', 'nodes',
        'variable_inputs')})
    profile['generation_node'] = '7'
    profile['input_slots'] = []
    lan = dict(profile, profile_id='h3-lanpaint-int8-256-cu130-v1',
        label='H3 LanPaint + ControlNet · 256×256 · 39 帧',
        workflow_template=json.loads((root / 'h3-lanpaint-256.workflow.json').read_text()),
        models=json.loads((root / 'h3-lanpaint.models.json').read_text()),
        variable_inputs=[['5', 'prompt', 'text'], ['6', 'noise_seed', 'seed']],
        generation_node='5', input_slots=[('video', 'video/mp4'), ('image', 'image/png'),
            ('video', 'video/mp4'), ('video', 'video/mp4')],
        validation='同图已在 Mac 生成；CUDA 0.39 须完成实例功能验证',
        precision='H3 INT8 convrot / Qwen NVFP4 AWQ / ControlNet INT8 / 无 Turbo',
        nodes={'schema_version': 1, 'nodes': [{'name': 'LanPaint',
            'repository': 'https://github.com/scraed/LanPaint.git',
            'commit': '2d7912f9a5efe5ece8de334c7ca18317b8288c39',
            'requirements': None,
            'classes': ['LanPaint_VideoMaskEditor', 'LanPaint_AVEncode', 'LanPaint_AVDecode']}]})
    adapter = Path(__file__).resolve().parents[1] / 'client/comfy_h3_contract_node/__init__.py'
    lan['runtime_adapter'] = {'node':'H3AVMaskPrepare', 'source_sha256':hashlib.sha256(adapter.read_bytes()).hexdigest()}
    lan['models_digest'], lan['nodes_digest'] = digest(lan['models']), digest(lan['nodes'])
    lan['profile_digest'] = digest({k: lan[k] for k in ('profile_id', 'backend', 'gpu_vendor',
        'comfy_version', 'comfy_commit', 'torch_version', 'workflow_template',
        'models', 'nodes', 'variable_inputs', 'generation_node', 'input_slots', 'runtime_adapter')})
    audio = dict(profile, profile_id='h3-a2va-int8-544x960-cu130-v1',
        label='H3 音频驱动 CUDA INT8 · 544×960 · 158 帧 · 20步',
        workflow_template=json.loads((root / 'h3-a2va-544x960.workflow.json').read_text()),
        native_generation=json.loads((root / 'h3-a2va-generation.json').read_text()),
        models=json.loads((root / 'h3-a2va.models.json').read_text()),
        input_slots=[('audio', 'audio/wav'), ('image', 'image/png')],
        precision='CUDA H3 INT8 / Qwen NVFP4 AWQ / 无 Turbo；与 Mac 原生精度不同',
        validation='音频 guide + 首帧 + 原音轨回写；CUDA 实例及质量须独立验收')
    audio['models_digest'] = digest(audio['models'])
    audio['profile_digest'] = digest({k: audio[k] for k in ('profile_id', 'backend', 'gpu_vendor',
        'comfy_version', 'comfy_commit', 'torch_version', 'workflow_template',
        'native_generation', 'models', 'nodes', 'variable_inputs', 'input_slots')})
    native_source = Path(__file__).with_name('cuda_h3.py')
    reference_source = adapter.parents[1] / 'comfy_h3_reference_node/__init__.py'
    native = dict(audio, profile_id='h3-native-int8-cu130-v1',
        label='H3 CUDA INT8 · 文本 / 首尾帧 / 给定音频 · 参数化',
        native_modes=['t2va', 'fl2va', 'a2va'], parameterized_native=True,
        validation='沿用已实测 A2VA 的运行环境；参数化与新增模式尚待 GPU 验收',
        runtime_sources={'shared/cuda_h3.py': hashlib.sha256(native_source.read_bytes()).hexdigest()})
    native.pop('native_generation')
    ref = dict(native, profile_id='h3-reference-int8-cu130-v1',
        label='H3 CUDA INT8 · 多参考图 / 视频 / 音频 · 参数化', native_modes=['ref2va'],
        models=copy.deepcopy(audio['models']),
        runtime_sources=dict(native['runtime_sources'], **{
            'client/comfy_h3_reference_node/__init__.py': hashlib.sha256(reference_source.read_bytes()).hexdigest()}))
    ref['models']['models'][0].update(
        source='diffusion_models/minimax_h3_ref2va_pruned_int8_convrot.safetensors',
        target='diffusion_models/minimax_h3_ref2va_pruned_int8_convrot.safetensors',
        sha256='9255f52b6677845ad238f20dfaafa94727053694127ab7f255c048f0f9365779')
    ref['workflow_template'] = copy.deepcopy(audio['workflow_template'])
    ref['workflow_template']['graph']['1']['inputs']['unet_name'] = 'minimax_h3_ref2va_pruned_int8_convrot.safetensors'
    for p in (native, ref):
        p['models_digest'] = digest(p['models'])
        p['profile_digest'] = digest({k:p[k] for k in ('profile_id', 'backend', 'gpu_vendor',
            'comfy_version', 'comfy_commit', 'torch_version', 'workflow_template', 'models',
            'nodes', 'native_modes', 'parameterized_native', 'runtime_sources')})
    from .comfy_variants import variants
    from .cuda_audio import profiles as audio_profiles
    return (profile, lan, audio, native, ref, *variants(profile), *audio_profiles(profile))


def profile_by_id(identity):
    return next(p for p in profiles() if p['profile_id'] == identity)


def generation_specification(task):
    matches = matching_profiles(task)
    if len(matches) != 1:
        raise ValueError('task has no unique reviewed execution profile')
    p = matches[0]
    workflow = execution_workflow(task, p)
    contract = p.get('output_contract') or {}
    if 'inherit_reference' in contract:
        reference = task['references'][contract['inherit_reference']]
        facts = (task.get('_runpod_media') or {}).get(reference['asset_id']) or {}
        if facts.get('sha256') != reference.get('sha256'):
            facts = {}
        return dict(width=facts.get('width'), height=facts.get('height'), length=facts.get('frames'),
                    fps=facts.get('fps'), audio=contract['audio'])
    specification = dict(workflow['graph'][p['generation_node']]['inputs'])
    specification.update(fps=workflow['graph'][contract['fps_node']]['inputs']['fps'] if contract else 24,
                         audio=contract.get('audio', 'required'))
    return specification


def task_references(task):
    refs = list(task.get('references') or [])
    for name in ('first', 'last'):
        if (task.get('anchors') or {}).get(name):
            refs.append(dict(task['anchors'][name], kind='image'))
    return refs


def execution_workflow(task, profile=None):
    profile = profile or next(iter(matching_profiles(task)), None)
    if profile and profile.get('parameterized_native'):
        from .cuda_h3 import workflow
        return workflow(task, profile)
    if task.get('mode') != 'a2va':
        workflow = copy.deepcopy(task['workflow'])
        profile = profile or next(iter(matching_profiles(task)), None)
        if profile and profile.get('runtime_adapter'):
            workflow['graph']['h3avmask'] = {'class_type':'H3AVMaskPrepare', 'inputs':{'latent':['30', 0]}}
            workflow['graph']['10']['inputs']['latent_image'] = ['h3avmask', 0]
        return workflow
    profile = profile or next(p for p in profiles() if p.get('native_generation'))
    workflow = copy.deepcopy(profile['workflow_template'])
    workflow['graph']['7']['inputs']['prompt'] = task['prompt']
    workflow['graph']['8']['inputs']['noise_seed'] = int(task['seed'])
    refs = task_references(task)
    for b in workflow['bindings']:
        b['asset_id'] = refs[int(b['asset_id'].removeprefix('slot_'))]['asset_id']
    return workflow


def _native_matches(task, profile):
    if task.get('mode') != 'a2va' or not profile.get('native_generation'):
        return False
    try:
        if (task.get('_native') or task.get('model_revision') != 'installed-model-revision'
                or task.get('preset_revision') != 'standard-v1'
                or task.get('generation') != profile['native_generation']
                or set(task.get('anchors') or {}) != {'first'}
                or len(task.get('references') or []) != 1
                or not isinstance(task.get('prompt'), str) or not task['prompt']
                or len(task['prompt'].encode()) > 16384 or type(task.get('seed')) is bool
                or not re.fullmatch(r'[0-9]{1,20}', str(task.get('seed')))
                or not 0 <= int(task['seed']) <= 2**64-1):
            return False
        refs = task_references(task)
        if len(set(r['asset_id'] for r in refs)) != 2:
            return False
        if any((ref.get('kind'), ref.get('content_type')) != tuple(slot)
               for ref, slot in zip(refs, profile['input_slots'])):
            return False
        from .comfy_policy import validate
        candidate = dict(task, mode='comfyui_video', model_revision='comfyui.video.v1', anchors={}, references=refs,
                         workflow=execution_workflow(task, profile))
        return not validate(candidate)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def _matches(task, profile):
    if profile['backend'] == 'audio_cuda':
        from .cuda_audio import compatible
        return compatible(task, profile)
    if profile.get('parameterized_native'):
        from .cuda_h3 import compatible
        # Keep the already deployed exact A2VA profile unambiguous.
        if task.get('mode') == 'a2va' and _native_matches(task, profiles()[2]):
            return False
        return compatible(task, profile)
    if profile.get('native_generation'):
        return _native_matches(task, profile)
    from .comfy_policy import validate
    from .comfy_variants import policy_copy, valid_parameter
    policy_task = policy_copy(task, profile) if profile.get('flexible_media') else task
    if validate(policy_task, _registered=False) or task.get('_native') or task.get('anchors'):
        return False
    workflow = copy.deepcopy(task.get('workflow'))
    if not isinstance(workflow, dict):
        return False
    # Only prompt, random seed and typed asset identities may change. All topology, model names,
    # quantization, bindings and generation settings remain exact.
    try:
        refs = task.get('references') or []
        if len(refs) != len(profile['input_slots']):
            return False
        ids = [r['asset_id'] for r in refs]
        if len(set(ids)) != len(ids):
            return False
        for ref, slot in zip(refs, profile['input_slots']):
            if profile.get('flexible_media'):
                if ref.get('kind') != slot[0] or not str(ref.get('content_type', '')).startswith(slot[0] + '/'):
                    return False
            elif (ref.get('kind'), ref.get('content_type')) != tuple(slot):
                return False
        for binding in workflow['bindings']:
            binding['asset_id'] = 'slot_' + str(ids.index(binding['asset_id']))
        for node_id, key, kind in profile['variable_inputs']:
            value = workflow['graph'][node_id]['inputs'][key]
            if profile.get('flexible_media'):
                if not valid_parameter(value, kind):
                    return False
            elif kind == 'text':
                if not isinstance(value, str) or len(value.encode()) > 16384:
                    return False
            elif type(value) is not int or not 0 <= value <= 2**64 - 1:
                return False
            workflow['graph'][node_id]['inputs'][key] = profile['workflow_template']['graph'][node_id]['inputs'][key]
        if workflow != profile['workflow_template']:
            return False
        return True
    except (KeyError, TypeError, ValueError):
        return False


def matching_profiles(task):
    if isinstance(task, str):
        task = json.loads(task)
    if not isinstance(task, dict) or task.get('mode') not in ('comfyui_video', 't2va', 'fl2va', 'ref2va', 'a2va', 'tts', 'align'):
        return ()
    return tuple(p for p in profiles() if _matches(task, p))


def policy_view(task):
    """Only registered fixed prompt sockets receive business text semantics."""
    from .comfy_variants import policy_copy
    try:
        match = next((p for p in matching_profiles(task) if p.get('flexible_media')), None)
        return policy_copy(task, match) if match else task
    except (KeyError, TypeError, ValueError):
        return task


def requirements_snapshot(task):
    """Server-owned immutable admission snapshot; never trust a caller's copy."""
    from .runpod_media import lanpaint_error
    return {'schema_version': 1, 'backend': 'audio_cuda' if task.get('mode') in ('tts','align') else 'comfyui',
            'profile_digests': [p['profile_digest'] for p in matching_profiles(task)
                if not p.get('runtime_adapter') or lanpaint_error(task) is None]}


def admitted_profiles(task):
    """Profiles supported by both current review and immutable admission."""
    if task.get('_execution_requirements') != requirements_snapshot(task):
        return ()
    allowed = requirements_snapshot(task)['profile_digests']
    return tuple(p for p in matching_profiles(task) if p['profile_digest'] in allowed)


def ready_profiles(capabilities, config, worker_id, *, first_task=False):
    """Explicit operator authorization plus generation-bound runtime evidence."""
    if not getattr(config, 'runpod_execution_enabled', False) \
            or worker_id not in getattr(config, 'runpod_worker_ids', ()) \
            or capabilities.get('execution_backend') != 'runpod':
        return []
    generation = capabilities.get('executor_generation')
    if not isinstance(generation, str) or not re.fullmatch('[A-Za-z0-9_.:-]{1,128}', generation):
        return []
    evidence = capabilities.get('execution_profiles')
    if not isinstance(evidence, list):
        return []
    approved = set(getattr(config, 'runpod_validated_profiles', ()))
    accepted = []
    for profile in profiles():
        if profile['profile_digest'] not in approved:
            continue
        for item in evidence[:32]:
            if not isinstance(item, dict):
                continue
            vram = item.get('vram_bytes')
            if item.get('profile_digest') == profile['profile_digest'] \
                    and item.get('generation') == generation \
                    and ((item.get('ready') is True and isinstance(item.get('validation_artifact_sha256'), str)
                          and re.fullmatch('[0-9a-f]{64}', item['validation_artifact_sha256']))
                         or (first_task and item.get('prepared') is True)) \
                    and item.get('comfy_version') == profile['comfy_version'] \
                    and item.get('comfy_commit') == profile['comfy_commit'] \
                    and item.get('torch_version') == profile['torch_version'] \
                    and item.get('gpu_vendor') == 'nvidia' \
                    and type(vram) is int and vram > 0 \
                    and item.get('models_digest') == profile['models_digest'] \
                    and item.get('nodes_digest') == profile['nodes_digest']:
                accepted.append(profile['profile_digest'])
                break
    return accepted


def claimable_profiles(capabilities, config, worker_id):
    """Validated profiles, or prepared resources for an already authorized business task.

    Prepared evidence remains unvalidated; no fabricated readiness or artifact.
    """
    return ready_profiles(capabilities, config, worker_id, first_task=True)


def advice(raw, *, config=None):
    """Display compatibility, not a claim that a GPU is provisioned or ready."""
    task = json.loads(raw) if isinstance(raw, str) else (raw or {})
    mode = task.get('mode')
    result = {'schema_version': 1, 'target': 'NVIDIA / RunPod',
        'automatic_execution': False, 'profile_digests': [], 'reasons': [],
        'execution_target':task.get('execution_target','auto'),'request_digest':digest(task),
        'scheduling': 'local', 'recommendation': '优先使用现有兼容节点。'}
    if task.get('_native') or (task.get('generation') or {}).get('use_int8_row_fc2'):
        result.update(status='local_only', label='保留本地执行',
            reasons=['任务绑定原生节点或 M5 优化，不能直接替换为 CUDA 工作流。'])
    elif mode in ('tts', 'align') and not matching_profiles(task):
        result.update(status='needs_adapter', label='需适配 CUDA 运行器',
            reasons=['现有音频执行配置使用 MLX；NVIDIA 需要单独的运行器和模型兼容验证。'])
    elif mode in ('t2va', 'fl2va', 'ref2va', 'a2va') and not matching_profiles(task):
        result.update(status='needs_adapter', label='需核对原生执行配置',
            reasons=['原生任务没有经验证的等价 CUDA 执行配置；不能自动更换精度或工作流。'])
    elif mode in ('comfyui_video', 't2va', 'fl2va', 'ref2va', 'a2va', 'tts', 'align'):
        matches = matching_profiles(task)
        if not matches:
            result.update(status='needs_review', label='需核对模型与节点',
                reasons=['尚未匹配批准的 NVIDIA 执行配置；任务类型相同不代表资源兼容。'],
                recommendation='核对模型精度、节点版本及生成规格；注册兼容配置后再考虑云端分流。')
        else:
            profile = matches[0]
            snapshot = task.get('_execution_requirements') or {}
            admitted = bool(admitted_profiles(task))
            from .runpod_media import lanpaint_error
            media_error = lanpaint_error(task) if profile.get('runtime_adapter') else None
            enabled = (getattr(config, 'runpod_execution_enabled', False)
                       and profile['profile_digest'] in getattr(config, 'runpod_validated_profiles', ()))
            result.update(status='compatible', label='适合 NVIDIA · 待就绪',
                scheduling='local_then_runpod', profile_digests=[p['profile_digest'] for p in matches],
                profile_label=profile['label'],
                precision=profile.get('precision', 'H3 INT8 convrot / Qwen NVFP4 AWQ / Turbo BF16'),
                comfy_version=profile['comfy_version'], required_nodes=[n['name'] for n in profile['nodes']['nodes']],
                reasons=['工作流拓扑、模型与生成规格匹配已登记的 CUDA 配置。', profile['validation']],
                admission_snapshot=admitted, automatic_execution=False,
                cloud_claim_enabled=bool(enabled and admitted),
                recommendation='本地兼容节点优先；兼容积压较大时准备 RunPod，资源与功能验证通过后再认领。')
            if media_error:
                result.update(status='needs_review',label='素材不符合云端配置',profile_digests=[],
                    cloud_claim_enabled=False,admission_snapshot=False,reasons=[media_error],
                    recommendation='在服务侧核对素材媒体信息后再考虑开机；不会自动转换素材。')
            if mode == 'a2va':
                result['reasons'].append('CUDA INT8 和音频 guide 不等于 Mac 原生算法或精度；须明确批准独立配置。')
            if profile['backend']=='audio_cuda':
                from .cuda_audio import differences
                result['backend_differences'] = differences(task,profile)
                result['reasons'].extend(result['backend_differences'])
            if not admitted:
                result['reasons'].append('历史任务没有对应的入队需求快照，当前不会被云端认领；需按新契约重新提交。')
            if not enabled:
                result['reasons'].append('云端自动认领未启用；此判断不代表已有可用 Pod。')
    else:
        result.update(status='needs_review', label='暂无法判断',
                      reasons=['该任务没有已登记的 NVIDIA 执行配置。'])
    target=task.get('execution_target','auto')
    if target == 'runpod':
        result.update(scheduling='runpod_only',recommendation='仅允许 RunPod 认领；云端未就绪时继续排队，不回退本地节点。')
    elif target == 'local':
        result.update(scheduling='local_only',recommendation='仅允许本地节点认领，不分流到 RunPod。',cloud_claim_enabled=False)
    return result
