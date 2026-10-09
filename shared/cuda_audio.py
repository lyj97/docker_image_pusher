"""Fixed upstream audio recipes; no CUDA package is imported by admission."""
import hashlib
import json
from pathlib import Path

UPSTREAM = {
    'qwen_tts': ('https://github.com/QwenLM/Qwen3-TTS.git', '022e286b98fbec7e1e916cb940cdf532cd9f488e'),
    'qwen_align': ('https://github.com/QwenLM/Qwen3-ASR.git', '7c6daf77a2421100f5fb066495372c00129d39ff'),
    'cosyvoice3': ('https://github.com/QwenAudio/CosyVoice.git', '074ca6dc9e80a2f424f1f74b48bdd7d3fea531cc'),
}
RECIPES = (
    ('qwen-voice-design', 'qwen_tts', 'tts', ['tts.qwen3.voice_design'], 'Qwen3 VoiceDesign 1.7B BF16'),
    ('qwen-custom-voice', 'qwen_tts', 'tts', ['tts.qwen3.custom_voice'], 'Qwen3 CustomVoice 0.6B BF16'),
    ('qwen-custom-instruct', 'qwen_tts', 'tts', ['tts.qwen3.custom_voice'], 'Qwen3 CustomVoice 1.7B BF16 · 支持风格指令'),
    ('qwen-clone', 'qwen_tts', 'tts', ['tts.qwen3.clone'], 'Qwen3 Clone 0.6B BF16'),
    ('qwen-align', 'qwen_align', 'align', ['audio.qwen3.forced_align'], 'Qwen3 ForcedAligner 0.6B BF16'),
    ('cosyvoice3', 'cosyvoice3', 'tts', ['cosyvoice3-mlx', 'tts.cosyvoice3.clone'], 'CosyVoice3 0.5B CUDA FP32'),
)


def profiles(base):
    from .execution import digest
    root = Path(__file__).with_name('execution_profiles')
    # Regular Worker packages contain shared/client, not the cloud-only burst
    # runtime. Keep its pinned SHA manifest in shared and verify real files in CI.
    runtime_sources = json.loads((root/'audio-runtime.sources.json').read_text())
    runtime_sources['shared/cuda_audio.py'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    result = []
    for identity, runtime, mode, revisions, label in RECIPES:
        models = json.loads((root / (identity + '.models.json')).read_text())
        profile = dict(base, profile_id=identity+'-cu130-v1', label=label, backend='audio_cuda',
            native_modes=[mode], model_revisions=revisions, audio_runtime=runtime,
            model_directory='h3_audio/'+identity, models=models,
            nodes={'schema_version':1, 'nodes':[]}, variable_inputs=[], input_slots=[],
            precision=label+'；CosyVoice 与 Mac MLX 4bit 不同；CUDA 音频不承诺位级一致',
            validation='复用官方 CUDA 推理接口；真实 GPU 推理和语音质量尚待验收',
            upstream={'repository':UPSTREAM[runtime][0], 'commit':UPSTREAM[runtime][1]},
            runtime_sources=dict(runtime_sources))
        # Audio execution has no Comfy graph; these fields only attest the shared image runtime.
        profile.pop('generation_node', None)
        profile['workflow_template'] = None
        profile['models_digest'], profile['nodes_digest'] = digest(models), digest(profile['nodes'])
        profile['profile_digest'] = digest({k:profile[k] for k in ('profile_id','backend','gpu_vendor',
            'torch_version','models','nodes','native_modes','model_revisions','audio_runtime',
            'model_directory','upstream','runtime_sources')})
        result.append(profile)
    return result


def compatible(task, profile):
    from .h3proto import validate_task_request
    if task.get('mode') not in profile['native_modes'] or task.get('model_revision') not in profile['model_revisions'] or task.get('_native'):
        return False
    try:
        if validate_task_request(task):return False
        if task.get('model_revision') == 'tts.qwen3.custom_voice':
            instructed = bool((task.get('tts') or {}).get('instruct'))
            if instructed != (profile['profile_id'] == 'qwen-custom-instruct-cu130-v1'):return False
        if task.get('mode') == 'tts':
            seed = task.get('seed', '42')
            if isinstance(seed, bool) or not str(seed).isdigit() or not 0 <= int(seed) < 2**64:return False
        return True
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def language(value):
    aliases = {'zh':'Chinese','en':'English','ja':'Japanese','ko':'Korean','de':'German',
        'fr':'French','ru':'Russian','pt':'Portuguese','es':'Spanish','it':'Italian'}
    return aliases.get(str(value).lower(), value)


def differences(task, profile):
    notes = []
    if profile['audio_runtime']=='cosyvoice3':
        notes.extend(['Mac MLX 4bit 替换为官方 CUDA FP32 权重，音色质量可能不同。',
            'language 转为自然语言提示；temperature 使用独立版本化 token 采样适配器。'])
        if not (task.get('tts') or {}).get('ref_text'):
            notes.append('无参考转录时使用官方 instruct2 克隆路径；参考音色条件保留，LLM 参考语音 token 不参与。')
    if profile['profile_id']=='qwen-custom-instruct-cu130-v1':
        notes.append('为应用 instruct，从 Mac 的 CustomVoice 0.6B 升为官方 1.7B BF16；不承诺相同输出。')
    if task.get('model_revision')=='tts.qwen3.clone' and (task.get('tts') or {}).get('instruct'):
        notes.append('Qwen3 Base clone 无 instruct 接口：克隆照常执行，此风格指令未应用。')
    if profile['audio_runtime']=='qwen_tts' and float((task.get('tts') or {}).get('speed',1.0))!=1:
        notes.append('speed 在官方生成后通过 atempo 调整，保持音高；不是模型原生语速控制。')
    return notes
