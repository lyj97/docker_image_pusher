"""One supervised CUDA audio job, run in its baked isolated Python environment.

No model imports or inference during module import. Input/output paths are supplied
by the durable Pod owner, never by arbitrary task workflow code.
"""
import json
from pathlib import Path
import random
import subprocess

from shared.cuda_audio import language
from shared.h3proto import validate_alignment_payload


def qwen_options(task):
    settings = task.get('tts') or {}
    temperature = float(settings.get('temperature', 0.7))
    return dict(text=task['prompt'], language=language(settings.get('language') or 'zh'),
        do_sample=temperature > 0, subtalker_dosample=temperature > 0,
        **({'temperature':temperature,'subtalker_temperature':temperature} if temperature > 0 else {}))


def alignment_payload(items, text):
    payload = {'text':text,'segments':[{'text':i.text,'start':float(i.start_time),
        'end':float(i.end_time),'duration':float(i.end_time)-float(i.start_time)} for i in items]}
    problems = validate_alignment_payload(payload, requested_text=text)
    if problems:raise ValueError('invalid forced alignment: '+'; '.join(problems[:3]))
    return payload


def qwen_generate(model, task, audio):
    settings = task.get('tts') or {}
    options = qwen_options(task)
    operation = settings.get('operation', 'clone')
    if operation == 'voice_design':
        return model.generate_voice_design(**options, instruct=settings['instruct'])
    if operation == 'custom_voice':
        return model.generate_custom_voice(**options, speaker=settings['voice'], instruct=settings.get('instruct'))
    # Base voice cloning has no instruction socket. Expose the difference, never
    # claim that it has been applied by passing it as an unsupported generate kwarg.
    return model.generate_voice_clone(**options, ref_audio=str(audio), ref_text=settings['ref_text'])


def temperature_sampler(original, temperature):
    """Temperature acts on the official token sampler, including its repetition rule."""
    def sample(scores, decoded_tokens, sampling):
        if temperature == 0:return scores.argmax().reshape(1)
        return original(scores / temperature, decoded_tokens, sampling)
    return sample


def cosy_generate(model, task, audio, torch):
    settings = task.get('tts') or {}
    model.model.llm.sampling = temperature_sampler(model.model.llm.sampling, float(settings.get('temperature', 0.7)))
    instruct = settings.get('instruct') or ''
    lang = settings.get('language') or 'zh'
    prompt = 'Speak in '+str(language(lang))+'. '+instruct
    # Official CosyVoice3 examples put instructions before endofprompt and
    # the reference transcript after it, preserving both clone conditioning inputs.
    prompt = prompt.replace('<|endofprompt|>', '') + '<|endofprompt|>' + (settings.get('ref_text') or '')
    if settings.get('ref_text'):
        chunks = model.inference_zero_shot(task['prompt'], prompt, str(audio),
            stream=False, speed=float(settings.get('speed', 1.0)))
    else:
        chunks = model.inference_instruct2(task['prompt'], prompt, str(audio),
            stream=False, speed=float(settings.get('speed', 1.0)))
    waves = [c['tts_speech'].detach().cpu() for c in chunks]
    if not waves:raise RuntimeError('CosyVoice produced no audio')
    return torch.cat(waves, dim=1).squeeze(0).numpy(), model.sample_rate


def cosy_audio_loader(torch):
    """Replace removed torchaudio backend selection with SoundFile, keeping resampling."""
    def load(path, target_sr, min_sr=16000):
        import soundfile as sf
        import torchaudio
        data, rate = sf.read(str(path),dtype='float32',always_2d=True)
        speech = torch.from_numpy(data.T.copy()).mean(dim=0,keepdim=True)
        if rate!=target_sr:
            if rate<min_sr:raise ValueError('reference sample rate is below upstream minimum')
            speech = torchaudio.transforms.Resample(orig_freq=rate,new_freq=target_sr)(speech)
        return speech
    return load


def run(job):
    import torch
    if not torch.cuda.is_available():raise RuntimeError('CUDA unavailable for audio task')
    from shared.execution import profile_by_id
    profile = profile_by_id(job['profile_id'])
    from shared.cuda_audio import compatible
    task = job['task']
    if profile['backend'] != 'audio_cuda' or not compatible(task, profile):raise ValueError('audio recipe identity conflict')
    model_dir = Path(job['models_root']) / profile['model_directory']
    output = Path(job['output'])
    output.parent.mkdir(parents=True, exist_ok=True)
    audio = next(iter(job['inputs'].values()), None)
    seed = int(task.get('seed', 42))
    random.seed(seed);torch.manual_seed(seed);torch.cuda.manual_seed_all(seed)
    import numpy as np
    np.random.seed(seed % 2**32)
    if task['mode'] == 'align':
        from qwen_asr import Qwen3ForcedAligner
        model = Qwen3ForcedAligner.from_pretrained(str(model_dir), device_map='cuda:0', dtype=torch.bfloat16)
        items = model.align(audio=str(audio), text=task['prompt'], language=language((task.get('align') or {}).get('language','Chinese')))[0]
        output.write_text(json.dumps(alignment_payload(items, task['prompt']), ensure_ascii=False))
    else:
        import soundfile as sf
        if profile['audio_runtime'] == 'qwen_tts':
            from qwen_tts import Qwen3TTSModel
            model = Qwen3TTSModel.from_pretrained(str(model_dir), device_map='cuda:0', dtype=torch.bfloat16)
            waves, rate = qwen_generate(model, task, audio)
            sf.write(str(output), waves[0], rate, subtype='PCM_16')
            speed = float((task.get('tts') or {}).get('speed',1.0))
            if speed != 1.0:
                temporary = output.with_name('speed.pending.wav')
                subprocess.run(['ffmpeg','-v','error','-y','-i',str(output),'-af','atempo='+str(speed),
                    '-c:a','pcm_s16le',str(temporary)],check=True,capture_output=True)
                temporary.replace(output)
        else:
            import onnxruntime
            # ORT 1.30 CUDA13/cuDNN9 loads the same libraries as the pinned Torch.
            onnxruntime.preload_dlls()
            from cosyvoice.cli.cosyvoice import AutoModel
            import cosyvoice.cli.frontend as frontend
            import cosyvoice.utils.file_utils as file_utils
            file_utils.load_wav = frontend.load_wav = cosy_audio_loader(torch)
            model = AutoModel(model_dir=str(model_dir), load_trt=False, load_vllm=False, fp16=False)
            # CosyVoice YAML resets seeds during construction; restore user seed afterward.
            random.seed(seed);np.random.seed(seed % 2**32);torch.manual_seed(seed);torch.cuda.manual_seed_all(seed)
            wave, rate = cosy_generate(model, task, audio, torch)
            sf.write(str(output), wave, rate, subtype='PCM_16')


def main():
    import argparse
    from .prepare import atomic_json
    parser = argparse.ArgumentParser();parser.add_argument('--job',type=Path,required=True)
    args = parser.parse_args();job = json.loads(args.job.read_text())
    try:
        run(job)
        atomic_json(args.job.with_name('outcome.json'), {'state':'completed'})
    except Exception as exc:
        from h3worker.http import sanitize_error_text
        diagnostic = sanitize_error_text(str(exc))
        atomic_json(args.job.with_name('outcome.json'), {'state':'failed','error_category':type(exc).__name__,
            'error':diagnostic[:1000]})
        raise SystemExit(1)


if __name__ == '__main__':main()
