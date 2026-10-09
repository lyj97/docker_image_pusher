"""Offline import/metadata checks only. No model construction or media decoding."""
import json
from importlib.metadata import version
from pathlib import Path
import sys

from shared.cuda_audio import UPSTREAM

runtime=sys.argv[1]
root=Path('/opt/h3-audio')/runtime
proof=json.loads((root/'provenance.json').read_text())
assert proof['commit']==UPSTREAM[runtime][1]
assert version('torch')=='2.10.0+cu130'
if runtime=='qwen_tts':
    assert version('transformers')=='4.57.3'
    from qwen_tts import Qwen3TTSModel
    for method in ('generate_voice_clone','generate_voice_design','generate_custom_voice'):
        assert callable(getattr(Qwen3TTSModel,method))
elif runtime=='qwen_align':
    assert version('transformers')=='4.57.6'
    from qwen_asr import Qwen3ForcedAligner
    assert callable(Qwen3ForcedAligner.align)
else:
    assert version('transformers')=='4.51.3'
    import onnxruntime
    assert 'CUDAExecutionProvider' in onnxruntime.get_available_providers()
    from cosyvoice.cli.cosyvoice import AutoModel
    import cosyvoice.llm.llm, cosyvoice.flow.flow, cosyvoice.flow.flow_matching
    import cosyvoice.flow.DiT.dit, cosyvoice.hifigan.generator, matcha.models.components.flow_matching
    assert callable(AutoModel)
print(json.dumps({'runtime':runtime,'imports':'passed','gpu_inference':False,'media_test':False}))
