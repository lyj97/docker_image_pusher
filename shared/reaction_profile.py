"""Exact full14B production relighting window; no model or topology conversion."""
import hashlib
import json
from pathlib import Path


def profile(base):
    from .execution import digest
    root = Path(__file__).with_name('execution_profiles')
    result = dict(base, profile_id='vace-reaction25-production-fp8-scaled-cu130-v1',
        label='Wan VACE full14B FP8 scaled · reaction25 relighting / final60 backfill',
        workflow_template=json.loads((root / 'vace-reaction25-production.workflow.json').read_text()),
        models=json.loads((root / 'vace-full158.models.json').read_text()),
        nodes={'schema_version': 1, 'nodes': [{'name': 'LanPaint',
            'repository': 'https://github.com/scraed/LanPaint.git',
            'commit': '2d7912f9a5efe5ece8de334c7ca18317b8288c39',
            'requirements': None, 'classes': ['LanPaint_VideoMaskEditor']}]},
        variable_inputs=[['4', 'text', 'text'], ['11', 'seed', 'seed']],
        input_slots=[('video', 'video/x-matroska')] * 4 + [('image', 'image/png')],
        generation_node='10', flexible_media=True,
        output_contract={'audio': 'none', 'fps_node': '14'},
        precision='Complete14B FP8 scaled; exact50steps CFG5 shift16 uni_pc/simple native reference prefix',
        validation='Static production contract; actual CUDA resources and final60 quality require production monitoring/review',
        runtime_sources={'shared/reaction_profile.py': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    result['models_digest'], result['nodes_digest'] = digest(result['models']), digest(result['nodes'])
    result['profile_digest'] = digest({k: result[k] for k in ('profile_id', 'backend', 'gpu_vendor',
        'comfy_version', 'comfy_commit', 'torch_version', 'workflow_template', 'models', 'nodes',
        'variable_inputs', 'input_slots', 'generation_node', 'output_contract', 'flexible_media',
        'runtime_sources')})
    return result
