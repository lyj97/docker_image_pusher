"""Platform-neutral, closed LTX keyframe contract. No runtime imports."""
import re
from fractions import Fraction

MODE = 'ltx2_video'
REVISION = 'ltx2.mlx.v1.keyframe.q8'
PRESET = 'ltx2-mlx-keyframe-poc-v1'
RUNTIME_COMMIT = '1724ca673d59f023a8a95efee06e5d36d61c2765'
MODEL_REVISION = '746ca9aacb697d2c739f544d68b584214dedcc75'
UV_VERSION = '0.12.23'
DEFAULT_GENERATION = dict(width=704, height=480, fps=24, frames=49)
SETTINGS = dict(operation='keyframe', audio_policy='generated')
PROVENANCE = dict(runtime_repository='dgrauet/ltx-2-mlx', runtime_commit=RUNTIME_COMMIT,
                  runtime_version='0.15.12', model_repository='dgrauet/ltx-2.5-mlx-q8',
                  model_revision=MODEL_REVISION, quantization='q8')


def is_family(value):
    return isinstance(value, str) and value.lower().startswith('ltx2')


def validate(task):
    problems = []
    allowed = {'mode','model_revision','preset_revision','prompt','seed','generation','ltx2',
               'references','anchors','required_artifacts','priority','max_attempts','execution_seconds','request_id'}
    if set(task) - allowed: problems.append('unknown LTX request keys')
    if task.get('mode') != MODE or task.get('model_revision') != REVISION or task.get('preset_revision') != PRESET:
        problems.append('unsupported LTX mode, revision or preset')
    prompt = task.get('prompt')
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 32768 or '\x00' in prompt:
        problems.append('video prompt must be nonempty and at most 32768 chars')
    seed = task.get('seed')
    if not isinstance(seed, str) or not re.fullmatch(r'[0-9]{1,10}', seed) or int(seed) > 4294967295:
        problems.append('LTX seed must be a uint32 decimal string')
    g = task.get('generation')
    if (not isinstance(g, dict) or set(g) != set(DEFAULT_GENERATION)
            or any(type(g.get(k)) is not int for k in DEFAULT_GENERATION)
            or any(g.get(k) != v for k,v in DEFAULT_GENERATION.items() if k != 'frames')
            or g.get('frames') not in (49,97)):
        problems.append('LTX generation requires 704x480, 24fps, 49 or 97 frames only')
    if task.get('ltx2') != SETTINGS: problems.append('LTX only supports keyframe with generated audio')
    if task.get('anchors') != {}: problems.append('LTX anchors are forbidden')
    refs = task.get('references')
    if not isinstance(refs,list) or len(refs) != 2 or any(not isinstance(r,dict) for r in refs):
        problems.append('LTX requires exactly two image references')
    else:
        if (sorted(str(r.get('role')) for r in refs) != ['end','start']
                or any(r.get('kind') != 'image' or not isinstance(r.get('asset_id'),str) or not r['asset_id'] for r in refs)
                or refs[0].get('asset_id') == refs[1].get('asset_id')
                or any(set(r)-{'asset_id','role','kind','sha256','size_bytes','content_type','duration_seconds'} for r in refs)
                or any('content_type' in r and not str(r['content_type']).startswith('image/') for r in refs)):
            problems.append('LTX requires distinct start/end image assets')
        hashes = [r.get('sha256') for r in refs]
        if all(hashes) and hashes[0] == hashes[1]: problems.append('LTX image contents must be distinct')
    if task.get('required_artifacts') != ['video','manifest']: problems.append('LTX artifacts must be video and manifest')
    for k,lo,hi in [('execution_seconds',120,86400),('priority',-1000,1000),('max_attempts',1,10)]:
        if k in task and (type(task[k]) is not int or not lo <= task[k] <= hi): problems.append(f'invalid {k}')
    return problems


def input_hashes(request):
    return {r['role']: r['sha256'] for r in request['references']}


def validate_artifacts(request, manifest, probe, result):
    """Structural validation only; no claim of semantic interpolation quality."""
    from shared.h3proto import digest_obj
    try:
        g = request['generation']
        return (not validate(request) and probe.get('probed') is True and probe.get('has_audio') is True
                and manifest['engine'] == 'ltx2-mlx' and manifest['engine_version'] == '0.15.12'
                and manifest['preset_revision'] == PRESET and manifest['model_revision'] == REVISION and manifest['seed'] == request['seed']
                and manifest['request_digest'] == digest_obj(request)
                and manifest['provenance'] == PROVENANCE and manifest['effective_params'] == dict(g, **SETTINGS)
                and manifest['input_digests'] == input_hashes(request)
                and manifest['result'] == result
                and all(type(result[k]) is int and result[k] == g[k] for k in g)
                and all(probe[k] == g[k] for k in ('width','height','frames'))
                and Fraction(str(probe['fps'])) == 24
                and abs(probe['duration_seconds'] - g['frames']/24) <= 0.15)
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return False
