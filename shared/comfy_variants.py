"""Registered portable Comfy graphs; parameters vary, model identity does not."""
import copy
import hashlib
import json
import math
from pathlib import Path


def variants(base):
    from .execution import digest
    root=Path(__file__).with_name('execution_profiles')
    definitions=[
        ('vace-control','Wan VACE 14B · 参考图与控制视频','10',
         [('video','video/mp4'),('image','image/png')],
         [['4','text','text'],['5','text','text'],['11','seed','seed'],['10','width','dimension'],
          ['10','height','dimension'],['10','length','wan_frames'],['10','strength','number'],
          ['11','steps','positive_integer'],['11','cfg','number'],['11','denoise','unit_interval'],['14','fps','positive_number']]),
        ('vace-joiner','Wan VACE 14B · 左右视频接缝','14',
         [('video','video/mp4'),('video','video/mp4')],
         [['4','text','text'],['5','text','text'],['15','seed','seed'],['14','width','dimension'],
          ['14','height','dimension'],['14','length','wan_frames'],['14','strength','number'],
          ['15','steps','positive_integer'],['15','cfg','number'],['15','denoise','unit_interval'],
          ['18','fps','positive_number'],['9','width','dimension'],['9','height','dimension'],
          ['12','width','dimension'],['12','height','dimension'],['13','context_frames','positive_integer'],
          ['13','new_frames','positive_integer'],['13','replace_frames','nonnegative_integer']]),
        ('da3-depth','DA3 · 视频深度预处理',None,[('video','video/mp4')],
         [['4','resolution','positive_integer']]),
        ('sdpose','SDPose · 视频姿态预处理',None,[('video','video/mp4')],
         [['4','batch_size','positive_integer'],['5','stick_width','positive_integer'],
          ['5','face_point_size','positive_integer'],['5','score_threshold','unit_interval'],
          *[['5',k,'boolean'] for k in ('draw_body','draw_face','draw_feet','draw_head','draw_hands')]])]
    result=[]
    for name,label,generation_node,slots,inputs in definitions:
        models_name='vace-control' if name=='vace-joiner' else name
        p=dict(base, profile_id=name+'-cu130-v1',label=label,
               workflow_template=json.loads((root/(name+'.workflow.json')).read_text()),
               models=json.loads((root/(models_name+'.models.json')).read_text()),
               variable_inputs=inputs, input_slots=slots, generation_node=generation_node,
               flexible_media=True, precision='原工作流 FP16 权重；CUDA 执行，不改模型族',
               validation='原图有 Mac 历史验收；CUDA 真实推理与新参数尚待验收',
               output_contract={'audio':'none','fps_node':'18' if name=='vace-joiner' else '14'} if generation_node else
                   {'audio':'none','inherit_reference':0},
               runtime_sources={'shared/comfy_variants.py':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        if name=='vace-joiner':
            p['nodes']={'schema_version':1,'nodes':[{'name':'WanVACEPrep',
                'repository':'https://github.com/stuttlepress/ComfyUI-Wan-VACE-Prep.git',
                'commit':'a9ad9c782b6f69fbb56cb7e2fe4d69cb6ed8596c',
                'requirements':None,'classes':['WanVACEPrep']}]}
        p['models_digest'],p['nodes_digest']=digest(p['models']),digest(p['nodes'])
        p['profile_digest']=digest({k:p[k] for k in ('profile_id','backend','gpu_vendor','comfy_version',
            'comfy_commit','torch_version','workflow_template','models','nodes','variable_inputs',
            'input_slots','generation_node','output_contract','flexible_media','runtime_sources')})
        result.append(p)
    return result


def valid_parameter(value, kind):
    if kind=='text':return isinstance(value,str) and len(value)<=32768
    if kind=='boolean':return type(value) is bool
    if kind in ('seed','dimension','wan_frames','positive_integer','nonnegative_integer'):
        if type(value) is not int:return False
        if kind=='seed':return 0<=value<=2**64-1
        if kind=='dimension':return value>0 and value%16==0
        if kind=='wan_frames':return value>0 and value%4==1
        return value>0 if kind=='positive_integer' else value>=0
    if type(value) not in (int,float) or not math.isfinite(value):return False
    if kind=='positive_number':return value>0
    if kind=='unit_interval':return 0<=value<=1
    return kind=='number'


def policy_copy(task, profile):
    result=copy.deepcopy(task)
    workflow=result.get('workflow')
    if not isinstance(workflow,dict) or not isinstance(workflow.get('graph'),dict):
        return result
    for node,key,kind in profile['variable_inputs']:
        item=workflow['graph'].get(node)
        inputs=item.get('inputs', {}) if isinstance(item,dict) else {}
        if not isinstance(inputs,dict):
            continue
        if kind=='text' and key in inputs:
            inputs[key]='registered prompt contract'
    return result
