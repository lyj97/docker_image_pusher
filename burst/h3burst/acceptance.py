"""Explicit operator acceptance request. This command generates one paid GPU video."""
import argparse
import copy
import json
import os
import sys
from pathlib import Path
import time
import uuid
from shared.execution import profiles, matching_profiles, requirements_snapshot
from .transport import Transport


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding',type=Path,required=True)
    parser.add_argument('--task',type=Path,help='reviewed task JSON for asset or a2va acceptance')
    parser.add_argument('--inputs',type=Path,help='operator JSON mapping ref:asset_id and anchors.first to local files')
    parser.add_argument('--confirm-paid-generation',action='store_true')
    parser.add_argument('--resume',action='store_true',help='GET-only recovery of an existing acceptance intent')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not args.resume and not args.confirm_paid_generation:
        raise ValueError('new acceptance requires explicit paid generation flag')
    if args.output.exists() and not args.resume:
        raise ValueError('acceptance output must be a new directory')
    binding=json.loads(args.binding.read_text())
    remote=Transport(binding['pod_url'],os.environ['H3BURST_POD_TOKEN'],binding['generation'])
    from .control import Provider, H3Control
    provider=Provider(os.environ.get('RUNPOD_API_KEY'))
    inventory=provider.inventory()
    if len(inventory)!=1 or inventory[0]['id']!=binding['pod_id'] or inventory[0]['name']!=binding['pod_name']:
        raise ValueError('acceptance requires exactly one account Pod matching the binding')
    control=H3Control(binding.get('h3_admin_url','http://127.0.0.1:8730'),binding['worker_id'])
    control.drain()
    if not control.idle():
        raise ValueError('acceptance requires no active business attempt or unresolved claim')
    try:
        run(args,binding,remote)
    except BaseException as error:
        failure_stop(binding,remote,provider,args.output,error)
        raise


def failure_stop(binding,remote,provider,output,error):
    """Bounded diagnostics then immediate test abort; never wait for engine idle."""
    from .prepare import atomic_json
    report={'error_type':type(error).__name__,'pod_id':binding['pod_id'],
            'generation':binding['generation'],'stop_requested':False}
    from .transport import RemoteError
    if isinstance(error, RemoteError):
        report['http_status'] = error.status
    try:
        try:
            output.mkdir(parents=True,exist_ok=True,mode=0o700)
            diagnostics=remote.json('/v1/diagnostics')
            atomic_json(output/'failure-diagnostics.json',diagnostics)
            report['diagnostics_saved']=True
        except Exception:
            report['diagnostics_saved']=False
    finally:
        # A failed log export must never keep the paid test Pod running.
        try:
            provider.stop(binding['pod_id'])
            report['stop_requested']=True
        except Exception:
            report['stop_operation_uncertain']=True
        try:
            pod=provider.get(binding['pod_id'])
            report['provider_status']=pod.get('status')
            report['provider_confirmed_stopped']=pod.get('status') in ('STOPPED','EXITED')
        except Exception:
            report['provider_confirmation_unavailable']=True
        try:
            atomic_json(output/'failure-stop.json',report)
        except Exception:
            print('Failure record could not be saved; stop was still attempted.',file=sys.stderr)
        if not report.get('provider_confirmed_stopped'):
            print('Test failed; Pod stop not confirmed. Reconcile provider state immediately.',file=sys.stderr)


def run(args,binding,remote):
    profile=profiles()[0]
    if args.resume:
        saved=json.loads((args.output/'binding.json').read_text())
        if saved != {'pod_url':binding['pod_url'],'generation':binding['generation']}:
            raise ValueError('acceptance recovery binding differs from durable intent')
        intent=json.loads((args.output/'intent.json').read_text())
        task=intent['task'];execution=intent['execution_id']
    else:
        args.output.mkdir(parents=True,mode=0o700)
        task={'mode':'comfyui_video','model_revision':'comfyui.video.v1',
              'workflow':copy.deepcopy(profile['workflow_template']),'references':[],'anchors':{}}
        task['workflow']['graph']['7']['inputs']['prompt']='A person walking through a quiet garden, natural light.'
        if args.task:
            task=json.loads(args.task.read_text())
        task['_execution_requirements']=requirements_snapshot(task)
        matches=matching_profiles(task)
        if len(matches)!=1:raise ValueError('acceptance requires a reviewed profile')
        profile=matches[0]
        while True:
            status=remote.json('/v1/status')
            if status.get('stop_required') or status.get('draining'):raise RuntimeError('preparation failed or draining')
            if any(p.get('prepared') is True and p.get('profile_digest')==profile['profile_digest'] for p in status.get('profiles',[status['profile']])):break
            if status['profile'].get('prepared'):remote.json('/v1/prepare',{'profile_digests':[profile['profile_digest']]})
            time.sleep(5)
        execution='att_acceptance_'+uuid.uuid4().hex
        intent={'execution_id':execution,'task':task,'acceptance':True}
        from .inputs import descriptors
        assets=descriptors(task)
        if assets:
            if args.inputs is None:raise ValueError('asset acceptance requires local inputs')
            local=json.loads(args.inputs.read_text())
            if task['mode']=='a2va':local['ref:'+task['anchors']['first']['asset_id']]=local.pop('anchors.first')
            if set(local)!={'ref:'+a for a in assets}:raise ValueError('acceptance inputs mismatch')
            for asset,item in assets.items():remote.upload(local['ref:'+asset],item)
            intent['inputs']=assets
        # Operator journal before POST, same no-repeat contract as CloudExecutor.
        (args.output/'binding.json').write_text(json.dumps({'pod_url':binding['pod_url'],'generation':binding['generation']}))
        with (args.output/'intent.json').open('w') as output:
            json.dump(intent,output);output.flush();os.fsync(output.fileno())
        remote.json('/v1/executions',intent)
    profile=matching_profiles(task)[0]
    while True:
        view=remote.json('/v1/executions/'+execution)
        (args.output/'observation.json').write_text(json.dumps(view))
        if view.get('terminal'):
            if view['state']!='completed':raise RuntimeError('functional acceptance failed')
            artifact=view['artifact']
            remote.download('/v1/executions/'+execution+'/artifact',args.output/'result.mp4',
                            artifact['size_bytes'],artifact['sha256'])
            from .media import video_result
            if task['mode']=='a2va':
                if args.inputs is None:raise ValueError('a2va result validation requires original local audio')
                from types import SimpleNamespace
                from .media import original_soundtrack
                source=json.loads(args.inputs.read_text())['ref:'+task['references'][0]['asset_id']]
                original_soundtrack(args.output/'result.mp4',source,task,SimpleNamespace(ffmpeg_path='ffmpeg',ffprobe_path='ffprobe'))
            video_result(args.output/'result.mp4','ffprobe',task)
            status=remote.json('/v1/status')
            (args.output/'validation.json').write_text(json.dumps(status))
            proof=next(p for p in status.get('profiles',[status['profile']]) if p.get('profile_digest')==profile['profile_digest'])
            print(json.dumps({'profile_digest':profile['profile_digest'],'generation':binding['generation'],
                'ready':proof['ready'],'validation_artifact_sha256':proof.get('validation_artifact_sha256')}))
            return
        if view['state']=='uncertain':raise RuntimeError('uncertain execution; retain intent and reconcile, never repeat POST')
        time.sleep(2)


if __name__=='__main__':main()
