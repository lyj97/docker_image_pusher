"""Prepare an inactive logical CloudExecutor identity; never starts a Pod or service."""
import argparse
import json
import os
from pathlib import Path
import re
import urllib.request
from .transport import NoRedirect


def provision(directory, worker_id, request):
    if not re.fullmatch('[A-Za-z0-9_-]{1,64}', worker_id):
        raise ValueError('invalid logical Worker id')
    directory = Path(directory)
    if not directory.is_absolute() or any(p.is_symlink() for p in (directory, *directory.parents)):
        raise ValueError('configuration directory must be absolute without symlinks')
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    target = directory / 'executor.env'
    if target.exists():
        raise ValueError('existing private configuration must be preserved; no token rotation')
    listing = request('GET', '/v1/admin/workers')
    if any(w['worker_id'] == worker_id for w in listing.get('workers', [])):
        raise ValueError('identity already exists; do not adopt or rotate another Worker')
    result = request('POST','/v1/admin/workers',{'worker_id':worker_id,'capacity':1,
        'display_name':'RunPod service-side executor (inactive)'})
    token = result['worker_token']
    if not re.fullmatch('[A-Za-z0-9_-]{16,256}', token):
        raise ValueError('invalid control-plane token format')
    # Save the once-returned credential before any subsequent operation.
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as output:
        output.write('H3WORKER_WORKER_ID=' + worker_id + '\nH3WORKER_WORKER_TOKEN=' + token + '\n'
            'H3WORKER_SERVER_URL=http://127.0.0.1:8730\n'
            'H3WORKER_DATA_DIR=/home/ubuntu/projects/github/h3-service/var/cloud/worker\n')
        output.flush();os.fsync(output.fileno())
    request('PATCH','/v1/admin/workers/'+worker_id,{'enabled':False,'operator_draining':True})
    return {'worker_id':worker_id,'capacity':1,'enabled':False,'credential_saved':True,
            'automatic_start':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',type=Path,required=True)
    parser.add_argument('--worker-id',default='runpod_slot_01')
    args=parser.parse_args()
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    def request(method,path,body=None):
        req=urllib.request.Request('http://127.0.0.1:8730'+path,method=method,
            data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
        with opener.open(req,timeout=5) as response:return json.load(response)
    print(json.dumps(provision(args.directory,args.worker_id,request)))


if __name__=='__main__':main()
