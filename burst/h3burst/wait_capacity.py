"""Read-only polling for the fixed A40 test specification; never provisions Pods."""
import argparse
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import time
import urllib.parse
from .control import Provider

# User-fixed specification: do not substitute GPU, cloud, disk, or CUDA floor.
SPEC = {'cloud':'SECURE','gpu':{'id':'NVIDIA A40','count':1,'minCudaVersion':'13.0'},
        'disk':150,'image':'ghcr.io/lyj97/h3-comfy@sha256:1d9fddcced68175f6f0fd28371623a71bc9cad40cad8156e267e03c27062a22d'}
MAX_HOURLY_USD = Decimal('0.49')


def evaluate(pods, gpu):
    if pods:
        return {'state':'existing_pod','pod_count':len(pods)}
    if gpu.get('id') != SPEC['gpu']['id']:
        raise ValueError('GPU identity differs from fixed specification')
    availability=gpu.get('availability')
    if availability not in ('NONE','LOW','MEDIUM','HIGH'):
        raise ValueError('invalid availability')
    if availability=='NONE':
        return {'state':'waiting_for_stock','pod_count':0}
    versions=gpu.get('cudaVersions')
    if not isinstance(versions,list) or not any(
        item.get('available') is True and tuple(map(int,item['version'].split('.'))) >= (13,0)
        for item in versions if isinstance(item,dict)):
        return {'state':'waiting_for_stock','pod_count':0}
    price=(gpu.get('price') or {}).get('secure')
    if isinstance(price,bool) or not isinstance(price,(int,float)):
        raise ValueError('missing secure price')
    amount=Decimal(str(price))
    if not amount.is_finite() or amount<=0:
        raise ValueError('invalid secure price')
    return {'state':'stock_ready' if amount<=MAX_HOURLY_USD else 'price_above_limit',
            'pod_count':0,'availability':availability,'hourly_usd':price}


def inspect(provider):
    pods=provider.inventory()
    if pods:
        return {'state':'existing_pod','pod_count':len(pods)}
    query=urllib.parse.urlencode({'include':'AVAILABILITY','product':'POD',
        'cloud':SPEC['cloud'],'count':SPEC['gpu']['count'],
        'minCudaVersion':SPEC['gpu']['minCudaVersion']})
    gpu=provider.request_url('https://api.runpod.io/v2/catalog/gpus/' +
        urllib.parse.quote(SPEC['gpu']['id'],safe='')+'?'+query)
    return evaluate(pods,gpu)


async def watch(provider,interval,deadline,emit,*,once=False):
    failures=0
    while True:
        started=time.monotonic()
        try:
            result=await asyncio.to_thread(inspect,provider)
            failures=0
        except Exception:
            failures+=1
            result={'state':'observation_unavailable'}
        result.update(observed_at=datetime.now(timezone.utc).isoformat(),spec=SPEC,
                      mode='read_only',automatic_create=False)
        emit(result)
        if result['state'] in ('stock_ready','existing_pod') or once:
            return result['state']
        delay=min(300,interval*2**min(failures,3)) if failures else interval
        remaining=deadline-time.monotonic()
        if remaining<=0:
            return 'wait_timeout'
        await asyncio.sleep(min(remaining,max(0,delay-(time.monotonic()-started))))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--interval',type=int,default=60)
    parser.add_argument('--max-wait',type=int,default=3600)
    parser.add_argument('--once',action='store_true')
    parser.add_argument('--log',type=Path,required=True,help='append safe observations as JSONL')
    args=parser.parse_args()
    if not 60<=args.interval<=300 or not 60<=args.max_wait<=86400:
        parser.error('interval must be 60..300 seconds; max-wait 60..86400 seconds')
    provider=Provider(os.environ.get('RUNPOD_API_KEY'))
    args.log.parent.mkdir(parents=True,exist_ok=True)
    def emit(result):
        line=json.dumps(result,sort_keys=True)
        with args.log.open('a') as output:
            output.write(line+'\n');output.flush()
        print(line,flush=True)
    state=asyncio.run(watch(provider,args.interval,time.monotonic()+args.max_wait,emit,once=args.once))
    print(json.dumps({'state':state,'automatic_create':False}),flush=True)


if __name__=='__main__':main()
