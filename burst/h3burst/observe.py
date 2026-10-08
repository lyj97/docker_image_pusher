"""Read-only queue observation using the scheduler's eligibility predicates."""
import argparse
import asyncio
import json
import os
import sys

from .policy import Profile, recommend


def candidate_sql(claim_sql):
    # Remove ordering/locking, retain every scheduler predicate. Fail if its
    # shape changes instead of accidentally running SELECT FOR UPDATE.
    query, separator, tail = claim_sql.partition('\n ORDER BY')
    if not separator or 'FOR UPDATE' not in tail or 'FOR UPDATE' in query:
        raise ValueError('claim SQL shape changed; review burst observation')
    return query + '\n ORDER BY priority DESC, created_at LIMIT $4'


async def snapshot(dsn, tenant, limit):
    import asyncpg
    from h3server.api_claim import BASE_CLAIMABLE_TASK_SQL
    conn = await asyncpg.connect(dsn, timeout=10)
    try:
        async with conn.transaction(isolation='repeatable_read', readonly=True):
            await conn.execute("SET LOCAL statement_timeout = '10s'")
            now = await conn.fetchval('SELECT now()')
            tasks = await conn.fetch(candidate_sql(BASE_CLAIMABLE_TASK_SQL), tenant,
                                     ['comfyui.video.v1', 'installed-model-revision'], ['comfyui_video', 'a2va'], limit + 1)
            return tasks[:limit], now, len(tasks) <= limit
    finally:
        await conn.close()


async def run(args):
    profile = Profile(**json.loads(args.profile.read_text()))
    dsn = os.environ.get('H3BURST_READONLY_DSN')
    if not dsn:
        raise ValueError('H3BURST_READONLY_DSN is required')
    from .control import Provider
    provider = Provider(os.environ.get('RUNPOD_API_KEY'))
    while True:
        tasks, now, complete = await snapshot(dsn, args.tenant, args.limit)
        try:
            pod_count = len(await asyncio.to_thread(provider.inventory))
        except Exception:
            pod_count = None
        result = recommend(tasks, profile, now=now,
                           managed_pods=pod_count,
                           snapshot_complete=complete)
        result['observed_at'] = now.isoformat()
        print(json.dumps(result), flush=True)  # no graphs, prompts or identities
        if not args.interval:
            return
        await asyncio.sleep(args.interval)


def main():
    from pathlib import Path
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', type=Path, required=True)
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--limit', type=int, default=1000)
    parser.add_argument('--interval', type=float, default=0)
    args = parser.parse_args()
    if not 1 <= args.limit <= 10000 \
            or args.interval < 0 or not __import__('math').isfinite(args.interval):
        parser.error('invalid limit or interval')
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except Exception:
        # Database exceptions can embed the DSN. Never serialize the exception.
        print('Observation failed; no actions taken. Check connection/profile.', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
