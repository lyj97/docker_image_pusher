"""Pure, conservative capacity recommendations; never mutate tasks or Pods."""
from dataclasses import dataclass
import hashlib
import json
import math


def workflow_digest(request):
    """Exact workflow identity: no silent conversion of prompts, models or graphs."""
    workflow = request.get('workflow')
    if not isinstance(workflow, dict):
        return None
    raw = json.dumps(workflow, sort_keys=True, separators=(',', ':'),
                     ensure_ascii=False, allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class Profile:
    approved_profiles: frozenset
    min_tasks: int = 3
    min_wait_seconds: float = 300
    cold_start_seconds: float = 1200
    task_seconds: float = 180
    local_slots: int = 1
    local_task_seconds: float | None = None
    max_pods: int = 1
    hourly_usd: float = .49
    batch_budget_usd: float = 2

    def __post_init__(self):
        import re
        if any(not isinstance(v, str) or not re.fullmatch('[0-9a-f]{64}', v)
               for v in self.approved_profiles):
            raise ValueError('approved_profiles must contain reviewed profile SHA256 digests')
        for name in ('min_tasks', 'local_slots', 'max_pods'):
            value = getattr(self, name)
            if type(value) is not int or value < (0 if name == 'local_slots' else 1):
                raise ValueError('invalid ' + name)
        if self.max_pods != 1:
            raise ValueError('max_pods must be exactly 1, including stopped Pods')
        value = self.local_task_seconds
        if value is not None and (isinstance(value, bool)
                or not isinstance(value, (int, float)) or not math.isfinite(value)
                or value <= 0):
            raise ValueError('invalid local_task_seconds')
        for name in ('min_wait_seconds', 'cold_start_seconds', 'task_seconds',
                     'hourly_usd', 'batch_budget_usd'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) \
                    or not math.isfinite(value) or value <= 0:
                raise ValueError('invalid ' + name)


def recommend(tasks, profile, *, now, managed_pods=None, snapshot_complete=True):
    from shared.execution import admitted_profiles
    groups = {}
    for task in tasks:
        request = task['normalized_request']
        if isinstance(request, str):
            request = json.loads(request)
        if (task['mode'] == request.get('mode')
                and task['model_revision'] == request.get('model_revision')
                and not request.get('_native') and request.get('execution_target') != 'local'):
            matches = [p for p in admitted_profiles(request) if p['profile_digest'] in profile.approved_profiles]
            if len(matches) == 1:
                groups.setdefault(matches[0]['profile_digest'], []).append(task)
    selected = max(groups, key=lambda key: (len(groups[key]),
        max((now-t['created_at']).total_seconds() for t in groups[key])), default=None)
    eligible = groups.get(selected, [])
    count = len(eligible)
    oldest = max((max(0, (now - t['created_at']).total_seconds())
                  for t in eligible), default=0)
    cloud_seconds = profile.cold_start_seconds + count * profile.task_seconds
    cost = cloud_seconds / 3600 * profile.hourly_usd
    local_seconds = (math.ceil(count / profile.local_slots) * profile.local_task_seconds
                     if profile.local_slots and profile.local_task_seconds else None)
    reason = 'recommended'
    if not snapshot_complete:
        reason = 'incomplete_snapshot'
    elif type(managed_pods) is not int or managed_pods < 0:
        reason = 'unknown_pod_inventory'
    elif managed_pods >= 1:
        reason = 'pod_limit'
    elif count < profile.min_tasks:
        reason = 'insufficient_compatible_tasks'
    elif oldest < profile.min_wait_seconds:
        reason = 'wait_threshold'
    elif cost > profile.batch_budget_usd:
        reason = 'budget'
    elif profile.local_slots and profile.local_task_seconds is None:
        reason = 'unknown_local_throughput'
    elif local_seconds is not None and local_seconds <= cloud_seconds:
        reason = 'cold_start_exceeds_local_backlog'
    return {'mode': 'observe_only', 'recommendation': 'request_capacity'
            if reason == 'recommended' else 'wait', 'reason': reason,
            'eligible_tasks': count, 'selected_profile_digest': selected,
            'eligible_tasks_by_profile': {k:len(v) for k,v in groups.items()}, 'oldest_wait_seconds': oldest,
            'estimated_cloud_seconds': cloud_seconds,
            'estimated_local_seconds': local_seconds,
            'estimated_batch_usd': round(cost, 4),
            'estimate_includes': 'startup plus sequential inference; upload and retry unmeasured'}
