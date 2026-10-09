"""Prompt-scoped audio subprocess ownership, under the same durable Pod intent."""
import json
import os
import signal
import shutil
import time
import uuid
from pathlib import Path
import subprocess

from .prepare import atomic_json, safe_root
from .pod import Refused


class AudioProcesses:
    def __init__(self, root, models_root, output_root):
        self.root, self.models_root, self.output_root = Path(root), Path(models_root), Path(output_root)
        self.jobs = {}

    def empty(self):
        return all(self.quiescent(item) for item in self.jobs.values())

    def quiescent(self, item):
        if item.get('group_gone'):return True
        process=item['process']
        if process.poll() is None:return False
        return self.settle_group(item)

    def group_exists(self, item):
        if item.get('group_gone'):return False
        item['process'].poll()  # Reap the parent, independently of its children.
        try:os.killpg(item['process'].pid,0)
        except ProcessLookupError:
            item['group_gone']=True
            item['log'].close()
            return False
        return True

    def settle_group(self, item):
        # Wait for the group, not parent.wait(): an exited parent cannot report
        # children which ignored TERM. Never signal a group again after it is gone.
        if not self.group_exists(item):return True
        for sig in (signal.SIGTERM,signal.SIGKILL):
            try:os.killpg(item['process'].pid,sig)
            except ProcessLookupError:
                item['group_gone']=True;item['log'].close();return True
            deadline=time.monotonic()+5
            while self.group_exists(item):
                if time.monotonic()>=deadline:break
                time.sleep(0.05)
            if item.get('group_gone'):return True
        return False  # Ownership stays fenced if kernel termination is unproven.

    def submit(self, prompt, job):
        root = self.root / prompt
        safe_root(root)
        if root.exists():raise Refused('audio_identity_uncertain')
        root.mkdir(mode=0o700, parents=True)
        output = self.output_root / ('h3_'+prompt.replace('-','')) / (
            'alignment.json' if job['task']['mode']=='align' else 'result.wav')
        safe_root(output)
        job = dict(job, output=str(output), models_root=str(self.models_root))
        atomic_json(root/'job.json',job)
        from shared.execution import profile_by_id
        runtime = profile_by_id(job['profile_id'])['audio_runtime']
        log = (root/'runner.log').open('ab',buffering=0)
        try:
            process = subprocess.Popen(['/opt/h3-audio/'+runtime+'/bin/python','-B','-m',
                'h3burst.audio_runner','--job',str(root/'job.json')],
                stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        except Exception:
            log.close()
            # No child was created: definite rejection, not an uncertain execution.
            raise Refused('prompt_rejected') from None
        self.jobs[prompt] = dict(process=process, log=log, root=root, output=output, cancelled=False)

    def observe(self, prompt):
        item = self.jobs.get(prompt)
        if item is None:return 'uncertain', None
        if not self.quiescent(item):return 'running', None
        if item['cancelled']:return 'cancelled', None
        path = item['root']/'outcome.json'
        if not path.is_file():return 'failed', None
        outcome = json.loads(path.read_text())
        if outcome.get('state')=='completed' and item['process'].returncode == 0:
            return 'completed', item['output']
        return 'failed', None

    def cancel(self, prompt):
        item = self.jobs.get(prompt)
        if item is None:raise Refused('audio_identity_uncertain')
        if item['process'].poll() is None:
            item['cancelled'] = True
        if not self.settle_group(item):raise Refused('audio_group_stop_unproven')

    def release(self, prompt):
        # Called only after Pod's durable record proves terminal. No live handle
        # is expected after a definite Popen rejection or terminal journal recovery.
        if str(uuid.UUID(prompt))!=prompt:raise Refused('unsafe_release_path')
        item=self.jobs.get(prompt)
        if item is not None and not self.quiescent(item):raise Refused('audio_group_stop_unproven')
        root=self.root/prompt;safe_root(root)
        if any(p.is_symlink() for p in root.rglob('*')):raise Refused('unsafe_release_path')
        if root.exists():shutil.rmtree(root)
        self.jobs.pop(prompt,None)

    def close(self):
        for prompt,item in self.jobs.items():
            self.cancel(prompt)
            item['log'].close()
