"""Attempt-local state. This module grants no scheduling or publication rights.

ContextVar bindings follow asyncio child tasks and asyncio.to_thread calls.
Node admission must still inspect the journal, rather than a task's binding.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps


class AttemptRuntime:
    def __init__(self, attempt_id=None):
        self.attempt_id = attempt_id
        self.values = {}


class RuntimeField:
    """Compatibility accessor for existing Worker and runner call sites."""
    def __init__(self, name):
        self.name = name

    def __get__(self, worker, owner=None):
        if worker is None:
            return self
        try:
            return runtime_for(worker).values[self.name]
        except KeyError:
            raise AttributeError(self.name) from None

    def __set__(self, worker, value):
        runtime_for(worker).values[self.name] = value


def runtime_for(worker):
    # Also supports the legacy tests/operators that construct Worker.__new__.
    if '_runtime_binding' not in worker.__dict__:
        worker._runtime_binding = ContextVar('h3_attempt_runtime', default=None)
        worker._default_runtime = AttemptRuntime()
        worker._attempt_runtimes = {}
    return worker._runtime_binding.get() or worker._default_runtime


@contextmanager
def bind_runtime(worker, runtime):
    runtime_for(worker)
    token = worker._runtime_binding.set(runtime)
    try:
        yield runtime
    finally:
        worker._runtime_binding.reset(token)


def attempt_scoped(method):
    @wraps(method)
    async def scoped(worker, attempt_id, *args, **kwargs):
        runtime = worker._runtime_for_attempt(attempt_id)
        with bind_runtime(worker, runtime):
            return await method(worker, attempt_id, *args, **kwargs)
    return scoped
