"""Bounded local preparation and controlled-update failure reporting."""
import json
import os
import tempfile
import hashlib
import argparse
from pathlib import Path

from .config import WorkerConfig


class PreparationError(RuntimeError):
    """A fixed, safe stage name, never an underlying exception/workflow."""


def _private_path(path):
    from .comfy_launch import absolute_path
    # Anchor relative paths without resolving and hiding symlink aliases.
    return absolute_path(str(Path(path).absolute()))


def _write_private(path, value):
    path = _private_path(path)
    if path.exists() and (not path.is_file() or path.stat().st_uid != os.getuid()
                          or path.stat().st_mode & 0o022):
        raise ValueError('unsafe preparation marker')
    fd, temporary = tempfile.mkstemp(prefix='.startup-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as fh:
            json.dump(value, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _read_reload(path, identity):
    path = _private_path(path)
    if (not path.is_file() or path.stat().st_uid != os.getuid()
            or path.stat().st_mode & 0o777 != 0o600):
        raise ValueError('unsafe reload receipt')
    intent = json.loads(path.read_text())
    if (set(intent) != set(identity) | {'old_pid', 'attempted'}
            or any(intent.get(key) != value for key, value in identity.items())
            or type(intent.get('old_pid')) is not int or intent['old_pid'] <= 0
            or type(intent.get('attempted')) is not bool):
        raise ValueError('reload receipt does not match configured bundle')
    return intent


def prepare_preview(worker):
    """Reconcile once, restart only changed runtime bytes, prove loaded health.

    Startup has no producers: no registration, claims or control tasks exist.
    The existing instance lock and maintenance gate protect local/runtime work.
    A private intent survives interruption between file publication and reload.
    """
    config = worker.config
    if not config.comfyui_preview_enabled:
        return False
    from . import comfy_admin as admin, comfy_runner as runner
    from .comfy_preview import available
    from .install_comfy_bridge import install
    source = Path(__file__).resolve().parents[1] / 'comfy_bridge_node'
    bundle = hashlib.sha256((source / '__init__.py').read_bytes() +
                            (source / 'web/bridge.js').read_bytes()).hexdigest()
    receipt = _private_path(Path(config.data_dir) / 'preview-reload.json')
    identity = dict(bundle=bundle, root=config.comfyui_root,
                    frontend=config.comfyui_frontend_root,
                    frontend_sha256=config.comfyui_frontend_sha256)
    stage = 'runtime version/instance proof'
    try:
        runner.runtime(worker)
        intent = _read_reload(receipt, identity) if receipt.exists() or receipt.is_symlink() else None

        def before_change():
            nonlocal stage, intent
            stage = 'maintenance safety'
            if not Path(config.update_marker_path).is_file():
                raise RuntimeError('automatic payload replacement requires a controlled update marker')
            runner.maintenance_preflight(config, config.journal_path)
            stage = 'dedicated LaunchAgent proof'
            job, launchctl, _, _ = admin.preview_launch_agent(config)
            old_pid = admin.preview_job_pid(config, launchctl, job)
            if intent is None:
                intent = dict(identity, old_pid=old_pid, attempted=False)
                _write_private(receipt, intent)

        stage = 'managed bridge reconciliation'
        changed = install(config.comfyui_root, config.comfyui_frontend_root,
                          config.data_dir, config.comfyui_frontend_sha256,
                          before_payload_change=before_change)
        if intent is not None:
            stage = 'maintenance safety'
            runner.maintenance_preflight(config, config.journal_path)
            stage = 'dedicated LaunchAgent proof'
            job, launchctl, _, _ = admin.preview_launch_agent(config)
            pid = admin.preview_job_pid(config, launchctl, job)

            def loaded():
                if not available(worker):
                    raise RuntimeError('matching preview status not loaded')

            if pid != intent['old_pid'] and available(worker):
                # May have restarted successfully before a startup interruption.
                stage = 'loaded bridge readiness'
                admin.verify(config, argparse.Namespace(class_name=[], model=[]))
                loaded()
            elif intent['attempted']:
                raise PreparationError('automatic restart already attempted; local runtime recovery required')
            else:
                stage = 'bounded dedicated restart/readiness'
                # Re-prove safety immediately before the only lifecycle action.
                runner.maintenance_preflight(config, config.journal_path)
                intent['old_pid'] = pid
                intent['attempted'] = True
                _write_private(receipt, intent)
                admin._restart_runtime(config, argparse.Namespace(class_name=[], model=[]),
                    pid_reader=lambda ctl, name: admin.preview_job_pid(config, ctl, name),
                    readiness=loaded, expected_old_pid=pid)
            receipt.unlink()
        stage = 'loaded bridge readiness'
        if not available(worker):
            raise RuntimeError('matching preview status not loaded')
        return changed
    except PreparationError:
        raise
    except Exception:
        raise PreparationError('Preview startup failed: ' + stage +
            '; inspect dedicated ComfyUI configuration/runtime locally before retrying update') from None


def report_failure(config, reason=None):
    """Keep marker until the Server acknowledges failure; never register ready."""
    path = _private_path(config.update_marker_path)
    if not path.is_file():
        return
    if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o022:
        raise ValueError('unsafe update marker')
    marker = json.loads(path.read_text())
    marker['startup_failed'] = True
    message = str(reason) if isinstance(reason, PreparationError) else marker.get(
        'startup_failure_reason', 'Startup preparation/readiness failed; inspect local startup log before retrying update')
    marker['startup_failure_reason'] = message[:500]
    _write_private(path, marker)
    # A fixed message cannot leak paths, tokens, or workflow data.
    from .http import HttpClient
    http = HttpClient(config.server_url, config.worker_token,
                      config.http_connect_timeout_seconds, config.http_timeout_seconds,
                      cf_access_client_id=config.cf_access_client_id,
                      cf_access_client_secret=config.cf_access_client_secret,
                      server_connect_ip=config.server_connect_ip)
    http.worker_update_status(config.worker_id, marker['request_id'], 'failed',
        marker['startup_failure_reason'])
    path.unlink()


def main():
    try:
        config = WorkerConfig.from_env()
    except Exception:
        # Reporting must remain possible when new capability config is invalid.
        # Load only transport/marker fields, never run with this partial config.
        from .config import _load_config_file
        from types import SimpleNamespace
        defaults = WorkerConfig()
        file_values = _load_config_file(os.environ.get('H3WORKER_CONFIG'))
        values = {}
        for name in ('server_url', 'server_connect_ip', 'worker_token', 'worker_id', 'data_dir',
                     'cf_access_client_id', 'cf_access_client_secret',
                     'http_connect_timeout_seconds', 'http_timeout_seconds'):
            value = os.environ.get('H3WORKER_' + name.upper(),
                                   file_values.get(name, getattr(defaults, name)))
            values[name] = float(value) if name.endswith('_seconds') else value
        config = SimpleNamespace(**values)
        config.update_marker_path = str(Path(config.data_dir) / 'update-complete.json')
        report_failure(config)
        raise SystemExit('Invalid startup configuration; review preview frontend pin locally') from None
    report_failure(config)


if __name__ == '__main__':
    main()
