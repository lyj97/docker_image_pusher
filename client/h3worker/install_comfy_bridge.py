"""Bounded managed bridge reconciliation, also used before Worker registration.

Copies the reviewed packaged bundle and pins the preinstalled frontend tree.
Does not run dependencies, download, update Worker code, or restart anything.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import ctypes
import sys
from .comfy_launch import absolute_path
from .comfy_preview import private_directory


def tree_digest(root):
    result = hashlib.sha256()
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError('frontend symlink refused')
        if path.is_file():
            result.update(path.relative_to(root).as_posix().encode() + b'\0')
            result.update(hashlib.sha256(path.read_bytes()).digest())
    return result.hexdigest()


def exchange(left, right):
    """Atomic directory exchange; never use a remove/rename gap."""
    _atomic_rename(left, right, linux_flags=2, mac_flags=2)


def _atomic_rename(left, right, *, linux_flags, mac_flags):
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == 'darwin':
        result = libc.renamex_np(os.fsencode(left), os.fsencode(right), mac_flags)
    elif sys.platform == 'linux':
        result = libc.renameat2(-100, os.fsencode(left), -100, os.fsencode(right), linux_flags)
    else:
        raise ValueError('atomic bridge replacement unsupported on this platform')
    if result:
        raise OSError(ctypes.get_errno(), 'atomic bridge replacement failed')


def install(root, frontend, data, expected_frontend=None, *, before_payload_change=None):
    """Return runtime payload change, excluding manifest-only adoption.

    The startup caller can prove maintenance and record restart intent before
    publishing any new runtime bytes. Legacy adoption requires exact bytes.
    """
    if any(str(Path(value)) != value for value in (root, frontend, data)):
        raise ValueError('canonical absolute paths required; path aliases refused')
    root = absolute_path(root, directory=True)
    frontend = absolute_path(frontend, directory=True)
    nodes = absolute_path(str(root / 'custom_nodes'), directory=True)
    data = absolute_path(data, directory=True)
    digest = tree_digest(frontend)
    if expected_frontend is not None and digest != expected_frontend:
        raise ValueError('pinned frontend digest mismatch')
    ipc = private_directory(data / 'preview')
    socket = str(ipc / 'rpc.sock')
    if len(socket.encode()) > 100:
        raise ValueError('preview socket path too long')
    destination = nodes / 'h3_preview_bridge'
    source = Path(__file__).resolve().parents[1] / 'comfy_bridge_node'
    files = {name: (source / name).read_bytes() for name in ('__init__.py', 'web/bridge.js')}
    manifest = dict(managed_by='h3-preview-v1', socket=socket, frontend_sha256=digest,
                    files={name: hashlib.sha256(value).hexdigest() for name, value in files.items()})
    payload_changed = True
    had_destination = destination.exists() or destination.is_symlink()
    if had_destination:
        absolute_path(str(destination), directory=True)
        original_inode = (destination.stat().st_dev, destination.stat().st_ino)
        entries = {p.relative_to(destination).as_posix() for p in destination.rglob('*')}
        if entries != {'__init__.py', 'web', 'web/bridge.js', 'bridge.json'}:
            raise ValueError('unmanaged or tampered bridge; archive locally after review')
        for p in (destination, *destination.rglob('*')):
            mode = 0o700 if p.is_dir() else 0o600
            if p.is_symlink() or p.stat().st_uid != os.getuid() or p.stat().st_mode & 0o777 != mode:
                raise ValueError('unsafe bridge ownership or permissions')
        previous = {name: (destination / name).read_bytes() for name in (*files, 'bridge.json')}
        old = json.loads(previous['bridge.json'])
        if old.get('socket') != socket or old.get('frontend_sha256') != digest:
            raise ValueError('managed bridge configuration changed; local review required')
        if set(old) == {'socket', 'frontend_sha256'}:
            if any((destination / name).read_bytes() != value for name, value in files.items()):
                raise ValueError('modified legacy bridge refused')
            # Only metadata changes. The existing runtime already uses these
            # exact socket/frontend values and packaged payload bytes.
            fd, temporary = tempfile.mkstemp(prefix='.bridge-manifest-', dir=destination)
            try:
                with os.fdopen(fd, 'wb') as fh:
                    fh.write(json.dumps(manifest).encode())
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(temporary, destination / 'bridge.json')
                fd = os.open(destination, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
            return False
        if set(old) != set(manifest) or old.get('managed_by') != 'h3-preview-v1' or set(old.get('files', {})) != set(files):
            raise ValueError('legacy/unmanaged bridge; operator must review and archive locally')
        for name in files:
            if hashlib.sha256((destination / name).read_bytes()).hexdigest() != old['files'][name]:
                raise ValueError('tampered managed bridge refused')
        if old == manifest:
            return False
        payload_changed = any((destination / name).read_bytes() != value for name, value in files.items())
    if payload_changed and before_payload_change is not None:
        before_payload_change()
    previous_umask = os.umask(0o077)
    staging = None
    try:
        staging = Path(tempfile.mkdtemp(prefix='.h3-preview-', dir=nodes))
        (staging / 'web').mkdir(mode=0o700)
        for name, value in {**files, 'bridge.json': json.dumps(manifest).encode()}.items():
            with (staging / name).open('wb') as fh:
                fh.write(value)
                fh.flush()
                os.fsync(fh.fileno())
            os.chmod(staging / name, 0o600)
        for directory in (staging / 'web', staging):
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        if had_destination:
            absolute_path(str(destination), directory=True)
            if ((destination.stat().st_dev, destination.stat().st_ino) != original_inode
                    or any((destination / name).is_symlink() or
                           (destination / name).read_bytes() != value for name, value in previous.items())
                    or any(p.is_symlink() or p.stat().st_uid != os.getuid() or
                           p.stat().st_mode & 0o777 != (0o700 if p.is_dir() else 0o600)
                           for p in (destination, *destination.rglob('*')))
                    or {p.relative_to(destination).as_posix() for p in destination.rglob('*')} != entries):
                raise ValueError('managed bridge changed during reconciliation')
            exchange(staging, destination)
        else:
            # Never replace even an empty operator directory that appeared
            # during the maintenance/launcher proof window.
            _atomic_rename(staging, destination, linux_flags=1, mac_flags=4)
        fd = os.open(nodes, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return payload_changed
    finally:
        if staging is not None and staging.exists():
            shutil.rmtree(staging)
        os.umask(previous_umask)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--frontend', required=True)
    parser.add_argument('--data-dir', required=True)
    args = parser.parse_args()
    install(args.root, args.frontend, args.data_dir)


if __name__ == '__main__':
    main()
