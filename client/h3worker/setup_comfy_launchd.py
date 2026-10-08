"""Install a standalone launcher and generate its per-user ComfyUI LaunchAgent."""
from __future__ import annotations

import argparse
import os
import plistlib
import stat
import tempfile
from pathlib import Path

if __package__:
    from .comfy_launch import absolute_path, arguments, validate
else:
    from comfy_launch import absolute_path, arguments, validate


def publish_private(path: Path, content: bytes, *, reuse_identical: bool) -> None:
    """Atomically publish mode 0600 without replacing an existing file."""
    absolute_path(str(path))
    fd, temporary = tempfile.mkstemp(prefix='.h3comfy-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if not reuse_identical:
                raise
            with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as old:
                info = os.fstat(old.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                        or stat.S_IMODE(info.st_mode) != 0o600
                        or old.read(len(content) + 1) != content):
                    raise FileExistsError(
                        'existing launcher differs or is unsafe; explicit operator '
                        'replacement/migration required'
                    )
    finally:
        os.unlink(temporary)


def generate(args: argparse.Namespace, home: Path | None = None) -> Path:
    validate(args)
    home = Path.home() if home is None else Path(home)
    absolute_path(str(home))
    if not home.is_dir() or home.stat().st_uid != os.getuid():
        raise ValueError('home must be an existing user-owned directory')
    source = absolute_path(str(Path(__file__).with_name('comfy_launch.py').absolute()))
    agents = home / 'Library' / 'LaunchAgents'
    logs = home / 'Library' / 'Logs' / 'H3ComfyUI'
    support = home / 'Library' / 'Application Support' / 'H3ComfyUI'
    for path in (agents, logs, support):
        absolute_path(str(path))
    plist = agents / 'com.relife.h3comfyui.plist'
    if plist.exists() or plist.is_symlink():
        raise FileExistsError('existing ComfyUI plist must be explicitly archived first')
    for path in (agents, logs, support):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = path.stat()
        if info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise ValueError('LaunchAgent directories must be user-owned and private')
    launcher = support / 'comfy_launch.py'
    publish_private(launcher, source.read_bytes(), reuse_identical=True)
    command = [args.python, '-I', '-B', str(launcher), '--root', args.root,
               '--python', args.python, '--frontend', args.frontend,
               '--port', str(args.port)]
    if args.no_keep_awake:
        command.append('--no-keep-awake')
    for path in getattr(args, 'tool_bin', []):
        command.extend(['--tool-bin', path])
    payload = {
        'Label': 'com.relife.h3comfyui',
        'ProgramArguments': command,
        'WorkingDirectory': args.root,
        'RunAtLoad': True,
        'KeepAlive': True,
        'ThrottleInterval': 60,
        'ExitTimeOut': 40,
        'ProcessType': 'Interactive',
        'Umask': 0o077,
        'EnvironmentVariables': {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin'},
        'StandardOutPath': str(logs / 'comfyui.stdout.log'),
        'StandardErrorPath': str(logs / 'comfyui.stderr.log'),
    }
    publish_private(plist, plistlib.dumps(payload), reuse_identical=False)
    return plist


if __name__ == '__main__':
    print(generate(arguments()))
