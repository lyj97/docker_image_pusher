"""Node-local, runtime-only ComfyUI launcher. Never provisions resources."""
from __future__ import annotations

import argparse
import os
import stat
import sys
from pathlib import Path


def absolute_path(
    value: str, *, directory: bool = False, executable: bool = False,
) -> Path:
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('explicit absolute path required')
    # Python virtualenv executables may be symlinks; roots must never be aliases.
    check = path.parent if executable else path
    if any(p.is_symlink() for p in (check, *check.parents)):
        raise ValueError('symlinked roots are forbidden')
    if directory and (not path.is_dir() or len(path.parts) < 4 or path == Path.home()):
        raise ValueError('dedicated, existing directory required')
    if executable and (not path.is_file() or not os.access(path, os.X_OK)):
        raise ValueError('executable is missing')
    return path


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--python', required=True)
    parser.add_argument('--frontend', required=True)
    parser.add_argument('--port', type=int, default=8188)
    parser.add_argument('--tool-bin', action='append', default=[],
                        help='Explicit trusted tool directory (for FFmpeg, for example)')
    parser.add_argument('--no-keep-awake', action='store_true')
    args = parser.parse_args(argv)
    validate(args)
    return args


def validate(args: argparse.Namespace) -> None:
    root = absolute_path(args.root, directory=True)
    absolute_path(args.python, executable=True)
    absolute_path(args.frontend, directory=True)
    for value in getattr(args, 'tool_bin', []):
        if os.pathsep in value:
            raise ValueError('tool directory must be one PATH component')
        path = absolute_path(value, directory=True)
        info = path.stat()
        if info.st_uid not in {0, os.getuid()} or stat.S_IMODE(info.st_mode) & 0o022:
            raise ValueError('tool directory must be operator-owned and not writable by others')
    if not 1024 <= args.port <= 65535:
        raise ValueError('port must be in 1024..65535')
    for name in ('main.py', 'input', 'output', 'custom_nodes'):
        path = absolute_path(str(root / name))
        if not (path.is_file() if name == "main.py" else path.is_dir()):
            raise ValueError('missing ComfyUI prerequisite: ' + name)
    # No universal installer-disable flag exists in 0.37.0. Built-in manager
    # stays disabled (no --enable-manager); refuse the legacy manager too.
    manager_names = {'comfyui-manager', 'comfyui_manager'}
    if any(p.name.casefold() in manager_names
           for p in (root / 'custom_nodes').iterdir()):
        raise ValueError('runtime managers must be removed by the operator')


def launch_spec(
    args: argparse.Namespace, inherited: dict[str, str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    validate(args)
    inherited = os.environ if inherited is None else inherited
    env = {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin',
           'HOME': args.root, 'LANG': 'en_US.UTF-8',
           'PYTHONNOUSERSITE': '1', 'PYTHONUNBUFFERED': '1',
           'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONHASHSEED': '0',
           # Quantized H3 uses integer matmul not yet implemented by MPS.
           'PYTORCH_ENABLE_MPS_FALLBACK': '1',
           'PIP_NO_INDEX': '1', 'PIP_DISABLE_PIP_VERSION_CHECK': '1',
           'HF_HUB_OFFLINE': '1', 'TRANSFORMERS_OFFLINE': '1'}
    tools = getattr(args, 'tool_bin', [])
    if tools:
        env['PATH'] += ':' + ':'.join(dict.fromkeys(tools))
    command = [args.python, '-s', '-B', '-u', str(Path(args.root) / 'main.py'),
               '--listen', '127.0.0.1', '--port', str(args.port),
               '--disable-auto-launch', '--disable-api-nodes', '--cache-none',
               '--front-end-root', args.frontend]
    if inherited.get('H3COMFY_CAFFEINATED') == '1':
        env['H3COMFY_CAFFEINATED'] = '1'
    elif not args.no_keep_awake:
        env['H3COMFY_CAFFEINATED'] = '1'
        command = ['/usr/bin/caffeinate', '-i', *command]
    return command, env


def main() -> None:
    if sys.platform != 'darwin':
        raise SystemExit('ComfyUI LaunchAgent runtime requires macOS')
    args = arguments()
    command, env = launch_spec(args)
    if not os.access(command[0], os.X_OK):
        raise SystemExit('launcher executable missing')
    os.umask(0o077)
    os.chdir(args.root)
    os.execve(command[0], command, env)


if __name__ == '__main__':
    main()
