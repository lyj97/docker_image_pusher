"""Download owner-authorized artifact bytes using the Worker's normal transport."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit

from .config import WorkerConfig
from .http import HttpClient


class Arguments(argparse.ArgumentParser):
    def error(self, message):
        # Invalid argv may contain a signed URL; never echo it.
        raise ValueError('invalid artifact-download arguments')


def download(config, args):
    if not re.fullmatch(r'task_[0-9a-f]{24}', args.task_id):
        raise ValueError('invalid task identity')
    if not re.fullmatch(r'art_[0-9a-f]{24}', args.artifact_id):
        raise ValueError('invalid artifact identity')
    if not re.fullmatch(r'[0-9a-f]{64}', args.sha256):
        raise ValueError('invalid digest')
    size, timeout = int(args.bytes), int(args.timeout_seconds)
    if not 1 <= size <= config.max_asset_bytes or not 1 <= timeout <= 300:
        raise ValueError('invalid transfer bounds')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', args.filename):
        raise ValueError('invalid filename')
    url = urlsplit(args.url)
    path = unquote(url.path).split('/')
    if (url.username or url.password or url.fragment
            or not url.path.startswith('/_storage/get/')
            or ['tasks', args.task_id] != path[5:7]
            or ['artifacts', args.artifact_id] != path[9:11]):
        raise ValueError('artifact URL identity mismatch')
    client = HttpClient(config.server_url, config.worker_token,
        config.http_connect_timeout_seconds, config.http_timeout_seconds,
        cf_access_client_id=config.cf_access_client_id,
        cf_access_client_secret=config.cf_access_client_secret,
        server_connect_ip=config.server_connect_ip)
    client._check_download_url(args.url)
    if client._origin(args.url) != client._origin(config.server_url):
        raise ValueError('artifact URL origin mismatch')
    root = Path(config.data_dir).resolve()
    for part in ['operator-artifacts', args.artifact_id]:
        root = root / part
        if root.is_symlink():
            raise ValueError('artifact directory is a symlink')
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with os.fdopen(os.open(root / '.download.lock',
            os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600), 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        target = root / args.filename
        partial = Path(str(target) + '.part')
        if target.is_symlink() or partial.is_symlink() or partial.exists():
            raise ValueError('unsafe or incomplete destination')
        cached = target.exists()
        if cached:
            if not target.is_file() or target.stat().st_size != size:
                raise ValueError('existing destination does not match')
            with target.open('rb') as source:
                if hashlib.file_digest(source, 'sha256').hexdigest() != args.sha256:
                    raise ValueError('existing destination does not match')
        else:
            client.download(args.url, str(target), args.sha256, size,
                            config.max_asset_bytes, timeout)
        return dict(task_id=args.task_id, artifact_id=args.artifact_id,
                    path=str(target), size_bytes=size, sha256=args.sha256, cached=cached)


def main(argv=None):
    parser = Arguments(description=__doc__)
    for name in ['task-id', 'artifact-id', 'url', 'sha256', 'bytes', 'filename']:
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--timeout-seconds', default='60')
    try:
        result = download(WorkerConfig.from_env(), parser.parse_args(argv))
    except Exception as error:
        print('artifact download refused (' + type(error).__name__
              + '); no verified output delivered', file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0
