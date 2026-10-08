"""Disposable container SSH acceptance; no models, GPU, or provider calls."""
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time

with tempfile.TemporaryDirectory() as folder:
    root = Path(folder)
    key = root / 'client'
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
    # Replace only the final engine command so startup cannot download models.
    (root / 'python3').write_text('#!/bin/sh\nexec sleep 60\n')
    (root / 'python3').chmod(0o700)
    env = dict(os.environ, PUBLIC_KEY=key.with_suffix('.pub').read_text(),
               PATH=str(root) + ':' + os.environ['PATH'])
    service = subprocess.Popen(['/bin/sh', '/opt/h3-service/start-ssh.sh'], env=env)
    tunnel = None
    backend = socket.socket()
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(('127.0.0.1', 22), timeout=1) as stream:
                    assert stream.recv(256).startswith(b'SSH-2.0-')
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.1)
        command = ['ssh', '-i', str(key), '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
                   '-o', 'StrictHostKeyChecking=accept-new', '-o', 'UserKnownHostsFile=' + str(root/'hosts')]
        result = subprocess.check_output(command + ['root@127.0.0.1', 'printf H3_SSH_OK'], timeout=10)
        assert result == b'H3_SSH_OK'
        backend.bind(('127.0.0.1', 8190)); backend.listen(1)
        def respond():
            connection, _ = backend.accept()
            with connection:
                connection.sendall(b'H3_TUNNEL_OK')
        threading.Thread(target=respond, daemon=True).start()
        tunnel = subprocess.Popen(command + ['-o', 'ExitOnForwardFailure=yes', '-N',
            '-L', '127.0.0.1:18790:127.0.0.1:8190', 'root@127.0.0.1'])
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(('127.0.0.1', 18790), timeout=2) as stream:
                    assert stream.recv(256) == b'H3_TUNNEL_OK'
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.1)
        print('SSH public-key login and loopback task tunnel passed; no GPU/model work')
    finally:
        for process in (tunnel, service):
            if process is not None:
                process.terminate(); process.wait(timeout=5)
        backend.close()
