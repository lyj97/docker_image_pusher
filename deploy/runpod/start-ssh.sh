#!/bin/sh
set -eu
: "${PUBLIC_KEY:?RunPod SSH public key must be injected at creation}"
install -d -m 700 /root/.ssh
install -d -m 755 /run/sshd
umask 077
printf '%s\n' "$PUBLIC_KEY" > /root/.ssh/authorized_keys
ssh-keygen -l -f /root/.ssh/authorized_keys >/dev/null
# Generate host keys only at boot, never publish shared private keys.
ssh-keygen -A
/usr/sbin/sshd -e -o PasswordAuthentication=no -o KbdInteractiveAuthentication=no \
    -o PermitRootLogin=prohibit-password -o AllowTcpForwarding=local \
    -o GatewayPorts=no -o PermitOpen=127.0.0.1:8190 -o X11Forwarding=no
exec python3 -B -m h3burst.engine
