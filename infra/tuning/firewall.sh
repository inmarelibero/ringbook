#!/usr/bin/env bash
# Denies all inbound traffic except SSH and web traffic. Run as root, after
# confirming that SSH key login works.
set -euo pipefail

if grep -Eq '^\s*PasswordAuthentication\s+yes' /etc/ssh/sshd_config; then
    echo "Refusing to continue: SSH still allows password login." >&2
    exit 1
fi

ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable
ufw status verbose

cat <<'EOF'

Docker publishes ports through its own firewall rules, so keep services off the
public interface in the compose file: only the web service publishes a public
port, and Grafana is bound to 127.0.0.1.
EOF
