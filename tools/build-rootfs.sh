#!/usr/bin/env bash
# Build a minimal ext4 rootfs image for the Firecracker backend.
#
# The image contains:
#   - /bin/sh, /usr/bin/python3, socat, nc
#   - the agentscope guest agent at /root/.agentscope/_guest_agent.py
#   - an /etc/rc.local entry that starts the guest agent on boot
#
# Usage:
#   sudo tools/build-rootfs.sh /var/lib/firecracker/rootfs.ext4
#
# Requires: debootstrap, e2fsprogs, qemu-nbd (or qemu-tools), sudo, python3.
# Tested on Debian 12 / Ubuntu 22.04 hosts.
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <output.ext4> [size-mib]" >&2
    exit 64
fi

OUTPUT="${1:-/var/lib/firecracker/rootfs.ext4}"
SIZE_MIB="${2:-512}"
SUITE="${SUITE:-bookworm}"
MIRROR="${MIRROR:-http://deb.debian.org/debian}"

# Locate the guest agent source bundled in this package.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
GUEST_AGENT_SRC="$(python3 - <<PY
from pathlib import Path
import sys
sys.path.insert(0, "${PROJECT_ROOT}/src")
try:
    from agentscope_sandbox_ext._firecracker._guest_agent import GUEST_AGENT_SOURCE
except Exception as e:
    print(f"ERR: cannot import GUEST_AGENT_SOURCE: {e}", file=sys.stderr)
    sys.exit(1)
out = Path("/tmp/_agentscope_guest_agent.py")
out.write_text(GUEST_AGENT_SOURCE, encoding="utf-8")
print(out)
PY
)"

if [[ ! -f "${GUEST_AGENT_SRC}" ]]; then
    echo "ERR: guest agent source not written" >&2
    exit 1
fi

echo "==> Building ${SIZE_MIB} MiB ext4 image at ${OUTPUT}"

# 1. Create the ext4 image.
truncate -s "${SIZE_MIB}M" "${OUTPUT}"
mkfs.ext4 -F -L agentscope-rootfs "${OUTPUT}"

# 2. Mount it via a temporary directory.
MNT="$(mktemp -d /tmp/agentscope-rootfs.XXXXXX)"
trap 'umount "${MNT}" 2>/dev/null || true; rm -rf "${MNT}" "${GUEST_AGENT_SRC}"' EXIT
mount -o loop "${OUTPUT}" "${MNT}"

# 3. Bootstrap a minimal Debian.
debootstrap --variant=minbase --include=python3,socat,netcat-openbsd,init \
    "${SUITE}" "${MNT}" "${MIRROR}"

# 4. Install the agentscope guest agent.
install -d -m 0755 "${MNT}/root/.agentscope"
install -m 0755 "${GUEST_AGENT_SRC}" "${MNT}/root/.agentscope/_guest_agent.py"

# 5. Add an init entry (rc.local) that starts the guest agent on boot.
RC_LOCAL="${MNT}/etc/rc.local"
cat > "${RC_LOCAL}" <<'EOF'
#!/bin/sh
# Start the agentscope Firecracker guest agent on boot.
mkdir -p /var/log/agentscope
python3 /root/.agentscope/_guest_agent.py \
    >> /var/log/agentscope/guest_agent.log 2>&1 &
exit 0
EOF
chmod 0755 "${RC_LOCAL}"

# 6. Set hostname.
echo "agentscope-fc" > "${MNT}/etc/hostname"

# 7. Unmount.
sync
umount "${MNT}"

echo "==> rootfs built: ${OUTPUT}"
echo "    Verify with: file ${OUTPUT}"
