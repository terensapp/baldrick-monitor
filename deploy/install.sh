#!/bin/bash
# Installs (or re-installs) baldrick-monitor on a Podman host as a Quadlet,
# with the host-side updater behind the dashboard's "Update now" button.
#
#   sudo ./deploy/install.sh               # tracks :main (every push)
#   sudo ./deploy/install.sh --tag latest  # tracks releases only
set -euo pipefail

TAG=main
while [ $# -gt 0 ]; do
	case "$1" in
	--tag) TAG=$2; shift 2 ;;
	*) echo "usage: $0 [--tag main|latest|X.Y]" >&2; exit 2 ;;
	esac
done
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }

IMAGE=ghcr.io/terensapp/baldrick-monitor:$TAG
HERE=$(cd "$(dirname "$0")" && pwd)
TZ_NAME=$(timedatectl show -p Timezone --value 2>/dev/null || true)
TZ_NAME=${TZ_NAME:-UTC}

install -d /etc/baldrick-monitor /var/lib/baldrick-monitor/update /usr/local/libexec
[ -f /etc/baldrick-monitor/config.json ] || install -m 644 "$HERE/../config.example.json" /etc/baldrick-monitor/config.json

sed -e "s|@IMAGE@|$IMAGE|g" -e "s|@TZ@|$TZ_NAME|g" "$HERE/baldrick-monitor.container" \
	>/etc/containers/systemd/baldrick-monitor.container
install -m 755 "$HERE/update.sh" /usr/local/libexec/baldrick-monitor-update
install -m 644 "$HERE/baldrick-monitor-update.service" "$HERE/baldrick-monitor-update.path" /etc/systemd/system/
echo 1 >/var/lib/baldrick-monitor/update/installed

podman pull "$IMAGE"
systemctl daemon-reload
systemctl enable --now baldrick-monitor-update.path
systemctl restart baldrick-monitor.service

if command -v firewall-cmd >/dev/null && firewall-cmd -q --state 2>/dev/null; then
	firewall-cmd -q --permanent --add-port=8420/tcp && firewall-cmd -q --reload
fi
echo "baldrick-monitor installed from $IMAGE (timezone $TZ_NAME)"
