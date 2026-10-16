#!/bin/bash
# Host-side updater for baldrick-monitor, run by baldrick-monitor-update.service
# when the dashboard requests an update. It pulls the image named in the
# Quadlet (the request itself carries no data), restarts the service, and
# rolls back to the previous image if the new one fails its health check.
set -uo pipefail

QUADLET=${QUADLET:-/etc/containers/systemd/baldrick-monitor.container}
SERVICE=${SERVICE:-baldrick-monitor.service}
HEALTH_URL=${HEALTH_URL:-http://127.0.0.1:8420/healthz}
DIR=${UPDATE_DIR:-/var/lib/baldrick-monitor/update}

IMAGE=$(sed -n 's/^Image=//p' "$QUADLET" | head -n1)

status() { # state message
	local msg=${2//[\"\\]/}
	printf '{"state":"%s","message":"%s","time":"%s","image":"%s"}\n' \
		"$1" "$msg" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$IMAGE" >"$DIR/status.json.tmp" &&
		mv -f "$DIR/status.json.tmp" "$DIR/status.json"
	echo "$1: $2"
}

rm -f "$DIR/request"
if [ -z "$IMAGE" ]; then
	status failed "No Image= line in $QUADLET"
	exit 1
fi

status running "Pulling $IMAGE"
old=$(podman image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null || true)
if ! err=$(podman pull -q "$IMAGE" 2>&1 >/dev/null); then
	status failed "Pull failed: $(echo "$err" | tail -c 300 | tr '\n' ' ')"
	exit 1
fi
new=$(podman image inspect --format '{{.Id}}' "$IMAGE")
if [ "$old" = "$new" ]; then
	status done "Already up to date"
	exit 0
fi

status running "Restarting with the new image"
systemctl restart "$SERVICE"
for _ in $(seq 1 45); do
	sleep 2
	if curl -fsS -m 2 "$HEALTH_URL" >/dev/null 2>&1; then
		status done "Updated"
		# Drop only the image we replaced; leave every other image alone.
		[ -n "$old" ] && podman rmi "$old" >/dev/null 2>&1
		exit 0
	fi
done

if [ -n "$old" ]; then
	podman tag "$old" "$IMAGE"
	systemctl restart "$SERVICE"
	status failed "The new version failed its health check, so the previous version was restored"
else
	status failed "The new version failed its health check"
fi
exit 1
