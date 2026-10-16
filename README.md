# baldrick-monitor

Monitors [Baldrick Board](https://baldrickboard.com) pixel controllers. It checks
that each board is alive, serving HTTP, and processing DDP, and tracks latency,
jitter and board health. It serves a web dashboard, a JSON API and Prometheus
metrics. It is a single static Go binary with no dependencies.

## What it checks

| Check | How | Kind |
|---|---|---|
| Reachability, latency, jitter, loss | ICMP echo | polled (`intervals.icmp`, default 1s) |
| HTTP liveness | `GET /ping` → 200 + JSON | polled (`intervals.http_ping`, 5s) |
| Web UI served | `GET /` → 200 `text/html` | polled (`intervals.web_ui`, 1m) |
| State API | `GET /system_state` → 200 + JSON with `uptime` | polled (`intervals.system_state`, 15s) |
| Config (hostname, ports/models) | `GET /settings` | polled (`intervals.settings`, 5m) |
| Board state (uptime, temp, RAM, OOM, firmware, frame rate, stream counters, FPP peers) | WebSocket `/ws` (subprotocol `baldrick`); the board pushes deltas, and we send the same `{}` keepalive as its UI | streamed, with the `/system_state` poll as a backstop |
| DDP processing | see below | polled (`intervals.ddp_probe`, 1m) |

Every value on the dashboard has a hover tooltip. It says whether the value is
polled (↻) or streamed (≋), how often it is collected, and when it last changed
and how: WebSocket push or poll.

### DDP check

The board doesn't answer DDP status/config queries. Its own counters in
`system_state.streaming` (`ddp_recvd`, `ddp_dropped`, `ddp_sources`) are the
source of truth. They cover ~5-second windows, reset when a stream stops,
and ignore a lone packet. `ddp_dropped` counts DDP sequence-number gaps.

`ddp_probe.mode`:

- **`auto`** (default): if another source (e.g. FPP) is already streaming, it
  checks that stream passively and injects nothing. Otherwise it sends a short
  burst (`packets` at `rate`/s) of black frames to DDP byte offset `offset`
  (default 1,000,000), past every configured channel, so no pixel changes. It
  then polls `/system_state` until the board counts them.
- **`always`**: injects even while another source is streaming.
- **`off`**: never injects. Stream counters are still shown.

Set `ddp_probe` on a single board to override the mode for that board.

### Health

- **down**: `http_fail_down` consecutive `/ping` failures, or no ICMP replies.
- **degraded** if any of:
  - ICMP loss or p95 RTT over threshold;
  - an HTTP endpoint failing;
  - the WebSocket is disconnected;
  - the board state is stale;
  - the last DDP check failed;
  - DDP loss over threshold on an active stream;
  - temperature, the RAM low-water mark, an OOM count, or board UI messages.

Events are logged when any of these happen:

- a reboot, detected by uptime going backwards;
- a firmware or settings change;
- an HTTP outage or recovery;
- a WebSocket drop;
- a DDP check failure or recovery.

## History

Every `history.resolution` (default 1m), the monitor rolls up the current
stats and appends them to `<data_dir>/<board>/samples-YYYY-MM-DD.jsonl`.
Events go to `events-YYYY-MM-DD.jsonl`. Files older than
`history.retention_days` (default 7) are deleted. History is reloaded on
restart. Without a `data_dir`, history is kept in memory only.

## Running

```bash
go run . -boards 192.168.6.4            # quick look, defaults
go run . -config config.json            # see config.example.json
```

The dashboard is at `http://host:8420/`. Other endpoints:

- `GET /metrics`: Prometheus metrics (`baldrick_*`)
- `GET /api/boards`: live state of every board
- `GET /api/boards/{name}/history?range=24h`
- `POST /api/boards/{name}/probe`: run a DDP check now
- `GET /api/meta`: intervals, retention, and how each value is collected
- `GET /healthz`

ICMP needs root or `CAP_NET_RAW` on Linux (it falls back to unprivileged
datagram ICMP where allowed, e.g. macOS).

## Deploying with Podman Quadlet

```bash
sudo podman build -t localhost/baldrick-monitor:latest .
sudo install -d /etc/baldrick-monitor /var/lib/baldrick-monitor
sudo install -m 644 config.example.json /etc/baldrick-monitor/config.json   # edit boards
sudo install -m 644 baldrick-monitor.container /etc/containers/systemd/
sudo systemctl daemon-reload && sudo systemctl start baldrick-monitor
sudo firewall-cmd --permanent --add-port=8420/tcp && sudo firewall-cmd --reload
```

The container uses host networking so DDP probes come from the host's own IP.

## Prometheus

```yaml
scrape_configs:
  - job_name: baldrick
    static_configs:
      - targets: ["srv:8420"]
```

Useful alerts: `baldrick_up == 0`, `baldrick_ddp_probe_success == 0`,
`increase(baldrick_reboots_detected_total[1h]) > 0`,
`baldrick_icmp_loss_ratio > 0.05`, `baldrick_temperature_celsius > 70`.
