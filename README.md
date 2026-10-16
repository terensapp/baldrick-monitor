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

Temperatures are shown in °F by default (`"temperature_unit": "C"` to switch).
Thresholds (`temp_warn_c`) and Prometheus metrics stay in Celsius.

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

## Incidents: reboots, lockups and DDP failures

When something goes wrong, the monitor opens an **incident** and keeps the
telemetry that led up to it. It keeps a rolling raw recorder (default 30
minutes) of every ping, HTTP check (including error bodies), WebSocket state
update, DDP check and event.

| Incident | Detected when | What it captures |
|---|---|---|
| **Stopped responding** (lockup) | `/ping` fails `http_fail_down` times in a row (503s, timeouts, refused) | An immediate diagnostic sweep (TCP connect; every endpoint's status and error page; 5 pings) to separate "web server up but the firmware refuses (503)", "network stack alive but HTTP hung" and "nothing answers (power/network)". It stays open until HTTP works again and records whether it recovered on its own or only after a reboot. |
| **Reboot** | The board's boot time (now − uptime) jumps forward, including reboots that happened while the monitor was down | Classified as *unexpected*, *firmware update* or *after a settings change* (the last two are auto-acknowledged). Findings cover how the board went quiet (instant crash vs hang vs long outage), memory and leak trends, temperature, active streams, crash loops, and any message the board shows after booting. |
| **Not processing DDP** | Two DDP checks in a row fail while HTTP still works | The probe results and the board's stream counters. |
| **Manual capture** | **Collect diagnostics now** in a board's ••• menu | The recorder plus a fresh sweep. Use it the moment something looks off. |

An incident keeps the board **degraded** until you **Acknowledge** it, with
an optional note, from the card banner, the **Incidents** list or the detail
view. **Export telemetry** downloads a JSON bundle containing:

- a readable summary;
- the full raw timeline;
- two hours of rollups and events;
- the board's settings;
- the monitor configuration.

Attach it to a bug report. Incidents are kept `incidents.retention_days`
(default 90) under `<data_dir>/<board>/incidents/`.

The board's firmware doesn't expose a reset reason, so the "likely cause" is
inferred from these observations. A reboot started from the board's own web
UI looks the same as a crash.

### Alerts

```json
"alerts": {
  "webhook_url": "https://ntfy.sh/my-baldrick-alerts",
  "format": "ntfy",
  "dashboard_url": "http://srv:8420"
}
```

`format` is `json` (default; POSTs the alert object), `ntfy`, `discord` or
`slack`. Alerts fire for unexpected reboots, lockups, DDP failures and their
recoveries, with a link straight to the incident. Prometheus also gets
`baldrick_incidents_unacknowledged{type}` and
`baldrick_incidents_ongoing{type}`.

## Managing boards

Add, edit and remove boards from the dashboard. Changes apply right away,
with no restart:

- **＋ Add board** takes an IP or hostname. **Test connection** shows which
  board answers there (model, hostname, firmware) before you add it.
- **⌕ Scan network** checks every address in a subnet (up to a /22) for a
  Baldrick web API and lists what it finds, with one-click **Add**. The
  subnets of boards you already monitor are offered as suggestions.
- Each card's **•••** menu has **Edit** (rename, change host or DDP mode;
  a rename keeps the board's history) and **Remove**, which can optionally
  delete the stored history.

The board list is saved to `<data_dir>/boards.json`. The `boards` list in the
config file only seeds that file on first start; after that, `boards.json`
is the source of truth. To start over from the config, delete it. Without a
`data_dir`, UI changes are lost on restart.

Set `"admin_token"` in the config to require
`Authorization: Bearer <token>` for these changes and for manual DDP checks.
The dashboard asks for the token once and remembers it in the browser.
Viewing stays open.

API: `POST /api/boards`, `PUT /api/boards/{name}`,
`DELETE /api/boards/{name}?purge=1`, `GET /api/inspect?host=`,
`GET /api/scan?cidr=`.

## History

Every `history.resolution` (default 1m), the monitor rolls up the current
stats and appends them to `<data_dir>/<board>/samples-YYYY-MM-DD.jsonl`.
Events go to `events-YYYY-MM-DD.jsonl`. Files older than
`history.retention_days` (default 7) are deleted. History is reloaded on
restart. Without a `data_dir`, history is kept in memory only.

## Testing without hardware

`cmd/fakeboard` imitates a board's HTTP, WebSocket and DDP interfaces and can
simulate failures:

```bash
go run ./cmd/fakeboard &                       # http://127.0.0.1:8081, DDP on 127.0.0.1:4048
go run . -boards 127.0.0.1:8081
curl -XPOST 'localhost:8081/_sim/hang?secs=60'  # 503 lockup (add &reboot=1 to end in a reboot)
curl -XPOST localhost:8081/_sim/crash           # sudden crash + reboot
curl -XPOST localhost:8081/_sim/leak            # heap drains until it crashes
curl -XPOST localhost:8081/_sim/ddpstall        # stops counting DDP
curl -XPOST localhost:8081/_sim/reset
```

A board address can include a port (`host:port`). The port applies to HTTP
and the WebSocket; ICMP and DDP always go to the host.

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
