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

Hovering over any chart shows the exact time and values at that point
(sparklines, history charts, the status strip and incident telemetry).

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

**Probes and live shows.** In `auto` mode, probes are built to stay out of
a show's way:

- **FPP-aware:** before each probe, the monitor asks every FPP the board
  reports (`/api/fppd/status`) whether it's playing or has a show
  scheduled within 5 minutes, and waits if so. This covers the gaps
  between sequences in a playlist. Turn it off with
  `ddp_probe.ignore_fpp` (Settings → Monitoring → During shows).
- **Gets out of the way:** if another DDP source appears mid-probe, the
  probe stops sending immediately.
- **Harmless even if they overlap:** probe packets never set DDP's PUSH
  flag, so they can't trigger an output frame. They use sequence number 0
  ("not used"), so they can't show up as dropped packets in a real stream.
  And they write black to an offset past every configured channel. Tested
  on a Baldrick 8 Port v1 (v3.8.9): it counts such packets and lists the
  sender as a source.

When a check is held back, the dashboard shows it as paused (for example
"paused for the show"), keeps the last real result, and doesn't count the
skip as a failure.

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

**Show output verification.** Probes pause during shows, so instead the
monitor checks that the show actually reaches the board. Every 5 seconds it
asks each FPP the board reports for its status (the same cached
`/api/fppd/status` read as above, at most once per 20s), and, for an FPP
that's playing, its channel outputs (`/api/channel/output/universeOutputs`,
cached 5 minutes). If those outputs send DDP, unicast E1.31, unicast Art-Net
or KiNET to the board's address (or its hostname), the board must list that
FPP's IP among its stream sources. If it doesn't for 20 seconds, a **show
output** incident opens; it resolves when the stream appears, or when FPP
stops playing or no longer outputs to the board. Multicast E1.31 and Art-Net
broadcast outputs can't be tied to one board and aren't checked. The monitor
only ever sends FPP GET requests, and an FPP it can't reach is left alone.

## Incidents: reboots, lockups, DDP failures and missing shows

When something goes wrong, the monitor opens an **incident** and keeps the
telemetry that led up to it. It keeps a rolling raw recorder (default 30
minutes) of every ping, HTTP check (including error bodies), WebSocket state
update, DDP check and event.

| Incident | Detected when | What it captures |
|---|---|---|
| **Stopped responding** (lockup) | `/ping` fails `http_fail_down` times in a row (503s, timeouts, refused) | An immediate diagnostic sweep (TCP connect; every endpoint's status and error page; 5 pings) to separate "web server up but the firmware refuses (503)", "network stack alive but HTTP hung" and "nothing answers (power/network)". It stays open until HTTP works again and records whether it recovered on its own or only after a reboot. |
| **Reboot** | The board's boot time (now − uptime) jumps forward, including reboots that happened while the monitor was down | Classified as *unexpected*, *firmware update* or *after a settings change* (the last two are auto-acknowledged). Findings cover how the board went quiet (instant crash vs hang vs long outage), memory and leak trends, temperature, active streams, crash loops, and any message the board shows after booting. |
| **Not processing DDP** | Two DDP checks in a row fail while HTTP still works | The probe results and the board's stream counters. |
| **Show output** | An FPP whose outputs include the board is playing, but the board hasn't listed that FPP as a stream source for 20s (not raised during a lockup) | FPP's full status and output config, the outputs aimed at the board, and the board's sources at the time, including any other controller it's receiving from instead. |
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

### Notifications

Set these up in **⚙ Settings → Notifications**. Add any number of channels:

| Channel | What you need |
|---|---|
| **Discord** | A channel webhook URL (Server Settings → Integrations → Webhooks). Alerts arrive as color-coded embeds. |
| **Slack** | An incoming-webhook URL. Alerts arrive as formatted messages with a link. |
| **Email** | Any SMTP server (STARTTLS, TLS or plain), optional login, one or more recipients. |
| **ntfy** | A topic URL such as `https://ntfy.sh/your-random-topic` (or a self-hosted server, with an optional token). It's free push to the ntfy phone app. |
| **Webhook** | Any URL. The alert is POSTed as JSON. |

Each channel picks its events: **stopped responding**, **unexpected
reboot**, **DDP failure**, **show not reaching board**, **low memory**,
**recovered**, and optionally **monitor update
available**. **Send test** checks a channel before you save. Repeats of the
same event for the same board are held back for 2 minutes, so a flapping
board doesn't flood you. Set the **dashboard URL** so alerts link straight
to the incident.

Prometheus also gets `baldrick_incidents_unacknowledged{type}` and
`baldrick_incidents_ongoing{type}`.

## Settings

**⚙ Settings** in the header covers what you'd otherwise edit in the config
file:

- **Notifications:** channels, events, the dashboard URL.
- **Monitoring:** every polling interval, the default DDP check mode, the
  probe offset and burst.
- **Alert rules:** down-after-N-failures, loss and latency warnings,
  temperature (in °F or °C) and low-memory thresholds, the display unit.
- **Data:** history and incident retention, rollup resolution, the
  telemetry kept per incident.
- **Access & updates:** set, rotate or remove the admin token; the update
  check interval.

Notifications, rules and access apply immediately. Monitoring and data
changes restart the board monitors for a moment. History, incidents and
ongoing lockups carry over, but the raw incident recorder starts over.

Settings are saved to `<data_dir>/settings.json` (mode 0600, since it holds
webhook URLs and passwords). They override the same keys in the config file;
delete the file to fall back to the config file. Secrets are never sent back
to the browser. Saved values show as "saved", and leaving the field blank
keeps them. Values are range-checked, so you can't set, say, a 1 ms ping.

The listen address and data directory stay in the config file.

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
curl -XPOST 'localhost:8081/_sim/fpp?state=playing&stream=0'  # its FPP plays, stream never arrives
curl -XPOST 'localhost:8081/_sim/fpp?stream=1'  # ...now it does
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

Images are published to `ghcr.io/terensapp/baldrick-monitor` for amd64 and
arm64:

| Tag | Built from |
|---|---|
| `main` | every push to `main` |
| `sha-<commit>` | that commit |
| `latest`, `X.Y`, `X.Y.Z` | release tags `vX.Y.Z` |

On the host, from a checkout of this repo:

```bash
sudo ./deploy/install.sh               # track :main
sudo ./deploy/install.sh --tag latest  # or track releases only
```

This installs:

- the Quadlet, as `/etc/containers/systemd/baldrick-monitor.container`
  (host networking, `CAP_NET_RAW` for ICMP, your host's timezone);
- a starter `/etc/baldrick-monitor/config.json`, if there isn't one;
- the host updater;
- a firewalld rule for port 8420.

Re-running the script is safe. It keeps your config and data.

### Updating from the dashboard

The monitor checks the registry every `updates.check_interval` (default 6h)
for the image its tag points at. When a newer build exists, the header shows
**Update available**. **Update now** only drops a request file into the data
volume. On the host, `baldrick-monitor-update.path` runs
`/usr/local/libexec/baldrick-monitor-update`, which:

1. pulls the image named in the Quadlet's `Image=` (the container never
   chooses what gets pulled);
2. restarts the service;
3. waits for `/healthz`, and rolls back to the previous image if the new
   one doesn't come up.

Progress and the result appear in the update dialog. The updater's log is in
`journalctl -u baldrick-monitor-update`. Without the host updater installed,
the dashboard still shows when an update is available.

### Cutting a release

```bash
git tag v0.2.0 && git push origin v0.2.0
```

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
