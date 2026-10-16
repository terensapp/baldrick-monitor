package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// A board address is "host" or "host:port". The port applies to HTTP and
// the WebSocket only; ICMP and DDP always go to the host itself.
func hostOnly(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

func httpAddr(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return net.JoinHostPort(addr, "80")
}

// Duration accepts Go duration strings ("5s", "1m") in JSON.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	d.Duration = v
	return err
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// String drops zero units: 6h rather than 6h0m0s.
func (d Duration) String() string {
	s := d.Duration.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

type BoardConfig struct {
	Name string `json:"name"`
	Host string `json:"host"`
	// Per-board override of ddp_probe.mode.
	DDPProbe string `json:"ddp_probe,omitempty"`
	// Set while the board is in maintenance (alerts silenced).
	Maintenance *Maintenance `json:"maintenance,omitempty"`
}

// Intervals covers everything the monitor polls. Values that the board
// streams over its WebSocket arrive as fast as the board pushes them.
type Intervals struct {
	ICMP        Duration `json:"icmp"`         // ICMP echo
	HTTPPing    Duration `json:"http_ping"`    // GET /ping
	SystemState Duration `json:"system_state"` // GET /system_state (backs up the WebSocket)
	WebUI       Duration `json:"web_ui"`       // GET / (web UI served)
	Settings    Duration `json:"settings"`     // GET /settings (hostname, ports)
	DDPProbe    Duration `json:"ddp_probe"`    // DDP check
}

type ProbeConfig struct {
	// off: never inject DDP. auto: inject only when no other source is
	// streaming, otherwise judge the live stream passively. always: inject
	// even while another source is streaming.
	Mode string `json:"mode"`
	// DDP byte offset to write to. Keep it well past the board's configured
	// channels so probe packets never reach a pixel.
	Offset  uint32  `json:"offset"`
	Packets int     `json:"packets"`
	Rate    float64 `json:"rate"`
}

type HistoryConfig struct {
	RetentionDays int      `json:"retention_days"`
	DataDir       string   `json:"data_dir"`   // empty: keep history in memory only
	Resolution    Duration `json:"resolution"` // rollup period for stored metrics
}

type Thresholds struct {
	HTTPFailDown    int     `json:"http_fail_down"`
	ICMPLossWarn    float64 `json:"icmp_loss_warn"`
	RTTWarnMs       float64 `json:"rtt_warn_ms"`
	DDPLossWarn     float64 `json:"ddp_loss_warn"`
	TempWarnC       float64 `json:"temp_warn_c"`
	RAMMinWarnBytes float64 `json:"ram_min_warn_bytes"`
}

type IncidentsConfig struct {
	RetentionDays int `json:"retention_days"`
	// How much raw telemetry before an incident goes into its export.
	RecorderWindow Duration `json:"recorder_window"`
}

type AlertsConfig struct {
	// Webhook to POST alerts to (reboots, lockups, DDP failures, recoveries).
	WebhookURL string `json:"webhook_url"`
	// json (default), ntfy, discord or slack.
	Format string `json:"format"`
	// Public URL of this dashboard, for links in alerts.
	DashboardURL string `json:"dashboard_url"`
}

type Config struct {
	Listen string `json:"listen"`
	// If set, changing boards or triggering probes requires
	// "Authorization: Bearer <admin_token>".
	AdminToken string `json:"admin_token,omitempty"`
	// Display unit for temperatures: "F" (default) or "C". Thresholds and
	// Prometheus metrics stay in Celsius.
	TemperatureUnit string          `json:"temperature_unit"`
	Boards          []BoardConfig   `json:"boards"`
	Intervals       Intervals       `json:"intervals"`
	DDPProbe        ProbeConfig     `json:"ddp_probe"`
	History         HistoryConfig   `json:"history"`
	Incidents       IncidentsConfig `json:"incidents"`
	// Where alerts go. The older single-webhook "alerts" block is still
	// read and becomes one channel.
	Notifications []Channel     `json:"notifications"`
	Alerts        AlertsConfig  `json:"alerts"`
	DashboardURL  string        `json:"dashboard_url"`
	Updates       UpdatesConfig `json:"updates"`
	Thresholds    Thresholds    `json:"thresholds"`
	// Hold notifications overnight (or whenever) and summarise them after.
	QuietHours QuietHours `json:"quiet_hours"`

	live *liveHolder // settings applied without restarting monitoring
}

// liveSettings can change while boards are being monitored; every copy of
// a Config shares one holder.
type liveSettings struct {
	thresholds Thresholds
	unit       string
	token      string
	notifier   *Notifier
}

type liveHolder struct{ atomic.Pointer[liveSettings] }

func (c *Config) publishLive() {
	if c.live == nil {
		c.live = &liveHolder{}
	}
	n := NewNotifier(slices.Clone(c.Notifications), c.DashboardURL)
	q := c.QuietHours
	q.Days = slices.Clone(q.Days)
	n.quiet = &q
	var old *Notifier
	if cur := c.live.Load(); cur != nil {
		old = cur.notifier
	}
	c.live.Store(&liveSettings{thresholds: c.Thresholds, unit: c.TemperatureUnit, token: c.AdminToken, notifier: n})
	n.adoptHeld(old.takeHeld())
}

func (c *Config) th() Thresholds    { return c.live.Load().thresholds }
func (c *Config) unit() string      { return c.live.Load().unit }
func (c *Config) token() string     { return c.live.Load().token }
func (c *Config) notify() *Notifier { return c.live.Load().notifier }

func loadConfig(path, listen, boards, dataDir string) (*Config, error) {
	cfg := &Config{}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
	if listen != "" {
		cfg.Listen = listen
	}
	if dataDir != "" {
		cfg.History.DataDir = dataDir
	}
	for _, h := range strings.Split(boards, ",") {
		if h = strings.TrimSpace(h); h != "" {
			cfg.Boards = append(cfg.Boards, BoardConfig{Host: h})
		}
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.publishLive()
	return cfg, nil
}

func (c *Config) applyDefaults() {
	def := func(d *Duration, v time.Duration) {
		if d.Duration <= 0 {
			d.Duration = v
		}
	}
	if c.Listen == "" {
		c.Listen = ":8420"
	}
	c.TemperatureUnit = strings.ToUpper(c.TemperatureUnit)
	if c.TemperatureUnit == "" {
		c.TemperatureUnit = "F"
	}
	iv := &c.Intervals
	def(&iv.ICMP, time.Second)
	def(&iv.HTTPPing, 5*time.Second)
	def(&iv.SystemState, 15*time.Second)
	def(&iv.WebUI, time.Minute)
	def(&iv.Settings, 5*time.Minute)
	def(&iv.DDPProbe, time.Minute)
	if c.DDPProbe.Mode == "" {
		c.DDPProbe.Mode = "auto"
	}
	if c.DDPProbe.Offset == 0 {
		c.DDPProbe.Offset = 1_000_000
	}
	if c.DDPProbe.Packets <= 0 {
		c.DDPProbe.Packets = 60
	}
	if c.DDPProbe.Rate <= 0 {
		c.DDPProbe.Rate = 20
	}
	if c.History.RetentionDays <= 0 {
		c.History.RetentionDays = 7
	}
	def(&c.History.Resolution, time.Minute)
	if c.Incidents.RetentionDays <= 0 {
		c.Incidents.RetentionDays = 90
	}
	def(&c.Incidents.RecorderWindow, 30*time.Minute)
	def(&c.Updates.CheckInterval, 6*time.Hour)
	if c.DashboardURL == "" {
		c.DashboardURL = c.Alerts.DashboardURL
	}
	if len(c.Notifications) == 0 && c.Alerts.WebhookURL != "" {
		typ := c.Alerts.Format
		if typ == "" || typ == "json" {
			typ = "webhook"
		}
		c.Notifications = []Channel{{ID: "legacy", Name: "Webhook", Type: typ, Enabled: true, URL: c.Alerts.WebhookURL}}
	}
	t := &c.Thresholds
	if t.HTTPFailDown <= 0 {
		t.HTTPFailDown = 3
	}
	if t.ICMPLossWarn <= 0 {
		t.ICMPLossWarn = 0.05
	}
	if t.RTTWarnMs <= 0 {
		t.RTTWarnMs = 50
	}
	if t.DDPLossWarn <= 0 {
		t.DDPLossWarn = 0.01
	}
	if t.TempWarnC <= 0 {
		t.TempWarnC = 70
	}
	if t.RAMMinWarnBytes <= 0 {
		t.RAMMinWarnBytes = 20_000
	}
}

func (c *Config) validate() error {
	if c.TemperatureUnit != "F" && c.TemperatureUnit != "C" {
		return fmt.Errorf("temperature_unit must be F or C, got %q", c.TemperatureUnit)
	}
	if err := validFormat(c.Alerts.Format); err != nil {
		return err
	}
	if err := c.validateRanges(); err != nil {
		return err
	}
	ids := map[string]bool{}
	for i := range c.Notifications {
		if err := c.Notifications[i].normalize(); err != nil {
			return err
		}
		if ids[c.Notifications[i].ID] {
			c.Notifications[i].ID = newChannelID()
		}
		ids[c.Notifications[i].ID] = true
	}
	if err := c.QuietHours.normalize(); err != nil {
		return err
	}
	if err := validMode(c.DDPProbe.Mode); err != nil {
		return err
	}
	seen := map[string]bool{}
	for i := range c.Boards {
		if err := normalizeBoard(&c.Boards[i]); err != nil {
			return err
		}
		if seen[c.Boards[i].Name] {
			return fmt.Errorf("duplicate board name %q", c.Boards[i].Name)
		}
		seen[c.Boards[i].Name] = true
	}
	return nil
}

func validFormat(f string) error {
	switch f {
	case "", "json", "ntfy", "discord", "slack":
		return nil
	}
	return fmt.Errorf("alerts.format must be json, ntfy, discord or slack, got %q", f)
}

func validMode(m string) error {
	switch m {
	case "", "off", "auto", "always":
		return nil
	}
	return fmt.Errorf("invalid ddp_probe mode %q (want off, auto or always)", m)
}

// normalizeBoard trims and checks a board definition, defaulting its name
// to its host.
func normalizeBoard(b *BoardConfig) error {
	b.Name, b.Host = strings.TrimSpace(b.Name), strings.TrimSpace(b.Host)
	if b.Host == "" {
		return fmt.Errorf("board %q has no host", b.Name)
	}
	if strings.ContainsAny(b.Host, "/@ ") {
		return fmt.Errorf("host %q should be an IP or hostname, optionally with :port", b.Host)
	}
	if strings.Contains(b.Host, ":") {
		_, port, err := net.SplitHostPort(b.Host)
		if n, perr := strconv.Atoi(port); err != nil || perr != nil || n < 1 || n > 65535 {
			return fmt.Errorf("host %q should be an IP or hostname, optionally with :port", b.Host)
		}
	}
	if b.Name == "" {
		b.Name = b.Host
	}
	if len(b.Name) > 64 || strings.ContainsAny(b.Name, "/\\") {
		return fmt.Errorf("board name %q is too long or contains a slash", b.Name)
	}
	return validMode(b.DDPProbe)
}

// validateRanges rejects values that would hammer a board or the disk.
func (c *Config) validateRanges() error {
	iv := c.Intervals
	for _, r := range []struct {
		name      string
		d, lo, hi time.Duration
	}{
		{"ICMP interval", iv.ICMP.Duration, 200 * time.Millisecond, time.Hour},
		{"HTTP /ping interval", iv.HTTPPing.Duration, time.Second, time.Hour},
		{"system_state interval", iv.SystemState.Duration, time.Second, time.Hour},
		{"web UI check interval", iv.WebUI.Duration, 5 * time.Second, 24 * time.Hour},
		{"settings check interval", iv.Settings.Duration, 10 * time.Second, 24 * time.Hour},
		{"DDP check interval", iv.DDPProbe.Duration, 10 * time.Second, 24 * time.Hour},
		{"history resolution", c.History.Resolution.Duration, 10 * time.Second, time.Hour},
		{"incident recorder window", c.Incidents.RecorderWindow.Duration, time.Minute, 6 * time.Hour},
		{"update check interval", c.Updates.CheckInterval.Duration, 5 * time.Minute, 30 * 24 * time.Hour},
	} {
		if r.d < r.lo || r.d > r.hi {
			return fmt.Errorf("%s must be between %s and %s", r.name, r.lo, r.hi)
		}
	}
	if c.History.RetentionDays > 3650 || c.Incidents.RetentionDays > 3650 {
		return errors.New("retention is limited to 3650 days")
	}
	t := c.Thresholds
	if t.ICMPLossWarn > 1 || t.DDPLossWarn > 1 {
		return errors.New("loss thresholds are fractions between 0 and 1")
	}
	if t.HTTPFailDown > 100 {
		return errors.New("HTTP failures before down must be at most 100")
	}
	p := c.DDPProbe
	if p.Packets > 1000 || p.Rate > 200 {
		return errors.New("DDP probe is limited to 1000 packets at 200/s")
	}
	return nil
}

// fmtTemp renders a Celsius reading in the configured display unit.
func (c *Config) fmtTemp(celsius float64) string {
	if c.unit() == "C" {
		return fmt.Sprintf("%.0f°C", celsius)
	}
	return fmt.Sprintf("%.0f°F", celsius*9/5+32)
}

func (c *Config) retention() time.Duration {
	return time.Duration(c.History.RetentionDays) * 24 * time.Hour
}

// Source says how a value on the dashboard is collected.
type Source struct {
	Kind     string `json:"kind"` // polled, streamed, derived
	Via      string `json:"via"`
	Interval string `json:"interval,omitempty"`
}

func (c *Config) sources() map[string]Source {
	iv := c.Intervals
	ws := "WebSocket /ws: the board pushes changes (about every 1–5s); also refreshed by the GET /system_state poll every " + iv.SystemState.String()
	return map[string]Source{
		"icmp":         {"polled", "ICMP echo request", iv.ICMP.String()},
		"http_ping":    {"polled", "HTTP GET /ping (expects 200 + JSON)", iv.HTTPPing.String()},
		"web_ui":       {"polled", "HTTP GET / (expects 200 text/html)", iv.WebUI.String()},
		"system_state": {"polled", "HTTP GET /system_state (expects 200 + JSON with uptime)", iv.SystemState.String()},
		"settings":     {"polled", "HTTP GET /settings (hostname, port/model config)", iv.Settings.String()},
		"ddp_probe":    {"polled", "DDP check: passive if a live stream is present, else a short burst of black frames to an unused offset, confirmed via the board's counters", iv.DDPProbe.String()},
		"ws":           {"streamed", "WebSocket /ws connection health (keepalive sent every 1s, stale after 6s)", ""},
		"board":        {"streamed", ws, ""},
		"stream":       {"streamed", ws + ". The board counts packets in ~5s windows; the counters reset when a stream stops", ""},
		"history":      {"derived", "Rolled up from the live values every " + c.History.Resolution.String(), c.History.Resolution.String()},
	}
}
