package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SystemState mirrors the board's /system_state document (also pushed as
// deltas over the /ws WebSocket).
type SystemState struct {
	BoardManufacturer  string    `json:"board_manufacturer"`
	BoardModel         string    `json:"board_model"`
	BoardID            string    `json:"board_id"`
	Temperature        []float64 `json:"temperature"`
	LastSettingsUpdate int64     `json:"last_settings_update"`
	OTA                struct {
		Updatable []struct {
			CurrentFirmware   string `json:"current_firmware_version"`
			CurrentFactory    string `json:"current_factory_version"`
			AvailableFirmware string `json:"available_firmware_version"`
		} `json:"updatable"`
	} `json:"ota"`
	RAMAvailable      float64           `json:"ram_available"`
	RAMMinimum        float64           `json:"ram_minimum"`
	RAMLargestBlock   float64           `json:"ram_largest_block"`
	Uptime            float64           `json:"uptime"`
	OOMCount          float64           `json:"oom_count"`
	UIMessages        []json.RawMessage `json:"ui_messages"`
	NetworkThroughput float64           `json:"network_throughput"`
	FPP               []FPPInstance     `json:"fpp"`
	FrameRate         float64           `json:"frame_rate"`
	TestModeActive    bool              `json:"test_mode_active"`
	FXState           bool              `json:"fx_state"`
	FXActive          string            `json:"fx_active"`
	Streaming         struct {
		DDPSources    []string `json:"ddp_sources"`
		DDPDropped    float64  `json:"ddp_dropped"`
		DDPRecvd      float64  `json:"ddp_recvd"`
		SACNSources   []string `json:"sacn_sources"`
		SACNDropped   float64  `json:"sacn_dropped"`
		SACNRecvd     float64  `json:"sacn_recvd"`
		ArtNetSources []string `json:"artnet_sources"`
		ArtNetDropped float64  `json:"artnet_dropped"`
		ArtNetRecvd   float64  `json:"artnet_recvd"`
	} `json:"streaming"`
}

type FPPInstance struct {
	Hostname  string `json:"hostname"`
	SWVersion string `json:"sw_version"`
	IP        string `json:"ip"`
	Hardware  string `json:"hardware"`
}

func (s *SystemState) firmware() (cur, factory, avail string) {
	if len(s.OTA.Updatable) == 0 {
		return
	}
	u := s.OTA.Updatable[0]
	return u.CurrentFirmware, u.CurrentFactory, u.AvailableFirmware
}

type Settings struct {
	Hostname string `json:"hostname"`
	Ports    []struct {
		PortType string `json:"port_type"`
		Models   []struct {
			Name        string `json:"name"`
			NumChannels int    `json:"num_channels"`
		} `json:"models"`
	} `json:"ports"`
}

type Event struct {
	Time  time.Time `json:"time"`
	Level string    `json:"level"` // info, warn, error
	Msg   string    `json:"msg"`
}

type EndpointStats struct {
	OK         uint64    `json:"ok"`
	Fail       uint64    `json:"fail"`
	LastOK     bool      `json:"last_ok"`
	LastStatus int       `json:"last_status"`
	LastMs     float64   `json:"last_ms"`
	LastErr    string    `json:"last_error,omitempty"`
	LastTime   time.Time `json:"last_time"`
}

type DDPProbe struct {
	Time     time.Time `json:"time"`
	OK       bool      `json:"ok"`
	Passive  bool      `json:"passive"`           // judged from a live stream, nothing injected
	Skipped  bool      `json:"skipped,omitempty"` // couldn't or shouldn't run (e.g. a show is playing)
	Note     string    `json:"note"`
	Sent     int       `json:"sent"`
	DetectMs float64   `json:"detect_ms"`
	Recvd    float64   `json:"recvd"`
	Dropped  float64   `json:"dropped"`
	Sources  []string  `json:"sources"`
}

const wsStale = 6 * time.Second // the board's own UI gives up after 5s

// FieldUpdate records when and how a top-level system_state field last
// changed: "ws" (pushed by the board) or "poll" (GET /system_state).
type FieldUpdate struct {
	Src string    `json:"src"`
	At  time.Time `json:"at"`
}

type Board struct {
	cfg    BoardConfig
	glob   *Config
	pinger *Pinger
	client *http.Client

	mu          sync.RWMutex
	raw         map[string]json.RawMessage
	st          SystemState
	haveState   bool
	stateAt     time.Time
	settings    *Settings
	settingsRaw json.RawMessage
	fieldAt     map[string]FieldUpdate
	prevRun     *lastState // last state saved by a previous run, until first contact

	icmp               *ring
	icmpN              int // samples per rollup window
	icmpSent, icmpLost uint64
	httpPing           *ring
	httpN              int
	pingFails          int
	firstFailAt        time.Time // first failure of the current /ping failure streak
	lastHTTPOK         time.Time
	recoverOKs         int       // /ping answers in a row during a lockup
	recoverSince       time.Time // first answer of that streak
	httpEverOK         bool
	endpoints          map[string]*EndpointStats

	wsConnected  bool
	wsLastMsg    time.Time
	wsReconnects uint64
	wsMessages   uint64
	wsErr        string

	reboots         uint64
	probe           *DDPProbe
	probeSkip       *DDPProbe // the current skip, while checks are paused
	probeSkipReason string
	probeCounts     map[string]uint64
	events          []Event
	history         []Sample
	store           *store
	probeNow        chan struct{}

	self         string  // our source address as the board sees it
	probeTripped bool    // a probe was followed by a reboot; never probe again
	onProbeTrip  func()  // persists the trip (set by the registry)
	lowLogged    float64 // lowest free memory since boot, as last logged
	rec          recorder
	incidents    []*Incident
	timelines    map[string][]RecPoint // incident timelines when there's no data dir
	activeLockup *Incident
	activeDDP    *Incident
	ddpFails     int
	maint        *Maintenance     // b.cfg.Maintenance is only the starting value
	rebootReq    *requestedReboot // a reboot someone asked for, until it shows up
	activeShow   *Incident
	showMissing  map[string]time.Time // FPP IP → when its stream was first missing during a show
	fpps         *FPPRegistry         // nil when FPPs aren't watched (tests)
}

func NewBoard(cfg BoardConfig, glob *Config, p *Pinger) *Board {
	res := glob.History.Resolution.Duration
	icmpN := max(1, int(res/glob.Intervals.ICMP.Duration))
	httpN := max(1, int(res/glob.Intervals.HTTPPing.Duration))
	b := &Board{
		cfg: cfg, glob: glob, pinger: p,
		client: &http.Client{Transport: &http.Transport{
			// The board is an ESP32 with a handful of sockets; stay frugal.
			MaxIdleConnsPerHost: 1,
			MaxConnsPerHost:     2,
			IdleConnTimeout:     30 * time.Second,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialBoard(ctx, network, addr, 2*time.Second)
			},
		}},
		raw:         map[string]json.RawMessage{},
		fieldAt:     map[string]FieldUpdate{},
		icmp:        newRing(max(300, icmpN)),
		icmpN:       icmpN,
		httpPing:    newRing(max(120, httpN)),
		httpN:       httpN,
		endpoints:   map[string]*EndpointStats{},
		probeCounts: map[string]uint64{},
		probeNow:    make(chan struct{}, 1),
		store:       openStore(glob.History.DataDir, cfg.Name, glob.retention()),
		rec:         recorder{window: glob.Incidents.RecorderWindow.Duration},
		timelines:   map[string][]RecPoint{},
		maint:       cfg.Maintenance,
	}
	b.loadIncidents()
	b.prevRun = b.loadLastState()
	b.self = localIP(hostOnly(cfg.Host))
	since := time.Now().Add(-glob.retention())
	b.events = loadLines(b.store, "events", time.Now(), func(e *Event) bool { return e.Time.After(since) })
	b.history = loadLines(b.store, "samples", time.Now(), func(s *Sample) bool { return s.T.After(since) })
	if n := len(b.events) + len(b.history); n > 0 {
		log.Printf("[%s] loaded %d events and %d history samples", cfg.Name, len(b.events), len(b.history))
	}
	return b
}

const maxEvents = 20000

func (b *Board) probeMode() string {
	if b.cfg.DDPProbe != "" {
		return b.cfg.DDPProbe
	}
	return b.glob.DDPProbe.Mode
}

// Run monitors the board until ctx is cancelled and every loop has exited.
func (b *Board) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, loop := range []func(context.Context){b.icmpLoop, b.wsLoop, b.ddpLoop, b.showLoop, b.rollupLoop, b.httpLoop} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(ctx)
		}()
	}
	wg.Wait()
	b.client.CloseIdleConnections()
}

// TriggerProbe requests an immediate DDP check; it is a no-op if one is
// already queued.
func (b *Board) TriggerProbe() {
	select {
	case b.probeNow <- struct{}{}:
	default:
	}
}

// addEvent must be called with b.mu held.
func (b *Board) addEvent(level, format string, args ...any) {
	e := Event{Time: time.Now(), Level: level, Msg: fmt.Sprintf(format, args...)}
	b.events = append(b.events, e)
	if len(b.events) > maxEvents {
		b.events = b.events[len(b.events)-maxEvents:]
	}
	b.store.append("events", e.Time, e)
	b.rec.add("event", e)
	log.Printf("[%s] %s: %s", b.cfg.Name, level, e.Msg)
}

// ---- state ----

func (b *Board) mergeState(data []byte, src string) {
	var delta map[string]json.RawMessage
	if err := json.Unmarshal(data, &delta); err != nil || len(delta) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	prevUptimeAt := b.fieldAt["uptime"].At
	for k, v := range delta {
		b.raw[k] = mergeJSON(b.raw[k], v)
		b.fieldAt[k] = FieldUpdate{Src: src, At: now}
	}
	b.rec.add("state", json.RawMessage(fmt.Sprintf(`{"src":%q,"d":%s}`, src, data)))
	merged, _ := json.Marshal(b.raw)
	var st SystemState
	if err := json.Unmarshal(merged, &st); err != nil {
		log.Printf("[%s] bad system_state: %v", b.cfg.Name, err)
		return
	}
	has := func(k string) bool { _, ok := delta[k]; return ok }
	if !b.haveState {
		fw, _, _ := st.firmware()
		b.addEvent("info", "Found %s %s (id %s, firmware %s)", st.BoardManufacturer, st.BoardModel, st.BoardID, fw)
		b.lowLogged = st.RAMMinimum
		// Did it reboot while the monitor wasn't running?
		if prev := b.prevRun; prev != nil && has("uptime") {
			b.prevRun = nil
			if now.Add(-secs(st.Uptime)).Sub(prev.At.Add(-secs(prev.Uptime))) > rebootGrace {
				b.onReboot(nil, prev, time.Time{}, &st, now)
			}
		}
	} else {
		old := b.st
		// A reboot shows up as the boot time (now - uptime) jumping forward,
		// which also catches boards that were down longer than their uptime.
		rebooted := false
		if has("uptime") {
			oldBoot := prevUptimeAt.Add(-secs(old.Uptime))
			if st.Uptime+5 < old.Uptime || now.Add(-secs(st.Uptime)).Sub(oldBoot) > rebootGrace {
				b.onReboot(&old, nil, prevUptimeAt, &st, now)
				rebooted = true
				b.lowLogged = st.RAMMinimum
				// Record the new boot right away, so a restart of the
				// monitor can't count this reboot a second time.
				defer func() { go b.saveLastState() }()
			}
		}
		if has("ram_minimum") && !rebooted {
			b.noteLowWater(st.RAMMinimum)
		}
		if has("oom_count") && st.OOMCount > old.OOMCount {
			b.addEvent("error", "Out-of-memory count rose to %.0f", st.OOMCount)
		}
		oldFW, _, _ := old.firmware()
		if newFW, _, _ := st.firmware(); has("ota") && newFW != "" && newFW != oldFW {
			b.addEvent("info", "Firmware changed: %s → %s", oldFW, newFW)
		}
		// The field also changes on every boot; that isn't a settings change.
		if has("last_settings_update") && st.LastSettingsUpdate != old.LastSettingsUpdate && !rebooted {
			b.addEvent("info", "Board settings were changed")
		}
		if has("ui_messages") && len(st.UIMessages) > len(old.UIMessages) {
			b.addEvent("warn", "Board raised a UI message: %s", truncate(string(st.UIMessages[len(st.UIMessages)-1]), 160))
		}
	}
	b.st, b.haveState, b.stateAt = st, true, time.Now()
}

// noteLowWater tracks the board's lowest free memory since boot. A falling
// low-water mark was the clearest early sign of the lockups seen on a
// Baldrick 8: it slid from 81 KB to 26 KB over a day of hourly lockups.
// Each further 8 KB drop is logged, and crossing the warning threshold
// alerts (b.mu held).
func (b *Board) noteLowWater(low float64) {
	if low <= 0 || b.lowLogged <= 0 || low > b.lowLogged-8192 {
		return
	}
	prev := b.lowLogged
	b.lowLogged = low
	b.addEvent("warn", "Lowest free memory since boot fell to %.0f KB (was %.0f KB)", low/1024, prev/1024)
	if th := b.glob.th().RAMMinWarnBytes; low < th && prev >= th {
		b.send(Alert{Event: "memory", Severity: "warn", Board: b.cfg.Name, Host: b.cfg.Host,
			Title:   b.cfg.Name + ": Low memory",
			Message: fmt.Sprintf("The board's lowest free memory since boot has fallen to %.0f KB (warning below %.0f KB). On this board, a falling low-water mark came before repeated lockups; a reboot resets it.", low/1024, th/1024)})
	}
}

// firmwareAtLeast compares versions like "v3.8.9" or "3.8.9-sim". Unknown
// firmware never passes, so it is never probed; min "none" passes everything.
func firmwareAtLeast(fw, min string) bool {
	if min == "none" {
		return true
	}
	parse := func(v string) []int {
		v = strings.TrimPrefix(strings.TrimSpace(v), "v")
		var out []int
		for _, part := range strings.SplitN(v, ".", 4) {
			n, digits := 0, 0
			for _, r := range part {
				if r < '0' || r > '9' {
					break
				}
				n, digits = n*10+int(r-'0'), digits+1
			}
			if digits == 0 {
				break
			}
			out = append(out, n)
		}
		return out
	}
	f, m := parse(fw), parse(min)
	if len(f) == 0 {
		return false
	}
	for i := 0; i < max(len(f), len(m)); i++ {
		a, b := 0, 0
		if i < len(f) {
			a = f[i]
		}
		if i < len(m) {
			b = m[i]
		}
		if a != b {
			return a > b
		}
	}
	return true
}

// checkProbeTrip turns probes off for good if the board rebooted within
// seconds of one starting: on v3.5.4 every probe crashed the board (b.mu
// held). It returns a finding for the reboot incident, or "".
func (b *Board) checkProbeTrip(rebootAt time.Time) string {
	if b.probeTripped {
		return ""
	}
	for i := len(b.rec.pts) - 1; i >= 0; i-- {
		p := b.rec.pts[i]
		if p.K != "probe_start" {
			continue
		}
		gap := rebootAt.Sub(p.T)
		if gap < -5*time.Second || gap > 20*time.Second {
			if gap > 20*time.Second {
				return ""
			}
			continue
		}
		b.probeTripped = true
		b.addEvent("error", "DDP probes turned off for this board: it rebooted %s after a probe started. Its firmware may not handle probe packets; stream counters are still watched", gap.Round(time.Second))
		if b.onProbeTrip != nil {
			go b.onProbeTrip()
		}
		return fmt.Sprintf("A monitor DDP probe started %s before it rebooted, so probes are now off for this board (set its DDP check back to auto to re-enable).", gap.Round(time.Second))
	}
	return ""
}

// mergeJSON overlays an update onto the previous value. Boards send partial
// nested objects (a "streaming" update may carry the packet counts but not
// the source list), so objects merge key by key instead of being replaced.
func mergeJSON(old, upd json.RawMessage) json.RawMessage {
	var o, u map[string]json.RawMessage
	if len(old) == 0 || json.Unmarshal(old, &o) != nil || o == nil || json.Unmarshal(upd, &u) != nil || u == nil {
		return upd
	}
	for k, v := range u {
		o[k] = mergeJSON(o[k], v)
	}
	out, err := json.Marshal(o)
	if err != nil {
		return upd
	}
	return out
}

// ---- HTTP ----

type httpResult struct {
	body    []byte
	code    int
	header  http.Header
	elapsed time.Duration
	err     error
}

func (b *Board) get(ctx context.Context, path string, timeout time.Duration) httpResult {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+b.cfg.Host+path, nil)
	start := time.Now()
	resp, err := b.client.Do(req)
	if err != nil {
		return httpResult{elapsed: time.Since(start), err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	r := httpResult{body: body, code: resp.StatusCode, header: resp.Header, elapsed: time.Since(start), err: err}
	if err == nil && resp.StatusCode != http.StatusOK {
		r.err = fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return r
}

func (b *Board) record(name string, r httpResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.endpoints[name]
	if e == nil {
		e = &EndpointStats{}
		b.endpoints[name] = e
	}
	e.LastStatus, e.LastMs, e.LastTime = r.code, ms(r.elapsed), time.Now()
	e.LastOK = r.err == nil
	pt := map[string]any{"ep": name, "ok": r.err == nil, "status": r.code, "ms": ms(r.elapsed)}
	if r.err != nil {
		e.Fail++
		e.LastErr = r.err.Error()
		pt["err"] = r.err.Error()
		if r.code >= 400 && len(r.body) > 0 {
			pt["body"] = bodySnippet(r.body)
			e.LastErr += ": " + truncate(bodySnippet(r.body), 120)
		}
	} else {
		e.OK++
		e.LastErr = ""
	}
	b.rec.add("http", pt)
}

func (b *Board) checkPing(ctx context.Context) {
	r := b.get(ctx, "/ping", 2*time.Second)
	if r.err == nil && !json.Valid(r.body) {
		r.err = errors.New("/ping returned invalid JSON")
	}
	b.record("/ping", r)
	start, recoveredAt := b.notePing(r.err, r.elapsed, time.Now())
	if start {
		b.startLockup(ctx, r.err)
	}
	if !recoveredAt.IsZero() {
		// Read the state first so a reboot during the lockup is noticed
		// before the recovery is described.
		b.fetchState(ctx)
		b.mu.Lock()
		b.endLockup(recoveredAt)
		b.mu.Unlock()
		b.checkPage(ctx) // clear stale endpoint failures right away
		b.checkSettings(ctx)
	}
}

// A locked-up board counts as recovered only once /ping has answered
// lockupRecoverOKs times in a row over at least lockupRecoverFor. A board on
// its way down often answers once between failures; ending the lockup on
// that splits one outage into many.
const (
	lockupRecoverOKs = 3
	lockupRecoverFor = time.Minute
)

// notePing records a /ping result. It reports whether a lockup should start,
// and when a lockup has recovered, the time of the first answer in the
// streak that confirmed it.
func (b *Board) notePing(err error, elapsed time.Duration, now time.Time) (start bool, recoveredAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.httpPing.add(math.NaN())
		b.pingFails++
		if b.pingFails == 1 {
			b.firstFailAt = now.Add(-elapsed)
		}
		if b.activeLockup != nil && b.recoverOKs > 0 {
			b.addEvent("warn", "Answered %d× then failed again; still not responding", b.recoverOKs)
		}
		b.recoverOKs, b.recoverSince = 0, time.Time{}
		return b.pingFails == b.glob.th().HTTPFailDown, time.Time{}
	}
	b.httpPing.add(ms(elapsed))
	b.pingFails, b.httpEverOK, b.lastHTTPOK = 0, true, now
	if b.activeLockup == nil {
		return false, time.Time{}
	}
	if b.recoverOKs == 0 {
		b.recoverSince = now.Add(-elapsed)
	}
	b.recoverOKs++
	if b.recoverOKs < lockupRecoverOKs || now.Sub(b.recoverSince) < lockupRecoverFor {
		return false, time.Time{}
	}
	at := b.recoverSince
	b.recoverOKs, b.recoverSince = 0, time.Time{}
	return false, at
}

func (b *Board) fetchState(ctx context.Context) (*SystemState, error) {
	r := b.get(ctx, "/system_state", 2*time.Second)
	var st SystemState
	if r.err == nil {
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(r.body, &probe); err != nil {
			r.err = fmt.Errorf("invalid JSON: %w", err)
		} else if _, ok := probe["uptime"]; !ok {
			r.err = errors.New("response missing uptime")
		} else {
			json.Unmarshal(r.body, &st)
		}
	}
	b.record("/system_state", r)
	if r.err != nil {
		return nil, r.err
	}
	b.mergeState(r.body, "poll")
	return &st, nil
}

func (b *Board) checkPage(ctx context.Context) {
	r := b.get(ctx, "/", 3*time.Second)
	if r.err == nil {
		switch {
		case !strings.HasPrefix(r.header.Get("Content-Type"), "text/html"):
			r.err = fmt.Errorf("unexpected content-type %q", r.header.Get("Content-Type"))
		case len(r.body) < 500:
			r.err = fmt.Errorf("web UI suspiciously small (%d bytes)", len(r.body))
		}
	}
	b.record("/", r)
}

func (b *Board) checkSettings(ctx context.Context) {
	r := b.get(ctx, "/settings", 3*time.Second)
	var s Settings
	if r.err == nil {
		if err := json.Unmarshal(r.body, &s); err != nil {
			r.err = fmt.Errorf("invalid JSON: %w", err)
		}
	}
	b.record("/settings", r)
	if r.err == nil {
		b.mu.Lock()
		b.settings, b.settingsRaw = &s, r.body
		b.mu.Unlock()
	}
}

// Once a lockup is confirmed, the monitor eases off so it doesn't add to the
// board's trouble: one /ping per lockupRetry, nothing else over HTTP.
const lockupRetry = 30 * time.Second

// struggling reports whether recent /ping checks are failing.
func (b *Board) struggling() (failing, lockedUp bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.pingFails > 0, b.activeLockup != nil
}

// dialBoard connects with SO_LINGER 0: closing a connection the board has
// stopped answering resets it at once, so a struggling board gets the slot
// back immediately instead of after the kernel's ~15-minute retry window.
func dialBoard(ctx context.Context, network, addr string, timeout time.Duration) (net.Conn, error) {
	c, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, addr)
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetLinger(0)
	}
	return c, err
}

// httpLoop runs every HTTP check sequentially so the board never sees more
// than one monitoring request at a time from this loop.
func (b *Board) httpLoop(ctx context.Context) {
	b.checkSettings(ctx)
	b.fetchState(ctx)
	b.checkPage(ctx)
	iv := b.glob.Intervals
	ping := time.NewTicker(iv.HTTPPing.Duration)
	state := time.NewTicker(iv.SystemState.Duration)
	page := time.NewTicker(iv.WebUI.Duration)
	settings := time.NewTicker(iv.Settings.Duration)
	defer ping.Stop()
	defer state.Stop()
	defer page.Stop()
	defer settings.Stop()
	b.checkPing(ctx)
	lastPing := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if _, locked := b.struggling(); locked && time.Since(lastPing) < lockupRetry {
				continue
			}
			lastPing = time.Now()
			b.checkPing(ctx)
		case <-state.C:
			if failing, _ := b.struggling(); !failing {
				b.fetchState(ctx)
			}
		case <-page.C:
			if failing, _ := b.struggling(); !failing {
				b.checkPage(ctx)
			}
		case <-settings.C:
			if failing, _ := b.struggling(); !failing {
				b.checkSettings(ctx)
			}
		}
	}
}

// ---- ICMP ----

func (b *Board) icmpLoop(ctx context.Context) {
	if b.pinger == nil {
		return
	}
	t := time.NewTicker(b.glob.Intervals.ICMP.Duration)
	defer t.Stop()
	for {
		rtt, err := b.pinger.Ping(hostOnly(b.cfg.Host), time.Second)
		b.mu.Lock()
		b.icmpSent++
		if err != nil {
			b.icmpLost++
			b.icmp.add(math.NaN())
			b.rec.add("icmp", map[string]bool{"lost": true})
		} else {
			b.icmp.add(ms(rtt))
			b.rec.add("icmp", map[string]float64{"ms": ms(rtt)})
		}
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ---- WebSocket ----

func (b *Board) wsLoop(ctx context.Context) {
	first := true
	// Reconnects back off exponentially (and stay slow while the board is
	// failing HTTP checks) so a struggling board isn't hammered.
	const minBackoff, maxBackoff = 2 * time.Second, time.Minute
	backoff := minBackoff
	wait := func() {
		d := backoff
		if failing, _ := b.struggling(); failing {
			d = max(d, lockupRetry)
		}
		sleepCtx(ctx, d)
		backoff = min(backoff*2, maxBackoff)
	}
	for ctx.Err() == nil {
		c, err := wsDial(b.cfg.Host, 3*time.Second)
		if err != nil {
			b.mu.Lock()
			b.wsErr = err.Error()
			b.mu.Unlock()
			wait()
			continue
		}
		connectedAt := time.Now()
		b.mu.Lock()
		b.wsConnected, b.wsLastMsg, b.wsErr = true, time.Now(), ""
		b.rec.add("ws", map[string]any{"connected": true})
		if !first {
			b.wsReconnects++
			b.addEvent("info", "WebSocket reconnected")
		}
		b.mu.Unlock()
		first = false

		stop := make(chan struct{})
		reason := make(chan string, 1)
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					c.Close()
					return
				case <-stop:
					return
				case <-t.C:
					b.mu.RLock()
					age := time.Since(b.wsLastMsg)
					b.mu.RUnlock()
					if age > wsStale {
						reason <- fmt.Sprintf("no messages for %s", age.Round(time.Second))
						c.Close()
						return
					}
					// Same keepalive the board's UI sends; the board echoes it.
					if err := c.writeText([]byte("{}")); err != nil {
						c.Close()
						return
					}
				}
			}
		}()
		var rerr error
		for {
			msg, err := c.read(10 * time.Second)
			if err != nil {
				rerr = err
				break
			}
			b.mu.Lock()
			b.wsLastMsg = time.Now()
			b.wsMessages++
			b.mu.Unlock()
			b.mergeState(msg, "ws")
		}
		close(stop)
		c.Close()
		select {
		case why := <-reason:
			rerr = errors.New(why)
		default:
		}
		if ctx.Err() != nil {
			return
		}
		b.mu.Lock()
		b.wsConnected, b.wsErr = false, rerr.Error()
		b.rec.add("ws", map[string]any{"connected": false, "err": rerr.Error()})
		b.addEvent("warn", "WebSocket lost: %v", rerr)
		b.mu.Unlock()
		if time.Since(connectedAt) > 30*time.Second {
			backoff = minBackoff // it had been healthy; reconnect promptly
		}
		wait()
	}
}

// ---- DDP ----

// ddpFrame builds a probe packet. It deliberately leaves PUSH unset, so it
// never makes the board output a frame, and uses sequence 0 ("not used"),
// so it can't disturb the board's gap counting for a real stream. Baldrick
// boards still count such packets and list their sender as a source.
func ddpFrame(offset uint32, data []byte) []byte {
	f := make([]byte, 10+len(data))
	f[0] = 0x40 // version 1, no PUSH
	f[1] = 0    // sequence not used
	f[3] = 1    // destination: default output device
	binary.BigEndian.PutUint32(f[4:], offset)
	binary.BigEndian.PutUint16(f[8:], uint16(len(data)))
	copy(f[10:], data)
	return f
}

func (b *Board) ddpLoop(ctx context.Context) {
	mode := b.probeMode()
	if mode == "off" {
		return
	}
	if !sleepCtx(ctx, 10*time.Second) { // let state arrive first
		return
	}
	t := time.NewTicker(b.glob.Intervals.DDPProbe.Duration)
	defer t.Stop()
	for {
		b.mu.RLock()
		tripped := b.probeTripped
		b.mu.RUnlock()
		if failing, _ := b.struggling(); !failing && !tripped {
			b.runProbe(ctx, mode)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-b.probeNow:
		}
	}
}

// runProbe checks that the board is accepting DDP. When another source is
// already streaming it judges that stream; otherwise it sends a short burst
// of black frames to an offset past every configured channel and watches the
// board's own receive counters. The board publishes those counters in ~5s
// windows and ignores a lone packet, hence a burst plus polling.
func (b *Board) runProbe(ctx context.Context, mode string) {
	pc := b.glob.DDPProbe
	res := DDPProbe{Time: time.Now()}
	st, err := b.fetchState(ctx)
	if err != nil {
		res.Skipped, res.Note = true, "cannot read system_state: "+err.Error()
		b.finishProbe(res)
		return
	}
	local := b.self
	var foreign []string
	for _, s := range st.Streaming.DDPSources {
		if s != local {
			foreign = append(foreign, s)
		}
	}
	if len(foreign) > 0 && mode != "always" {
		res.Passive = true
		res.OK = st.Streaming.DDPRecvd > 0
		res.Recvd, res.Dropped, res.Sources = st.Streaming.DDPRecvd, st.Streaming.DDPDropped, st.Streaming.DDPSources
		res.Note = fmt.Sprintf("live DDP from %s (not injecting): %.0f received, %.0f dropped, %.1f fps",
			strings.Join(foreign, ", "), res.Recvd, res.Dropped, st.FrameRate)
		b.finishProbe(res)
		return
	}
	// Don't inject while a show is playing or about to start, even in the
	// gap between sequences.
	// Never probe firmware the probe hasn't been proven safe on.
	if fw, _, _ := st.firmware(); !firmwareAtLeast(fw, pc.MinFirmware) {
		if fw == "" {
			fw = "unknown"
		}
		res.Skipped = true
		res.Note = fmt.Sprintf("probes are off for firmware %s: they only run from %s up (v3.5.4 crashed on every probe); live streams are still checked", fw, pc.MinFirmware)
		b.finishProbe(res)
		return
	}
	if mode != "always" && !pc.IgnoreFPP {
		if busy := b.fpps.busyFor(b.cfg.Name, st.FPP); busy != "" {
			res.Skipped, res.Note = true, busy
			b.finishProbe(res)
			return
		}
	}

	conn, err := net.Dial("udp", net.JoinHostPort(hostOnly(b.cfg.Host), "4048"))
	if err != nil {
		res.Skipped, res.Note = true, "cannot open UDP socket: "+err.Error()
		b.finishProbe(res)
		return
	}
	defer conn.Close()
	b.mu.Lock()
	b.rec.add("probe_start", map[string]any{"offset": pc.Offset, "packets": pc.Packets, "rate": pc.Rate})
	b.mu.Unlock()
	gap := time.Duration(float64(time.Second) / pc.Rate)
	sent := make(chan int, 1)
	var stop atomic.Bool
	go func() {
		n, frame := 0, ddpFrame(pc.Offset, make([]byte, 3))
		for i := 0; i < pc.Packets && ctx.Err() == nil && !stop.Load(); i++ {
			if _, err := conn.Write(frame); err == nil {
				n++
			}
			time.Sleep(gap)
		}
		sent <- n
	}()
	start := time.Now()
	deadline := start.Add(time.Duration(pc.Packets)*gap + 7*time.Second)
	polled := 0
	var pollErr error
	for time.Now().Before(deadline) && sleepCtx(ctx, 500*time.Millisecond) {
		st, err := b.fetchState(ctx)
		if err != nil {
			pollErr = err
			continue
		}
		polled++
		var others []string
		for _, s := range st.Streaming.DDPSources {
			if s != local {
				others = append(others, s)
			}
		}
		if len(others) > 0 && mode != "always" {
			// A show started mid-check: get out of its way at once.
			stop.Store(true)
			res.Sent = <-sent
			res.Passive = true
			res.OK = st.Streaming.DDPRecvd > 0
			res.Recvd, res.Dropped, res.Sources = st.Streaming.DDPRecvd, st.Streaming.DDPDropped, st.Streaming.DDPSources
			res.Note = fmt.Sprintf("live DDP from %s started during the check, so the probe stopped after %d packets", strings.Join(others, ", "), res.Sent)
			b.finishProbe(res)
			return
		}
		if st.Streaming.DDPRecvd > 0 || slices.Contains(st.Streaming.DDPSources, local) {
			res.OK = true
			res.DetectMs = ms(time.Since(start))
			res.Recvd, res.Dropped, res.Sources = st.Streaming.DDPRecvd, st.Streaming.DDPDropped, st.Streaming.DDPSources
			break
		}
	}
	res.Sent = <-sent
	if !res.OK && polled == 0 && pollErr != nil {
		// Never saw the counters, so this says nothing about DDP.
		res.Skipped, res.Note = true, "cannot read system_state: "+pollErr.Error()
		b.finishProbe(res)
		return
	}
	if res.OK {
		res.Note = fmt.Sprintf("board counted our DDP packets after %.1fs", res.DetectMs/1000)
	} else {
		res.Note = fmt.Sprintf("sent %d DDP packets to offset %d; board never reported receiving them", res.Sent, pc.Offset)
	}
	b.finishProbe(res)
}

func (b *Board) finishProbe(res DDPProbe) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rec.add("probe", res)
	// A skipped check says nothing about DDP: keep the last real result and
	// log only when the reason for skipping changes.
	if res.Skipped {
		reason, _, _ := strings.Cut(res.Note, ":")
		if b.probeSkipReason != reason {
			level := "warn"
			if strings.HasPrefix(res.Note, "FPP") {
				level = "info"
			}
			b.addEvent(level, "DDP check paused: %s", res.Note)
		}
		b.probeSkipReason = reason
		b.probeSkip = &res
		return
	}
	if b.probeSkipReason != "" && strings.HasPrefix(b.probeSkipReason, "FPP") {
		b.addEvent("info", "DDP checks resumed")
	}
	b.probeSkipReason, b.probeSkip = "", nil
	prev := b.probe
	b.probe = &res
	b.noteDDP(res)
	key := "fail"
	if res.OK {
		key = "ok"
	}
	b.probeCounts[key]++
	switch {
	case !res.OK && (prev == nil || prev.OK):
		b.addEvent("error", "DDP check failed: %s", res.Note)
	case res.OK && prev != nil && !prev.OK:
		b.addEvent("info", "DDP check recovered: %s", res.Note)
	case res.OK && prev == nil:
		b.addEvent("info", "DDP check OK: %s", res.Note)
	}
}

// ---- health & views ----

type StreamStats struct {
	Recvd   float64  `json:"recvd"`
	Dropped float64  `json:"dropped"`
	Sources []string `json:"sources"`
}

type PortView struct {
	Type     string `json:"type"`
	Models   int    `json:"models"`
	Channels int    `json:"channels"`
}

type BoardView struct {
	Name    string   `json:"name"`
	Host    string   `json:"host"`
	Status  string   `json:"status"` // up, degraded, down, unknown, maintenance
	Reasons []string `json:"reasons"`
	// Set while alerts for the board are silenced.
	Maintenance *Maintenance `json:"maintenance"`

	Hostname          string                 `json:"hostname"`
	Manufacturer      string                 `json:"manufacturer"`
	Model             string                 `json:"model"`
	BoardID           string                 `json:"board_id"`
	Firmware          string                 `json:"firmware"`
	FactoryFirmware   string                 `json:"factory_firmware"`
	AvailableFirmware string                 `json:"available_firmware"`
	UpdateAvailable   bool                   `json:"update_available"`
	Uptime            float64                `json:"uptime"`
	Reboots           uint64                 `json:"reboots"`
	Temperature       []float64              `json:"temperature"`
	RAMAvailable      float64                `json:"ram_available"`
	RAMMinimum        float64                `json:"ram_minimum"`
	RAMLargestBlock   float64                `json:"ram_largest_block"`
	OOMCount          float64                `json:"oom_count"`
	UIMessages        int                    `json:"ui_messages"`
	TestMode          bool                   `json:"test_mode"`
	Ports             []PortView             `json:"ports"`
	FPP               []FPPInstance          `json:"fpp"`
	StateAge          float64                `json:"state_age_s"`
	HaveState         bool                   `json:"have_state"`
	FieldUpdates      map[string]FieldUpdate `json:"field_updates"`

	ICMPAvailable bool                     `json:"icmp_available"`
	ICMP          Summary                  `json:"icmp"`
	ICMPSeries    []*float64               `json:"icmp_series"`
	ICMPTimes     []int64                  `json:"icmp_times"`
	ICMPSent      uint64                   `json:"icmp_sent"`
	ICMPLost      uint64                   `json:"icmp_lost"`
	HTTP          Summary                  `json:"http"`
	HTTPSeries    []*float64               `json:"http_series"`
	HTTPTimes     []int64                  `json:"http_times"`
	HTTPFails     int                      `json:"http_consecutive_failures"`
	Endpoints     map[string]EndpointStats `json:"endpoints"`

	WSConnected  bool    `json:"ws_connected"`
	WSAge        float64 `json:"ws_last_message_age_s"`
	WSReconnects uint64  `json:"ws_reconnects"`
	WSMessages   uint64  `json:"ws_messages"`
	WSError      string  `json:"ws_error,omitempty"`

	DDP               StreamStats       `json:"ddp"`
	SACN              StreamStats       `json:"sacn"`
	ArtNet            StreamStats       `json:"artnet"`
	FrameRate         float64           `json:"frame_rate"`
	NetworkThroughput float64           `json:"network_throughput"`
	ProbeOnly         bool              `json:"probe_only"` // the only DDP source is our own probe
	ProbeMode         string            `json:"probe_mode"`
	ProbeOverride     string            `json:"ddp_probe"` // this board's own setting; "" = default
	Probe             *DDPProbe         `json:"probe"`
	ProbePaused       *DDPProbe         `json:"probe_paused,omitempty"`
	ProbeCounts       map[string]uint64 `json:"probe_counts"`
	ShowRunning       bool              `json:"show_running"` // an FPP driving or reported by this board is running a show

	Events []Event `json:"events"`

	// Incidents still alerting (not acknowledged), newest first.
	Incidents     []Incident `json:"incidents"`
	IncidentTotal int        `json:"incident_total"`
}

func (b *Board) View() BoardView {
	b.mu.RLock()
	defer b.mu.RUnlock()
	st := b.st
	fw, factory, avail := st.firmware()
	v := BoardView{
		Name: b.cfg.Name, Host: b.cfg.Host,
		Manufacturer: st.BoardManufacturer, Model: st.BoardModel, BoardID: st.BoardID,
		Firmware: fw, FactoryFirmware: factory, AvailableFirmware: avail,
		UpdateAvailable: avail != "" && fw != "" && avail != fw,
		Uptime:          st.Uptime, Reboots: b.reboots, Temperature: st.Temperature,
		RAMAvailable: st.RAMAvailable, RAMMinimum: st.RAMMinimum, RAMLargestBlock: st.RAMLargestBlock,
		OOMCount: st.OOMCount, UIMessages: len(st.UIMessages), TestMode: st.TestModeActive,
		FPP: st.FPP, HaveState: b.haveState,

		ICMPAvailable: b.pinger != nil,
		ICMP:          summarize(b.icmp.last(b.icmpN)),
		ICMPSeries:    series(b.icmp.last(120)),
		ICMPTimes:     b.icmp.lastTimes(120),
		ICMPSent:      b.icmpSent, ICMPLost: b.icmpLost,
		HTTP:       summarize(b.httpPing.last(b.httpN)),
		HTTPSeries: series(b.httpPing.last(120)),
		HTTPTimes:  b.httpPing.lastTimes(120),
		HTTPFails:  b.pingFails,
		Endpoints:  map[string]EndpointStats{},

		WSConnected: b.wsConnected, WSReconnects: b.wsReconnects, WSMessages: b.wsMessages, WSError: b.wsErr,

		DDP:               StreamStats{st.Streaming.DDPRecvd, st.Streaming.DDPDropped, st.Streaming.DDPSources},
		SACN:              StreamStats{st.Streaming.SACNRecvd, st.Streaming.SACNDropped, st.Streaming.SACNSources},
		ArtNet:            StreamStats{st.Streaming.ArtNetRecvd, st.Streaming.ArtNetDropped, st.Streaming.ArtNetSources},
		FrameRate:         st.FrameRate,
		NetworkThroughput: st.NetworkThroughput,
		ProbeOnly:         len(st.Streaming.DDPSources) > 0 && !slices.ContainsFunc(st.Streaming.DDPSources, func(s string) bool { return s != b.self }),
		ProbeMode:         b.probeMode(),
		ProbeOverride:     b.cfg.DDPProbe,
		ProbeCounts:       map[string]uint64{"ok": b.probeCounts["ok"], "fail": b.probeCounts["fail"]},
	}
	if b.haveState {
		v.StateAge = time.Since(b.stateAt).Seconds()
	}
	if b.wsConnected {
		v.WSAge = time.Since(b.wsLastMsg).Seconds()
	}
	if b.probe != nil {
		p := *b.probe
		v.Probe = &p
	}
	if b.probeSkip != nil {
		p := *b.probeSkip
		v.ProbePaused = &p
	}
	// The board counts our probe packets as frames; that isn't a show.
	if v.ProbeOnly {
		v.FrameRate = 0
	}
	// With no stream at all, the board's frame-rate average is just decaying.
	if len(v.DDP.Sources)+len(v.SACN.Sources)+len(v.ArtNet.Sources) == 0 {
		v.FrameRate = 0
	}
	for k, e := range b.endpoints {
		v.Endpoints[k] = *e
	}
	v.FieldUpdates = make(map[string]FieldUpdate, len(b.fieldAt))
	for k, f := range b.fieldAt {
		v.FieldUpdates[k] = f
	}
	if s := b.settings; s != nil {
		v.Hostname = s.Hostname
		for _, p := range s.Ports {
			pv := PortView{Type: p.PortType, Models: len(p.Models)}
			for _, m := range p.Models {
				pv.Channels += m.NumChannels
			}
			v.Ports = append(v.Ports, pv)
		}
	}
	for i := len(b.events) - 1; i >= 0 && len(v.Events) < 15; i-- {
		v.Events = append(v.Events, b.events[i])
	}
	for i := len(b.incidents) - 1; i >= 0; i-- {
		if inc := b.incidents[i]; inc.Alerting() {
			v.Incidents = append(v.Incidents, *inc)
		}
	}
	v.IncidentTotal = len(b.incidents)
	if v.Incidents == nil {
		v.Incidents = []Incident{}
	}
	if v.Events == nil {
		v.Events = []Event{}
	}
	v.ShowRunning = b.fpps.showingFor(b.cfg.Name, st.FPP)
	v.Status, v.Reasons = b.health(&v)
	if m := b.maintenanceLocked(time.Now()); m != nil {
		mc := *m
		v.Maintenance = &mc
		// Keep the reasons so whoever is working on it can see what's wrong.
		v.Status = "maintenance"
	}
	return v
}

// health applies the alerting rules to a view (b.mu held).
func (b *Board) health(v *BoardView) (string, []string) {
	th := b.glob.th()
	var down, warn []string
	if v.HTTPFails >= th.HTTPFailDown {
		down = append(down, fmt.Sprintf("HTTP /ping failing (%d in a row)", v.HTTPFails))
	}
	if v.ICMPAvailable && v.ICMP.Count >= min(5, b.icmpN) && v.ICMP.Loss == 1 {
		down = append(down, "no ICMP replies")
	} else if v.ICMPAvailable && v.ICMP.Count >= min(10, b.icmpN) {
		if v.ICMP.Loss > th.ICMPLossWarn {
			warn = append(warn, fmt.Sprintf("ICMP loss %.0f%%", v.ICMP.Loss*100))
		}
		// With few samples p95 is just the max; one Wi-Fi blip shouldn't alert.
		if v.ICMP.Count >= 20 && v.ICMP.P95 > th.RTTWarnMs {
			warn = append(warn, fmt.Sprintf("ICMP p95 latency %.0f ms", v.ICMP.P95))
		}
	}
	if v.HTTP.Count >= min(5, b.httpN) && v.HTTP.P95 > th.RTTWarnMs*2 {
		warn = append(warn, fmt.Sprintf("HTTP p95 latency %.0f ms", v.HTTP.P95))
	}
	for _, ep := range []string{"/", "/system_state", "/settings"} {
		if e, ok := v.Endpoints[ep]; ok && !e.LastOK {
			warn = append(warn, fmt.Sprintf("GET %s failing: %s", ep, e.LastErr))
		}
	}
	if !v.WSConnected {
		warn = append(warn, "WebSocket disconnected")
	}
	if stale := max(30, 3*b.glob.Intervals.SystemState.Seconds()); v.HaveState && v.StateAge > stale {
		warn = append(warn, fmt.Sprintf("board state is %.0fs old", v.StateAge))
	}
	if p := v.Probe; p != nil && !p.OK {
		warn = append(warn, "DDP check failed")
	}
	// Only judge loss on an active stream: the board's trailing window after a
	// stream stops can hold a handful of packets and a misleading ratio.
	if tot := v.DDP.Recvd + v.DDP.Dropped; len(v.DDP.Sources) > 0 && tot >= 50 && v.DDP.Dropped/tot > th.DDPLossWarn {
		warn = append(warn, fmt.Sprintf("DDP loss %.1f%%", v.DDP.Dropped/tot*100))
	}
	for _, t := range v.Temperature {
		if t > th.TempWarnC {
			warn = append(warn, "temperature "+b.glob.fmtTemp(t))
		}
	}
	if v.HaveState && v.RAMMinimum > 0 && v.RAMMinimum < th.RAMMinWarnBytes {
		warn = append(warn, fmt.Sprintf("RAM low-water mark %.0f KB", v.RAMMinimum/1024))
	}
	if v.OOMCount > 0 {
		warn = append(warn, fmt.Sprintf("%.0f out-of-memory events", v.OOMCount))
	}
	if v.UIMessages > 0 {
		warn = append(warn, fmt.Sprintf("%d board UI message(s)", v.UIMessages))
	}
	var unacked []Incident
	for _, inc := range v.Incidents {
		switch {
		case inc.Ongoing && inc.Type == "lockup":
			down = append(down, fmt.Sprintf("%s since %s ago", inc.Title, agoText(inc.StartedAt)))
		case inc.Ongoing:
			warn = append(warn, fmt.Sprintf("%s since %s ago", inc.Title, agoText(inc.StartedAt)))
		default:
			unacked = append(unacked, inc)
		}
	}
	switch n := len(unacked); {
	case n == 1:
		warn = append(warn, fmt.Sprintf("%s %s ago, not acknowledged", unacked[0].Title, agoText(unacked[0].StartedAt)))
	case n > 1:
		warn = append(warn, fmt.Sprintf("%d incidents not acknowledged (latest: %s, %s ago)", n, unacked[0].Title, agoText(unacked[0].StartedAt)))
	}
	switch {
	case len(down) > 0:
		return "down", append(down, warn...)
	case !b.httpEverOK && !v.HaveState:
		return "unknown", append(warn, "waiting for first response")
	case len(warn) > 0:
		return "degraded", warn
	}
	return "up", []string{}
}

// ---- helpers ----

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func localIP(host string) string {
	c, err := net.Dial("udp", net.JoinHostPort(host, "4048"))
	if err != nil {
		return ""
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
}

func agoText(t time.Time) string {
	return time.Since(t).Round(time.Second).String()
}

func fmtDur(sec float64) string {
	return (time.Duration(sec) * time.Second).String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ---- history ----

func (b *Board) rollupLoop(ctx context.Context) {
	t := time.NewTicker(b.glob.History.Resolution.Duration)
	defer t.Stop()
	prune := time.NewTicker(time.Hour)
	defer prune.Stop()
	b.store.prune(time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case <-prune.C:
			b.store.prune(time.Now())
			b.pruneIncidents()
		case <-t.C:
			s := b.View().sample()
			cutoff := time.Now().Add(-b.glob.retention())
			b.mu.Lock()
			b.history = append(b.history, s)
			i := 0
			for i < len(b.history) && b.history[i].T.Before(cutoff) {
				i++
			}
			b.history = b.history[i:]
			j := 0
			for j < len(b.events) && b.events[j].Time.Before(cutoff) {
				j++
			}
			b.events = b.events[j:]
			b.mu.Unlock()
			b.store.append("samples", s.T, s)
			b.saveLastState()
		}
	}
}

func (v BoardView) sample() Sample {
	s := Sample{T: time.Now().UTC().Truncate(time.Second), Status: v.Status, V: map[string]float64{}}
	put := func(k string, x float64, ok bool) {
		if ok {
			s.V[k] = x
		}
	}
	if v.ICMPAvailable && v.ICMP.Count > 0 {
		ok := v.ICMP.Lost < v.ICMP.Count
		put("icmp_p50", v.ICMP.P50, ok)
		put("icmp_p95", v.ICMP.P95, ok)
		put("icmp_jitter", v.ICMP.Jitter, ok)
		put("icmp_loss", v.ICMP.Loss, true)
	}
	if v.HTTP.Count > 0 {
		ok := v.HTTP.Lost < v.HTTP.Count
		put("http_p50", v.HTTP.P50, ok)
		put("http_p95", v.HTTP.P95, ok)
		put("http_loss", v.HTTP.Loss, true)
	}
	if v.HaveState {
		for _, t := range v.Temperature[:min(1, len(v.Temperature))] {
			put("temp", t, true)
		}
		put("ram_available", v.RAMAvailable, true)
		put("ram_minimum", v.RAMMinimum, true)
		put("fps", v.FrameRate, true)
		if !v.ProbeOnly {
			put("ddp_recvd", v.DDP.Recvd, true)
			put("ddp_dropped", v.DDP.Dropped, true)
		}
	}
	put("show", b2f(v.ShowRunning), true)
	ws := 0.0
	if !v.WSConnected {
		ws = 1
	}
	put("ws_down", ws, true)
	if v.Probe != nil {
		ok := 0.0
		if v.Probe.OK {
			ok = 1
		}
		put("ddp_ok", ok, true)
	}
	return s
}

// History returns rollups and events from the last d, downsampled to at most
// points samples.
func (b *Board) History(d time.Duration, points int) ([]Sample, []Event) {
	cutoff := time.Now().Add(-d)
	b.mu.RLock()
	defer b.mu.RUnlock()
	i := sort.Search(len(b.history), func(i int) bool { return !b.history[i].T.Before(cutoff) })
	samples := downsample(append([]Sample(nil), b.history[i:]...), points)
	var ev []Event
	for k := len(b.events) - 1; k >= 0 && b.events[k].Time.After(cutoff) && len(ev) < 1000; k-- {
		ev = append(ev, b.events[k])
	}
	return samples, ev
}
