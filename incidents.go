package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Incident types:
//
//	reboot  the board restarted (Kind: unexpected, firmware_update, settings_change)
//	lockup  HTTP stopped working (503s, timeouts, refused); may end in a reboot
//	ddp     HTTP works but the board stopped processing DDP
//	manual  diagnostics captured on request
type Incident struct {
	ID    string `json:"id"`
	Board string `json:"board"`
	Host  string `json:"host"`
	Type  string `json:"type"`
	Kind  string `json:"kind"`
	Title string `json:"title"`

	StartedAt  time.Time  `json:"started_at"`
	DetectedAt time.Time  `json:"detected_at"`
	Ongoing    bool       `json:"ongoing"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
	DurationS  float64    `json:"duration_s,omitempty"`
	Resolution string     `json:"resolution,omitempty"`

	// Reboot details: set for reboots and for lockups that ended in one.
	Rebooted       bool       `json:"rebooted"`
	RebootAt       *time.Time `json:"reboot_at,omitempty"`
	LastSeenAt     time.Time  `json:"last_seen_at"`
	SilenceS       float64    `json:"silence_s,omitempty"`
	PrevUptimeS    float64    `json:"prev_uptime_s,omitempty"`
	NewUptimeS     float64    `json:"new_uptime_s,omitempty"`
	MonitorOffline bool       `json:"monitor_offline,omitempty"`

	Cause       string       `json:"cause"`
	Findings    []string     `json:"findings"`
	Before      Snapshot     `json:"before"`
	After       *Snapshot    `json:"after,omitempty"`
	Diagnostics []DiagResult `json:"diagnostics,omitempty"`
	Ack         *Ack         `json:"ack"`

	Timeline []RecPoint `json:"timeline,omitempty"`
}

// Alerting reports whether the incident should show as an active issue.
func (i *Incident) Alerting() bool { return i.Ack == nil }

type Ack struct {
	At   time.Time `json:"at"`
	Note string    `json:"note,omitempty"`
	By   string    `json:"by,omitempty"`
	Auto bool      `json:"auto,omitempty"`
}

// Snapshot is the board state at one moment.
type Snapshot struct {
	At              time.Time         `json:"at"`
	Uptime          float64           `json:"uptime"`
	Firmware        string            `json:"firmware"`
	RAMAvailable    float64           `json:"ram_available"`
	RAMMinimum      float64           `json:"ram_minimum"`
	RAMLargestBlock float64           `json:"ram_largest_block"`
	OOMCount        float64           `json:"oom_count"`
	Temperature     []float64         `json:"temperature"`
	FrameRate       float64           `json:"frame_rate"`
	Throughput      float64           `json:"network_throughput"`
	DDP             StreamStats       `json:"ddp"`
	SACN            StreamStats       `json:"sacn"`
	ArtNet          StreamStats       `json:"artnet"`
	TestMode        bool              `json:"test_mode"`
	FXActive        string            `json:"fx_active,omitempty"`
	UIMessages      []json.RawMessage `json:"ui_messages,omitempty"`
}

func snapshotOf(st *SystemState, at time.Time) Snapshot {
	fw, _, _ := st.firmware()
	s := st.Streaming
	return Snapshot{
		At: at, Uptime: st.Uptime, Firmware: fw,
		RAMAvailable: st.RAMAvailable, RAMMinimum: st.RAMMinimum, RAMLargestBlock: st.RAMLargestBlock,
		OOMCount: st.OOMCount, Temperature: st.Temperature, FrameRate: st.FrameRate, Throughput: st.NetworkThroughput,
		DDP:      StreamStats{s.DDPRecvd, s.DDPDropped, s.DDPSources},
		SACN:     StreamStats{s.SACNRecvd, s.SACNDropped, s.SACNSources},
		ArtNet:   StreamStats{s.ArtNetRecvd, s.ArtNetDropped, s.ArtNetSources},
		TestMode: st.TestModeActive, FXActive: st.FXActive, UIMessages: st.UIMessages,
	}
}

// DiagResult is one check from a diagnostic sweep.
type DiagResult struct {
	T      time.Time `json:"t"`
	Check  string    `json:"check"`
	OK     bool      `json:"ok"`
	Status int       `json:"status,omitempty"`
	Ms     float64   `json:"ms"`
	Detail string    `json:"detail,omitempty"`
}

func newIncidentID(t time.Time) string {
	b := make([]byte, 3)
	rand.Read(b)
	return t.UTC().Format("20060102T150405") + "-" + hex.EncodeToString(b)
}

// ======== persistence ========

func (b *Board) incidentDir() string {
	if b.store.dir == "" {
		return ""
	}
	return filepath.Join(b.store.dir, "incidents")
}

func (b *Board) incidentRetention() time.Duration {
	return time.Duration(b.glob.Incidents.RetentionDays) * 24 * time.Hour
}

// storedTimeline returns what has been saved for an incident so far.
func (b *Board) storedTimeline(id string) []RecPoint {
	if tl, ok := b.timelines[id]; ok {
		return tl
	}
	dir := b.incidentDir()
	if dir == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(dir, id+".json"))
	if err != nil {
		return nil
	}
	var full Incident
	if json.Unmarshal(data, &full) != nil {
		return nil
	}
	return full.Timeline
}

// persist saves the incident, extending its stored timeline with anything
// the recorder has seen since (b.mu held).
func (b *Board) persist(inc *Incident, tl []RecPoint) {
	if tl == nil {
		tl = b.storedTimeline(inc.ID)
		from := inc.StartedAt.Add(-b.glob.Incidents.RecorderWindow.Duration)
		if n := len(tl); n > 0 {
			from = tl[n-1].T.Add(time.Nanosecond)
		}
		tl = append(tl, b.rec.since(from)...)
	}
	dir := b.incidentDir()
	if dir == "" {
		b.timelines[inc.ID] = tl
		return
	}
	full := *inc
	full.Timeline = tl
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[%s] incident save: %v", b.cfg.Name, err)
		return
	}
	data, _ := json.Marshal(full)
	tmp := filepath.Join(dir, inc.ID+".json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err == nil {
		os.Rename(tmp, filepath.Join(dir, inc.ID+".json"))
	}
}

// loadIncidents reads incident summaries from disk, dropping expired ones
// and resuming any that were still ongoing.
func (b *Board) loadIncidents() {
	dir := b.incidentDir()
	if dir == "" {
		return
	}
	names, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	cutoff := time.Now().Add(-b.incidentRetention())
	for _, n := range names {
		data, err := os.ReadFile(n)
		if err != nil {
			continue
		}
		var inc Incident
		if json.Unmarshal(data, &inc) != nil {
			continue
		}
		if inc.DetectedAt.Before(cutoff) {
			os.Remove(n)
			continue
		}
		inc.Timeline = nil
		inc.Board, inc.Host = b.cfg.Name, b.cfg.Host
		b.incidents = append(b.incidents, &inc)
	}
	sort.Slice(b.incidents, func(i, j int) bool { return b.incidents[i].DetectedAt.Before(b.incidents[j].DetectedAt) })
	for _, inc := range b.incidents {
		if inc.Ongoing && inc.Type == "lockup" {
			b.activeLockup = inc
		}
		if inc.Ongoing && inc.Type == "ddp" {
			b.activeDDP = inc
		}
	}
}

func (b *Board) pruneIncidents() {
	cutoff := time.Now().Add(-b.incidentRetention())
	b.mu.Lock()
	defer b.mu.Unlock()
	keep := b.incidents[:0]
	for _, inc := range b.incidents {
		if inc.DetectedAt.Before(cutoff) && !inc.Ongoing {
			if dir := b.incidentDir(); dir != "" {
				os.Remove(filepath.Join(dir, inc.ID+".json"))
			}
			delete(b.timelines, inc.ID)
			continue
		}
		keep = append(keep, inc)
	}
	b.incidents = keep
}

// Incident returns a copy of an incident including its timeline so far.
func (b *Board) Incident(id string) (*Incident, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, inc := range b.incidents {
		if inc.ID == id {
			c := *inc
			c.Timeline = b.storedTimeline(id)
			if c.Ongoing && len(c.Timeline) > 0 {
				c.Timeline = append(c.Timeline, b.rec.since(c.Timeline[len(c.Timeline)-1].T.Add(time.Nanosecond))...)
			}
			return &c, true
		}
	}
	return nil, false
}

func (b *Board) Incidents() []Incident {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Incident, 0, len(b.incidents))
	for _, inc := range b.incidents {
		out = append(out, *inc)
	}
	return out
}

var errNoIncident = errors.New("no such incident")

// SetAck acknowledges (ack != nil) or reopens (ack == nil) an incident.
func (b *Board) SetAck(id string, ack *Ack) (Incident, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, inc := range b.incidents {
		if inc.ID != id {
			continue
		}
		inc.Ack = ack
		b.persist(inc, nil)
		when := inc.StartedAt.Local().Format("Jan 2 15:04:05")
		if ack != nil {
			b.addEvent("info", "%s at %s acknowledged%s", inc.Title, when, noteSuffix(ack.Note))
		} else {
			b.addEvent("info", "%s at %s reopened", inc.Title, when)
		}
		return *inc, nil
	}
	return Incident{}, errNoIncident
}

// AckAll acknowledges every incident still alerting, except ones in
// progress, and returns how many it acknowledged.
func (b *Board) AckAll(ack Ack) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, inc := range b.incidents {
		if inc.Ack != nil || inc.Ongoing {
			continue
		}
		a := ack
		inc.Ack = &a
		b.persist(inc, nil)
		n++
	}
	if n > 0 {
		b.addEvent("info", "%d incident(s) acknowledged%s", n, noteSuffix(ack.Note))
	}
	return n
}

func noteSuffix(n string) string {
	if n == "" {
		return ""
	}
	return ": " + n
}

func (b *Board) alert(inc *Incident, event, severity, msg string) {
	b.glob.notify().Send(Alert{
		Event: event, Severity: severity, Board: b.cfg.Name, Host: b.cfg.Host,
		Title: fmt.Sprintf("%s: %s", b.cfg.Name, inc.Title), Message: msg, Incident: inc.ID,
	})
}

// ======== last-seen state (reboots while the monitor was down) ========

type lastState struct {
	Uptime   float64   `json:"uptime"`
	At       time.Time `json:"at"` // when that uptime was observed
	Firmware string    `json:"firmware"`
	Settings int64     `json:"last_settings_update"`
}

func (b *Board) saveLastState() {
	b.mu.RLock()
	if !b.haveState || b.store.dir == "" {
		b.mu.RUnlock()
		return
	}
	fw, _, _ := b.st.firmware()
	ls := lastState{Uptime: b.st.Uptime, At: b.fieldAt["uptime"].At, Firmware: fw, Settings: b.st.LastSettingsUpdate}
	dir := b.store.dir
	b.mu.RUnlock()
	data, _ := json.Marshal(ls)
	tmp := filepath.Join(dir, "laststate.json.tmp")
	if os.WriteFile(tmp, data, 0o644) == nil {
		os.Rename(tmp, filepath.Join(dir, "laststate.json"))
	}
}

func (b *Board) loadLastState() *lastState {
	if b.store.dir == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(b.store.dir, "laststate.json"))
	if err != nil {
		return nil
	}
	var ls lastState
	if json.Unmarshal(data, &ls) != nil || ls.At.IsZero() {
		return nil
	}
	return &ls
}

// ======== reboots ========

// rebootGrace absorbs uptime granularity and update lag when comparing
// boot-time estimates.
const rebootGrace = 30 * time.Second

// onReboot records a reboot (b.mu held). old is the last state seen before
// it (nil if the monitor wasn't running; prev then holds what was saved).
func (b *Board) onReboot(old *SystemState, prev *lastState, oldUptimeAt time.Time, st *SystemState, now time.Time) {
	b.reboots++
	rebootAt := now.Add(-secs(st.Uptime))
	after := snapshotOf(st, now)

	var oldFW string
	lastSeen, prevUptime := b.stateAt, 0.0
	if old != nil {
		oldFW, _, _ = old.firmware()
		prevUptime = old.Uptime + b.stateAt.Sub(oldUptimeAt).Seconds()
	} else {
		oldFW, lastSeen, prevUptime = prev.Firmware, prev.At, prev.Uptime
	}
	newFW, _, _ := st.firmware()

	// A lockup that ends in a reboot is one incident, not two.
	if lk := b.lockupDuring(rebootAt); lk != nil {
		lk.Rebooted, lk.RebootAt, lk.NewUptimeS, lk.PrevUptimeS, lk.After = true, &rebootAt, st.Uptime, prevUptime, &after
		if !lk.Ongoing {
			lk.Resolution = "Recovered after a reboot"
		}
		b.addEvent("warn", "Reboot detected while it was not responding (uptime was %s)", fmtDur(prevUptime))
		b.analyzeLockup(lk)
		b.persist(lk, nil)
		return
	}

	inc := &Incident{
		ID: newIncidentID(now), Board: b.cfg.Name, Host: b.cfg.Host, Type: "reboot",
		StartedAt: rebootAt, DetectedAt: now, Rebooted: true, RebootAt: &rebootAt,
		LastSeenAt: lastSeen, PrevUptimeS: prevUptime, NewUptimeS: st.Uptime, After: &after,
		MonitorOffline: old == nil,
	}
	inc.SilenceS = math.Max(0, rebootAt.Sub(lastSeen).Seconds())
	if old != nil {
		inc.Before = snapshotOf(old, b.stateAt)
	} else {
		inc.Before = Snapshot{At: prev.At, Uptime: prev.Uptime, Firmware: prev.Firmware}
	}

	switch {
	case newFW != "" && oldFW != "" && newFW != oldFW:
		inc.Kind, inc.Title = "firmware_update", "Rebooted into new firmware "+newFW
		inc.Cause = fmt.Sprintf("Firmware update from %s to %s.", oldFW, newFW)
		inc.Ack = &Ack{At: now, Auto: true, Note: "Expected: firmware update"}
	// Only a settings change seen while the board was still running counts:
	// last_settings_update also changes on every boot, so comparing it
	// across the reboot would excuse every crash.
	case b.settingsChangedSince(rebootAt.Add(-5 * time.Minute)):
		inc.Kind, inc.Title = "settings_change", "Rebooted after a settings change"
		inc.Cause = "The board's settings changed shortly before it rebooted, so this was probably intentional."
		inc.Ack = &Ack{At: now, Auto: true, Note: "Probably intentional: followed a settings change"}
	default:
		inc.Kind, inc.Title = "unexpected", "Unexpected reboot"
	}

	var tl []RecPoint
	if !inc.MonitorOffline {
		tl = b.rec.since(lastSeen.Add(-b.glob.Incidents.RecorderWindow.Duration))
	}
	b.analyzeReboot(inc, tl)
	b.incidents = append(b.incidents, inc)
	b.persist(inc, tl)
	b.addEvent(map[bool]string{true: "error", false: "info"}[inc.Kind == "unexpected"],
		"%s (uptime was %s). %s", inc.Title, fmtDur(prevUptime), inc.Cause)
	if inc.Kind == "unexpected" {
		b.alert(inc, "reboot", "error", fmt.Sprintf("Rebooted around %s after %s of uptime. %s",
			rebootAt.Local().Format("Jan 2 15:04:05"), fmtDur(prevUptime), inc.Cause))
	}
}

// lockupDuring returns the lockup a reboot belongs to: one in progress, or
// one that ended right around the time the board booted.
func (b *Board) lockupDuring(rebootAt time.Time) *Incident {
	if lk := b.activeLockup; lk != nil && rebootAt.After(lk.StartedAt.Add(-rebootGrace)) {
		return lk
	}
	for i := len(b.incidents) - 1; i >= 0; i-- {
		inc := b.incidents[i]
		if inc.Type == "lockup" && !inc.Rebooted && inc.ResolvedAt != nil &&
			rebootAt.After(inc.StartedAt.Add(-rebootGrace)) && rebootAt.Before(inc.ResolvedAt.Add(rebootGrace)) {
			return inc
		}
	}
	return nil
}

func (b *Board) settingsChangedSince(t time.Time) bool {
	for i := len(b.events) - 1; i >= 0 && b.events[i].Time.After(t); i-- {
		if b.events[i].Msg == "Board settings were changed" {
			return true
		}
	}
	return false
}

func secs(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// ======== lockups ========

// startLockup opens a lockup incident when HTTP stops working, then runs a
// diagnostic sweep so the failure mode is captured while it's happening.
func (b *Board) startLockup(ctx context.Context, firstErr error) {
	now := time.Now()
	b.mu.Lock()
	if b.activeLockup != nil {
		b.mu.Unlock()
		return
	}
	inc := &Incident{
		ID: newIncidentID(now), Board: b.cfg.Name, Host: b.cfg.Host, Type: "lockup", Kind: "lockup",
		Title: "Stopped responding", DetectedAt: now, Ongoing: true, LastSeenAt: b.lastHTTPOK,
	}
	inc.StartedAt = b.firstFailAt
	if inc.StartedAt.IsZero() {
		inc.StartedAt = now
	}
	if b.haveState {
		inc.Before = snapshotOf(&b.st, b.stateAt)
	}
	tl := b.rec.since(inc.StartedAt.Add(-b.glob.Incidents.RecorderWindow.Duration))
	b.activeLockup = inc
	b.incidents = append(b.incidents, inc)
	b.persist(inc, tl)
	b.addEvent("error", "Stopped responding to HTTP: %v", firstErr)
	b.mu.Unlock()

	diag := b.diagSweep(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()
	inc.Diagnostics = append(inc.Diagnostics, diag...)
	b.analyzeLockup(inc)
	b.persist(inc, nil)
	b.alert(inc, "lockup", "error", inc.Cause)
}

// endLockup closes the active lockup when HTTP works again (b.mu held).
func (b *Board) endLockup(now time.Time) {
	inc := b.activeLockup
	if inc == nil {
		return
	}
	b.activeLockup = nil
	inc.Ongoing = false
	inc.ResolvedAt = &now
	inc.DurationS = now.Sub(inc.StartedAt).Seconds()
	d := fmtDur(inc.DurationS)
	if inc.Rebooted {
		inc.Resolution = "Recovered after a reboot"
	} else {
		inc.Resolution = "Recovered on its own"
	}
	b.analyzeLockup(inc)
	b.addEvent("info", "Responding again after %s (%s)", d, strings.ToLower(inc.Resolution))
	b.persist(inc, nil)
	b.alert(inc, "recovered", "info", fmt.Sprintf("Responding again after %s. %s.", d, inc.Resolution))
}

// diagSweep probes every way into the board once, keeping status codes and
// response bodies so a 503 page or similar is preserved.
func (b *Board) diagSweep(ctx context.Context) []DiagResult {
	var out []DiagResult
	addr := httpAddr(b.cfg.Host)
	start := time.Now()
	c, err := dialBoard(ctx, "tcp", addr, 3*time.Second)
	d := DiagResult{T: start, Check: "tcp connect " + addr, Ms: ms(time.Since(start)), OK: err == nil}
	if err != nil {
		d.Detail = err.Error()
	} else {
		c.Close()
	}
	out = append(out, d)
	for _, path := range []string{"/ping", "/system_state", "/settings", "/"} {
		r := b.get(ctx, path, 4*time.Second)
		d := DiagResult{T: time.Now(), Check: "GET " + path, OK: r.err == nil, Status: r.code, Ms: ms(r.elapsed)}
		switch {
		case r.err != nil && len(r.body) > 0:
			d.Detail = r.err.Error() + ": " + bodySnippet(r.body)
		case r.err != nil:
			d.Detail = r.err.Error()
		default:
			d.Detail = fmt.Sprintf("%d bytes", len(r.body))
		}
		out = append(out, d)
	}
	if b.pinger != nil {
		for i := 0; i < 5; i++ {
			rtt, err := b.pinger.Ping(hostOnly(b.cfg.Host), time.Second)
			d := DiagResult{T: time.Now(), Check: "ICMP echo", OK: err == nil, Ms: ms(rtt)}
			if err != nil {
				d.Detail = err.Error()
			}
			out = append(out, d)
		}
	}
	b.mu.Lock()
	for _, d := range out {
		b.rec.add("diag", d)
	}
	b.mu.Unlock()
	return out
}

func bodySnippet(body []byte) string {
	s := strings.Join(strings.Fields(string(body)), " ")
	return truncate(s, 300)
}

// ======== DDP ========

// noteDDP tracks DDP check results and opens or closes a DDP incident
// (b.mu held). Only checks made while HTTP works count, so a lockup isn't
// reported twice.
func (b *Board) noteDDP(res DDPProbe) {
	now := time.Now()
	if res.OK {
		b.ddpFails = 0
		if inc := b.activeDDP; inc != nil {
			b.activeDDP = nil
			inc.Ongoing, inc.ResolvedAt = false, &now
			inc.DurationS = now.Sub(inc.StartedAt).Seconds()
			inc.Resolution = "DDP processing resumed"
			inc.Title = fmt.Sprintf("Not processing DDP for %s", fmtDur(inc.DurationS))
			b.persist(inc, nil)
			b.alert(inc, "recovered", "info", "DDP processing resumed after "+fmtDur(inc.DurationS)+".")
		}
		return
	}
	if b.pingFails > 0 || b.activeLockup != nil {
		return
	}
	b.ddpFails++
	if b.ddpFails < 2 || b.activeDDP != nil {
		return
	}
	inc := &Incident{
		ID: newIncidentID(now), Board: b.cfg.Name, Host: b.cfg.Host, Type: "ddp", Kind: "ddp",
		Title: "Not processing DDP", StartedAt: now, DetectedAt: now, Ongoing: true, LastSeenAt: b.stateAt,
	}
	if b.haveState {
		inc.Before = snapshotOf(&b.st, b.stateAt)
	}
	inc.Cause = "The board answers HTTP but did not count DDP packets in two checks in a row: " + res.Note
	inc.Findings = append(inc.Findings, b.leadUpFindings(inc, b.rec.since(now.Add(-b.glob.Incidents.RecorderWindow.Duration)), now)...)
	b.activeDDP = inc
	b.incidents = append(b.incidents, inc)
	b.persist(inc, b.rec.since(now.Add(-b.glob.Incidents.RecorderWindow.Duration)))
	b.addEvent("error", "Not processing DDP: %s", res.Note)
	b.alert(inc, "ddp", "warn", inc.Cause)
}

// ======== manual capture ========

// Capture freezes the recorder and runs a diagnostic sweep on request.
func (b *Board) Capture(ctx context.Context, note string) *Incident {
	now := time.Now()
	diag := b.diagSweep(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	inc := &Incident{
		ID: newIncidentID(now), Board: b.cfg.Name, Host: b.cfg.Host, Type: "manual", Kind: "manual",
		Title: "Manual diagnostics capture", StartedAt: now, DetectedAt: now, LastSeenAt: b.stateAt,
		Diagnostics: diag, Ack: &Ack{At: now, Auto: true, Note: "Manual capture"},
	}
	if note != "" {
		inc.Title += ": " + truncate(note, 80)
	}
	if b.haveState {
		inc.Before = snapshotOf(&b.st, b.stateAt)
	}
	tl := b.rec.since(now.Add(-b.glob.Incidents.RecorderWindow.Duration))
	inc.Findings = b.leadUpFindings(inc, tl, now)
	inc.Cause = "Captured on request."
	b.incidents = append(b.incidents, inc)
	b.persist(inc, tl)
	b.addEvent("info", "Diagnostics captured%s", noteSuffix(note))
	return inc
}

// ======== analysis ========

type leadUp struct {
	ramFirst, ramLast, blkMin, tempMax float64
	ramSpan                            time.Duration
	oomEvent, probeNear                bool
	pings                              []float64 // last 2 minutes, NaN = lost
}

// scanLeadUp extracts memory, temperature and network signals recorded
// between from and until.
func scanLeadUp(tl []RecPoint, from, until time.Time) leadUp {
	l := leadUp{ramFirst: math.NaN(), ramLast: math.NaN(), blkMin: math.Inf(1), tempMax: math.Inf(-1)}
	var ramFirstT time.Time
	lastUptime := -1.0
	for _, p := range tl {
		if p.T.Before(from) {
			continue
		}
		if p.T.After(until) {
			break
		}
		switch p.K {
		case "state":
			var v struct {
				D map[string]json.RawMessage `json:"d"`
			}
			if json.Unmarshal(p.V, &v) != nil {
				continue
			}
			var x float64
			// Uptime going backwards means an earlier boot: start over.
			if raw, ok := v.D["uptime"]; ok && json.Unmarshal(raw, &x) == nil {
				if x+5 < lastUptime {
					l.ramFirst, l.ramLast, l.blkMin, l.tempMax = math.NaN(), math.NaN(), math.Inf(1), math.Inf(-1)
				}
				lastUptime = x
			}
			if raw, ok := v.D["ram_available"]; ok && json.Unmarshal(raw, &x) == nil {
				if math.IsNaN(l.ramFirst) {
					l.ramFirst, ramFirstT = x, p.T
				}
				l.ramLast, l.ramSpan = x, p.T.Sub(ramFirstT)
			}
			if raw, ok := v.D["ram_largest_block"]; ok && json.Unmarshal(raw, &x) == nil {
				l.blkMin = math.Min(l.blkMin, x)
			}
			var ts []float64
			if raw, ok := v.D["temperature"]; ok && json.Unmarshal(raw, &ts) == nil && len(ts) > 0 {
				l.tempMax = math.Max(l.tempMax, ts[0])
			}
		case "icmp":
			if p.T.After(until.Add(-2 * time.Minute)) {
				var v struct {
					Ms   *float64 `json:"ms"`
					Lost bool     `json:"lost"`
				}
				json.Unmarshal(p.V, &v)
				if v.Lost || v.Ms == nil {
					l.pings = append(l.pings, math.NaN())
				} else {
					l.pings = append(l.pings, *v.Ms)
				}
			}
		case "probe_start": // a burst lasts ~3s
			if p.T.After(until.Add(-5 * time.Second)) {
				l.probeNear = true
			}
		case "event":
			var e Event
			if json.Unmarshal(p.V, &e) == nil && strings.HasPrefix(e.Msg, "Out-of-memory") {
				l.oomEvent = true
			}
		}
	}
	return l
}

// leadUpFindings describes memory, temperature, load and network conditions
// before `until`, and returns a cause when one stands out.
func (b *Board) leadUpFindings(inc *Incident, tl []RecPoint, until time.Time) []string {
	f, _ := b.leadUp(inc, tl, until)
	return f
}

func (b *Board) leadUp(inc *Incident, tl []RecPoint, until time.Time) (f []string, cause string) {
	th := b.glob.th()
	add := func(format string, a ...any) { f = append(f, fmt.Sprintf(format, a...)) }
	setCause := func(c string) {
		if cause == "" {
			cause = c
		}
	}
	bs := inc.Before
	// Only this boot's readings: an earlier boot's memory says nothing now.
	var from time.Time
	if !bs.At.IsZero() {
		from = bs.At.Add(-secs(bs.Uptime) - time.Second)
	}
	l := scanLeadUp(tl, from, until)
	if math.IsNaN(l.ramLast) {
		l.ramLast = bs.RAMAvailable
	}
	if math.IsInf(l.blkMin, 1) {
		l.blkMin = bs.RAMLargestBlock
	}
	for _, t := range bs.Temperature {
		l.tempMax = math.Max(l.tempMax, t)
	}

	if l.oomEvent || bs.OOMCount > 0 {
		add("The board reported out-of-memory events (OOM count %.0f).", bs.OOMCount)
		setCause("Out of memory: the board reported OOM events.")
	}
	if l.ramLast > 0 && (l.ramLast < 25_000 || (l.blkMin > 0 && l.blkMin < 12_000)) {
		add("Free memory was critically low: %.0f KB free, largest block %.0f KB.", l.ramLast/1024, l.blkMin/1024)
		setCause("Heap exhaustion: free memory was critically low.")
	}
	if l.ramFirst > 0 && l.ramSpan >= 3*time.Minute && (l.ramFirst-l.ramLast)/l.ramFirst > 0.25 {
		add("Free memory fell from %.0f KB to %.0f KB over %s, which looks like a leak.", l.ramFirst/1024, l.ramLast/1024, l.ramSpan.Round(time.Second))
		setCause("Possible memory leak: free memory fell steadily beforehand.")
	} else if l.ramLast > 0 && cause == "" {
		add("Memory looked healthy: %.0f KB free (low-water %.0f KB since boot), largest block %.0f KB.", l.ramLast/1024, bs.RAMMinimum/1024, l.blkMin/1024)
	}
	if !math.IsInf(l.tempMax, -1) {
		if l.tempMax > th.TempWarnC {
			add("Temperature was high: %s.", b.glob.fmtTemp(l.tempMax))
			setCause("Overheating: the board reached " + b.glob.fmtTemp(l.tempMax) + ".")
		} else {
			add("Temperature was normal (%s).", b.glob.fmtTemp(l.tempMax))
		}
	}
	if s := summarize(l.pings); s.Count >= 10 && (s.Loss > 0.05 || s.P95 > th.RTTWarnMs) {
		add("The network was degraded in the 2 minutes before: ICMP loss %.0f%%, p95 %.1f ms.", s.Loss*100, s.P95)
	}
	if len(bs.DDP.Sources) > 0 || bs.DDP.Recvd > 0 {
		add("It was receiving DDP from %s (%.0f packets in the board's last window, %.0f dropped, %.1f fps).", orNone(bs.DDP.Sources), bs.DDP.Recvd, bs.DDP.Dropped, bs.FrameRate)
	} else if !bs.At.IsZero() {
		add("No DDP stream was active.")
	}
	if len(bs.SACN.Sources) > 0 || len(bs.ArtNet.Sources) > 0 {
		add("It was also receiving sACN/Art-Net (%s).", orNone(append(append([]string(nil), bs.SACN.Sources...), bs.ArtNet.Sources...)))
	}
	if bs.TestMode {
		add("A test pattern was running.")
	}
	if bs.FXActive != "" {
		add("CunningFX effect %q was active.", bs.FXActive)
	}
	if l.probeNear && inc.Type != "ddp" {
		add("A monitor DDP probe was in progress at the time. If this repeats, set the board's DDP check to \"off\" to rule the probe out.")
	}
	return f, cause
}

func (b *Board) analyzeReboot(inc *Incident, tl []RecPoint) {
	var f []string
	add := func(format string, a ...any) { f = append(f, fmt.Sprintf(format, a...)) }
	cause := inc.Cause
	if inc.MonitorOffline {
		add("The reboot happened while the monitor wasn't running (last seen %s), so only minute-level history is available.", inc.LastSeenAt.Local().Format("Jan 2 15:04:05"))
		if cause == "" {
			cause = "Unknown: the reboot happened while the monitor was offline."
		}
	} else {
		lf, lc := b.leadUp(inc, tl, inc.LastSeenAt)
		f = append(f, lf...)
		if cause == "" {
			cause = lc
		}
		// How did it go quiet?
		var ok, lost, httpFail int
		for _, p := range tl {
			if !p.T.After(inc.LastSeenAt) || !p.T.Before(*inc.RebootAt) {
				continue
			}
			switch p.K {
			case "icmp":
				if strings.Contains(string(p.V), "lost") {
					lost++
				} else {
					ok++
				}
			case "http":
				if strings.Contains(string(p.V), `"ok":false`) {
					httpFail++
				}
			}
		}
		silence := secs(inc.SilenceS).Round(time.Second)
		var mode string
		switch {
		case inc.SilenceS >= 10 && ok+lost > 0 && ok*2 >= ok+lost:
			add("For %s before it booted, it still answered pings (%d of %d) but HTTP didn't (%d failed checks).", silence, ok, ok+lost, httpFail)
			mode = "Firmware hang: the network stack stayed up but the application stopped responding until it was reset."
		case inc.SilenceS >= 30:
			add("It was completely unreachable for %s before booting (%d of %d pings lost).", silence, lost, ok+lost)
			mode = "Power or network interruption: the board vanished entirely before booting. Check its power supply/PoE and switch, and whether other devices dropped too."
		default:
			add("It went silent and was booting again within %s, with no warning beforehand.", silence)
			mode = "Sudden crash: the board reset abruptly (panic, watchdog or brownout)."
		}
		if cause == "" {
			cause = mode
		}
	}
	if inc.PrevUptimeS > 0 && inc.PrevUptimeS < 600 {
		add("The previous boot only lasted %s, which may be a crash loop.", fmtDur(inc.PrevUptimeS))
	}
	recent := 0
	for _, o := range b.incidents {
		if o.Type == "reboot" && o.Kind == "unexpected" && o.DetectedAt.After(inc.DetectedAt.Add(-24*time.Hour)) {
			recent++
		}
	}
	if recent > 0 && inc.Kind == "unexpected" {
		add("This is unexpected reboot #%d in the last 24 hours.", recent+1)
	}
	if inc.After != nil {
		for _, m := range inc.After.UIMessages {
			add("After booting, the board showed: %s", truncate(string(m), 200))
		}
	}
	if inc.Kind == "unexpected" {
		add("A reboot started from the board's own web UI looks the same as a crash to the monitor.")
	}
	inc.Cause, inc.Findings = cause, f
}

// analyzeLockup explains how the board is failing, from the sweep and the
// HTTP results since the trouble started (b.mu held).
func (b *Board) analyzeLockup(inc *Incident) {
	tl := b.storedTimeline(inc.ID)
	tl = append(tl, b.rec.since(inc.DetectedAt)...)
	var codes = map[int]int{}
	var timeouts, refused, other int
	var sample string
	for _, p := range tl {
		if p.T.Before(inc.StartedAt) || (p.K != "http" && p.K != "diag") {
			continue
		}
		var v struct {
			OK     bool   `json:"ok"`
			Status int    `json:"status"`
			Err    string `json:"err"`
			Detail string `json:"detail"`
			Body   string `json:"body"`
			Check  string `json:"check"`
		}
		if json.Unmarshal(p.V, &v) != nil || v.OK || strings.HasPrefix(v.Check, "ICMP") || strings.HasPrefix(v.Check, "tcp") {
			continue
		}
		msg := v.Err + v.Detail
		switch {
		case v.Status >= 400:
			codes[v.Status]++
			if sample == "" && v.Body != "" {
				sample = v.Body
			}
		case strings.Contains(msg, "refused"):
			refused++
		case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
			timeouts++
		default:
			other++
		}
	}
	pingOK, pingN := 0, 0
	tcpOK := false
	for _, d := range inc.Diagnostics {
		if d.Check == "ICMP echo" {
			pingN++
			if d.OK {
				pingOK++
			}
		}
		if strings.HasPrefix(d.Check, "tcp") && d.OK {
			tcpOK = true
		}
		if strings.HasPrefix(d.Check, "GET") && d.Status >= 400 && sample == "" {
			sample = d.Detail
		}
	}

	var f []string
	add := func(format string, a ...any) { f = append(f, fmt.Sprintf(format, a...)) }
	var parts []string
	for c, n := range codes {
		parts = append(parts, fmt.Sprintf("%d× HTTP %d", n, c))
	}
	sort.Strings(parts)
	if timeouts > 0 {
		parts = append(parts, fmt.Sprintf("%d× timeout", timeouts))
	}
	if refused > 0 {
		parts = append(parts, fmt.Sprintf("%d× connection refused", refused))
	}
	if other > 0 {
		parts = append(parts, fmt.Sprintf("%d× other error", other))
	}
	if len(parts) > 0 {
		add("Failed HTTP requests: %s.", strings.Join(parts, ", "))
	}
	if sample != "" {
		add("Error response from the board: %q", truncate(sample, 300))
	}
	if pingN > 0 {
		add("Pings during the failure: %d of %d answered. TCP connect to the web port: %s.", pingOK, pingN, map[bool]string{true: "OK", false: "failed"}[tcpOK])
	}

	var mode, label string
	n5xx := 0
	for c, n := range codes {
		if c >= 500 {
			n5xx += n
		}
	}
	switch {
	case n5xx > 0 && pingOK > 0:
		label = fmt.Sprintf("HTTP %d", topCode(codes))
		mode = fmt.Sprintf("The web server is up but the firmware is refusing requests (%s) while the network stack works. The application is stuck or overloaded (e.g. a deadlock or exhausted resources), not a network problem.", label)
	case timeouts > 0 && pingOK > 0:
		label = "timeouts"
		mode = "The board answers pings but its web server times out: the firmware is hung while the network stack still runs."
	case refused > 0 && pingOK > 0:
		label = "connection refused"
		mode = "The board answers pings but refuses web connections: the web server stopped or ran out of sockets."
	case pingN > 0 && pingOK == 0:
		label = "unreachable"
		mode = "The board doesn't answer anything, not even pings: lost power, lost its network link, or hard-locked."
	default:
		label = "HTTP errors"
		mode = "The board's web server stopped answering normally."
	}
	inc.Title = "Stopped responding (" + label + ")"
	if !inc.Ongoing {
		inc.Title = fmt.Sprintf("Stopped responding for %s (%s)", fmtDur(inc.DurationS), label)
	}
	if inc.Rebooted && inc.RebootAt != nil {
		add("It rebooted at %s after %s of uptime: a power-cycle, a watchdog reset, or someone pressed reboot.", inc.RebootAt.Local().Format("15:04:05"), fmtDur(inc.PrevUptimeS))
		if inc.After != nil {
			if inc.After.Firmware != "" && inc.Before.Firmware != "" && inc.After.Firmware != inc.Before.Firmware {
				add("It came back with different firmware (%s → %s).", inc.Before.Firmware, inc.After.Firmware)
			}
			for _, m := range inc.After.UIMessages {
				add("After booting, the board showed: %s", truncate(string(m), 200))
			}
		}
	}
	lf, lc := b.leadUp(inc, tl, inc.StartedAt)
	inc.Findings = append(f, lf...)
	inc.Cause = mode
	if lc != "" {
		inc.Cause = lc + " " + mode
	}
}

func topCode(codes map[int]int) int {
	best, bn := 0, -1
	for c, n := range codes {
		if n > bn || (n == bn && c > best) {
			best, bn = c, n
		}
	}
	return best
}

func orNone(s []string) string {
	if len(s) == 0 {
		return "an unknown source"
	}
	return strings.Join(s, ", ")
}

// ======== export ========

type IncidentExport struct {
	Format      string          `json:"format"`
	GeneratedAt time.Time       `json:"generated_at"`
	Summary     []string        `json:"summary"`
	Incident    *Incident       `json:"incident"`
	Rollups     []Sample        `json:"rollups"`
	Events      []Event         `json:"events"`
	Settings    json.RawMessage `json:"board_settings,omitempty"`
	Monitor     map[string]any  `json:"monitor"`
}

func (b *Board) Export(id string) (*IncidentExport, bool) {
	inc, ok := b.Incident(id)
	if !ok {
		return nil, false
	}
	end := time.Now()
	if inc.ResolvedAt != nil {
		end = *inc.ResolvedAt
	}
	from, to := inc.StartedAt.Add(-2*time.Hour), end.Add(15*time.Minute)
	b.mu.RLock()
	var rollups []Sample
	for _, s := range b.history {
		if !s.T.Before(from) && !s.T.After(to) {
			rollups = append(rollups, s)
		}
	}
	var events []Event
	for _, e := range b.events {
		if !e.Time.Before(from) && !e.Time.After(to) {
			events = append(events, e)
		}
	}
	settings := b.settingsRaw
	b.mu.RUnlock()

	tf := func(t time.Time) string { return t.Local().Format(time.RFC1123) }
	sum := []string{fmt.Sprintf("%s on %s (%s)", inc.Title, inc.Board, inc.Host), "Started " + tf(inc.StartedAt) + ", detected " + tf(inc.DetectedAt) + "."}
	if inc.ResolvedAt != nil {
		sum = append(sum, fmt.Sprintf("%s at %s, after %s.", inc.Resolution, tf(*inc.ResolvedAt), fmtDur(inc.DurationS)))
	} else if inc.Ongoing {
		sum = append(sum, "Still ongoing when exported.")
	}
	if inc.RebootAt != nil {
		sum = append(sum, fmt.Sprintf("Rebooted around %s; uptime before was %s.", tf(*inc.RebootAt), fmtDur(inc.PrevUptimeS)))
	}
	sum = append(sum, "Likely cause: "+inc.Cause)
	for _, f := range inc.Findings {
		sum = append(sum, "- "+f)
	}
	return &IncidentExport{
		Format: "baldrick-monitor/incident/v1", GeneratedAt: time.Now(),
		Summary: sum, Incident: inc, Rollups: rollups, Events: events, Settings: settings,
		Monitor: map[string]any{
			"intervals": b.glob.Intervals, "thresholds": b.glob.th(),
			"ddp_probe": b.glob.DDPProbe, "board_probe_mode": b.probeMode(),
			"recorder_window": b.glob.Incidents.RecorderWindow, "temperature_unit": b.glob.unit(),
		},
	}, true
}
