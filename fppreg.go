package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// FPPConfig is one FPP the monitor watches. Auto entries come from what the
// boards report; removing one marks it ignored so it doesn't come back.
type FPPConfig struct {
	Name    string `json:"name"`
	Host    string `json:"host"`   // IP or hostname, optionally :port
	Source  string `json:"source"` // auto, manual
	Enabled bool   `json:"enabled"`
	Ignored bool   `json:"ignored,omitempty"`
}

// ShowSession is one stretch of an FPP running a playlist.
type ShowSession struct {
	FPP      string     `json:"fpp"`
	Playlist string     `json:"playlist"`
	Start    time.Time  `json:"start"`
	End      *time.Time `json:"end,omitempty"`
}

// fppUnreachableAfter is how many failed polls in a row make an FPP
// unreachable.
const fppUnreachableAfter = 3

// fppSourceRecheck is how long a stream source that turned out not to be
// FPP is left alone before discovery asks it again.
const fppSourceRecheck = 30 * time.Minute

// FPPRegistry watches every known FPP with one light poller each (status
// every intervals.fpp, outputs every few minutes) and keeps the list in
// <data_dir>/fpps.json.
//
// Lock order: a board's mutex may be held while calling into the registry,
// never the other way round. Pollers gather board facts with no lock held,
// update the registry, then tell boards.
type FPPRegistry struct {
	parent context.Context
	path   string
	boards func() []*Board

	mu       sync.Mutex
	cfg      *Config
	list     []*fppMon
	notFPP   map[string]time.Time // stream sources that didn't answer as FPP
	stopping bool
}

type fppMon struct {
	cfg    FPPConfig
	store  *store
	cancel context.CancelFunc
	gen    int // bumped on every start/stop so a stale poll is dropped

	// Guarded by FPPRegistry.mu.
	addrs      []string
	checked    bool // a poll has settled whether it answers
	up         bool
	fails      int
	lastOK     time.Time
	lastPoll   time.Time
	err        string
	st         *fppStatus
	outputs    *fppOutputs
	outAt      time.Time
	outErr     string
	drives     map[string][]fppTarget // board name → outputs aimed at it
	reportedBy []string               // boards listing this FPP in system_state.fpp
	hardware   string                 // as a board reported it
	show       *ShowSession           // the running show, if any
	shows      []ShowSession          // finished shows within retention
	events     []Event
	history    []Sample
}

func NewFPPRegistry(ctx context.Context, cfg *Config, boards func() []*Board) (*FPPRegistry, error) {
	r := &FPPRegistry{parent: ctx, cfg: cfg, boards: boards, notFPP: map[string]time.Time{}}
	var saved []FPPConfig
	if dir := cfg.History.DataDir; dir != "" {
		r.path = filepath.Join(dir, "fpps.json")
		if data, err := os.ReadFile(r.path); err == nil {
			if err := json.Unmarshal(data, &saved); err != nil {
				return nil, fmt.Errorf("%s: %w", r.path, err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range saved {
		if normalizeFPP(&c) == nil && r.findLocked(c.Name) == nil {
			r.startLocked(c)
		}
	}
	if len(saved) > 0 {
		log.Printf("using %d FPP(s) from %s", len(saved), r.path)
	}
	return r, nil
}

func normalizeFPP(c *FPPConfig) error {
	bc := BoardConfig{Name: c.Name, Host: c.Host}
	if err := normalizeBoard(&bc); err != nil {
		return errors.New(strings.Replace(err.Error(), "board", "FPP", 1))
	}
	c.Name, c.Host = bc.Name, bc.Host
	if c.Source != "auto" {
		c.Source = "manual"
	}
	if c.Ignored {
		c.Enabled = false
	}
	return nil
}

// Run discovers FPPs from the boards until ctx ends.
func (r *FPPRegistry) Run(ctx context.Context) {
	defer func() {
		r.mu.Lock()
		r.stopping = true
		r.mu.Unlock()
	}()
	if !sleepCtx(ctx, 3*time.Second) {
		return
	}
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		r.discover(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// SetConfig hands the registry a new config; pollers pick up a new interval
// on their next tick.
func (r *FPPRegistry) SetConfig(cfg *Config) {
	r.mu.Lock()
	r.cfg = cfg
	r.mu.Unlock()
}

func (r *FPPRegistry) config() *Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg
}

func (r *FPPRegistry) Persistent() bool { return r.path != "" }

func (r *FPPRegistry) findLocked(name string) *fppMon {
	for _, m := range r.list {
		if m.cfg.Name == name {
			return m
		}
	}
	return nil
}

// knownLocked finds the entry for a host, or for an IP it stands for.
func (r *FPPRegistry) knownLocked(host string) *fppMon {
	for _, m := range r.list {
		if m.cfg.Host == host {
			return m
		}
	}
	ip, port := hostPort(host)
	for _, m := range r.list {
		// The same IP on a different explicit port is a different FPP
		// (only seen with simulators).
		if mip, mport := hostPort(m.cfg.Host); (mip == ip || slices.Contains(m.addrs, ip)) && (mport == port || mport == "" || port == "") {
			return m
		}
	}
	return nil
}

// hostPort splits off an explicit, non-default port.
func hostPort(h string) (string, string) {
	host, port, err := net.SplitHostPort(h)
	if err != nil {
		return h, ""
	}
	if port == "80" {
		port = ""
	}
	return host, port
}

func (r *FPPRegistry) startLocked(c FPPConfig) *fppMon {
	cfg := r.cfg
	m := &fppMon{cfg: c, drives: map[string][]fppTarget{}}
	m.store = openStore(fppDataDir(cfg), c.Name, cfg.retention())
	since := time.Now().Add(-cfg.retention())
	m.events = loadLines(m.store, "events", time.Now(), func(e *Event) bool { return e.Time.After(since) })
	m.history = loadLines(m.store, "samples", time.Now(), func(s *Sample) bool { return s.T.After(since) })
	m.shows = loadLines(m.store, "shows", time.Now(), func(s *ShowSession) bool { return s.End != nil && s.End.After(since) })
	r.list = append(r.list, m)
	if c.Enabled && !c.Ignored {
		r.runLocked(m)
	}
	return m
}

// fppDataDir holds each FPP's history: <data_dir>/_fpp/<name>/.
func fppDataDir(cfg *Config) string {
	if cfg.History.DataDir == "" {
		return ""
	}
	return filepath.Join(cfg.History.DataDir, "_fpp")
}

func (r *FPPRegistry) runLocked(m *fppMon) {
	if m.cancel != nil || r.stopping {
		return
	}
	ctx, cancel := context.WithCancel(r.parent)
	m.cancel = cancel
	m.gen++
	gen := m.gen
	go r.poll(ctx, m, gen)
}

// stopLocked stops an entry's poller and forgets its live state.
func (r *FPPRegistry) stopLocked(m *fppMon) {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.gen++
	m.checked, m.up, m.fails, m.st, m.show, m.err = false, false, 0, nil, nil, ""
}

func (r *FPPRegistry) saveLocked() error {
	if r.path == "" {
		return nil
	}
	list := make([]FPPConfig, 0, len(r.list))
	for _, m := range r.list {
		list = append(list, m.cfg)
	}
	data, _ := json.MarshalIndent(list, "", "  ")
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

// ======== discovery ========

// discover adds every FPP the boards report, plus any stream source that
// answers FPP's status API (one GET, then left alone for a while if it
// doesn't).
func (r *FPPRegistry) discover(ctx context.Context) {
	type cand struct{ name, host, hardware, board string }
	var reported []cand
	var sources []string
	for _, b := range r.boards() {
		ref := b.fppRefs()
		for _, f := range ref.reported {
			if f.IP != "" {
				reported = append(reported, cand{f.Hostname, f.IP, f.Hardware, ref.name})
			}
		}
		for _, ip := range ref.sources {
			if ip != ref.self && !slices.Contains(ref.addrs, ip) && !slices.Contains(sources, ip) {
				sources = append(sources, ip)
			}
		}
	}
	r.mu.Lock()
	added := false
	for _, c := range reported {
		if m := r.knownLocked(c.host); m != nil {
			if c.hardware != "" {
				m.hardware = c.hardware
			}
			continue
		}
		r.addAutoLocked(c.name, c.host).hardware = c.hardware
		log.Printf("[fpp] discovered %s (%s), reported by %s", c.name, c.host, c.board)
		added = true
	}
	var ask []string
	for _, ip := range sources {
		if r.knownLocked(ip) == nil && time.Since(r.notFPP[ip]) > fppSourceRecheck && len(ask) < 4 {
			ask = append(ask, ip)
		}
	}
	r.mu.Unlock()
	for _, ip := range ask {
		st, err := fetchFPPStatus(ctx, ip)
		r.mu.Lock()
		if err != nil {
			r.notFPP[ip] = time.Now()
		} else if r.knownLocked(ip) == nil {
			r.addAutoLocked(st.HostName, ip)
			log.Printf("[fpp] discovered %s (%s) streaming to a board", orStr(st.HostName, ip), ip)
			added = true
		}
		r.mu.Unlock()
	}
	if added {
		r.mu.Lock()
		if err := r.saveLocked(); err != nil {
			log.Printf("[fpp] save: %v", err)
		}
		r.mu.Unlock()
	}
}

func (r *FPPRegistry) addAutoLocked(name, host string) *fppMon {
	name = orStr(name, hostOnly(host))
	if r.findLocked(name) != nil {
		name = fmt.Sprintf("%s (%s)", name, host)
	}
	c := FPPConfig{Name: name, Host: host, Source: "auto", Enabled: true}
	normalizeFPP(&c)
	return r.startLocked(c)
}

// ======== management (API) ========

var errNoFPP = errors.New("no such FPP")

func (r *FPPRegistry) Add(c FPPConfig) (FPPConfig, error) {
	c.Source, c.Enabled, c.Ignored = "manual", true, false
	if err := normalizeFPP(&c); err != nil {
		return c, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.knownLocked(c.Host); m != nil {
		if !m.cfg.Ignored {
			return c, fmt.Errorf("%s is already monitored as %q", c.Host, m.cfg.Name)
		}
		// Adding an ignored FPP by hand brings it back as a manual entry.
		r.stopLocked(m)
		r.list = slices.DeleteFunc(r.list, func(x *fppMon) bool { return x == m })
	}
	if r.findLocked(c.Name) != nil {
		return c, fmt.Errorf("an FPP named %q already exists", c.Name)
	}
	r.startLocked(c)
	log.Printf("[fpp] added %s (%s)", c.Name, c.Host)
	return c, r.saveLocked()
}

// FPPUpdate changes an entry's enabled/ignored flags.
type FPPUpdate struct {
	Enabled *bool `json:"enabled"`
	Ignored *bool `json:"ignored"`
}

func (r *FPPRegistry) Update(name string, u FPPUpdate) (FPPConfig, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.findLocked(name)
	if m == nil {
		return FPPConfig{}, errNoFPP
	}
	if u.Ignored != nil {
		m.cfg.Ignored = *u.Ignored
		m.cfg.Enabled = !*u.Ignored
	}
	if u.Enabled != nil && !m.cfg.Ignored {
		m.cfg.Enabled = *u.Enabled
	}
	if m.cfg.Enabled {
		r.runLocked(m)
	} else {
		r.stopLocked(m)
	}
	log.Printf("[fpp] %s: enabled %v, ignored %v", name, m.cfg.Enabled, m.cfg.Ignored)
	return m.cfg, r.saveLocked()
}

// Remove deletes a manual entry. An auto entry is kept as ignored so
// discovery doesn't bring it back.
func (r *FPPRegistry) Remove(name string) (ignored bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.findLocked(name)
	if m == nil {
		return false, errNoFPP
	}
	r.stopLocked(m)
	if m.cfg.Source == "auto" {
		m.cfg.Ignored, m.cfg.Enabled = true, false
		log.Printf("[fpp] %s ignored", name)
		return true, r.saveLocked()
	}
	r.list = slices.DeleteFunc(r.list, func(x *fppMon) bool { return x == m })
	log.Printf("[fpp] %s removed", name)
	return false, r.saveLocked()
}

// ======== polling ========

func (r *FPPRegistry) poll(ctx context.Context, m *fppMon, gen int) {
	r.pollOnce(ctx, m, gen)
	lastRollup := time.Now()
	for {
		cfg := r.config()
		if !sleepCtx(ctx, cfg.Intervals.FPP.Duration) {
			return
		}
		r.pollOnce(ctx, m, gen)
		if time.Since(lastRollup) >= cfg.History.Resolution.Duration {
			lastRollup = time.Now()
			r.rollup(m, gen, cfg)
		}
	}
}

// fppNote is something a poll found worth telling boards and people.
type fppNote struct {
	level, msg string
	rec        map[string]any // structured copy for the boards' flight recorders
	alert      string         // alert event, or "" for none
	severity   string
}

func (r *FPPRegistry) pollOnce(ctx context.Context, m *fppMon, gen int) {
	r.mu.Lock()
	host, outAt, wasActive := m.cfg.Host, m.outAt, m.st != nil && m.st.active()
	r.mu.Unlock()

	st, err := fetchFPPStatus(ctx, host)
	if ctx.Err() != nil {
		return
	}
	var out *fppOutputs
	var outErr error
	if err == nil && (time.Since(outAt) > fppOutputsTTL || (st.active() && !wasActive)) {
		out, outErr = fetchFPPOutputs(ctx, host)
	}
	addrs := resolveHosts(ctx, host)
	type boardRef struct {
		ref      fppBoardRefs
		reported bool
	}
	var refs []boardRef
	for _, b := range r.boards() {
		ref := b.fppRefs()
		rep := slices.ContainsFunc(ref.reported, func(f FPPInstance) bool {
			return f.IP == host || slices.Contains(addrs, hostOnly(f.IP))
		})
		refs = append(refs, boardRef{ref, rep})
	}

	now := time.Now()
	r.mu.Lock()
	if m.gen != gen { // stopped or restarted while polling
		r.mu.Unlock()
		return
	}
	m.addrs, m.lastPoll = addrs, now
	if out != nil {
		m.outputs, m.outAt, m.outErr = out, now, ""
	} else if outErr != nil {
		m.outErr = outErr.Error()
		m.outAt = now.Add(time.Minute - fppOutputsTTL) // retry in a minute
	}
	m.drives = map[string][]fppTarget{}
	m.reportedBy = nil
	for _, br := range refs {
		if m.outputs != nil {
			if t := m.outputs.targetsFor(ctx, br.ref.addrs); len(t) > 0 {
				m.drives[br.ref.name] = t
			}
		}
		if br.reported {
			m.reportedBy = append(m.reportedBy, br.ref.name)
		}
	}
	notes := m.applyLocked(st, err, now)
	name, related := m.cfg.Name, m.relatedLocked()
	r.mu.Unlock()

	r.deliver(name, host, notes, related)
}

// relatedLocked lists the boards this FPP drives or that report it.
func (m *fppMon) relatedLocked() []string {
	var out []string
	for b := range m.drives {
		out = append(out, b)
	}
	for _, b := range m.reportedBy {
		if !slices.Contains(out, b) {
			out = append(out, b)
		}
	}
	sort.Strings(out)
	return out
}

// applyLocked folds one poll into the entry and returns what changed.
func (m *fppMon) applyLocked(st *fppStatus, err error, now time.Time) []fppNote {
	var notes []fppNote
	name := m.cfg.Name
	if err != nil {
		m.fails++
		m.err = err.Error()
		if m.fails == fppUnreachableAfter {
			m.checked, m.up = true, false
			msg := fmt.Sprintf("FPP %s is unreachable: %s", name, m.err)
			if m.show != nil {
				msg += fmt.Sprintf(" (it was running %s)", m.show.Playlist)
			}
			notes = append(notes, fppNote{level: "warn", msg: msg, alert: "fpp_unreachable", severity: "warn",
				rec: map[string]any{"fpp": name, "change": "unreachable", "error": m.err}})
		}
		return notes
	}
	if m.checked && !m.up {
		notes = append(notes, fppNote{level: "info", msg: fmt.Sprintf("FPP %s is reachable again", name), alert: "fpp_unreachable", severity: "info",
			rec: map[string]any{"fpp": name, "change": "reachable"}})
	}
	first := !m.checked
	m.checked, m.up, m.fails, m.err, m.lastOK, m.st = true, true, 0, "", now, st

	// Show sessions. A playlist change while running is a new show.
	cur := ""
	if st.active() {
		cur = st.what()
	}
	if m.show != nil && m.show.Playlist != cur {
		end := now
		m.show.End = &end
		done := *m.show
		m.shows = append(m.shows, done)
		m.store.append("shows", end, done)
		m.show = nil
		notes = append(notes, fppNote{level: "info", msg: fmt.Sprintf("FPP %s stopped %s after %s", name, done.Playlist, fmtDur(end.Sub(done.Start).Seconds())),
			alert: "show_stopped", severity: "info",
			rec: map[string]any{"fpp": name, "change": "show_stopped", "playlist": done.Playlist, "started": done.Start}})
	}
	if cur != "" && m.show == nil {
		m.show = &ShowSession{FPP: name, Playlist: cur, Start: now}
		n := fppNote{level: "info", msg: fmt.Sprintf("FPP %s started %s", name, cur), alert: "show_started", severity: "info",
			rec: map[string]any{"fpp": name, "change": "show_started", "playlist": cur, "status": st.StatusName}}
		if first {
			// The monitor just started (or the FPP was just added); that's
			// not news worth an alert.
			n.msg = fmt.Sprintf("FPP %s is %s %s (already running when first checked)", name, strings.ToLower(st.StatusName), cur)
			n.alert = ""
		}
		notes = append(notes, n)
	}
	return notes
}

// deliver records notes on the FPP and on every related board, and alerts.
func (r *FPPRegistry) deliver(name, host string, notes []fppNote, related []string) {
	if len(notes) == 0 {
		return
	}
	r.mu.Lock()
	m := r.findLocked(name)
	cfg := r.cfg
	for _, n := range notes {
		if m != nil {
			e := Event{Time: time.Now(), Level: n.level, Msg: n.msg}
			m.events = append(m.events, e)
			if len(m.events) > maxEvents {
				m.events = m.events[len(m.events)-maxEvents:]
			}
			m.store.append("events", e.Time, e)
		}
		log.Printf("[fpp %s] %s: %s", name, n.level, n.msg)
	}
	r.mu.Unlock()
	for _, b := range r.boards() {
		if slices.Contains(related, b.cfg.Name) {
			for _, n := range notes {
				b.noteFPP(n.level, n.msg, n.rec)
			}
		}
	}
	for _, n := range notes {
		if n.alert == "" {
			continue
		}
		msg := n.msg + "."
		if len(related) > 0 {
			msg += " Boards: " + strings.Join(related, ", ") + "."
		}
		cfg.notify().Send(Alert{Event: n.alert, Severity: n.severity, Board: "FPP " + name, Host: host, Title: n.msg, Message: msg})
	}
}

// rollup stores one sample of the FPP's state: reachable and running a show.
func (r *FPPRegistry) rollup(m *fppMon, gen int, cfg *Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m.gen != gen {
		return
	}
	s := Sample{T: time.Now().UTC().Truncate(time.Second), Status: "unknown", V: map[string]float64{}}
	if m.checked {
		s.Status, s.V["up"] = "down", 0
		if m.up {
			s.Status, s.V["up"], s.V["show"] = "up", 1, b2f(m.show != nil)
		}
	}
	m.history = append(m.history, s)
	cutoff := time.Now().Add(-cfg.retention())
	i := 0
	for i < len(m.history) && m.history[i].T.Before(cutoff) {
		i++
	}
	m.history = m.history[i:]
	for len(m.shows) > 0 && m.shows[0].End.Before(cutoff) {
		m.shows = m.shows[1:]
	}
	m.store.append("samples", s.T, s)
	m.store.prune(time.Now())
}

// ======== what boards ask ========

// fppBoardRefs is what the registry needs to know about a board.
type fppBoardRefs struct {
	name     string
	addrs    []string // every address FPP may know it by
	self     string   // the monitor's own source address, as the board sees it
	reported []FPPInstance
	sources  []string // every DDP/sACN/Art-Net source it lists
}

// relatesLocked reports whether an enabled entry drives or is reported by a
// board.
func (m *fppMon) relatesLocked(board string, reported []FPPInstance) bool {
	if !m.cfg.Enabled || m.cfg.Ignored {
		return false
	}
	if _, ok := m.drives[board]; ok {
		return true
	}
	return slices.ContainsFunc(reported, func(f FPPInstance) bool {
		return f.IP == m.cfg.Host || slices.Contains(m.addrs, hostOnly(f.IP))
	})
}

// busyFor says why a board's DDP probes should wait: an FPP that drives or
// is reported by it is running something, has a show starting within
// showLeadTime, or hasn't been checked yet. Unreachable, disabled and
// ignored FPPs don't hold probes back. Safe to call with the board's lock
// held.
func (r *FPPRegistry) busyFor(board string, reported []FPPInstance) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range reported {
		if f.IP == "" {
			continue
		}
		if m := r.knownLocked(f.IP); m == nil || (m.cfg.Enabled && !m.checked) {
			name := orStr(f.Hostname, f.IP)
			if m != nil {
				name = m.cfg.Name
			}
			return fmt.Sprintf("FPP %s hasn't been checked yet, so probes wait for it", name)
		}
	}
	for _, m := range r.list {
		if !m.relatesLocked(board, reported) || !m.up || m.st == nil {
			continue
		}
		if m.st.active() {
			return fmt.Sprintf("FPP %s is %s %s, so no probe packets are sent during the show", m.cfg.Name, strings.ToLower(m.st.StatusName), m.st.what())
		}
		if name, start := m.st.nextShow(); !start.IsZero() {
			if until := time.Until(start); until > 0 && until < showLeadTime {
				return fmt.Sprintf("FPP %s starts %s at %s, so probes are paused until it's idle again", m.cfg.Name, name, start.Local().Format("15:04"))
			}
		}
	}
	return ""
}

func orStr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// feedsFor reports what each FPP related to a board is doing for it, for
// the show-output check.
func (r *FPPRegistry) feedsFor(board string, reported []FPPInstance) []fppFeed {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var feeds []fppFeed
	for _, m := range r.list {
		if !m.relatesLocked(board, reported) {
			continue
		}
		f := fppFeed{FPP: FPPInstance{Hostname: m.cfg.Name, IP: m.sourceAddrLocked()}, Name: m.cfg.Name,
			Status: m.st, Outputs: m.outputs, Targets: m.drives[board]}
		switch {
		case !m.up || m.st == nil:
			f.Err = errors.New("FPP unreachable or not checked yet")
		case m.st.playing() && m.outputs == nil:
			f.Err = errors.New("FPP outputs unknown: " + m.outErr)
		}
		feeds = append(feeds, f)
	}
	return feeds
}

// sourceAddrLocked is the IP the FPP's packets come from.
func (m *fppMon) sourceAddrLocked() string {
	if len(m.addrs) > 0 {
		return m.addrs[0]
	}
	return hostOnly(m.cfg.Host)
}

// showContext places a moment relative to the shows of the FPPs related to
// a board: during one, after one ended, or shortly before one.
func (r *FPPRegistry) showContext(board string, reported []FPPInstance, at time.Time) []string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, m := range r.list {
		if !m.relatesLocked(board, reported) {
			continue
		}
		sessions := slices.Clone(m.shows)
		if m.show != nil {
			sessions = append(sessions, *m.show)
		}
		var during, before, after *ShowSession
		for i := range sessions {
			s := &sessions[i]
			switch {
			case !s.Start.After(at) && (s.End == nil || !s.End.Before(at)):
				during = s
			case s.End != nil && s.End.Before(at):
				if before == nil || s.End.After(*before.End) {
					before = s
				}
			case s.Start.After(at):
				if after == nil || s.Start.Before(after.Start) {
					after = s
				}
			}
		}
		switch {
		case during != nil:
			out = append(out, fmt.Sprintf("Happened during FPP %s's show %s (started %s, %s in).", m.cfg.Name, during.Playlist,
				during.Start.Local().Format("15:04"), fmtDur(at.Sub(during.Start).Seconds())))
		case after != nil && after.Start.Sub(at) < time.Hour:
			out = append(out, fmt.Sprintf("Began %s before FPP %s started %s.", fmtDur(after.Start.Sub(at).Seconds()), m.cfg.Name, after.Playlist))
		case before != nil && at.Sub(*before.End) < 12*time.Hour:
			out = append(out, fmt.Sprintf("Began %s after FPP %s stopped %s.", fmtDur(at.Sub(*before.End).Seconds()), m.cfg.Name, before.Playlist))
		}
		if m.checked && !m.up {
			out = append(out, fmt.Sprintf("FPP %s was unreachable at the time (%s).", m.cfg.Name, m.err))
		}
	}
	return out
}

// showingFor reports whether an FPP related to the board is running a show.
func (r *FPPRegistry) showingFor(board string, reported []FPPInstance) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.list {
		if m.relatesLocked(board, reported) && m.up && m.show != nil {
			return true
		}
	}
	return false
}

// ======== views ========

type FPPDrive struct {
	Board     string      `json:"board"`
	Targets   []fppTarget `json:"targets"`
	Receiving bool        `json:"receiving"`          // the board lists this FPP as a source right now
	Incident  string      `json:"incident,omitempty"` // title of an ongoing show_output incident
}

type FPPView struct {
	FPPConfig
	Checked          bool          `json:"checked"`
	Up               bool          `json:"up"`
	Error            string        `json:"error,omitempty"`
	LastPoll         *time.Time    `json:"last_poll,omitempty"`
	LastOK           *time.Time    `json:"last_ok,omitempty"`
	Interval         string        `json:"interval"`
	Status           string        `json:"status"` // idle, playing, ... ("" when unknown)
	Mode             string        `json:"mode"`
	Playlist         string        `json:"playlist"`
	Sequence         string        `json:"sequence"`
	Song             string        `json:"song,omitempty"`
	SecondsRemaining float64       `json:"seconds_remaining"`
	NextPlaylist     string        `json:"next_playlist,omitempty"`
	NextStart        *time.Time    `json:"next_start,omitempty"`
	Version          string        `json:"version"`
	Platform         string        `json:"platform"`
	Hardware         string        `json:"hardware,omitempty"`
	HostName         string        `json:"host_name"`
	OutputsEnabled   *bool         `json:"outputs_enabled,omitempty"`
	Outputs          []fppTarget   `json:"outputs"`
	OutputsAt        *time.Time    `json:"outputs_at,omitempty"`
	OutputsError     string        `json:"outputs_error,omitempty"`
	Drives           []FPPDrive    `json:"drives"`
	ReportedBy       []string      `json:"reported_by"`
	Show             *ShowSession  `json:"show,omitempty"`
	Events           []Event       `json:"events"`
	Shows            []ShowSession `json:"recent_shows"`
	addr             string
}

func tptr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// Views lists every FPP, ignored ones last.
func (r *FPPRegistry) Views() []FPPView {
	if r == nil {
		return []FPPView{}
	}
	r.mu.Lock()
	out := make([]FPPView, 0, len(r.list))
	for _, m := range r.list {
		v := FPPView{FPPConfig: m.cfg, Checked: m.checked, Up: m.up, Error: m.err, LastPoll: tptr(m.lastPoll), LastOK: tptr(m.lastOK),
			Interval: r.cfg.Intervals.FPP.String(), ReportedBy: slices.Clone(m.reportedBy), Outputs: []fppTarget{},
			OutputsError: m.outErr, Events: []Event{}, Shows: []ShowSession{}, Drives: []FPPDrive{}, addr: m.sourceAddrLocked()}
		if v.ReportedBy == nil {
			v.ReportedBy = []string{}
		}
		if st := m.st; st != nil && m.up {
			v.Status, v.Mode, v.Version, v.Platform, v.HostName = st.StatusName, st.ModeName, st.Version, st.Platform, st.HostName
			v.Playlist, v.Sequence, v.Song, v.SecondsRemaining = st.CurrentPlaylist.Playlist, st.CurrentSequence, st.CurrentSong, float64(st.SecondsRemaining)
			v.OutputsEnabled = st.OutputsEnabled
			if n, t := st.nextShow(); !t.IsZero() {
				v.NextPlaylist, v.NextStart = n, &t
			}
		}
		v.Hardware = orStr(m.hardware, v.Platform)
		if m.outputs != nil {
			v.Outputs, v.OutputsAt = m.outputs.Targets, tptr(m.outAt)
		}
		for bn, t := range m.drives {
			v.Drives = append(v.Drives, FPPDrive{Board: bn, Targets: t})
		}
		sort.Slice(v.Drives, func(i, j int) bool { return v.Drives[i].Board < v.Drives[j].Board })
		if m.show != nil {
			s := *m.show
			v.Show = &s
		}
		for i := len(m.events) - 1; i >= 0 && len(v.Events) < 15; i-- {
			v.Events = append(v.Events, m.events[i])
		}
		for i := len(m.shows) - 1; i >= 0 && len(v.Shows) < 10; i-- {
			v.Shows = append(v.Shows, m.shows[i])
		}
		out = append(out, v)
	}
	r.mu.Unlock()
	// Each driven board's side, read without the registry lock.
	boards := map[string]*Board{}
	for _, b := range r.boards() {
		boards[b.cfg.Name] = b
	}
	for i := range out {
		for j := range out[i].Drives {
			if b := boards[out[i].Drives[j].Board]; b != nil {
				out[i].Drives[j].Receiving, out[i].Drives[j].Incident = b.receivingFromFPP(out[i].addr)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return !out[i].Ignored && out[j].Ignored })
	return out
}

// History returns an FPP's rollups and events from the last d.
func (r *FPPRegistry) History(name string, d time.Duration, points int) ([]Sample, []Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.findLocked(name)
	if m == nil {
		return nil, nil, errNoFPP
	}
	cutoff := time.Now().Add(-d)
	i := sort.Search(len(m.history), func(i int) bool { return !m.history[i].T.Before(cutoff) })
	samples := downsample(append([]Sample{}, m.history[i:]...), points)
	ev := []Event{}
	for k := len(m.events) - 1; k >= 0 && m.events[k].Time.After(cutoff) && len(ev) < 1000; k-- {
		ev = append(ev, m.events[k])
	}
	return samples, ev, nil
}

// FPPProbe is what "Test connection" shows.
type FPPProbe struct {
	Host      string      `json:"host"`
	HostName  string      `json:"host_name"`
	Version   string      `json:"version"`
	Platform  string      `json:"platform"`
	Mode      string      `json:"mode"`
	Status    string      `json:"status"`
	Outputs   []fppTarget `json:"outputs"`
	Drives    []string    `json:"drives"` // monitored boards among the outputs
	LatencyMs float64     `json:"latency_ms"`
	Monitored string      `json:"monitored,omitempty"`
}

// Inspect asks a host whether it's FPP and where its outputs go (GETs only).
func (r *FPPRegistry) Inspect(ctx context.Context, host string) (*FPPProbe, error) {
	start := time.Now()
	st, err := fetchFPPStatus(ctx, host)
	if err != nil {
		return nil, err
	}
	p := &FPPProbe{Host: host, HostName: st.HostName, Version: st.Version, Platform: st.Platform, Mode: st.ModeName,
		Status: st.StatusName, LatencyMs: ms(time.Since(start)), Outputs: []fppTarget{}, Drives: []string{}}
	if out, err := fetchFPPOutputs(ctx, host); err == nil {
		p.Outputs = out.Targets
		for _, b := range r.boards() {
			if len(out.targetsFor(ctx, b.fppRefs().addrs)) > 0 {
				p.Drives = append(p.Drives, b.cfg.Name)
			}
		}
	}
	r.mu.Lock()
	if m := r.knownLocked(host); m != nil && !m.cfg.Ignored {
		p.Monitored = m.cfg.Name
	}
	r.mu.Unlock()
	return p, nil
}
