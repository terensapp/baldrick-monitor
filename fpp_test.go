package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeFPP serves FPP's status and outputs; only GETs are allowed.
type fakeFPP struct {
	t      *testing.T
	mu     sync.Mutex
	status string
	target string
	srv    *httptest.Server
}

func newFakeFPP(t *testing.T, target string) *fakeFPP {
	f := &fakeFPP{t: t, status: `{"status_name":"idle","host_name":"fpp","version":"9.5.3","mode_name":"player"}`, target: target}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("FPP got a %s %s", r.Method, r.URL.Path)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/api/fppd/status":
			io.WriteString(w, f.status)
		case "/api/channel/output/universeOutputs":
			fmt.Fprintf(w, `{"channelOutputs":[{"enabled":1,"type":"universes","universes":[{"active":1,"address":%q,"description":"board","startChannel":1,"channelCount":300,"type":4}]}],"status":"OK"}`, f.target)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeFPP) host() string { return strings.TrimPrefix(f.srv.URL, "http://") }

func (f *fakeFPP) set(status string) {
	f.mu.Lock()
	f.status = status
	f.mu.Unlock()
}

const playingStatus = `{"status_name":"playing","host_name":"fpp","current_playlist":{"playlist":"Halloween"},"seconds_remaining":"42","channelOutputsEnabled":true}`

// testFPPs builds a registry whose pollers don't run; tests poll by hand.
func testFPPs(t *testing.T, dir string, boards ...*Board) *FPPRegistry {
	t.Helper()
	cfg, err := loadConfig("", "", "", dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewFPPRegistry(context.Background(), cfg, func() []*Board { return boards })
	if err != nil {
		t.Fatal(err)
	}
	r.stopping = true // no background pollers
	for _, b := range boards {
		b.fpps = r
	}
	return r
}

func boardReporting(t *testing.T, fppHost string) *Board {
	b := testBoard(t)
	b.mergeState([]byte(fmt.Sprintf(`{"uptime":100,"fpp":[{"hostname":"fpp","ip":%q,"hardware":"Raspberry Pi 5"}],
		"streaming":{"ddp_sources":[],"sacn_sources":[],"artnet_sources":[]}}`, fppHost)), "poll")
	return b
}

func (r *FPPRegistry) entry(t *testing.T, name string) *fppMon {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.findLocked(name)
	if m == nil {
		t.Fatalf("no FPP %q", name)
	}
	return m
}

func (r *FPPRegistry) pollNow(m *fppMon) { r.pollOnce(context.Background(), m, m.gen) }

func TestFPPDiscoveryAndIgnore(t *testing.T) {
	dir := t.TempDir()
	b := boardReporting(t, "10.0.0.100")
	r := testFPPs(t, dir, b)
	r.discover(context.Background())
	views := r.Views()
	if len(views) != 1 || views[0].Name != "fpp" || views[0].Host != "10.0.0.100" || views[0].Source != "auto" || views[0].Hardware != "Raspberry Pi 5" {
		t.Fatalf("views = %+v", views)
	}
	// Removing an auto entry keeps it as ignored, across restarts too.
	if ignored, err := r.Remove("fpp"); err != nil || !ignored {
		t.Fatalf("Remove = %v, %v", ignored, err)
	}
	r2 := testFPPs(t, dir, b)
	r2.discover(context.Background())
	v := r2.Views()
	if len(v) != 1 || !v[0].Ignored || v[0].Enabled {
		t.Fatalf("after restart: %+v", v)
	}
	if busy := r2.busyFor(b.cfg.Name, b.st.FPP); busy != "" {
		t.Errorf("an ignored FPP held probes back: %q", busy)
	}
	// A manual entry is really deleted; adding the ignored host by hand
	// brings it back as manual.
	c, err := r2.Add(FPPConfig{Name: "main", Host: "10.0.0.100"})
	if err != nil || c.Source != "manual" {
		t.Fatalf("Add = %+v, %v", c, err)
	}
	if _, err := r2.Add(FPPConfig{Name: "dup", Host: "10.0.0.100"}); err == nil {
		t.Error("added the same host twice")
	}
	if ignored, _ := r2.Remove("main"); ignored || len(r2.Views()) != 0 {
		t.Errorf("manual remove: ignored=%v views=%+v", ignored, r2.Views())
	}
}

// The monitor's own probe address is never taken for an FPP.
func TestFPPDiscoverySkipsSelf(t *testing.T) {
	b := testBoard(t)
	b.mergeState([]byte(fmt.Sprintf(`{"uptime":1,"streaming":{"ddp_sources":[%q],"sacn_sources":[],"artnet_sources":[]}}`, b.self)), "poll")
	r := testFPPs(t, "", b)
	r.discover(context.Background())
	if len(r.Views()) != 0 || len(r.notFPP) != 0 {
		t.Fatalf("asked our own address: views=%+v notFPP=%v", r.Views(), r.notFPP)
	}
}

func TestFPPShowEventsAndPause(t *testing.T) {
	f := newFakeFPP(t, "127.0.0.1")
	b := boardReporting(t, f.host())
	r := testFPPs(t, "", b)
	if busy := r.busyFor(b.cfg.Name, b.st.FPP); !strings.Contains(busy, "hasn't been checked") {
		t.Errorf("unknown FPP: busy = %q", busy)
	}
	r.discover(context.Background())
	m := r.entry(t, "fpp")
	r.pollNow(m)
	if busy := r.busyFor(b.cfg.Name, b.st.FPP); busy != "" {
		t.Errorf("idle: busy = %q", busy)
	}
	if v := r.Views()[0]; !v.Up || v.Status != "idle" || len(v.Drives) != 1 || v.Drives[0].Board != b.cfg.Name {
		t.Fatalf("view = %+v", v)
	}

	f.set(fmt.Sprintf(`{"status_name":"idle","scheduler":{"nextPlaylist":{"playlistName":"Halloween","scheduledStartTime":%d}}}`, time.Now().Add(2*time.Minute).Unix()))
	r.pollNow(m)
	if busy := r.busyFor(b.cfg.Name, b.st.FPP); !strings.Contains(busy, "starts Halloween") {
		t.Errorf("show soon: busy = %q", busy)
	}

	f.set(playingStatus)
	r.pollNow(m)
	if busy := r.busyFor(b.cfg.Name, b.st.FPP); !strings.Contains(busy, "is playing Halloween") {
		t.Errorf("playing: busy = %q", busy)
	}
	feeds := r.feedsFor(b.cfg.Name, b.st.FPP)
	if len(feeds) != 1 || !feeds[0].Playing() || feeds[0].FPP.IP != "127.0.0.1" {
		t.Fatalf("feeds = %+v", feeds)
	}
	if !b.View().ShowRunning {
		t.Error("board view doesn't know a show is running")
	}
	f.set(`{"status_name":"idle"}`)
	r.pollNow(m)

	var msgs []string
	for _, e := range b.events {
		msgs = append(msgs, e.Msg)
	}
	got := strings.Join(msgs, "\n")
	if !strings.Contains(got, "FPP fpp started Halloween") || !strings.Contains(got, "FPP fpp stopped Halloween") {
		t.Errorf("board events:\n%s", got)
	}
	recs := 0
	for _, p := range b.rec.since(time.Time{}) {
		if p.K == "fpp" {
			recs++
		}
	}
	if recs != 2 {
		t.Errorf("%d fpp points in the flight recorder, want 2", recs)
	}
	if v := r.Views()[0]; len(v.Shows) != 1 || v.Shows[0].Playlist != "Halloween" || len(v.Events) != 2 {
		t.Errorf("FPP view shows=%+v events=%+v", v.Shows, v.Events)
	}
	// Cross-reference for an incident an hour later.
	if c := r.showContext(b.cfg.Name, b.st.FPP, time.Now().Add(time.Hour)); len(c) != 1 || !strings.Contains(c[0], "after FPP fpp stopped Halloween") {
		t.Errorf("showContext = %q", c)
	}

	// Unreachable after three failed polls; then it doesn't hold probes.
	f.set(playingStatus)
	r.pollNow(m)
	f.srv.Close()
	for range fppUnreachableAfter {
		r.pollNow(m)
	}
	if v := r.Views()[0]; v.Up || !v.Checked {
		t.Fatalf("still up: %+v", v)
	}
	if busy := r.busyFor(b.cfg.Name, b.st.FPP); busy != "" {
		t.Errorf("unreachable FPP held probes back: %q", busy)
	}
	if c := r.showContext(b.cfg.Name, b.st.FPP, time.Now()); len(c) != 2 || !strings.Contains(c[0], "during FPP fpp's show Halloween") || !strings.Contains(c[1], "unreachable") {
		t.Errorf("showContext = %q", c)
	}
	if !strings.Contains(b.events[len(b.events)-1].Msg, "FPP fpp is unreachable") {
		t.Errorf("last board event = %q", b.events[len(b.events)-1].Msg)
	}
}

// A show already running when the monitor starts is recorded but not
// alerted as news.
func TestFPPAlreadyPlayingIsNotNews(t *testing.T) {
	m := &fppMon{cfg: FPPConfig{Name: "fpp"}, store: &store{}}
	notes := m.applyLocked(&fppStatus{StatusName: "playing", CurrentPlaylist: struct {
		Playlist string `json:"playlist"`
	}{"Halloween"}}, nil, time.Now())
	if len(notes) != 1 || notes[0].alert != "" || !strings.Contains(notes[0].msg, "already running") {
		t.Fatalf("notes = %+v", notes)
	}
}

func TestOptInEvents(t *testing.T) {
	ch := Channel{}
	if !ch.wants("fpp_unreachable") || ch.wants("show_started") || ch.wants("show_stopped") || ch.wants("update") {
		t.Error("default events wrong")
	}
}
