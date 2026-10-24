package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- a fake monitor for the commands ----

type fakeBackend struct {
	mu        sync.Mutex
	views     []BoardView
	fppViews  []FPPView
	lists     map[string][]string
	volume    map[string]int
	rebooted  []string
	started   []string
	stopped   []string
	maint     map[string]bool
	acked     int
	ackedIDs  []string
	rebootErr error
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		views: []BoardView{
			{Name: "pixel1", Status: "up", HaveState: true, Uptime: 3600, OOMCount: 2, RAMMinimum: 20480},
			{Name: "pixel2", Status: "down", Reasons: []string{"Stopped responding since 5m ago"},
				Incidents: []Incident{{Type: "lockup", Title: "Stopped responding", Ongoing: true, StartedAt: time.Now().Add(-5 * time.Minute)}}},
			{Name: "pixeltree", Status: "up"},
		},
		fppViews: []FPPView{
			{FPPConfig: FPPConfig{Name: "fpp", Enabled: true}, Checked: true, Up: true, Status: "idle", NextPlaylist: "Halloween", NextStart: tptr(time.Now().Add(time.Hour))},
			{FPPConfig: FPPConfig{Name: "fpp-main", Enabled: true}, Checked: true, Up: true, Status: "playing", Playlist: "xmas Mix", SecondsRemaining: 95},
		},
		lists:  map[string][]string{"fpp": {"Halloween"}, "fpp-main": {"Post Christmas", "Taylor Tuesdays", "xmas Mix"}},
		volume: map[string]int{"fpp": 40, "fpp-main": 100},
		maint:  map[string]bool{},
	}
}

func (f *fakeBackend) boards() []BoardView {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]BoardView(nil), f.views...)
}
func (f *fakeBackend) rebootBoard(ctx context.Context, name, by string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rebooted = append(f.rebooted, name+" by "+by)
	return f.rebootErr
}
func (f *fakeBackend) setMaintenance(name string, on bool, d time.Duration, note, by string) (*Maintenance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.maint[name] = on
	if !on {
		return nil, nil
	}
	m := &Maintenance{Since: time.Now(), Note: note, By: by}
	if d > 0 {
		u := time.Now().Add(d)
		m.Until = &u
	}
	return m, nil
}
func (f *fakeBackend) ackAll(board, note, by string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked++
	return 3
}
func (f *fakeBackend) ackIncident(id, note, by string) (Incident, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.HasPrefix(id, "2026") {
		return Incident{}, errNoIncident
	}
	f.ackedIDs = append(f.ackedIDs, id+"|"+note+"|"+by)
	return Incident{ID: id, Board: "pixel2", Title: "Stopped responding", StartedAt: time.Now().Add(-time.Minute)}, nil
}

func (f *fakeBackend) fpps() []FPPView {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FPPView(nil), f.fppViews...)
}
func (f *fakeBackend) fppPlaylists(ctx context.Context, fpp string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists[fpp], nil
}
func (f *fakeBackend) fppStart(ctx context.Context, fpp, playlist string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, playlist+" on "+fpp)
	return nil
}
func (f *fakeBackend) fppStop(ctx context.Context, fpp string, graceful bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := fpp + " now"
	if graceful {
		s = fpp + " after"
	}
	f.stopped = append(f.stopped, s)
	return nil
}
func (f *fakeBackend) fppVolume(ctx context.Context, fpp string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.volume[fpp], nil
}
func (f *fakeBackend) fppSetVolume(ctx context.Context, fpp string, level int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volume[fpp] = level
	return nil
}

// ---- parsing ----

func TestParseBotText(t *testing.T) {
	lv := func(n int) *int { return &n }
	cases := []struct {
		in   string
		want botCmd
	}{
		{"reboot pixel1", botCmd{Name: "reboot", Board: "pixel1"}},
		{"!Reboot Pixel1", botCmd{Name: "reboot", Board: "Pixel1"}},
		{"play Halloween on fpp", botCmd{Name: "play", Playlist: "Halloween", FPP: "fpp"}},
		{"play Taylor Tuesdays ON fpp-main", botCmd{Name: "play", Playlist: "Taylor Tuesdays", FPP: "fpp-main"}},
		{"play xmas Mix", botCmd{Name: "play", Playlist: "xmas Mix"}},
		{"stop fpp", botCmd{Name: "stop", FPP: "fpp", When: "now"}},
		{"stop after current fpp", botCmd{Name: "stop", FPP: "fpp", When: "after"}},
		{"stop-after fpp-main", botCmd{Name: "stop", FPP: "fpp-main", When: "after"}},
		{"stop", botCmd{Name: "stop", When: "now"}},
		{"volume fpp 50", botCmd{Name: "volume", FPP: "fpp", Level: lv(50)}},
		{"vol 30% fpp-main", botCmd{Name: "volume", FPP: "fpp-main", Level: lv(30)}},
		{"volume fpp", botCmd{Name: "volume", FPP: "fpp"}},
		{"status", botCmd{Name: "status"}},
		{"s pixel2", botCmd{Name: "status", Board: "pixel2"}},
		{"fpp", botCmd{Name: "fpp"}},
		{"playlists fpp-main", botCmd{Name: "playlists", FPP: "fpp-main"}},
		{"maint pixel1 3h swapping PSU", botCmd{Name: "maint", Board: "pixel1", Action: "on", Duration: "3h", Note: "swapping PSU"}},
		{"maint pixel1 on", botCmd{Name: "maint", Board: "pixel1", Action: "on"}},
		{"maint pixel1 on 30m", botCmd{Name: "maint", Board: "pixel1", Action: "on", Duration: "30m"}},
		{"maint pixel1 off", botCmd{Name: "maint", Board: "pixel1", Action: "off"}},
		{"maint", botCmd{Name: "maint"}},
		{"ack all looked at it", botCmd{Name: "ack", Board: "all", Note: "looked at it"}},
		{"ack", botCmd{Name: "ack"}},
		{"incidents", botCmd{Name: "incidents"}},
	}
	for _, c := range cases {
		got, err := parseBotText(c.in)
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		gj, _ := json.Marshal(got)
		wj, _ := json.Marshal(c.want)
		if string(gj) != string(wj) {
			t.Errorf("%q:\n got %s\nwant %s", c.in, gj, wj)
		}
	}
	for _, bad := range []string{"", "reboot", "play", "play on fpp", "dance"} {
		if _, err := parseBotText(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestAckReply(t *testing.T) {
	cases := map[string]string{"ack": "", "ACK: power cycled it": "power cycled it", "ack - swapped the PSU": "swapped the PSU",
		"acknowledged": "", "Acknowledge, on it": "on it", "👍": "", "✅ done": "done"}
	for in, want := range cases {
		if note, ok := ackReply(in); !ok || note != want {
			t.Errorf("%q: got %q %v, want %q", in, note, ok, want)
		}
	}
	for _, in := range []string{"acked", "reboot pixel1", "status", "ackbar"} {
		if _, ok := ackReply(in); ok {
			t.Errorf("%q read as an ack", in)
		}
	}
	a := Alert{Event: "digest", Held: []Alert{{Incident: "a"}, {Incident: "b"}, {Incident: "a"}, {}}}
	if ids := a.incidentIDs(); strings.Join(ids, ",") != "a,b" {
		t.Errorf("digest ids %v", ids)
	}
}

// ---- the commands ----

func runText(t *testing.T, e *botEngine, user, text string) botReply {
	t.Helper()
	if out, ok := e.answer(context.Background(), user, text); ok {
		return botReply{Text: out}
	}
	c, err := parseBotText(text)
	if err != nil {
		t.Fatalf("%q: %v", text, err)
	}
	return e.run(context.Background(), user, "tester", c)
}

func TestRebootNeedsConfirmFromSameUser(t *testing.T) {
	be := newFakeBackend()
	e := newBotEngine(be)
	r := runText(t, e, "u1", "reboot pixelt")
	if r.Confirm == nil || !strings.Contains(r.Text, "pixeltree") {
		t.Fatalf("reboot by unique prefix should ask first: %+v", r)
	}
	if len(be.rebooted) != 0 {
		t.Fatal("rebooted before confirming")
	}
	// Someone else's yes doesn't count.
	if r2 := runText(t, e, "u2", "yes"); !strings.Contains(r2.Text, "Nothing is waiting") || len(be.rebooted) != 0 {
		t.Fatalf("another user confirmed: %q %v", r2.Text, be.rebooted)
	}
	if r2 := runText(t, e, "u1", "yes"); !strings.Contains(r2.Text, "Reboot sent") || len(be.rebooted) != 1 || be.rebooted[0] != "pixeltree by tester" {
		t.Fatalf("confirm: %q %v", r2.Text, be.rebooted)
	}
	// Only once.
	if r2 := runText(t, e, "u1", "yes"); !strings.Contains(r2.Text, "Nothing is waiting") || len(be.rebooted) != 1 {
		t.Fatalf("second yes: %q %v", r2.Text, be.rebooted)
	}
	// "pixel" is ambiguous; "no" cancels.
	if r := runText(t, e, "u1", "reboot pixel"); r.Confirm != nil {
		t.Fatalf("ambiguous board accepted: %+v", r)
	}
	runText(t, e, "u1", "reboot PIXEL2")
	if r := runText(t, e, "u1", "no"); !strings.Contains(r.Text, "Cancelled") || len(be.rebooted) != 1 {
		t.Fatalf("cancel: %q %v", r.Text, be.rebooted)
	}
}

func TestConfirmExpires(t *testing.T) {
	be := newFakeBackend()
	e := newBotEngine(be)
	now := time.Now()
	e.now = func() time.Time { return now }
	r := runText(t, e, "u1", "stop fpp-main")
	if r.Confirm == nil {
		t.Fatal("stop should ask first")
	}
	now = now.Add(botConfirmTTL + time.Second)
	if a := e.take("u1", r.Confirm.ID); a != nil {
		t.Fatal("an expired confirmation still ran")
	}
	if len(be.stopped) != 0 {
		t.Fatal("stopped")
	}
}

func TestStopPicksThePlayingFPP(t *testing.T) {
	be := newFakeBackend()
	e := newBotEngine(be)
	r := runText(t, e, "u1", "stop after")
	if r.Confirm == nil || !strings.Contains(r.Text, "fpp-main") || !strings.Contains(r.Text, "after the current song") {
		t.Fatalf("got %+v", r)
	}
	if a := e.take("u1", r.Confirm.ID); a == nil || !strings.Contains(a.run(context.Background()), "after the current song") {
		t.Fatal("confirm failed")
	}
	if len(be.stopped) != 1 || be.stopped[0] != "fpp-main after" {
		t.Fatalf("stopped %v", be.stopped)
	}
}

func TestPlay(t *testing.T) {
	be := newFakeBackend()
	e := newBotEngine(be)
	// Idle FPP: starts straight away, playlist matched case-insensitively.
	if r := runText(t, e, "u1", "play halloween on fpp"); r.Confirm != nil || !strings.Contains(r.Text, "Started **Halloween** on **fpp**") {
		t.Fatalf("got %+v", r)
	}
	// No FPP named: found on whichever has it; that one is playing, so ask.
	r := runText(t, e, "u1", "play taylor")
	if r.Confirm == nil || !strings.Contains(r.Text, "Replace it with **Taylor Tuesdays**") {
		t.Fatalf("got %+v", r)
	}
	runText(t, e, "u1", "yes")
	if strings.Join(be.started, "|") != "Halloween on fpp|Taylor Tuesdays on fpp-main" {
		t.Fatalf("started %v", be.started)
	}
	if r := runText(t, e, "u1", "play nope"); !strings.Contains(r.Text, "no playlist") {
		t.Fatalf("got %+v", r)
	}
	be.lists["fpp"] = append(be.lists["fpp"], "xmas Mix")
	if r := runText(t, e, "u1", "play xmas mix"); !strings.Contains(r.Text, "ambiguous") {
		t.Fatalf("got %+v", r)
	}
}

func TestVolumeStatusMaintAck(t *testing.T) {
	be := newFakeBackend()
	e := newBotEngine(be)
	if r := runText(t, e, "u1", "volume fpp 50"); !strings.Contains(r.Text, "set to 50%") || be.volume["fpp"] != 50 {
		t.Fatalf("got %+v %v", r, be.volume)
	}
	if r := runText(t, e, "u1", "volume fpp 150"); !strings.Contains(r.Text, "0-100") || be.volume["fpp"] != 50 {
		t.Fatalf("got %+v", r)
	}
	if r := runText(t, e, "u1", "volume 50"); !strings.Contains(r.Text, "which FPP") {
		t.Fatalf("got %+v", r)
	}
	if r := runText(t, e, "u1", "volume fpp-main"); !strings.Contains(r.Text, "100%") {
		t.Fatalf("got %+v", r)
	}
	st := runText(t, e, "u1", "status").Text
	for _, want := range []string{"🟢 **pixel1**", "OOM 2", "low 20KB", "🔴 **pixel2** LOCKED UP", "Stopped responding since"} {
		if !strings.Contains(st, want) {
			t.Errorf("status lacks %q:\n%s", want, st)
		}
	}
	fs := runText(t, e, "u1", "fpp").Text
	for _, want := range []string{"**fpp** idle", "🔊 50%", "next: Halloween", "playing **xmas Mix**", "1:35 left"} {
		if !strings.Contains(fs, want) {
			t.Errorf("fpp status lacks %q:\n%s", want, fs)
		}
	}
	if r := runText(t, e, "u1", "playlists fpp-main"); !strings.Contains(r.Text, "Taylor Tuesdays") {
		t.Fatalf("got %+v", r)
	}
	if r := runText(t, e, "u1", "maint pixel2 3h"); !strings.Contains(r.Text, "in maintenance until") || !be.maint["pixel2"] {
		t.Fatalf("got %+v", r)
	}
	if r := runText(t, e, "u1", "maint pixel2 off"); !strings.Contains(r.Text, "out of maintenance") || be.maint["pixel2"] {
		t.Fatalf("got %+v", r)
	}
	if r := runText(t, e, "u1", "ack all"); !strings.Contains(r.Text, "Acknowledged 3") {
		t.Fatalf("got %+v", r)
	}
	if r := runText(t, e, "u1", "incidents"); !strings.Contains(r.Text, "**pixel2** Stopped responding") {
		t.Fatalf("got %+v", r)
	}
}

// ---- the board and FPP calls ----

func TestRequestedRebootIsExpected(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/reboot" {
			hits.Add(1)
		}
	}))
	defer srv.Close()
	b := testBoard(t)
	b.cfg.Host = strings.TrimPrefix(srv.URL, "http://")
	b.mergeState(stateJSON(5000, "v1"), "ws")
	if err := b.Reboot(context.Background(), "Discord @me"); err != nil || hits.Load() != 1 {
		t.Fatalf("reboot: %v (hits %d)", err, hits.Load())
	}
	b.mergeState(stateJSON(3, "v1"), "poll")
	if len(b.incidents) != 1 || b.incidents[0].Kind != "requested" || b.incidents[0].Ack == nil || !strings.Contains(b.incidents[0].Cause, "Discord @me") {
		t.Fatalf("got %+v", b.incidents)
	}
	// The next one is a crash again.
	b.mergeState(stateJSON(5000, "v1"), "ws")
	b.mergeState(stateJSON(4, "v1"), "poll")
	if len(b.incidents) != 2 || b.incidents[1].Kind != "unexpected" {
		t.Fatalf("got %+v", b.incidents[1])
	}
}

func TestRebootErrorClearsRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	b := testBoard(t)
	b.cfg.Host = strings.TrimPrefix(srv.URL, "http://")
	if err := b.Reboot(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("got %v", err)
	}
	if b.rebootReq != nil {
		t.Fatal("a failed reboot request is still pending")
	}
}

func TestFPPControl(t *testing.T) {
	var mu sync.Mutex // the race detector can't see through the socket
	var got []string
	var vol map[string]int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, r.Method+" "+r.URL.EscapedPath())
		switch r.URL.Path {
		case "/api/playlists":
			w.Write([]byte(`["Halloween","Taylor Tuesdays"]`))
		case "/api/system/volume":
			if r.Method == http.MethodPost {
				json.NewDecoder(r.Body).Decode(&vol)
			}
			w.Write([]byte(`{"status":"OK","method":"FPPD","volume":"40"}`))
		case "/api/playlist/Nope/start":
			w.Write([]byte(`{"status":"ERROR","message":"Playlist not found"}`))
		default:
			w.Write([]byte(`{"status":"OK"}`))
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	fr := &FPPRegistry{list: []*fppMon{{cfg: FPPConfig{Name: "fpp", Host: strings.TrimPrefix(srv.URL, "http://")}}}}
	m := monitorBackend{fppr: fr}
	if pls, err := m.fppPlaylists(ctx, "fpp"); err != nil || len(pls) != 2 {
		t.Fatalf("playlists %v %v", pls, err)
	}
	if err := m.fppStart(ctx, "fpp", "Taylor Tuesdays"); err != nil {
		t.Fatal(err)
	}
	if err := m.fppStart(ctx, "fpp", "Nope"); err == nil || !strings.Contains(err.Error(), "Playlist not found") {
		t.Fatalf("FPP's ERROR status was ignored: %v", err)
	}
	m.fppStop(ctx, "fpp", false)
	m.fppStop(ctx, "fpp", true)
	if v, err := m.fppVolume(ctx, "fpp"); err != nil || v != 40 {
		t.Fatalf("volume %v %v", v, err)
	}
	if err := m.fppSetVolume(ctx, "fpp", 55); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if vol["volume"] != 55 {
		t.Fatalf("set volume sent %v", vol)
	}
	want := []string{"GET /api/playlists", "GET /api/playlist/Taylor%20Tuesdays/start", "GET /api/playlist/Nope/start",
		"GET /api/playlists/stop", "GET /api/playlists/stopgracefully", "GET /api/system/volume", "POST /api/system/volume"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests:\n%s", strings.Join(got, "\n"))
	}
	if _, err := m.fppPlaylists(ctx, "nope"); !errors.Is(err, errNoFPP) {
		t.Fatalf("unknown FPP: %v", err)
	}
}

// ---- settings ----

func TestDiscordSettingsKeepTheTokenSecret(t *testing.T) {
	cfg, _ := loadConfig("", "", "", "")
	cfg.History.DataDir = t.TempDir()
	u := SettingsUpdate{UISettings: cfg.uiSettings()}
	u.Discord = DiscordConfig{Enabled: true, Token: "secret-token", ChannelID: "123456789012345678", AllowedUsers: []string{" 223456789012345678 ", ""}}
	n, _, err := cfg.withSettings(u)
	if err != nil {
		t.Fatal(err)
	}
	if n.Discord.Token != "secret-token" || len(n.Discord.AllowedUsers) != 1 || n.Discord.AllowedUsers[0] != "223456789012345678" {
		t.Fatalf("got %+v", n.Discord)
	}
	view, _ := json.Marshal(n.settingsView(""))
	if strings.Contains(string(view), "secret-token") || !strings.Contains(string(view), `"token_set":true`) {
		t.Fatalf("view leaks the token or hides that it's set: %s", view)
	}
	// Saving the form back (blank token) keeps it.
	var back SettingsUpdate
	json.Unmarshal(view, &back)
	n2, _, err := n.withSettings(back)
	if err != nil || n2.Discord.Token != "secret-token" || !n2.Discord.Enabled {
		t.Fatalf("token lost on save: %+v %v", n2.Discord, err)
	}
	if err := n2.saveSettings(); err != nil {
		t.Fatal(err)
	}
	re, _ := loadConfig("", "", "", "")
	re.History.DataDir = cfg.History.DataDir
	if err := loadSettingsFile(re); err != nil || re.Discord.Token != "secret-token" {
		t.Fatalf("reload: %+v %v", re.Discord, err)
	}
	back.ClearDiscordToken, back.Discord.Enabled = true, false
	if n3, _, err := n2.withSettings(back); err != nil || n3.Discord.Token != "" {
		t.Fatalf("clear: %+v %v", n3.Discord, err)
	}
	for _, bad := range []DiscordConfig{
		{Enabled: true, Token: "t", ChannelID: "123456789"},
		{Enabled: true, ChannelID: "123456789", AllowedUsers: []string{"123456"}},
		{ChannelID: "#general"},
		{AllowedUsers: []string{"@me"}},
	} {
		u.Discord = bad
		if _, _, err := cfg.withSettings(u); err == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
}

// ---- the bot against a fake Discord ----

// fakeDiscord is a gateway and REST API, enough to drive the bot.
type fakeDiscord struct {
	t     *testing.T
	srv   *httptest.Server
	rest  chan restCall
	conns chan *fakeGW
}

type restCall struct {
	Method, Path string
	Body         map[string]any
	Raw          string
}

type fakeGW struct {
	c  net.Conn
	br *bufio.Reader
}

func newFakeDiscord(t *testing.T) *fakeDiscord {
	f := &fakeDiscord{t: t, rest: make(chan restCall, 64), conns: make(chan *fakeGW, 4)}
	mux := http.NewServeMux()
	mux.HandleFunc("/gw/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || strings.Contains(r.URL.RawQuery, "tok") {
			t.Error("the token went into the gateway URL or headers")
		}
		h := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		c, brw, _ := w.(http.Hijacker).Hijack()
		brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
			base64.StdEncoding.EncodeToString(h[:]) + "\r\n\r\n")
		brw.Flush()
		f.conns <- &fakeGW{c: c, br: brw.Reader}
	})
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bot tok" {
			t.Errorf("%s %s: auth %q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		}
		b, _ := io.ReadAll(r.Body)
		call := restCall{Method: r.Method, Path: strings.TrimPrefix(r.URL.Path, "/api"), Raw: string(b)}
		json.Unmarshal(b, &call.Body)
		f.rest <- call
		if strings.Contains(call.Path, "/messages/") && r.Method == http.MethodGet {
			w.Write([]byte(`{"id":"n2","embeds":[{"title":"x","footer":{"text":"Incident 20261024T190000-abcdef · reply \"ack\" to acknowledge"}}]}`))
			return
		}
		if strings.HasPrefix(call.Path, "/channels/") && r.Method == http.MethodGet {
			w.Write([]byte(`{"id":"100","guild_id":"200"}`))
			return
		}
		w.Write([]byte(`{"id":"1"}`))
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeDiscord) bot(be botBackend) *DiscordBot {
	d := NewDiscordBot(be)
	d.api = f.srv.URL + "/api"
	d.gateway = "ws://" + strings.TrimPrefix(f.srv.URL, "http://") + "/gw/?v=10&encoding=json"
	return d
}

func (f *fakeDiscord) accept() *fakeGW {
	select {
	case g := <-f.conns:
		return g
	case <-time.After(5 * time.Second):
		f.t.Fatal("the bot didn't connect")
		return nil
	}
}

func (f *fakeDiscord) next() restCall {
	f.t.Helper()
	select {
	case c := <-f.rest:
		return c
	case <-time.After(5 * time.Second):
		f.t.Fatal("no REST call")
		return restCall{}
	}
}

func (g *fakeGW) send(t *testing.T, v any) {
	p, _ := json.Marshal(v)
	g.frame(t, 0x1, p)
}

func (g *fakeGW) frame(t *testing.T, op byte, p []byte) {
	f := []byte{0x80 | op}
	switch {
	case len(p) < 126:
		f = append(f, byte(len(p)))
	default:
		f = append(f, 126, byte(len(p)>>8), byte(len(p)))
	}
	if _, err := g.c.Write(append(f, p...)); err != nil {
		t.Error(err)
	}
}

// read returns the next payload the bot sends, skipping heartbeats.
func (g *fakeGW) read(t *testing.T) map[string]any {
	t.Helper()
	for {
		g.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		var h [2]byte
		if _, err := io.ReadFull(g.br, h[:]); err != nil {
			t.Fatal(err)
		}
		n := int(h[1] & 0x7f)
		switch n {
		case 126:
			var e [2]byte
			io.ReadFull(g.br, e[:])
			n = int(binary.BigEndian.Uint16(e[:]))
		case 127:
			var e [8]byte
			io.ReadFull(g.br, e[:])
			n = int(binary.BigEndian.Uint64(e[:]))
		}
		var mask [4]byte
		io.ReadFull(g.br, mask[:])
		p := make([]byte, n)
		io.ReadFull(g.br, p)
		for i := range p {
			p[i] ^= mask[i%4]
		}
		var m map[string]any
		json.Unmarshal(p, &m)
		if m["op"] != float64(1) {
			return m
		}
	}
}

func dispatchEv(seq int, t string, d any) map[string]any {
	return map[string]any{"op": 0, "s": seq, "t": t, "d": d}
}

func TestDiscordBotEndToEnd(t *testing.T) {
	f := newFakeDiscord(t)
	be := newFakeBackend()
	d := f.bot(be)
	d.SetConfig(DiscordConfig{Enabled: true, Token: "tok", ChannelID: "100", AllowedUsers: []string{"7"}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	g := f.accept()
	g.send(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 45000}})
	id := g.read(t)
	dd, _ := id["d"].(map[string]any)
	if id["op"] != float64(2) || dd["token"] != "tok" || dd["intents"] != float64(discordIntents) {
		t.Fatalf("identify: %v", id)
	}
	g.send(t, dispatchEv(1, "READY", map[string]any{"session_id": "s1", "resume_gateway_url": "ws://unused",
		"user": map[string]any{"id": "99", "username": "baldrick"}, "application": map[string]any{"id": "55"}}))
	if c := f.next(); c.Method != "GET" || c.Path != "/channels/100" {
		t.Fatalf("got %+v", c)
	}
	if c := f.next(); c.Method != "PUT" || c.Path != "/applications/55/guilds/200/commands" || !strings.Contains(c.Raw, `"name":"reboot"`) {
		t.Fatalf("got %+v", c)
	}
	for i := 0; i < 100 && !d.Status().SlashCommands; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if st := d.Status(); st.State != "connected" || !st.SlashCommands || !st.TextCommands || st.User != "baldrick" {
		t.Fatalf("status %+v", st)
	}

	msg := func(seq int, user, channel, text string) map[string]any {
		return dispatchEv(seq, "MESSAGE_CREATE", map[string]any{"id": "m" + user, "channel_id": channel, "content": text,
			"author": map[string]any{"id": user, "username": "user" + user}})
	}
	// Strangers, other channels and bots are ignored: the next REST call is
	// the reply to the allowed user.
	g.send(t, msg(2, "8", "100", "reboot pixel1"))
	g.send(t, msg(3, "7", "101", "reboot pixel1"))
	g.send(t, dispatchEv(4, "MESSAGE_CREATE", map[string]any{"id": "x", "channel_id": "100", "content": "status", "author": map[string]any{"id": "7", "bot": true}}))
	time.Sleep(50 * time.Millisecond)
	g.send(t, msg(5, "7", "100", "<@99> status"))
	c := f.next()
	if c.Method != "POST" || c.Path != "/channels/100/messages" || !strings.Contains(c.Body["content"].(string), "pixel1") {
		t.Fatalf("status reply: %+v", c)
	}
	if !strings.Contains(c.Raw, `"allowed_mentions":{"parse":[]}`) {
		t.Errorf("replies must not ping anyone: %s", c.Raw)
	}

	g.send(t, msg(6, "7", "100", "reboot pixel1"))
	c = f.next()
	if !strings.Contains(c.Raw, "bm:yes:") || !strings.Contains(c.Body["content"].(string), "Reboot **pixel1**?") {
		t.Fatalf("confirm prompt: %+v", c)
	}
	if len(be.rebooted) != 0 {
		t.Fatal("rebooted without a yes")
	}
	g.send(t, msg(7, "7", "100", "yes"))
	if c = f.next(); !strings.Contains(c.Body["content"].(string), "Reboot sent") {
		t.Fatalf("after yes: %+v", c)
	}
	be.mu.Lock()
	if len(be.rebooted) != 1 || be.rebooted[0] != "pixel1 by Discord @user7" {
		t.Fatalf("rebooted %v", be.rebooted)
	}
	be.mu.Unlock()

	inter := func(seq int, typ int, user, name string, data map[string]any) map[string]any {
		data["name"] = name
		return dispatchEv(seq, "INTERACTION_CREATE", map[string]any{"id": "i" + user, "token": "itok", "type": typ,
			"application_id": "55", "channel_id": "100", "member": map[string]any{"user": map[string]any{"id": user, "username": "user" + user}},
			"data": data})
	}
	g.send(t, inter(8, 2, "7", "volume", map[string]any{"options": []any{
		map[string]any{"name": "fpp", "type": 3, "value": "fpp"}, map[string]any{"name": "level", "type": 4, "value": 50}}}))
	if c = f.next(); c.Path != "/interactions/i7/itok/callback" || c.Body["type"] != float64(5) {
		t.Fatalf("defer: %+v", c)
	}
	if c = f.next(); c.Method != "PATCH" || c.Path != "/webhooks/55/itok/messages/@original" || !strings.Contains(c.Body["content"].(string), "set to 50%") {
		t.Fatalf("slash reply: %+v", c)
	}

	// A slash /stop asks; the button confirms.
	g.send(t, inter(9, 2, "7", "stop", map[string]any{"options": []any{map[string]any{"name": "fpp", "type": 3, "value": "fpp-main"}}}))
	f.next()
	c = f.next()
	if !strings.Contains(c.Raw, "bm:yes:") {
		t.Fatalf("stop prompt: %+v", c)
	}
	btn := c.Raw[strings.Index(c.Raw, "bm:yes:"):]
	btn = btn[:strings.Index(btn, `"`)]
	// Someone else's click is refused.
	g.send(t, inter(10, 3, "8", "", map[string]any{"custom_id": btn}))
	if c = f.next(); c.Body["type"] != float64(4) || !strings.Contains(c.Raw, `"flags":64`) {
		t.Fatalf("stranger's click: %+v", c)
	}
	g.send(t, inter(11, 3, "7", "", map[string]any{"custom_id": btn}))
	if c = f.next(); c.Body["type"] != float64(7) || !strings.Contains(c.Raw, "Working") {
		t.Fatalf("click ack: %+v", c)
	}
	if c = f.next(); c.Method != "PATCH" || !strings.Contains(c.Body["content"].(string), "Stopped **fpp-main**") {
		t.Fatalf("click result: %+v", c)
	}
	be.mu.Lock()
	if len(be.stopped) != 1 || be.stopped[0] != "fpp-main now" {
		t.Fatalf("stopped %v", be.stopped)
	}
	be.mu.Unlock()

	// Replying "ack: …" to a monitor notice acknowledges its incident.
	notice := map[string]any{"id": "n1", "channel_id": "100", "author": map[string]any{"id": "500", "bot": true},
		"embeds": []any{map[string]any{"title": "pixel2: Stopped responding", "url": "http://dash/#incident=20261024T180000-0a1b2c",
			"footer": map[string]any{"text": "Incident 20261024T180000-0a1b2c · reply \"ack\" to acknowledge"}}}}
	reply := func(seq int, user, text string, replied any) map[string]any {
		d := map[string]any{"id": "r" + user, "channel_id": "100", "content": text, "author": map[string]any{"id": user, "username": "user" + user},
			"message_reference": map[string]any{"message_id": "n1"}, "referenced_message": replied}
		return dispatchEv(seq, "MESSAGE_CREATE", d)
	}
	g.send(t, reply(13, "8", "ack", notice)) // a stranger: ignored
	time.Sleep(50 * time.Millisecond)
	g.send(t, reply(14, "7", "ack: power cycled it", notice))
	if c = f.next(); c.Method != "POST" || !strings.Contains(c.Body["content"].(string), "Acknowledged **pixel2** Stopped responding") ||
		!strings.Contains(c.Raw, `"message_id":"r7"`) {
		t.Fatalf("ack reply: %+v", c)
	}
	// Without referenced_message the bot fetches the notice.
	g.send(t, reply(15, "7", "👍", nil))
	if c = f.next(); c.Method != "GET" || c.Path != "/channels/100/messages/n1" {
		t.Fatalf("fetch: %+v", c)
	}
	if c = f.next(); !strings.Contains(c.Body["content"].(string), "Acknowledged") {
		t.Fatalf("ack after fetch: %+v", c)
	}
	// A reply to something that isn't an incident notice says so.
	g.send(t, reply(16, "7", "ack", map[string]any{"id": "n3", "content": "hello", "author": map[string]any{"id": "7"}}))
	if c = f.next(); !strings.Contains(c.Body["content"].(string), "isn't about an incident") {
		t.Fatalf("non-incident: %+v", c)
	}
	be.mu.Lock()
	if strings.Join(be.ackedIDs, "\n") != "20261024T180000-0a1b2c|power cycled it|Discord @user7\n20261024T190000-abcdef||Discord @user7" || be.acked != 0 {
		t.Fatalf("acked %v (ack-all %d)", be.ackedIDs, be.acked)
	}
	be.mu.Unlock()

	// Autocomplete offers playlists.
	g.send(t, inter(12, 4, "7", "play", map[string]any{"options": []any{map[string]any{"name": "playlist", "type": 3, "value": "tay", "focused": true}}}))
	if c = f.next(); c.Body["type"] != float64(8) || !strings.Contains(c.Raw, "Taylor Tuesdays") || strings.Contains(c.Raw, "Halloween") {
		t.Fatalf("autocomplete: %+v", c)
	}
}

func TestDiscordBotFallsBackWithoutMessageContent(t *testing.T) {
	f := newFakeDiscord(t)
	d := f.bot(newFakeBackend())
	d.SetConfig(DiscordConfig{Enabled: true, Token: "tok", ChannelID: "100", AllowedUsers: []string{"7"}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	g := f.accept()
	g.send(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 45000}})
	g.read(t)
	g.frame(t, 0x8, []byte{4014 >> 8, 4014 & 0xff})
	g.c.Close()

	g = f.accept()
	g.send(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 45000}})
	id := g.read(t)
	if dd, _ := id["d"].(map[string]any); dd["intents"] != float64(intentGuildMessages) {
		t.Fatalf("second identify: %v", id)
	}
	g.send(t, dispatchEv(1, "READY", map[string]any{"session_id": "s1", "user": map[string]any{"id": "99", "username": "b"}, "application": map[string]any{"id": "55"}}))
	f.next()
	f.next()
	if st := d.Status(); st.State != "connected" || st.TextCommands {
		t.Fatalf("status %+v", st)
	}

	// A bad token stops retrying until the settings change.
	d.SetConfig(DiscordConfig{Enabled: true, Token: "tok2", ChannelID: "100", AllowedUsers: []string{"7"}})
	g = f.accept()
	g.send(t, map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 45000}})
	if dd, _ := g.read(t)["d"].(map[string]any); dd["token"] != "tok2" || dd["intents"] != float64(discordIntents) {
		t.Fatalf("after a token change: %v", dd)
	}
	g.frame(t, 0x8, []byte{4004 >> 8, 4004 & 0xff})
	g.c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for d.Status().State != "error" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if st := d.Status(); st.State != "error" || !strings.Contains(st.Error, "token") {
		t.Fatalf("status %+v", st)
	}
	select {
	case <-f.conns:
		t.Fatal("retried with a rejected token")
	case <-time.After(200 * time.Millisecond):
	}
	d.SetConfig(DiscordConfig{})
	deadline = time.Now().Add(3 * time.Second)
	for d.Status().State != "off" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if d.Status().State != "off" {
		t.Fatalf("status %+v", d.Status())
	}
}
