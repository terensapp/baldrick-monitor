package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testBoard(t *testing.T) *Board {
	t.Helper()
	cfg, err := loadConfig("", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	return NewBoard(BoardConfig{Name: "t", Host: "127.0.0.1"}, cfg, nil)
}

func stateJSON(uptime float64, fw string) []byte { return stateJSONSettings(uptime, fw, 1) }

func stateJSONSettings(uptime float64, fw string, settings int64) []byte {
	return []byte(fmt.Sprintf(`{"board_model":"Baldrick 8 Port v1","uptime":%v,"ram_available":110000,"ram_largest_block":90000,
		"temperature":[30],"last_settings_update":%d,"ota":{"updatable":[{"current_firmware_version":%q}]}}`, uptime, settings, fw))
}

// Baldrick boards report a new last_settings_update on every boot; that
// alone must not make a crash look intentional.
func TestRebootIsUnexpectedDespiteNewSettingsStamp(t *testing.T) {
	b := testBoard(t)
	b.mergeState(stateJSONSettings(5000, "v1", 1481795442), "ws")
	b.mergeState(stateJSONSettings(4, "v1", 1717911826), "poll")
	if len(b.incidents) != 1 || b.incidents[0].Kind != "unexpected" || b.incidents[0].Ack != nil {
		t.Fatalf("got %+v", b.incidents)
	}
	for _, e := range b.events {
		if e.Msg == "Board settings were changed" {
			t.Error("a reboot shouldn't log a settings change")
		}
	}
}

func TestRebootAfterSettingsChangeIsExpected(t *testing.T) {
	b := testBoard(t)
	b.mergeState(stateJSONSettings(5000, "v1", 1), "ws")
	b.mergeState(stateJSONSettings(5010, "v1", 2), "ws") // saved while running
	b.mergeState(stateJSONSettings(3, "v1", 3), "poll")
	if len(b.incidents) != 1 || b.incidents[0].Kind != "settings_change" {
		t.Fatalf("got %+v", b.incidents)
	}
}

func TestRebootWhenUptimeDrops(t *testing.T) {
	b := testBoard(t)
	b.mergeState(stateJSON(5000, "v1"), "ws")
	b.mergeState(stateJSON(5004, "v1"), "ws")
	if len(b.incidents) != 0 {
		t.Fatalf("steady uptime opened an incident: %+v", b.incidents[0])
	}
	b.mergeState(stateJSON(3, "v1"), "ws")
	if len(b.incidents) != 1 {
		t.Fatalf("want 1 incident, got %d", len(b.incidents))
	}
	inc := b.incidents[0]
	if inc.Type != "reboot" || inc.Kind != "unexpected" || inc.Ack != nil {
		t.Errorf("got type=%s kind=%s ack=%v", inc.Type, inc.Kind, inc.Ack)
	}
	if inc.PrevUptimeS < 5004 {
		t.Errorf("prev uptime %v", inc.PrevUptimeS)
	}
}

// A board that was down longer than its previous uptime comes back with a
// *higher* uptime; the boot-time jump must still catch it.
func TestRebootAfterLongOutage(t *testing.T) {
	b := testBoard(t)
	b.mergeState(stateJSON(100, "v1"), "poll")
	b.mu.Lock()
	f := b.fieldAt["uptime"]
	f.At = f.At.Add(-10 * time.Minute)
	b.fieldAt["uptime"] = f
	b.stateAt = f.At
	b.mu.Unlock()
	b.mergeState(stateJSON(300, "v1"), "poll")
	if len(b.incidents) != 1 {
		t.Fatalf("want 1 incident, got %d", len(b.incidents))
	}
	if s := b.incidents[0].SilenceS; s < 200 {
		t.Errorf("silence before boot = %vs, want ~300s", s)
	}
}

func TestFirmwareUpdateIsExpected(t *testing.T) {
	b := testBoard(t)
	b.mergeState(stateJSON(5000, "v1"), "ws")
	b.mergeState(stateJSON(20, "v2"), "ws")
	inc := b.incidents[0]
	if inc.Kind != "firmware_update" || inc.Ack == nil || !inc.Ack.Auto {
		t.Errorf("got kind=%s ack=%+v", inc.Kind, inc.Ack)
	}
}

func TestRebootDuringLockupIsOneIncident(t *testing.T) {
	b := testBoard(t)
	b.mergeState(stateJSON(5000, "v1"), "ws")
	now := time.Now()
	b.mu.Lock()
	lk := &Incident{ID: "lk", Type: "lockup", Kind: "lockup", StartedAt: now.Add(-2 * time.Minute), Ongoing: true}
	b.incidents = append(b.incidents, lk)
	b.activeLockup = lk
	b.mu.Unlock()
	b.mergeState(stateJSON(10, "v1"), "ws")
	if len(b.incidents) != 1 || !lk.Rebooted {
		t.Fatalf("want the reboot folded into the lockup; incidents=%d rebooted=%v", len(b.incidents), lk.Rebooted)
	}
	// A reboot well after a lockup ended is its own incident.
	b.mu.Lock()
	b.activeLockup = nil
	end := now.Add(-10 * time.Minute)
	lk.Ongoing, lk.ResolvedAt, lk.Rebooted = false, &end, false
	b.mu.Unlock()
	b.mergeState(stateJSON(2, "v1"), "ws") // rebooted again, long after the lockup
	if len(b.incidents) != 2 {
		t.Fatalf("want a separate reboot incident, got %d incidents", len(b.incidents))
	}
}

func TestSummarize(t *testing.T) {
	s := summarize([]float64{10, 12, math.NaN(), 11, 30})
	if s.Count != 5 || s.Lost != 1 || s.Loss != 0.2 {
		t.Errorf("count/lost/loss = %d/%d/%v", s.Count, s.Lost, s.Loss)
	}
	if s.P50 != 11 || s.Max != 30 || s.Min != 10 {
		t.Errorf("p50/max/min = %v/%v/%v", s.P50, s.Max, s.Min)
	}
	// |12-10| + |11-12| + |30-11| over 3 pairs
	if want := (2.0 + 1 + 19) / 3; math.Abs(s.Jitter-want) > 1e-9 {
		t.Errorf("jitter = %v, want %v", s.Jitter, want)
	}
	if e := summarize(nil); e.Count != 0 || e.Last != nil {
		t.Errorf("empty summary = %+v", e)
	}
}

func TestNormalizeBoard(t *testing.T) {
	for host, ok := range map[string]bool{
		"192.168.6.4": true, "pixel1.local": true, "127.0.0.1:8081": true,
		"": false, "http://x": false, "a b": false, "1.2.3.4:0": false, "1.2.3.4:http": false,
	} {
		b := BoardConfig{Host: host}
		if err := normalizeBoard(&b); (err == nil) != ok {
			t.Errorf("normalizeBoard(%q) err=%v, want ok=%v", host, err, ok)
		}
	}
	if hostOnly("127.0.0.1:8081") != "127.0.0.1" || httpAddr("10.0.0.1") != "10.0.0.1:80" {
		t.Error("hostOnly/httpAddr")
	}
}

func TestParseRef(t *testing.T) {
	cases := map[string][3]string{
		"ghcr.io/terensapp/baldrick-monitor:main": {"ghcr.io", "terensapp/baldrick-monitor", "main"},
		"ghcr.io/terensapp/baldrick-monitor":      {"ghcr.io", "terensapp/baldrick-monitor", "latest"},
		"registry.local:5000/x/y:1.2":             {"registry.local:5000", "x/y", "1.2"},
	}
	for ref, want := range cases {
		h, r, tag, err := parseRef(ref)
		if err != nil || [3]string{h, r, tag} != want {
			t.Errorf("parseRef(%q) = %q %q %q %v", ref, h, r, tag, err)
		}
	}
	if _, _, _, err := parseRef("baldrick-monitor:latest"); err == nil {
		t.Error("want an error for a ref without a registry")
	}
}

func TestPickPlatformSkipsAttestations(t *testing.T) {
	var m manifest
	json.Unmarshal([]byte(`{"manifests":[
		{"digest":"sha256:amd","platform":{"os":"linux","architecture":"amd64"}},
		{"digest":"sha256:arm","platform":{"os":"linux","architecture":"arm64"}},
		{"digest":"sha256:att","platform":{"os":"unknown","architecture":"unknown"}}]}`), &m)
	if d := pickPlatform(m.Manifests, "linux", "arm64"); d != "sha256:arm" {
		t.Errorf("got %q", d)
	}
	if d := pickPlatform(m.Manifests, "linux", "riscv64"); d != "" {
		t.Errorf("got %q for a missing platform", d)
	}
}

func TestDownsampleKeepsWorst(t *testing.T) {
	var in []Sample
	t0 := time.Now()
	for i := 0; i < 10; i++ {
		st, loss := "up", 0.0
		if i == 3 {
			st, loss = "down", 1
		}
		in = append(in, Sample{T: t0.Add(time.Duration(i) * time.Minute), Status: st, V: map[string]float64{"icmp_loss": loss, "temp": float64(20 + i)}})
	}
	out := downsample(in, 2)
	if len(out) != 2 {
		t.Fatalf("len %d", len(out))
	}
	if out[0].Status != "down" || out[0].V["icmp_loss"] != 1 {
		t.Errorf("worst values lost: %+v", out[0])
	}
	if out[0].V["temp"] != 22 { // mean of 20..24
		t.Errorf("temp mean = %v", out[0].V["temp"])
	}
}

func TestDurationString(t *testing.T) {
	for d, want := range map[time.Duration]string{6 * time.Hour: "6h", 30 * time.Minute: "30m", 90 * time.Second: "1m30s", 5 * time.Second: "5s", 1500 * time.Millisecond: "1.5s", 2*time.Hour + 5*time.Minute: "2h5m"} {
		if got := (Duration{d}).String(); got != want {
			t.Errorf("%v: got %q, want %q", d, got, want)
		}
	}
}

func TestProbeFrameIsShowSafe(t *testing.T) {
	f := ddpFrame(1_000_000, make([]byte, 3))
	if f[0]&0x01 != 0 {
		t.Error("probe frames must not set PUSH")
	}
	if f[0]>>6 != 1 || f[1] != 0 || f[3] != 1 || len(f) != 13 {
		t.Errorf("frame header % x", f[:10])
	}
}

func TestFPPBusy(t *testing.T) {
	status := `{"status_name":"idle","scheduler":{"nextPlaylist":{"playlistName":"Halloween","scheduledStartTime":0}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, status) }))
	defer srv.Close()
	ip := strings.TrimPrefix(srv.URL, "http://")
	fpps := []FPPInstance{{Hostname: "fpp", IP: ip}, {Hostname: "gone", IP: "127.0.0.1:1"}}
	check := func(want string) {
		t.Helper()
		fppCacheMu.Lock()
		clear(fppCache)
		fppCacheMu.Unlock()
		if got := fppBusy(context.Background(), fpps); !strings.Contains(got, want) || (want == "" && got != "") {
			t.Errorf("fppBusy = %q, want it to contain %q", got, want)
		}
	}
	check("") // idle, nothing scheduled, unreachable FPP ignored
	status = `{"status_name":"playing","current_playlist":{"playlist":"Halloween"}}`
	check("is playing Halloween")
	status = fmt.Sprintf(`{"status_name":"idle","scheduler":{"nextPlaylist":{"playlistName":"Halloween","scheduledStartTime":%d}}}`, time.Now().Add(2*time.Minute).Unix())
	check("starts Halloween")
	status = fmt.Sprintf(`{"status_name":"idle","scheduler":{"nextPlaylist":{"playlistName":"Halloween","scheduledStartTime":%d}}}`, time.Now().Add(3*time.Hour).Unix())
	check("")
}
