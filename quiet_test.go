package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// webhookSink records the alerts a test webhook receives.
type webhookSink struct {
	mu  sync.Mutex
	got []Alert
	srv *httptest.Server
}

func newSink(t *testing.T) *webhookSink {
	s := &webhookSink{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var a Alert
		json.Unmarshal(b, &a)
		s.mu.Lock()
		s.got = append(s.got, a)
		s.mu.Unlock()
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *webhookSink) wait(n int) []Alert {
	for i := 0; i < 50; i++ {
		s.mu.Lock()
		if len(s.got) >= n {
			out := append([]Alert(nil), s.got...)
			s.mu.Unlock()
			return out
		}
		s.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Alert(nil), s.got...)
}

func at(day time.Weekday, hhmm string) time.Time {
	// 2026-10-11 is a Sunday.
	t, _ := time.ParseInLocation("2006-01-02 15:04", "2026-10-11 "+hhmm, time.Local)
	return t.AddDate(0, 0, int(day))
}

func TestQuietHoursWindow(t *testing.T) {
	q := QuietHours{Enabled: true, Start: "22:00", End: "7:00", Days: []int{5, 6}} // Fri and Sat nights
	if err := q.normalize(); err != nil {
		t.Fatal(err)
	}
	if q.End != "07:00" {
		t.Errorf("end normalised to %q", q.End)
	}
	for _, c := range []struct {
		t    time.Time
		want bool
	}{
		{at(time.Friday, "21:59"), false},
		{at(time.Friday, "22:00"), true},
		{at(time.Saturday, "03:00"), true}, // Friday night
		{at(time.Saturday, "07:00"), false},
		{at(time.Saturday, "23:30"), true},
		{at(time.Sunday, "23:00"), false},
		{at(time.Monday, "02:00"), false}, // Sunday night isn't quiet
	} {
		if got := q.Active(c.t); got != c.want {
			t.Errorf("%s: active=%v, want %v", c.t.Format("Mon 15:04"), got, c.want)
		}
	}
	if got := q.Active(at(time.Saturday, "23:30").Add(7 * time.Hour)); !got {
		t.Error("Saturday night into Sunday morning should be quiet")
	}
	day := QuietHours{Enabled: true, Start: "09:00", End: "17:00"}
	day.normalize()
	if !day.Active(at(time.Wednesday, "12:00")) || day.Active(at(time.Wednesday, "18:00")) {
		t.Error("daytime window")
	}
	day.Enabled = false
	if day.Active(at(time.Wednesday, "12:00")) {
		t.Error("disabled quiet hours are never active")
	}
	for _, bad := range []QuietHours{{Start: "25:00"}, {Start: "10:00", End: "10:00"}, {Days: []int{7}}} {
		if err := bad.normalize(); err == nil {
			t.Errorf("want an error for %+v", bad)
		}
	}
}

func TestQuietHoursHoldsAndSummarises(t *testing.T) {
	sink := newSink(t)
	n := NewNotifier([]Channel{{Type: "webhook", URL: sink.srv.URL, Enabled: true, Events: []string{"lockup", "reboot", "recovered", "update"}}}, "")
	var mu sync.Mutex
	now := at(time.Tuesday, "23:00")
	n.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	n.quiet = &QuietHours{Enabled: true, Start: "22:00", End: "07:00", AllowLockups: true}
	n.recheck = 20 * time.Millisecond

	n.Send(Alert{Event: "reboot", Severity: "error", Board: "a", Title: "a: Unexpected reboot"})
	n.Send(Alert{Event: "update", Severity: "info", Board: "baldrick-monitor", Title: "Monitor update available"})
	n.Send(Alert{Event: "lockup", Severity: "error", Board: "b", Title: "b: Stopped responding", Incident: "lk1"})
	n.Send(Alert{Event: "recovered", Severity: "info", Board: "b", Title: "b: Stopped responding", Incident: "lk1"})
	n.Send(Alert{Event: "recovered", Severity: "info", Board: "a", Title: "a: something else", Incident: "x"})
	got := sink.wait(2)
	if len(got) != 2 || got[0].Board != "b" || got[1].Board != "b" || got[0].Event == got[1].Event {
		t.Fatalf("during quiet hours only the allowed lockup and its recovery should go out, got %+v", got)
	}

	// Still quiet: nothing more goes out.
	time.Sleep(80 * time.Millisecond)
	if got := sink.wait(3); len(got) != 2 {
		t.Fatalf("held alerts leaked during quiet hours: %+v", got[2:])
	}

	mu.Lock()
	now = at(time.Wednesday, "07:01")
	mu.Unlock()
	got = sink.wait(3)
	if len(got) != 3 {
		t.Fatalf("want a summary after quiet hours, got %d alerts", len(got))
	}
	d := got[2]
	if d.Event != "digest" || len(d.Held) != 3 || d.Severity != "error" {
		t.Errorf("summary: event=%s held=%d severity=%s", d.Event, len(d.Held), d.Severity)
	}
}

func TestQuietHoursSurviveSettingsChange(t *testing.T) {
	sink := newSink(t)
	cfg, err := loadConfig("", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Notifications = []Channel{{Type: "webhook", URL: sink.srv.URL, Enabled: true}}
	cfg.QuietHours = QuietHours{Enabled: true, Start: "00:00", End: "23:59"}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	cfg.publishLive()
	n := cfg.notify()
	n.now = func() time.Time { return at(time.Monday, "12:00") }
	n.Send(Alert{Event: "lockup", Board: "a", Title: "a: Stopped responding"})
	if len(n.held) != 1 {
		t.Fatalf("held = %d", len(n.held))
	}

	cfg.QuietHours.Enabled = false
	cfg.publishLive()          // the new notifier takes over the held alert...
	cfg.notify().flushIfDone() // ...and sends it at its next check
	if got := sink.wait(1); len(got) != 1 || got[0].Event != "lockup" {
		t.Fatalf("held alert lost across a settings change: %+v", got)
	}
}

func TestMaintenanceSilencesAndAutoAcks(t *testing.T) {
	sink := newSink(t)
	dir := t.TempDir()
	cfg, err := loadConfig("", "", "", dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Notifications = []Channel{{Type: "webhook", URL: sink.srv.URL, Enabled: true}}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	cfg.publishLive()
	cfg.Boards = []BoardConfig{{Name: "t", Host: "127.0.0.1:1"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reg, err := NewRegistry(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetMaintenance("t", true, 0, "swapping PSU", "test"); err != nil {
		t.Fatal(err)
	}
	b := reg.Get("t")
	b.mergeState(stateJSON(5000, "v1"), "ws")
	b.mergeState(stateJSON(3, "v1"), "ws") // unexpected reboot during maintenance

	b.mu.RLock()
	inc := b.incidents[len(b.incidents)-1]
	b.mu.RUnlock()
	if inc.Kind != "unexpected" || inc.Ack == nil || inc.Ack.Note != maintenanceAckNote {
		t.Errorf("reboot during maintenance: kind=%s ack=%+v", inc.Kind, inc.Ack)
	}
	v := b.View()
	if v.Status != "maintenance" || v.Maintenance == nil || v.Maintenance.Note != "swapping PSU" {
		t.Errorf("view: status=%s maintenance=%+v", v.Status, v.Maintenance)
	}
	time.Sleep(100 * time.Millisecond)
	if got := sink.wait(0); len(got) != 0 {
		t.Errorf("alert sent during maintenance: %+v", got)
	}

	// boards.json keeps it across a restart.
	data, _ := os.ReadFile(filepath.Join(dir, "boards.json"))
	var saved []BoardConfig
	json.Unmarshal(data, &saved)
	if len(saved) != 1 || saved[0].Maintenance == nil || saved[0].Maintenance.Note != "swapping PSU" {
		t.Errorf("boards.json: %s", data)
	}

	// Editing the board keeps its maintenance window.
	if _, err := reg.Update("t", BoardConfig{Name: "t", Host: "127.0.0.1:2"}); err != nil {
		t.Fatal(err)
	}
	if !reg.Get("t").InMaintenance() {
		t.Error("editing the board ended maintenance")
	}
	if _, err := reg.SetMaintenance("t", false, 0, "", ""); err != nil {
		t.Fatal(err)
	}
	if reg.Get("t").InMaintenance() {
		t.Error("still in maintenance after turning it off")
	}
}

func TestMaintenanceExpires(t *testing.T) {
	dir := t.TempDir()
	cfg, err := loadConfig("", "", "", dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Boards = []BoardConfig{{Name: "t", Host: "127.0.0.1:1"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reg, err := NewRegistry(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	m, err := reg.SetMaintenance("t", true, 50*time.Millisecond, "", "")
	if err != nil || m.Until == nil {
		t.Fatalf("m=%+v err=%v", m, err)
	}
	if !reg.Get("t").InMaintenance() {
		t.Fatal("not in maintenance")
	}
	time.Sleep(150 * time.Millisecond)
	b := reg.Get("t")
	if b.InMaintenance() || b.View().Status == "maintenance" {
		t.Error("maintenance didn't expire")
	}
	b.mu.RLock()
	cleared := b.maint == nil
	b.mu.RUnlock()
	data, _ := os.ReadFile(filepath.Join(dir, "boards.json"))
	var saved []BoardConfig
	json.Unmarshal(data, &saved)
	if !cleared || len(saved) != 1 || saved[0].Maintenance != nil {
		t.Errorf("expired maintenance still saved: cleared=%v %s", cleared, data)
	}
	if _, err := reg.SetMaintenance("t", true, -time.Second, "", ""); err == nil {
		t.Error("want an error for a negative duration")
	}
	if _, err := reg.SetMaintenance("nope", true, 0, "", ""); err != errNotFound {
		t.Errorf("unknown board: %v", err)
	}
}

func TestMaintenanceMetric(t *testing.T) {
	var sb strings.Builder
	writeMetrics(&sb, []BoardView{{Name: "a", Status: "maintenance", Maintenance: &Maintenance{}}, {Name: "b", Status: "up"}})
	for _, want := range []string{`baldrick_maintenance{board="a"} 1`, `baldrick_maintenance{board="b"} 0`, `baldrick_health_status{board="a",status="maintenance"} 1`} {
		if !strings.Contains(sb.String(), want+"\n") {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// maintBoard is a board alerting to sink, with or without maintenance.
func maintBoard(t *testing.T, sink *webhookSink, maint bool) *Board {
	t.Helper()
	cfg, err := loadConfig("", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Notifications = []Channel{{Type: "webhook", URL: sink.srv.URL, Enabled: true}}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	cfg.publishLive()
	b := NewBoard(BoardConfig{Name: "t", Host: "127.0.0.1:1"}, cfg, nil)
	if maint {
		b.setMaintenance(&Maintenance{Since: time.Now()})
	}
	return b
}

func TestMemoryAlertRespectsMaintenance(t *testing.T) {
	mem := func(low int) []byte { return []byte(fmt.Sprintf(`{"uptime":100,"ram_minimum":%d}`, low)) }
	for _, maint := range []bool{false, true} {
		sink := newSink(t)
		b := maintBoard(t, sink, maint)
		b.mergeState(mem(83000), "ws")
		b.mergeState(mem(40000), "ws") // below the 48 KB warning
		time.Sleep(100 * time.Millisecond)
		got := sink.wait(1)
		if maint && len(got) != 0 {
			t.Errorf("memory alert sent during maintenance: %+v", got)
		}
		if !maint && (len(got) != 1 || got[0].Event != "memory") {
			t.Errorf("want one memory alert outside maintenance, got %+v", got)
		}
	}
}

func TestQuietHoursHoldEveryEvent(t *testing.T) {
	sink := newSink(t)
	n := NewNotifier([]Channel{{Type: "webhook", URL: sink.srv.URL, Enabled: true}}, "")
	n.now = func() time.Time { return at(time.Tuesday, "23:00") }
	n.quiet = &QuietHours{Enabled: true, Start: "22:00", End: "07:00", AllowLockups: true}
	for _, ev := range AlertEvents {
		if ev == "lockup" {
			continue // let through by AllowLockups
		}
		n.Send(Alert{Event: ev, Board: "a", Title: "a: " + ev})
	}
	time.Sleep(50 * time.Millisecond)
	if got := sink.wait(1); len(got) != 0 {
		t.Errorf("sent during quiet hours: %+v", got)
	}
	n.mu.Lock()
	held := len(n.held)
	n.mu.Unlock()
	if held != len(AlertEvents)-1 {
		t.Errorf("held %d alerts, want %d", held, len(AlertEvents)-1)
	}
	n.takeHeld()
}

func TestAckAllKeepsMaintenanceAcks(t *testing.T) {
	sink := newSink(t)
	b := maintBoard(t, sink, false)
	b.mergeState(stateJSON(5000, "v1"), "ws")
	b.mergeState(stateJSON(3, "v1"), "ws") // reboot outside maintenance: alerts
	b.setMaintenance(&Maintenance{Since: time.Now()})
	b.mergeState(stateJSON(5000, "v1"), "ws")
	b.mergeState(stateJSON(3, "v1"), "ws") // reboot during maintenance: auto-acked
	if n := b.AckAll(Ack{At: time.Now(), Note: "bulk"}); n != 1 {
		t.Errorf("AckAll acknowledged %d, want only the one not already acknowledged", n)
	}
	b.mu.RLock()
	first, second := b.incidents[0].Ack, b.incidents[1].Ack
	b.mu.RUnlock()
	if first == nil || first.Note != "bulk" || second == nil || second.Note != maintenanceAckNote {
		t.Errorf("acks: first=%+v second=%+v", first, second)
	}
	time.Sleep(100 * time.Millisecond)
	if got := sink.wait(2); len(got) != 1 || got[0].Event != "reboot" {
		t.Errorf("want only the pre-maintenance reboot alert, got %+v", got)
	}
}
