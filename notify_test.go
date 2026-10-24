package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWithSettingsKeepsSecretsAndScopesRestarts(t *testing.T) {
	cfg, err := loadConfig("", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Notifications = []Channel{{ID: "a", Name: "Discord", Type: "discord", Enabled: true, URL: "https://discord.com/api/webhooks/1/secret"}}
	cfg.publishLive()

	view := cfg.settingsView("")
	if view.Notifications[0].URL != "" || !view.Notifications[0].URLSet {
		t.Fatalf("secret leaked or flag missing: %+v", view.Notifications[0])
	}

	// Change only a threshold and the channel name, sending the masked view back.
	u := SettingsUpdate{UISettings: view.UISettings}
	u.Notifications = []Channel{view.Notifications[0].Channel}
	u.Notifications[0].Name = "Ops Discord"
	u.Thresholds.TempWarnC = 65
	n, restart, err := cfg.withSettings(u)
	if err != nil {
		t.Fatal(err)
	}
	if restart {
		t.Error("a threshold change shouldn't restart monitoring")
	}
	if got := n.Notifications[0]; got.URL != "https://discord.com/api/webhooks/1/secret" || got.Name != "Ops Discord" {
		t.Errorf("channel after update: %+v", got)
	}

	u.Intervals.HTTPPing = Duration{10 * time.Second}
	if _, restart, _ = cfg.withSettings(u); !restart {
		t.Error("an interval change should restart monitoring")
	}

	u.NewAdminToken = "short"
	if _, _, err := cfg.withSettings(u); err == nil {
		t.Error("want an error for a short admin token")
	}
	u.NewAdminToken = "long-enough-token"
	if n, _, _ := cfg.withSettings(u); n.AdminToken != "long-enough-token" {
		t.Error("admin token not set")
	}

	u.NewAdminToken = ""
	u.Intervals.ICMP = Duration{time.Millisecond}
	if _, _, err := cfg.withSettings(u); err == nil {
		t.Error("want a range error for a 1ms ICMP interval")
	}
}

func TestChannelNormalize(t *testing.T) {
	ok := []Channel{
		{Type: "slack", URL: "https://hooks.slack.com/services/x"},
		{Type: "email", SMTPHost: "smtp.example.com", From: "a@b.c", To: []string{"x@y.z", " "}},
	}
	for _, ch := range ok {
		if err := ch.normalize(); err != nil {
			t.Errorf("%s: %v", ch.Type, err)
		}
	}
	e := ok[1]
	e.normalize()
	if e.SMTPPort != 587 || e.TLS != "starttls" || len(e.To) != 1 {
		t.Errorf("email defaults: port=%d tls=%s to=%v", e.SMTPPort, e.TLS, e.To)
	}
	bad := []Channel{{Type: "discord", URL: "ftp://x"}, {Type: "email", SMTPHost: "h"}, {Type: "pager"},
		{Type: "slack", URL: "https://x", Events: []string{"nope"}}}
	for _, ch := range bad {
		if err := ch.normalize(); err == nil {
			t.Errorf("want an error for %+v", ch)
		}
	}
	if (&Channel{}).wants("update") || !(&Channel{}).wants("lockup") {
		t.Error("default events should be everything except update")
	}
}

func TestWebhookPayloads(t *testing.T) {
	got := map[string]map[string]any{}
	var ntfyHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.URL.Path == "/ntfy" {
			ntfyHeaders = r.Header.Clone()
			got["ntfy"] = map[string]any{"body": string(b)}
			return
		}
		var m map[string]any
		json.Unmarshal(b, &m)
		got[strings.TrimPrefix(r.URL.Path, "/")] = m
	}))
	defer srv.Close()

	n := NewNotifier(nil, "http://dash")
	a := Alert{Event: "lockup", Severity: "error", Board: "pixel1", Host: "192.168.6.4", Title: "pixel1: Stopped responding", Message: "HTTP 503", Incident: "abc"}
	n.prepare(&a)
	for _, ch := range []Channel{{Type: "discord", URL: srv.URL + "/discord"}, {Type: "slack", URL: srv.URL + "/slack"},
		{Type: "webhook", URL: srv.URL + "/webhook"}, {Type: "ntfy", URL: srv.URL + "/ntfy", Token: "tk"}} {
		if err := n.deliver(ch, a); err != nil {
			t.Fatalf("%s: %v", ch.Type, err)
		}
	}
	embed := got["discord"]["embeds"].([]any)[0].(map[string]any)
	if embed["title"] != a.Title || embed["url"] != "http://dash/#incident=abc" || embed["color"].(float64) != 0xdc2626 {
		t.Errorf("discord embed: %+v", embed)
	}
	if f, _ := embed["footer"].(map[string]any); f == nil || !strings.HasPrefix(f["text"].(string), "Incident abc ·") {
		t.Errorf("discord footer should carry the incident for reply-to-ack: %+v", embed["footer"])
	}
	if !strings.Contains(got["slack"]["text"].(string), "HTTP 503") || len(got["slack"]["blocks"].([]any)) != 2 {
		t.Errorf("slack payload: %+v", got["slack"])
	}
	if got["webhook"]["incident_id"] != "abc" {
		t.Errorf("webhook payload: %+v", got["webhook"])
	}
	if ntfyHeaders.Get("Title") != a.Title || ntfyHeaders.Get("Authorization") != "Bearer tk" || ntfyHeaders.Get("Priority") != "high" {
		t.Errorf("ntfy headers: %v", ntfyHeaders)
	}
}

// fakeSMTP is just enough of an SMTP server to accept one message.
func fakeSMTP(t *testing.T) (port int, msgs chan string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	msgs = make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		say := func(s string) { c.Write([]byte(s + "\r\n")) }
		say("220 fake ESMTP")
		var data strings.Builder
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			cmd := strings.ToUpper(strings.TrimSpace(line))
			switch {
			case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
				say("250 fake")
			case strings.HasPrefix(cmd, "MAIL"), strings.HasPrefix(cmd, "RCPT"):
				data.WriteString(strings.TrimSpace(line) + "\n")
				say("250 ok")
			case cmd == "DATA":
				say("354 go ahead")
				for {
					l, _ := r.ReadString('\n')
					if l == ".\r\n" {
						break
					}
					data.WriteString(l)
				}
				say("250 queued")
				msgs <- data.String()
			case cmd == "QUIT":
				say("221 bye")
				return
			default:
				say("250 ok")
			}
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port, msgs
}

func TestEmail(t *testing.T) {
	port, msgs := fakeSMTP(t)
	ch := Channel{Type: "email", SMTPHost: "127.0.0.1", SMTPPort: port, TLS: "none",
		From: "monitor@example.com", To: []string{"ops@example.com"}}
	if err := ch.normalize(); err != nil {
		t.Fatal(err)
	}
	a := Alert{Event: "reboot", Severity: "error", Board: "pixel1", Host: "192.168.6.4", Title: "pixel1: Unexpected reboot", Message: "Sudden crash.", URL: "http://dash/#incident=x", Time: time.Now()}
	if err := sendEmail(ch, a); err != nil {
		t.Fatal(err)
	}
	m := <-msgs
	for _, want := range []string{"RCPT TO:<ops@example.com>", "Subject: [Baldrick] pixel1: Unexpected reboot", "Sudden crash.", "http://dash/#incident=x", "Board: pixel1 (192.168.6.4)"} {
		if !strings.Contains(m, want) {
			t.Errorf("message missing %q:\n%s", want, m)
		}
	}
	// STARTTLS must be required, not silently skipped.
	port2, _ := fakeSMTP(t)
	ch.SMTPPort, ch.TLS = port2, "starttls"
	if err := sendEmail(ch, a); err == nil || !strings.Contains(err.Error(), "STARTTLS") {
		t.Errorf("want a STARTTLS error, got %v", err)
	}
}

func TestCooldown(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()
	n := NewNotifier([]Channel{{Type: "webhook", URL: srv.URL, Enabled: true}}, "")
	n.Send(Alert{Event: "lockup", Board: "b"})
	n.Send(Alert{Event: "lockup", Board: "b"}) // within cooldown: dropped
	n.Send(Alert{Event: "recovered", Board: "b"})
	time.Sleep(300 * time.Millisecond)
	if h := hits.Load(); h != 2 {
		t.Errorf("hits = %d, want 2", h)
	}
}
