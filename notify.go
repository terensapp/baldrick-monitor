package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/smtp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Alert is something worth pushing to a person.
type Alert struct {
	Event    string    `json:"event"` // lockup, reboot, ddp, recovered, update, test
	Severity string    `json:"severity"`
	Board    string    `json:"board"`
	Host     string    `json:"host"`
	Title    string    `json:"title"`
	Message  string    `json:"message"`
	Incident string    `json:"incident_id,omitempty"`
	URL      string    `json:"url,omitempty"`
	Time     time.Time `json:"time"`
}

// AlertEvents are the events a channel can subscribe to.
var AlertEvents = []string{"lockup", "reboot", "ddp", "memory", "recovered", "update"}

// Channel is one place alerts go.
type Channel struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Type    string   `json:"type"` // discord, slack, ntfy, webhook, email
	Enabled bool     `json:"enabled"`
	Events  []string `json:"events"` // empty = lockup, reboot, ddp, recovered

	URL   string `json:"url,omitempty"`   // discord/slack/webhook URL, ntfy topic URL
	Token string `json:"token,omitempty"` // ntfy access token (optional)

	SMTPHost string   `json:"smtp_host,omitempty"`
	SMTPPort int      `json:"smtp_port,omitempty"`
	TLS      string   `json:"tls,omitempty"` // starttls (default), tls, none
	Username string   `json:"username,omitempty"`
	Password string   `json:"password,omitempty"`
	From     string   `json:"from,omitempty"`
	To       []string `json:"to,omitempty"`
}

func newChannelID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (ch *Channel) wants(event string) bool {
	if event == "test" {
		return true
	}
	if len(ch.Events) == 0 {
		return event != "update"
	}
	return slices.Contains(ch.Events, event)
}

func (ch *Channel) normalize() error {
	ch.Type = strings.ToLower(strings.TrimSpace(ch.Type))
	ch.Name, ch.URL = strings.TrimSpace(ch.Name), strings.TrimSpace(ch.URL)
	if ch.ID == "" {
		ch.ID = newChannelID()
	}
	if ch.Name == "" {
		ch.Name = ch.Type
	}
	for _, e := range ch.Events {
		if !slices.Contains(AlertEvents, e) {
			return fmt.Errorf("%s: unknown event %q", ch.Name, e)
		}
	}
	switch ch.Type {
	case "discord", "slack", "ntfy", "webhook":
		if !strings.HasPrefix(ch.URL, "https://") && !strings.HasPrefix(ch.URL, "http://") {
			return fmt.Errorf("%s: needs an http(s) URL", ch.Name)
		}
	case "email":
		var to []string
		for _, a := range ch.To {
			if a = strings.TrimSpace(a); a != "" {
				to = append(to, a)
			}
		}
		ch.To, ch.SMTPHost, ch.From = to, strings.TrimSpace(ch.SMTPHost), strings.TrimSpace(ch.From)
		if ch.TLS == "" {
			ch.TLS = "starttls"
		}
		if ch.TLS != "starttls" && ch.TLS != "tls" && ch.TLS != "none" {
			return fmt.Errorf("%s: tls must be starttls, tls or none", ch.Name)
		}
		if ch.SMTPPort == 0 {
			ch.SMTPPort = map[string]int{"starttls": 587, "tls": 465, "none": 25}[ch.TLS]
		}
		if ch.SMTPHost == "" || ch.From == "" || len(ch.To) == 0 {
			return fmt.Errorf("%s: email needs an SMTP server, a from address and at least one recipient", ch.Name)
		}
	default:
		return fmt.Errorf("unknown notification type %q", ch.Type)
	}
	return nil
}

// Notifier fans alerts out to the configured channels.
type Notifier struct {
	channels  []Channel
	dashboard string
	client    *http.Client

	mu   sync.Mutex
	last map[string]time.Time // board/event -> last sent, to damp flapping
}

const alertCooldown = 2 * time.Minute

func NewNotifier(channels []Channel, dashboardURL string) *Notifier {
	return &Notifier{channels: channels, dashboard: strings.TrimRight(dashboardURL, "/"),
		client: &http.Client{Timeout: 10 * time.Second}, last: map[string]time.Time{}}
}

func (n *Notifier) Enabled() bool {
	if n == nil {
		return false
	}
	for _, c := range n.channels {
		if c.Enabled {
			return true
		}
	}
	return false
}

func (n *Notifier) prepare(a *Alert) {
	if a.Time.IsZero() {
		a.Time = time.Now()
	}
	if n.dashboard != "" && a.URL == "" {
		a.URL = n.dashboard + "/"
		if a.Incident != "" {
			a.URL += "#incident=" + a.Incident
		}
	}
}

// Send delivers the alert in the background; failures are only logged.
func (n *Notifier) Send(a Alert) {
	if !n.Enabled() {
		return
	}
	key := a.Board + "/" + a.Event
	n.mu.Lock()
	if t, ok := n.last[key]; ok && time.Since(t) < alertCooldown && a.Event != "test" {
		n.mu.Unlock()
		log.Printf("alert %s suppressed (cooldown)", key)
		return
	}
	n.last[key] = time.Now()
	n.mu.Unlock()
	n.prepare(&a)
	for _, ch := range n.channels {
		if ch.Enabled && ch.wants(a.Event) {
			go func() {
				if err := n.deliver(ch, a); err != nil {
					log.Printf("alert to %s (%s): %v", ch.Name, ch.Type, err)
				}
			}()
		}
	}
}

// Test sends a test alert to one channel and reports the result.
func (n *Notifier) Test(ch Channel) error {
	a := Alert{Event: "test", Severity: "info", Board: "baldrick-monitor", Title: "Test notification",
		Message: "If you can read this, Baldrick Monitor can reach this channel."}
	n.prepare(&a)
	return n.deliver(ch, a)
}

func (n *Notifier) deliver(ch Channel, a Alert) error {
	if ch.Type == "email" {
		return sendEmail(ch, a)
	}
	text := a.Title + "\n" + a.Message
	if a.URL != "" {
		text += "\n" + a.URL
	}
	var body []byte
	ctype := "application/json"
	headers := map[string]string{}
	switch ch.Type {
	case "ntfy":
		body, ctype = []byte(a.Message), "text/plain; charset=utf-8"
		headers["Title"] = a.Title
		headers["Tags"] = map[string]string{"error": "rotating_light", "warn": "warning", "info": "white_check_mark"}[a.Severity]
		headers["Priority"] = map[string]string{"error": "high", "warn": "default", "info": "low"}[a.Severity]
		if a.URL != "" {
			headers["Click"] = a.URL
		}
		if ch.Token != "" {
			headers["Authorization"] = "Bearer " + ch.Token
		}
	case "discord":
		embed := map[string]any{
			"title": a.Title, "description": a.Message, "timestamp": a.Time.UTC().Format(time.RFC3339),
			"color": map[string]int{"error": 0xdc2626, "warn": 0xd97706, "info": 0x16a34a}[a.Severity],
		}
		if a.URL != "" {
			embed["url"] = a.URL
		}
		if a.Host != "" {
			embed["fields"] = []map[string]any{{"name": "Board", "value": a.Board, "inline": true}, {"name": "Address", "value": a.Host, "inline": true}}
		}
		body, _ = json.Marshal(map[string]any{"username": "Baldrick Monitor", "embeds": []any{embed}})
	case "slack":
		title := "*" + slackEscape(a.Title) + "*"
		if a.URL != "" {
			title = "*<" + a.URL + "|" + slackEscape(a.Title) + ">*"
		}
		emoji := map[string]string{"error": ":rotating_light:", "warn": ":warning:", "info": ":white_check_mark:"}[a.Severity]
		ctx := a.Time.Local().Format("Jan 2 15:04:05")
		if a.Host != "" {
			ctx = a.Board + " · " + a.Host + " · " + ctx
		}
		body, _ = json.Marshal(map[string]any{
			"text": text,
			"blocks": []any{
				map[string]any{"type": "section", "text": map[string]string{"type": "mrkdwn", "text": emoji + " " + title + "\n" + slackEscape(a.Message)}},
				map[string]any{"type": "context", "elements": []any{map[string]string{"type": "mrkdwn", "text": slackEscape(ctx)}}},
			},
		})
	default: // webhook
		body, _ = json.Marshal(a)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ch.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", ctype)
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func slackEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}

func sendEmail(ch Channel, a Alert) error {
	addr := net.JoinHostPort(ch.SMTPHost, strconv.Itoa(ch.SMTPPort))
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var conn net.Conn
	var err error
	if ch.TLS == "tls" {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{ServerName: ch.SMTPHost})
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return err
	}
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	c, err := smtp.NewClient(conn, ch.SMTPHost)
	if err != nil {
		conn.Close()
		return err
	}
	defer c.Close()
	if ch.TLS == "starttls" {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return fmt.Errorf("%s doesn't offer STARTTLS (choose tls or none)", ch.SMTPHost)
		}
		if err := c.StartTLS(&tls.Config{ServerName: ch.SMTPHost}); err != nil {
			return err
		}
	}
	if ch.Username != "" {
		if err := c.Auth(smtp.PlainAuth("", ch.Username, ch.Password, ch.SMTPHost)); err != nil {
			return fmt.Errorf("login: %w", err)
		}
	}
	if err := c.Mail(ch.From); err != nil {
		return err
	}
	for _, to := range ch.To {
		if err := c.Rcpt(to); err != nil {
			return fmt.Errorf("recipient %s: %w", to, err)
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	body := a.Message + "\r\n"
	if a.Host != "" {
		body += "\r\nBoard: " + a.Board + " (" + a.Host + ")"
	}
	body += "\r\nTime: " + a.Time.Local().Format(time.RFC1123) + "\r\n"
	if a.URL != "" {
		body += "\r\n" + a.URL + "\r\n"
	}
	msg := "From: " + ch.From + "\r\n" +
		"To: " + strings.Join(ch.To, ", ") + "\r\n" +
		"Subject: " + mime.QEncoding.Encode("utf-8", "[Baldrick] "+a.Title) + "\r\n" +
		"Date: " + a.Time.Format(time.RFC1123Z) + "\r\n" +
		"MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n" +
		strings.ReplaceAll(strings.ReplaceAll(body, "\r\n", "\n"), "\n", "\r\n")
	if _, err := w.Write([]byte(msg)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
