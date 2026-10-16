package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// Alert is something worth pushing to a person.
type Alert struct {
	Event    string    `json:"event"` // reboot, down, recovered
	Severity string    `json:"severity"`
	Board    string    `json:"board"`
	Host     string    `json:"host"`
	Title    string    `json:"title"`
	Message  string    `json:"message"`
	Incident string    `json:"incident_id,omitempty"`
	URL      string    `json:"url,omitempty"`
	Time     time.Time `json:"time"`
}

type Notifier struct {
	cfg    AlertsConfig
	client *http.Client
}

func NewNotifier(cfg AlertsConfig) *Notifier {
	return &Notifier{cfg: cfg, client: &http.Client{Timeout: 10 * time.Second}}
}

func (n *Notifier) Enabled() bool { return n != nil && n.cfg.WebhookURL != "" }

// Send posts the alert in the background; failures are only logged.
func (n *Notifier) Send(a Alert) {
	if !n.Enabled() {
		return
	}
	if a.Time.IsZero() {
		a.Time = time.Now()
	}
	if n.cfg.DashboardURL != "" {
		a.URL = strings.TrimRight(n.cfg.DashboardURL, "/") + "/"
		if a.Incident != "" {
			a.URL += "#incident=" + a.Incident
		}
	}
	go func() {
		if err := n.post(a); err != nil {
			log.Printf("alert webhook: %v", err)
		}
	}()
}

func (n *Notifier) post(a Alert) error {
	text := a.Title + "\n" + a.Message
	if a.URL != "" {
		text += "\n" + a.URL
	}
	var body []byte
	ctype := "application/json"
	headers := map[string]string{}
	switch n.cfg.Format {
	case "ntfy":
		body, ctype = []byte(a.Message), "text/plain"
		headers["Title"] = a.Title
		headers["Tags"] = map[string]string{"error": "rotating_light", "warn": "warning"}[a.Severity]
		if a.Severity == "error" {
			headers["Priority"] = "high"
		}
		if a.URL != "" {
			headers["Click"] = a.URL
		}
	case "discord":
		body, _ = json.Marshal(map[string]string{"content": text})
	case "slack":
		body, _ = json.Marshal(map[string]string{"text": text})
	default:
		body, _ = json.Marshal(a)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.cfg.WebhookURL, bytes.NewReader(body))
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
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
