package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ======== control: the few writes the monitor makes ========

// requestedReboot is a reboot someone asked for; onReboot files the reboot
// that follows as expected instead of as a crash.
type requestedReboot struct {
	At time.Time
	By string
}

// requestedRebootWindow is how soon the board has to come back up for its
// reboot to count as the requested one.
const requestedRebootWindow = 3 * time.Minute

// requestedRebootLocked returns who asked for a reboot that happened at
// rebootAt (b.mu held).
func (b *Board) requestedRebootLocked(rebootAt time.Time) (string, bool) {
	r := b.rebootReq
	if r == nil || rebootAt.Before(r.At.Add(-rebootGrace)) || rebootAt.After(r.At.Add(requestedRebootWindow)) {
		return "", false
	}
	b.rebootReq = nil
	return r.By, true
}

// Reboot asks the board to restart, the same way its web UI does
// (GET /reboot, from startReboot() in the board's main.js).
func (b *Board) Reboot(ctx context.Context, by string) error {
	b.mu.Lock()
	b.rebootReq = &requestedReboot{At: time.Now(), By: by}
	b.addEvent("info", "Reboot requested by %s", by)
	b.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+b.cfg.Host+"/reboot", nil)
	resp, err := b.client.Do(req)
	if err != nil {
		// Keep the request: the board may have gone down before answering,
		// and the uptime will tell.
		switch {
		case strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "reset"):
			return nil // dropped the connection: it's restarting
		case errors.Is(err, context.DeadlineExceeded):
			return errors.New("no answer within 5s; it may still be rebooting, check its status")
		}
		b.mu.Lock()
		b.rebootReq = nil
		b.mu.Unlock()
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		b.mu.Lock()
		b.rebootReq = nil
		b.mu.Unlock()
		return fmt.Errorf("board answered HTTP %d", resp.StatusCode)
	}
	return nil
}

var fppControlClient = &http.Client{Timeout: 10 * time.Second}

// fppControl sends a command to FPP's REST API (/api/...).
func fppControl(ctx context.Context, host, method, path string, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+host+path, rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := fppControlClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("FPP answered HTTP %d %s", resp.StatusCode, truncate(strings.TrimSpace(string(out)), 200))
	}
	// FPP reports some failures as 200 with {"status": "ERROR"}.
	var st struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if json.Unmarshal(out, &st) == nil && strings.EqualFold(st.Status, "error") {
		return nil, fmt.Errorf("FPP: %s", orStr(st.Message, "error"))
	}
	return out, nil
}

// Host is the address of the FPP with this name.
func (r *FPPRegistry) Host(name string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.findLocked(name); m != nil {
		return m.cfg.Host, nil
	}
	return "", errNoFPP
}

// monitorBackend runs bot commands against the running monitor.
type monitorBackend struct {
	reg  *Registry
	fppr *FPPRegistry
}

func (m monitorBackend) boards() []BoardView {
	list := m.reg.List()
	out := make([]BoardView, len(list))
	for i, b := range list {
		out[i] = b.View()
	}
	return out
}

func (m monitorBackend) rebootBoard(ctx context.Context, name, by string) error {
	b := m.reg.Get(name)
	if b == nil {
		return errNotFound
	}
	return b.Reboot(ctx, by)
}

func (m monitorBackend) setMaintenance(name string, on bool, d time.Duration, note, by string) (*Maintenance, error) {
	return m.reg.SetMaintenance(name, on, d, note, by)
}

func (m monitorBackend) ackAll(board, note, by string) int {
	n := 0
	for _, b := range m.reg.List() {
		if board == "" || board == b.cfg.Name {
			n += b.AckAll(Ack{At: time.Now(), Note: strings.TrimSpace(note), By: by})
		}
	}
	return n
}

func (m monitorBackend) ackIncident(id, note, by string) (Incident, error) {
	for _, b := range m.reg.List() {
		if _, ok := b.Incident(id); ok {
			return b.SetAck(id, &Ack{At: time.Now(), Note: strings.TrimSpace(note), By: by})
		}
	}
	return Incident{}, errNoIncident
}

func (m monitorBackend) fpps() []FPPView { return m.fppr.Views() }

func (m monitorBackend) fppCall(ctx context.Context, fpp, method, path string, body any) ([]byte, error) {
	host, err := m.fppr.Host(fpp)
	if err != nil {
		return nil, err
	}
	return fppControl(ctx, host, method, path, body)
}

func (m monitorBackend) fppPlaylists(ctx context.Context, fpp string) ([]string, error) {
	body, err := m.fppCall(ctx, fpp, http.MethodGet, "/api/playlists", nil)
	if err != nil {
		return nil, err
	}
	var out []string
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("playlists: %w", err)
	}
	return out, nil
}

func (m monitorBackend) fppStart(ctx context.Context, fpp, playlist string) error {
	_, err := m.fppCall(ctx, fpp, http.MethodGet, "/api/playlist/"+url.PathEscape(playlist)+"/start", nil)
	return err
}

func (m monitorBackend) fppStop(ctx context.Context, fpp string, graceful bool) error {
	path := "/api/playlists/stop"
	if graceful {
		path = "/api/playlists/stopgracefully"
	}
	_, err := m.fppCall(ctx, fpp, http.MethodGet, path, nil)
	return err
}

func (m monitorBackend) fppVolume(ctx context.Context, fpp string) (int, error) {
	body, err := m.fppCall(ctx, fpp, http.MethodGet, "/api/system/volume", nil)
	if err != nil {
		return 0, err
	}
	var v struct {
		Volume flexNum `json:"volume"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return 0, fmt.Errorf("volume: %w", err)
	}
	return int(v.Volume), nil
}

func (m monitorBackend) fppSetVolume(ctx context.Context, fpp string, level int) error {
	_, err := m.fppCall(ctx, fpp, http.MethodPost, "/api/system/volume", map[string]int{"volume": level})
	return err
}
