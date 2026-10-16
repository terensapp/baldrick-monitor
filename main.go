// Command baldrick-monitor watches Baldrick pixel controllers: liveness
// (ICMP, HTTP), the board's WebSocket state feed, DDP processing, latency
// and jitter. It serves a dashboard, a JSON API and Prometheus metrics.
package main

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
)

//go:embed web
var webFS embed.FS

func main() {
	cfgPath := flag.String("config", "", "path to JSON config")
	listen := flag.String("listen", "", "listen address (overrides config)")
	boards := flag.String("boards", "", "comma-separated board hosts to add")
	dataDir := flag.String("data", "", "history directory (overrides config)")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath, *listen, *boards, *dataDir)
	if err != nil {
		log.Fatal(err)
	}
	pinger, err := NewPinger()
	if err != nil {
		log.Printf("ICMP disabled: %v", err)
		pinger = nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg, err := NewRegistry(ctx, cfg, pinger)
	if err != nil {
		log.Fatal(err)
	}
	views := func() []BoardView {
		list := reg.List()
		out := make([]BoardView, len(list))
		for i, b := range list {
			out[i] = b.View()
		}
		return out
	}
	board := func(w http.ResponseWriter, r *http.Request) *Board {
		b := reg.Get(r.PathValue("name"))
		if b == nil {
			httpError(w, http.StatusNotFound, errNotFound)
		}
		return b
	}
	admin := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if cfg.AdminToken != "" {
				got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				if subtle.ConstantTimeCompare([]byte(got), []byte(cfg.AdminToken)) != 1 {
					httpError(w, http.StatusUnauthorized, errors.New("admin token required"))
					return
				}
			}
			h(w, r)
		}
	}

	mux := http.NewServeMux()
	static, _ := fs.Sub(webFS, "web")
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		writeMetrics(w, views())
	})
	mux.HandleFunc("GET /api/meta", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"sources":                 cfg.sources(),
			"intervals":               cfg.Intervals,
			"retention_days":          cfg.History.RetentionDays,
			"resolution":              cfg.History.Resolution,
			"persistent":              cfg.History.DataDir != "",
			"boards_persistent":       reg.Persistent(),
			"auth_required":           cfg.AdminToken != "",
			"probe":                   cfg.DDPProbe,
			"thresholds":              cfg.Thresholds,
			"temperature_unit":        cfg.TemperatureUnit,
			"suggested_subnets":       suggestedSubnets(reg),
			"alerts_enabled":          cfg.notifier.Enabled(),
			"incident_retention_days": cfg.Incidents.RetentionDays,
			"recorder_window":         cfg.Incidents.RecorderWindow,
		})
	})
	mux.HandleFunc("GET /api/boards", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, views()) })
	mux.HandleFunc("POST /api/boards", admin(func(w http.ResponseWriter, r *http.Request) {
		var bc BoardConfig
		if err := json.NewDecoder(r.Body).Decode(&bc); err != nil {
			httpError(w, http.StatusBadRequest, err)
			return
		}
		bc, err := reg.Add(bc)
		if err != nil {
			httpError(w, http.StatusBadRequest, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(bc)
	}))
	mux.HandleFunc("PUT /api/boards/{name}", admin(func(w http.ResponseWriter, r *http.Request) {
		var bc BoardConfig
		if err := json.NewDecoder(r.Body).Decode(&bc); err != nil {
			httpError(w, http.StatusBadRequest, err)
			return
		}
		bc, err := reg.Update(r.PathValue("name"), bc)
		if errors.Is(err, errNotFound) {
			httpError(w, http.StatusNotFound, err)
			return
		} else if err != nil {
			httpError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, bc)
	}))
	mux.HandleFunc("DELETE /api/boards/{name}", admin(func(w http.ResponseWriter, r *http.Request) {
		err := reg.Remove(r.PathValue("name"), r.URL.Query().Get("purge") == "1")
		if errors.Is(err, errNotFound) {
			httpError(w, http.StatusNotFound, err)
			return
		} else if err != nil {
			httpError(w, http.StatusInternalServerError, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("GET /api/boards/{name}/history", func(w http.ResponseWriter, r *http.Request) {
		b := board(w, r)
		if b == nil {
			return
		}
		d, err := time.ParseDuration(r.URL.Query().Get("range"))
		if err != nil || d <= 0 {
			d = 24 * time.Hour
		}
		samples, events := b.History(min(d, cfg.retention()), 720)
		writeJSON(w, map[string]any{"samples": samples, "events": events})
	})
	mux.HandleFunc("POST /api/boards/{name}/probe", admin(func(w http.ResponseWriter, r *http.Request) {
		if b := board(w, r); b != nil {
			b.TriggerProbe()
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	incidentBoard := func(w http.ResponseWriter, r *http.Request) (*Board, string) {
		id := r.PathValue("id")
		for _, b := range reg.List() {
			if _, ok := b.Incident(id); ok {
				return b, id
			}
		}
		httpError(w, http.StatusNotFound, errNoIncident)
		return nil, id
	}
	mux.HandleFunc("GET /api/incidents", func(w http.ResponseWriter, r *http.Request) {
		var out []Incident
		for _, b := range reg.List() {
			if name := r.URL.Query().Get("board"); name != "" && name != b.cfg.Name {
				continue
			}
			out = append(out, b.Incidents()...)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].DetectedAt.After(out[j].DetectedAt) })
		if out == nil {
			out = []Incident{}
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /api/incidents/{id}", func(w http.ResponseWriter, r *http.Request) {
		if b, id := incidentBoard(w, r); b != nil {
			inc, _ := b.Incident(id)
			writeJSON(w, inc)
		}
	})
	mux.HandleFunc("GET /api/incidents/{id}/export", func(w http.ResponseWriter, r *http.Request) {
		b, id := incidentBoard(w, r)
		if b == nil {
			return
		}
		exp, _ := b.Export(id)
		name := fmt.Sprintf("baldrick-%s-%s-%s.json", unsafeChars.ReplaceAllString(exp.Incident.Board, "_"), exp.Incident.Type, exp.Incident.StartedAt.UTC().Format("20060102-150405"))
		w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", " ")
		enc.Encode(exp)
	})
	mux.HandleFunc("POST /api/incidents/{id}/ack", admin(func(w http.ResponseWriter, r *http.Request) {
		b, id := incidentBoard(w, r)
		if b == nil {
			return
		}
		var body struct {
			Note string `json:"note"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		inc, err := b.SetAck(id, &Ack{At: time.Now(), Note: strings.TrimSpace(body.Note), By: host})
		if err != nil {
			httpError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, inc)
	}))
	mux.HandleFunc("POST /api/incidents/{id}/reopen", admin(func(w http.ResponseWriter, r *http.Request) {
		b, id := incidentBoard(w, r)
		if b == nil {
			return
		}
		inc, err := b.SetAck(id, nil)
		if err != nil {
			httpError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, inc)
	}))
	mux.HandleFunc("POST /api/boards/{name}/capture", admin(func(w http.ResponseWriter, r *http.Request) {
		b := board(w, r)
		if b == nil {
			return
		}
		var body struct {
			Note string `json:"note"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		inc := b.Capture(ctx, strings.TrimSpace(body.Note))
		inc.Timeline = nil
		writeJSON(w, inc)
	}))
	mux.HandleFunc("GET /api/inspect", admin(func(w http.ResponseWriter, r *http.Request) {
		host := strings.TrimSpace(r.URL.Query().Get("host"))
		bc := BoardConfig{Host: host}
		if err := normalizeBoard(&bc); err != nil {
			httpError(w, http.StatusBadRequest, err)
			return
		}
		f, err := inspect(r.Context(), host)
		if err != nil {
			httpError(w, http.StatusBadGateway, err)
			return
		}
		writeJSON(w, markMonitored(reg, []Found{*f})[0])
	}))
	mux.HandleFunc("GET /api/scan", admin(func(w http.ResponseWriter, r *http.Request) {
		found, err := scanSubnet(r.Context(), r.URL.Query().Get("cidr"))
		if err != nil {
			httpError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, markMonitored(reg, found))
	}))

	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Printf("monitoring %d board(s); listening on %s; history %dd in %q",
		len(reg.List()), cfg.Listen, cfg.History.RetentionDays, cfg.History.DataDir)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func markMonitored(reg *Registry, found []Found) []Found {
	byHost := map[string]string{}
	for _, b := range reg.List() {
		byHost[b.cfg.Host] = b.cfg.Name
	}
	for i := range found {
		found[i].Monitored = byHost[found[i].Host]
	}
	return found
}

// suggestedSubnets offers the /24s of boards already being monitored as
// scan targets.
func suggestedSubnets(reg *Registry) []string {
	var out []string
	for _, b := range reg.List() {
		if a, err := netip.ParseAddr(hostOnly(b.cfg.Host)); err == nil && a.Is4() {
			p, _ := a.Prefix(24)
			if s := p.String(); !slices.Contains(out, s) {
				out = append(out, s)
			}
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
