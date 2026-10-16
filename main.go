// Command baldrick-monitor watches Baldrick pixel controllers: liveness
// (ICMP, HTTP), the board's WebSocket state feed, DDP processing, latency
// and jitter. It serves a dashboard, a JSON API and Prometheus metrics.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	var list []*Board
	byName := map[string]*Board{}
	for _, bc := range cfg.Boards {
		b := NewBoard(bc, cfg, pinger)
		list = append(list, b)
		byName[bc.Name] = b
		go b.Run(ctx)
	}
	views := func() []BoardView {
		out := make([]BoardView, len(list))
		for i, b := range list {
			out[i] = b.View()
		}
		return out
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
			"sources":          cfg.sources(),
			"intervals":        cfg.Intervals,
			"retention_days":   cfg.History.RetentionDays,
			"resolution":       cfg.History.Resolution,
			"persistent":       cfg.History.DataDir != "",
			"probe":            cfg.DDPProbe,
			"thresholds":       cfg.Thresholds,
			"temperature_unit": cfg.TemperatureUnit,
		})
	})
	mux.HandleFunc("GET /api/boards", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, views()) })
	mux.HandleFunc("GET /api/boards/{name}/history", func(w http.ResponseWriter, r *http.Request) {
		b := byName[r.PathValue("name")]
		if b == nil {
			http.NotFound(w, r)
			return
		}
		d, err := time.ParseDuration(r.URL.Query().Get("range"))
		if err != nil || d <= 0 {
			d = 24 * time.Hour
		}
		samples, events := b.History(min(d, cfg.retention()), 720)
		writeJSON(w, map[string]any{"samples": samples, "events": events})
	})
	mux.HandleFunc("POST /api/boards/{name}/probe", func(w http.ResponseWriter, r *http.Request) {
		b := byName[r.PathValue("name")]
		if b == nil {
			http.NotFound(w, r)
			return
		}
		b.TriggerProbe()
		w.WriteHeader(http.StatusAccepted)
	})

	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Printf("monitoring %d board(s); listening on %s; history %dd in %q",
		len(list), cfg.Listen, cfg.History.RetentionDays, cfg.History.DataDir)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}
