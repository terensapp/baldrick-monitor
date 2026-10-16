package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Sample is one rollup of a board's metrics. Missing values are omitted.
type Sample struct {
	T      time.Time          `json:"t"`
	Status string             `json:"status"`
	V      map[string]float64 `json:"v"`
}

// store persists one board's history as daily JSON Lines files:
// <data_dir>/<board>/{samples,events}-YYYY-MM-DD.jsonl
type store struct {
	dir       string // "" = memory only
	retention time.Duration
	mu        sync.Mutex
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func openStore(base, board string, retention time.Duration) *store {
	s := &store{retention: retention}
	if base == "" {
		return s
	}
	dir := filepath.Join(base, unsafeChars.ReplaceAllString(board, "_"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[%s] history disabled: %v", board, err)
		return s
	}
	s.dir = dir
	return s
}

func (s *store) append(kind string, t time.Time, v any) {
	if s.dir == "" {
		return
	}
	line, err := json.Marshal(v)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name := filepath.Join(s.dir, kind+"-"+t.UTC().Format("2006-01-02")+".jsonl")
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("history write: %v", err)
		return
	}
	defer f.Close()
	f.Write(append(line, '\n'))
}

// files lists a kind's daily files within retention, oldest first.
func (s *store) files(kind string, now time.Time) []string {
	if s.dir == "" {
		return nil
	}
	names, _ := filepath.Glob(filepath.Join(s.dir, kind+"-*.jsonl"))
	sort.Strings(names)
	cutoff := now.Add(-s.retention).UTC().Format("2006-01-02")
	var keep []string
	for _, n := range names {
		if day := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(n), kind+"-"), ".jsonl"); day >= cutoff {
			keep = append(keep, n)
		}
	}
	return keep
}

func loadLines[T any](s *store, kind string, now time.Time, keep func(*T) bool) []T {
	var out []T
	for _, name := range s.files(kind, now) {
		f, err := os.Open(name)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			var v T
			if json.Unmarshal(sc.Bytes(), &v) == nil && keep(&v) {
				out = append(out, v)
			}
		}
		f.Close()
	}
	return out
}

// prune deletes daily files that are entirely older than the retention.
func (s *store) prune(now time.Time) {
	if s.dir == "" {
		return
	}
	cutoff := now.Add(-s.retention).UTC().Format("2006-01-02")
	names, _ := filepath.Glob(filepath.Join(s.dir, "*.jsonl"))
	for _, n := range names {
		base := strings.TrimSuffix(filepath.Base(n), ".jsonl")
		if len(base) > 10 && base[len(base)-10:] < cutoff { // trailing YYYY-MM-DD
			os.Remove(n)
		}
	}
}

var statusRank = map[string]int{"up": 0, "unknown": 1, "degraded": 2, "down": 3}

// downsample merges samples into at most n buckets. Latency percentiles,
// loss and drops keep the worst value so short incidents stay visible;
// everything else is averaged.
func downsample(in []Sample, n int) []Sample {
	if n <= 0 || len(in) <= n {
		return in
	}
	size := (len(in) + n - 1) / n
	out := make([]Sample, 0, n)
	for i := 0; i < len(in); i += size {
		chunk := in[i:min(i+size, len(in))]
		agg := Sample{T: chunk[0].T, Status: "up", V: map[string]float64{}}
		cnt := map[string]int{}
		for _, s := range chunk {
			if statusRank[s.Status] > statusRank[agg.Status] {
				agg.Status = s.Status
			}
			for k, v := range s.V {
				if worstKey(k) {
					if c, ok := agg.V[k]; !ok || v > c {
						agg.V[k] = v
					}
				} else {
					agg.V[k] += v
					cnt[k]++
				}
			}
		}
		for k, c := range cnt {
			agg.V[k] /= float64(c)
		}
		out = append(out, agg)
	}
	return out
}

func worstKey(k string) bool {
	return strings.Contains(k, "p95") || strings.Contains(k, "loss") || strings.Contains(k, "dropped") || k == "ws_down"
}
