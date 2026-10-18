package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

var errNotFound = errors.New("no such board")

// Registry owns the running set of boards. Boards added, edited or removed at
// runtime are saved to <data_dir>/boards.json; once that file exists it takes
// precedence over the boards listed in the config file.
type Registry struct {
	cfg    *Config
	pinger *Pinger
	fpps   *FPPRegistry
	parent context.Context
	path   string // "" = changes are not persisted

	mu    sync.RWMutex
	order []string
	m     map[string]*entry
}

type entry struct {
	b      *Board
	cancel context.CancelFunc
	done   chan struct{}
}

func NewRegistry(ctx context.Context, cfg *Config, p *Pinger, fpps *FPPRegistry) (*Registry, error) {
	r := &Registry{cfg: cfg, pinger: p, fpps: fpps, parent: ctx, m: map[string]*entry{}}
	boards := cfg.Boards
	if dir := cfg.History.DataDir; dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		r.path = filepath.Join(dir, "boards.json")
		if data, err := os.ReadFile(r.path); err == nil {
			var saved []BoardConfig
			if err := json.Unmarshal(data, &saved); err != nil {
				return nil, fmt.Errorf("%s: %w", r.path, err)
			}
			boards = saved
			log.Printf("using %d board(s) from %s", len(saved), r.path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, bc := range boards {
		if err := normalizeBoard(&bc); err != nil {
			return nil, err
		}
		if r.m[bc.Name] != nil {
			return nil, fmt.Errorf("duplicate board name %q", bc.Name)
		}
		r.startLocked(bc)
	}
	return r, r.saveLocked()
}

func (r *Registry) Persistent() bool { return r.path != "" }

// Reconfigure restarts every board monitor with a new config. History,
// incidents and ongoing lockups carry over from disk; the in-memory flight
// recorder starts fresh.
func (r *Registry) Reconfigure(cfg *Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cfg = cfg
	var boards []BoardConfig
	for _, n := range r.order {
		boards = append(boards, r.m[n].b.cfg)
	}
	for _, bc := range boards {
		r.stopLocked(bc.Name)
	}
	for _, bc := range boards {
		r.startLocked(bc)
	}
}

func (r *Registry) startLocked(bc BoardConfig) {
	ctx, cancel := context.WithCancel(r.parent)
	e := &entry{b: NewBoard(bc, r.cfg, r.pinger), cancel: cancel, done: make(chan struct{})}
	e.b.fpps = r.fpps
	r.m[bc.Name] = e
	r.order = append(r.order, bc.Name)
	go func() {
		defer close(e.done)
		e.b.Run(ctx)
	}()
}

// stopLocked cancels a board and waits for its loops to finish so its
// history files are no longer being written.
func (r *Registry) stopLocked(name string) *entry {
	e := r.m[name]
	e.cancel()
	<-e.done
	delete(r.m, name)
	r.order = slices.DeleteFunc(r.order, func(n string) bool { return n == name })
	return e
}

func (r *Registry) saveLocked() error {
	if r.path == "" {
		return nil
	}
	list := make([]BoardConfig, 0, len(r.order))
	for _, n := range r.order {
		list = append(list, r.m[n].b.cfg)
	}
	data, _ := json.MarshalIndent(list, "", "  ")
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

func (r *Registry) List() []*Board {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Board, 0, len(r.order))
	for _, n := range r.order {
		out = append(out, r.m[n].b)
	}
	return out
}

func (r *Registry) Get(name string) *Board {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e := r.m[name]; e != nil {
		return e.b
	}
	return nil
}

func (r *Registry) conflictLocked(bc BoardConfig, except string) error {
	for _, n := range r.order {
		if n == except {
			continue
		}
		if n == bc.Name {
			return fmt.Errorf("a board named %q already exists", bc.Name)
		}
		if r.m[n].b.cfg.Host == bc.Host {
			return fmt.Errorf("%s is already monitored as %q", bc.Host, n)
		}
	}
	return nil
}

func (r *Registry) Add(bc BoardConfig) (BoardConfig, error) {
	if err := normalizeBoard(&bc); err != nil {
		return bc, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.conflictLocked(bc, ""); err != nil {
		return bc, err
	}
	r.startLocked(bc)
	log.Printf("[%s] added (%s)", bc.Name, bc.Host)
	return bc, r.saveLocked()
}

// Update replaces a board's definition, restarting its monitor. A rename
// carries the board's history along.
func (r *Registry) Update(name string, bc BoardConfig) (BoardConfig, error) {
	if err := normalizeBoard(&bc); err != nil {
		return bc, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m[name] == nil {
		return bc, errNotFound
	}
	if err := r.conflictLocked(bc, name); err != nil {
		return bc, err
	}
	pos := slices.Index(r.order, name)
	old := r.stopLocked(name)
	if bc.Name != name && old.b.store.dir != "" {
		newDir := filepath.Join(r.cfg.History.DataDir, unsafeChars.ReplaceAllString(bc.Name, "_"))
		if err := os.Rename(old.b.store.dir, newDir); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("[%s] keeping history under old name: %v", bc.Name, err)
		}
	}
	r.startLocked(bc)
	// Keep the board where it was in the list.
	r.order = slices.Insert(r.order[:len(r.order)-1], pos, bc.Name)
	log.Printf("[%s] updated (%s)", bc.Name, bc.Host)
	return bc, r.saveLocked()
}

func (r *Registry) Remove(name string, purge bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m[name] == nil {
		return errNotFound
	}
	e := r.stopLocked(name)
	if purge && e.b.store.dir != "" {
		if err := os.RemoveAll(e.b.store.dir); err != nil {
			log.Printf("[%s] removing history: %v", name, err)
		}
	}
	log.Printf("[%s] removed (history purged: %v)", name, purge)
	return r.saveLocked()
}
