package main

import (
	"errors"
	"log"
	"strings"
	"time"
)

// Maintenance silences a board's alerts while someone works on it. The
// monitor still detects and records incidents; it just doesn't notify
// anyone, and it acknowledges them on its own.
type Maintenance struct {
	Since time.Time  `json:"since"`
	Until *time.Time `json:"until,omitempty"` // nil: until turned off
	Note  string     `json:"note,omitempty"`
	By    string     `json:"by,omitempty"`
}

func (m *Maintenance) activeAt(t time.Time) bool {
	return m != nil && (m.Until == nil || t.Before(*m.Until))
}

const maintenanceAckNote = "During maintenance"

// maintenanceLocked returns the board's maintenance window if it's in effect
// (b.mu held).
func (b *Board) maintenanceLocked(now time.Time) *Maintenance {
	if b.maint.activeAt(now) {
		return b.maint
	}
	return nil
}

func (b *Board) InMaintenance() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.maintenanceLocked(time.Now()) != nil
}

// config is the board's definition as saved to boards.json.
func (b *Board) config() BoardConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()
	bc := b.cfg
	bc.Maintenance = b.maintenanceLocked(time.Now())
	return bc
}

// maintenanceAck acknowledges an incident opened during maintenance (b.mu
// held).
func (b *Board) maintenanceAck(inc *Incident, now time.Time) {
	if inc.Ack == nil && b.maintenanceLocked(now) != nil {
		inc.Ack = &Ack{At: now, Auto: true, Note: maintenanceAckNote}
	}
}

// setMaintenance starts (m != nil) or ends maintenance and logs it.
func (b *Board) setMaintenance(m *Maintenance) {
	b.mu.Lock()
	defer b.mu.Unlock()
	was := b.maint != nil
	b.maint = m
	switch {
	case m != nil:
		until := "until turned off"
		if m.Until != nil {
			until = "until " + m.Until.Local().Format("Jan 2 15:04")
		}
		b.addEvent("info", "Maintenance mode on %s%s; alerts are silenced", until, noteSuffix(m.Note))
	case was:
		b.addEvent("info", "Maintenance mode off; alerts resume")
		// Anything still wrong now is news to whoever is on call.
		for _, inc := range []*Incident{b.activeLockup, b.activeDDP} {
			if inc == nil || inc.Ack == nil || !inc.Ack.Auto || inc.Ack.Note != maintenanceAckNote {
				continue
			}
			inc.Ack = nil
			b.persist(inc, nil)
			if inc.Type == "lockup" {
				b.alert(inc, "lockup", "error", "Still not responding after maintenance ended. "+inc.Cause)
			} else {
				b.alert(inc, "ddp", "warn", "Still not processing DDP after maintenance ended. "+inc.Cause)
			}
		}
	}
}

var errBadMaintenance = errors.New("duration must be a positive Go duration like 30m or 2h, or empty for until turned off")

// SetMaintenance puts a board in maintenance for d (0 = until turned off),
// or takes it out when on is false. The window survives a restart.
func (r *Registry) SetMaintenance(name string, on bool, d time.Duration, note, by string) (*Maintenance, error) {
	if d < 0 || d > 30*24*time.Hour {
		return nil, errBadMaintenance
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.m[name]
	if e == nil {
		return nil, errNotFound
	}
	var m *Maintenance
	if on {
		now := time.Now()
		m = &Maintenance{Since: now, Note: truncate(strings.TrimSpace(note), 200), By: by}
		if d > 0 {
			until := now.Add(d)
			m.Until = &until
		}
	}
	e.b.setMaintenance(m)
	r.scheduleExpiryLocked(e.b, m)
	log.Printf("[%s] maintenance %v (for %s)", name, on, d)
	return m, r.saveLocked()
}

// scheduleExpiryLocked ends a timed maintenance window when it runs out, so
// the board's event log says so and boards.json drops it.
func (r *Registry) scheduleExpiryLocked(b *Board, m *Maintenance) {
	if m == nil || m.Until == nil {
		return
	}
	time.AfterFunc(time.Until(*m.Until), func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if e := r.m[b.cfg.Name]; e == nil || e.b != b {
			return // removed or restarted; the new board has its own timer
		}
		b.mu.Lock()
		current := b.maint == m
		b.mu.Unlock()
		if !current {
			return
		}
		b.setMaintenance(nil)
		log.Printf("[%s] maintenance ended", b.cfg.Name)
		if err := r.saveLocked(); err != nil {
			log.Printf("saving boards: %v", err)
		}
	})
}
