package main

import (
	"fmt"
	"log"
	"slices"
	"strings"
	"time"
)

// QuietHours holds notifications during a daily window (in the monitor's
// local time zone) and sends what was held as one summary when it ends.
type QuietHours struct {
	Enabled bool   `json:"enabled"`
	Start   string `json:"start"` // "22:00"
	End     string `json:"end"`   // "07:00"; earlier than start = overnight
	// Days the window starts on, 0 = Sunday. Empty = every day.
	Days []int `json:"days"`
	// Still send "stopped responding" alerts (and their recoveries) right away.
	AllowLockups bool `json:"allow_lockups"`
}

func parseClock(s string) (int, error) {
	t, err := time.Parse("15:04", strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("quiet hours: %q is not a time like 22:00", s)
	}
	return t.Hour()*60 + t.Minute(), nil
}

func (q *QuietHours) normalize() error {
	if q.Start == "" {
		q.Start = "22:00"
	}
	if q.End == "" {
		q.End = "07:00"
	}
	s, err := parseClock(q.Start)
	if err != nil {
		return err
	}
	e, err := parseClock(q.End)
	if err != nil {
		return err
	}
	if s == e {
		return fmt.Errorf("quiet hours start and end are both %s", q.Start)
	}
	q.Start, q.End = fmt.Sprintf("%02d:%02d", s/60, s%60), fmt.Sprintf("%02d:%02d", e/60, e%60)
	for _, d := range q.Days {
		if d < 0 || d > 6 {
			return fmt.Errorf("quiet hours: day %d is not 0 (Sunday) to 6 (Saturday)", d)
		}
	}
	slices.Sort(q.Days)
	q.Days = slices.Compact(q.Days)
	if len(q.Days) == 7 {
		q.Days = nil
	}
	return nil
}

func (q *QuietHours) startsOn(d time.Weekday) bool {
	return len(q.Days) == 0 || slices.Contains(q.Days, int(d))
}

// Active reports whether t falls in the quiet window. An overnight window
// belongs to the day it starts on.
func (q *QuietHours) Active(t time.Time) bool {
	if q == nil || !q.Enabled {
		return false
	}
	s, err1 := parseClock(q.Start)
	e, err2 := parseClock(q.End)
	if err1 != nil || err2 != nil {
		return false
	}
	t = t.Local()
	m := t.Hour()*60 + t.Minute()
	if s < e {
		return m >= s && m < e && q.startsOn(t.Weekday())
	}
	return m >= s && q.startsOn(t.Weekday()) || m < e && q.startsOn(t.AddDate(0, 0, -1).Weekday())
}

// holds reports whether an alert waits for the end of quiet hours (n.mu
// held). A lockup let through also lets its recovery through.
func (n *Notifier) holds(a Alert, now time.Time) bool {
	if a.Event == "test" || !n.quiet.Active(now) {
		return false
	}
	if n.quiet.AllowLockups {
		if a.Event == "lockup" {
			if a.Incident != "" {
				n.passed[a.Incident] = true
			}
			return false
		}
		if a.Event == "recovered" && n.passed[a.Incident] {
			delete(n.passed, a.Incident)
			return false
		}
	}
	return true
}

const maxHeld = 200

// hold queues an alert and makes sure something will send it (n.mu held).
func (n *Notifier) hold(a Alert) {
	if len(n.held) >= maxHeld {
		n.held = n.held[1:]
	}
	n.held = append(n.held, a)
	log.Printf("alert %s/%s held (quiet hours)", a.Board, a.Event)
	n.armLocked()
}

func (n *Notifier) armLocked() {
	if n.timer == nil && len(n.held) > 0 && !n.stopped {
		n.timer = time.AfterFunc(n.recheck, n.flushIfDone)
	}
}

// flushIfDone sends held alerts once quiet hours are over; until then it
// checks back every minute.
func (n *Notifier) flushIfDone() {
	n.mu.Lock()
	n.timer = nil
	if n.stopped {
		n.mu.Unlock()
		return
	}
	if n.quiet.Active(n.now()) {
		n.armLocked()
		n.mu.Unlock()
		return
	}
	held := n.held
	n.held = nil
	clear(n.passed)
	n.mu.Unlock()
	n.flush(held)
}

// flush sends each channel one message: the alert itself if only one was
// held for it, otherwise a summary.
func (n *Notifier) flush(held []Alert) {
	if len(held) == 0 {
		return
	}
	for _, ch := range n.channels {
		if !ch.Enabled {
			continue
		}
		var mine []Alert
		for _, a := range held {
			if ch.wants(a.Event) {
				mine = append(mine, a)
			}
		}
		if len(mine) == 0 {
			continue
		}
		a := mine[0]
		if len(mine) > 1 {
			a = digest(mine)
			n.prepare(&a)
		}
		go func() {
			if err := n.deliver(ch, a); err != nil {
				log.Printf("alert to %s (%s): %v", ch.Name, ch.Type, err)
			}
		}()
	}
}

func digest(held []Alert) Alert {
	rank := map[string]int{"info": 0, "warn": 1, "error": 2}
	d := Alert{Event: "digest", Severity: "info", Board: "baldrick-monitor", Time: time.Now(),
		Title: fmt.Sprintf("%d alerts during quiet hours", len(held)), Held: held}
	var lines []string
	for _, a := range held {
		if rank[a.Severity] > rank[d.Severity] {
			d.Severity = a.Severity
		}
		lines = append(lines, a.Time.Local().Format("Jan 2 15:04")+" · "+a.Title)
	}
	d.Message = strings.Join(lines, "\n")
	return d
}

// takeHeld stops this notifier and hands over what it was holding, so a
// settings change doesn't lose alerts.
func (n *Notifier) takeHeld() ([]Alert, map[string]bool) {
	if n == nil {
		return nil, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopped = true
	if n.timer != nil {
		n.timer.Stop()
		n.timer = nil
	}
	held, passed := n.held, n.passed
	n.held, n.passed = nil, map[string]bool{}
	return held, passed
}

// adoptHeld takes over alerts held by the notifier this one replaces.
func (n *Notifier) adoptHeld(held []Alert, passed map[string]bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.held = append(held, n.held...)
	for k, v := range passed {
		n.passed[k] = v
	}
	n.armLocked()
}
