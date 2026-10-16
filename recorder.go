package main

import (
	"encoding/json"
	"time"
)

// RecPoint is one raw observation in a board's flight recorder.
//
//	icmp   {"ms": 3.1} or {"lost": true}
//	http   {"ep": "/ping", "ok": true, "status": 200, "ms": 7.9, "err": ""}
//	state  {"src": "ws"|"poll", "d": <system_state delta exactly as received>}
//	ws     {"connected": true|false, "err": "..."}
//	probe_start {"offset": 1000000, "packets": 60}
//	probe  <DDPProbe>
//	event  <Event>
type RecPoint struct {
	T time.Time       `json:"t"`
	K string          `json:"k"`
	V json.RawMessage `json:"v"`
}

// recorder keeps the last window of raw observations so an incident can be
// exported with everything that led up to it. Callers hold Board.mu.
type recorder struct {
	window time.Duration
	pts    []RecPoint
}

const recorderCap = 100_000

func (r *recorder) add(kind string, v any) {
	var raw json.RawMessage
	switch x := v.(type) {
	case json.RawMessage:
		raw = x
	default:
		raw, _ = json.Marshal(v)
	}
	now := time.Now()
	r.pts = append(r.pts, RecPoint{T: now, K: kind, V: raw})
	cut := now.Add(-r.window)
	i := 0
	for i < len(r.pts) && (r.pts[i].T.Before(cut) || len(r.pts)-i > recorderCap) {
		i++
	}
	if i > 0 {
		r.pts = r.pts[i:]
		if cap(r.pts) > 4*len(r.pts)+1024 { // release the dropped prefix
			r.pts = append([]RecPoint(nil), r.pts...)
		}
	}
}

// since copies every observation at or after t.
func (r *recorder) since(t time.Time) []RecPoint {
	for i, p := range r.pts {
		if !p.T.Before(t) {
			return append([]RecPoint(nil), r.pts[i:]...)
		}
	}
	return nil
}
