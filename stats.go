package main

import (
	"math"
	"sort"
)

// ring holds the most recent latency samples in milliseconds; NaN marks a
// lost sample.
type ring struct {
	buf  []float64
	next int
	full bool
}

func newRing(n int) *ring { return &ring{buf: make([]float64, n)} }

func (r *ring) add(v float64) {
	r.buf[r.next] = v
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
}

// last returns up to n samples, oldest first.
func (r *ring) last(n int) []float64 {
	var all []float64
	if r.full {
		all = append(append(all, r.buf[r.next:]...), r.buf[:r.next]...)
	} else {
		all = append(all, r.buf[:r.next]...)
	}
	if len(all) > n {
		all = all[len(all)-n:]
	}
	return all
}

type Summary struct {
	Count  int      `json:"count"`
	Lost   int      `json:"lost"`
	Loss   float64  `json:"loss"`
	Last   *float64 `json:"last"`
	Min    float64  `json:"min"`
	P50    float64  `json:"p50"`
	P95    float64  `json:"p95"`
	Max    float64  `json:"max"`
	Mean   float64  `json:"mean"`
	Jitter float64  `json:"jitter"` // mean |Δ| between consecutive replies
}

func summarize(vals []float64) Summary {
	s := Summary{Count: len(vals)}
	if len(vals) == 0 {
		return s
	}
	var ok []float64
	prev, jsum, jn := math.NaN(), 0.0, 0
	for _, v := range vals {
		if math.IsNaN(v) {
			s.Lost++
			continue
		}
		ok = append(ok, v)
		if !math.IsNaN(prev) {
			jsum += math.Abs(v - prev)
			jn++
		}
		prev = v
	}
	s.Loss = float64(s.Lost) / float64(len(vals))
	if last := vals[len(vals)-1]; !math.IsNaN(last) {
		s.Last = &last
	}
	if len(ok) == 0 {
		return s
	}
	if jn > 0 {
		s.Jitter = jsum / float64(jn)
	}
	sort.Float64s(ok)
	var sum float64
	for _, v := range ok {
		sum += v
	}
	s.Mean = sum / float64(len(ok))
	s.Min, s.Max = ok[0], ok[len(ok)-1]
	s.P50, s.P95 = quantile(ok, 0.5), quantile(ok, 0.95)
	return s
}

func quantile(sorted []float64, q float64) float64 {
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

// series converts samples to JSON-friendly values (null for lost).
func series(vals []float64) []*float64 {
	out := make([]*float64, len(vals))
	for i := range vals {
		if !math.IsNaN(vals[i]) {
			out[i] = &vals[i]
		}
	}
	return out
}
