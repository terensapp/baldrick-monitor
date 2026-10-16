package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// fppStatus is the part of FPP's /api/fppd/status the monitor cares about.
type fppStatus struct {
	StatusName      string `json:"status_name"` // idle, playing, ...
	CurrentSequence string `json:"current_sequence"`
	CurrentPlaylist struct {
		Playlist string `json:"playlist"`
	} `json:"current_playlist"`
	Scheduler struct {
		NextPlaylist struct {
			Name  string `json:"playlistName"`
			Start int64  `json:"scheduledStartTime"`
		} `json:"nextPlaylist"`
	} `json:"scheduler"`
}

// showLeadTime is how long before a scheduled show probes stop.
const showLeadTime = 5 * time.Minute

var (
	fppClient = &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
		DisableKeepAlives: true, DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext}}
	fppCacheMu sync.Mutex
	fppCache   = map[string]fppCached{}
)

type fppCached struct {
	at  time.Time
	st  *fppStatus
	err error
}

// fetchFPP returns an FPP instance's status, cached briefly so several
// boards fed by the same FPP don't each ask it.
func fetchFPP(ctx context.Context, ip string) (*fppStatus, error) {
	fppCacheMu.Lock()
	if c, ok := fppCache[ip]; ok && time.Since(c.at) < 20*time.Second {
		fppCacheMu.Unlock()
		return c.st, c.err
	}
	fppCacheMu.Unlock()
	st, err := func() (*fppStatus, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ip+"/api/fppd/status", nil)
		resp, err := fppClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		var st fppStatus
		return &st, json.NewDecoder(resp.Body).Decode(&st)
	}()
	fppCacheMu.Lock()
	fppCache[ip] = fppCached{at: time.Now(), st: st, err: err}
	fppCacheMu.Unlock()
	return st, err
}

// fppBusy reports why probing should wait: an FPP the board knows about is
// playing, or has a show scheduled to start soon. FPPs that can't be reached
// don't hold probes back.
func fppBusy(ctx context.Context, fpps []FPPInstance) string {
	for _, f := range fpps {
		if f.IP == "" {
			continue
		}
		st, err := fetchFPP(ctx, f.IP)
		if err != nil {
			continue
		}
		name := f.Hostname
		if name == "" {
			name = f.IP
		}
		if s := strings.ToLower(st.StatusName); s != "" && s != "idle" {
			what := st.CurrentPlaylist.Playlist
			if what == "" {
				what = st.CurrentSequence
			}
			if what != "" {
				what = " " + what
			}
			return fmt.Sprintf("FPP %s is %s%s, so no probe packets are sent during the show", name, s, what)
		}
		if t := st.Scheduler.NextPlaylist.Start; t > 0 {
			start := time.Unix(t, 0)
			if until := time.Until(start); until > 0 && until < showLeadTime {
				return fmt.Sprintf("FPP %s starts %s at %s, so probes are paused until it's idle again", name, st.Scheduler.NextPlaylist.Name, start.Local().Format("15:04"))
			}
		}
	}
	return ""
}
