package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

// fppStatus is the part of FPP's /api/fppd/status the monitor cares about.
type fppStatus struct {
	StatusName      string `json:"status_name"` // idle, playing, ...
	CurrentSequence string `json:"current_sequence"`
	OutputsEnabled  *bool  `json:"channelOutputsEnabled"`
	CurrentPlaylist struct {
		Playlist string `json:"playlist"`
	} `json:"current_playlist"`
	Scheduler struct {
		NextPlaylist struct {
			Name  string `json:"playlistName"`
			Start int64  `json:"scheduledStartTime"`
		} `json:"nextPlaylist"`
	} `json:"scheduler"`

	raw json.RawMessage // the whole document, for incident evidence
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
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return nil, err
		}
		var st fppStatus
		if err := json.Unmarshal(body, &st); err != nil {
			return nil, err
		}
		st.raw = body
		return &st, nil
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

// ======== show output verification ========

// fppTarget is one controller an FPP sends channel data to.
type fppTarget struct {
	Protocol     string `json:"protocol"` // ddp, e131, artnet, kinet
	Address      string `json:"address"`
	Description  string `json:"description,omitempty"`
	StartChannel int    `json:"start_channel"`
	ChannelCount int    `json:"channel_count"`
}

// fppOutputs is an FPP's network channel-output config
// (/api/channel/output/universeOutputs, i.e. co-universes.json).
type fppOutputs struct {
	Targets []fppTarget     `json:"targets"` // enabled, active, unicast outputs only
	Raw     json.RawMessage `json:"raw"`
}

// fppUniverseTypes maps FPP's universe "type" codes to protocols. Multicast
// E1.31 (0) and Art-Net broadcast (2) have no single destination, so a
// board can't be matched to them.
var fppUniverseTypes = map[int]string{1: "e131", 3: "artnet", 4: "ddp", 5: "ddp", 6: "kinet", 7: "kinet"}

func parseFPPOutputs(body []byte) (*fppOutputs, error) {
	var doc struct {
		ChannelOutputs []struct {
			Enabled   int    `json:"enabled"`
			Type      string `json:"type"`
			Universes []struct {
				Active       int    `json:"active"`
				Address      string `json:"address"`
				Description  string `json:"description"`
				StartChannel int    `json:"startChannel"`
				ChannelCount int    `json:"channelCount"`
				Type         int    `json:"type"`
			} `json:"universes"`
		} `json:"channelOutputs"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	out := &fppOutputs{Targets: []fppTarget{}, Raw: body}
	for _, co := range doc.ChannelOutputs {
		if co.Enabled == 0 {
			continue
		}
		for _, u := range co.Universes {
			proto, ok := fppUniverseTypes[u.Type]
			if u.Active == 0 || !ok || strings.TrimSpace(u.Address) == "" {
				continue
			}
			out.Targets = append(out.Targets, fppTarget{Protocol: proto, Address: strings.TrimSpace(u.Address),
				Description: u.Description, StartChannel: u.StartChannel, ChannelCount: u.ChannelCount})
		}
	}
	return out, nil
}

// fppOutputsTTL is how long an FPP's output config is cached; it only
// changes when someone edits it.
const fppOutputsTTL = 5 * time.Minute

var (
	fppOutCacheMu sync.Mutex
	fppOutCache   = map[string]fppOutCached{}
)

type fppOutCached struct {
	at  time.Time
	out *fppOutputs
	err error
}

// fetchFPPOutputs returns an FPP's network outputs (one GET, cached).
func fetchFPPOutputs(ctx context.Context, ip string) (*fppOutputs, error) {
	fppOutCacheMu.Lock()
	if c, ok := fppOutCache[ip]; ok && time.Since(c.at) < fppOutputsTTL {
		fppOutCacheMu.Unlock()
		return c.out, c.err
	}
	fppOutCacheMu.Unlock()
	out, err := func() (*fppOutputs, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ip+"/api/channel/output/universeOutputs", nil)
		resp, err := fppClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return nil, err
		}
		return parseFPPOutputs(body)
	}()
	at := time.Now()
	if err != nil {
		at = at.Add(time.Minute - fppOutputsTTL) // retry a failed fetch after a minute
	}
	fppOutCacheMu.Lock()
	fppOutCache[ip] = fppOutCached{at: at, out: out, err: err}
	fppOutCacheMu.Unlock()
	return out, err
}

// resolveHosts returns the IP addresses a host name or address stands for.
func resolveHosts(ctx context.Context, host string) []string {
	host = hostOnly(host)
	if net.ParseIP(host) != nil {
		return []string{host}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil
	}
	return addrs
}

// targetsFor picks the outputs aimed at any of the board's addresses.
func (o *fppOutputs) targetsFor(ctx context.Context, boardAddrs []string) []fppTarget {
	var hit []fppTarget
	for _, t := range o.Targets {
		addrs := resolveHosts(ctx, t.Address)
		if slices.ContainsFunc(addrs, func(a string) bool { return slices.Contains(boardAddrs, a) }) ||
			slices.ContainsFunc(boardAddrs, func(a string) bool { return strings.EqualFold(a, t.Address) }) {
			hit = append(hit, t)
		}
	}
	return hit
}

// fppFeed is what one FPP the board knows about is doing for it right now.
type fppFeed struct {
	FPP     FPPInstance
	Name    string
	Err     error // status or outputs couldn't be read: nothing is known
	Status  *fppStatus
	Outputs *fppOutputs
	Targets []fppTarget // outputs aimed at this board
}

// Playing reports whether the FPP is running a show that drives the board.
func (f fppFeed) Playing() bool {
	return f.Err == nil && strings.EqualFold(f.Status.StatusName, "playing") && len(f.Targets) > 0
}

// What names what the FPP is playing.
func (f fppFeed) What() string {
	if f.Status == nil {
		return ""
	}
	if p := f.Status.CurrentPlaylist.Playlist; p != "" {
		return p
	}
	if s := f.Status.CurrentSequence; s != "" {
		return s
	}
	return "a show"
}

// fppFeeds asks each FPP the board reports what it's doing and whether its
// outputs include the board. Only GETs, both cached.
func fppFeeds(ctx context.Context, fpps []FPPInstance, boardAddrs []string) []fppFeed {
	var feeds []fppFeed
	for _, f := range fpps {
		if f.IP == "" {
			continue
		}
		fd := fppFeed{FPP: f, Name: f.Hostname}
		if fd.Name == "" {
			fd.Name = f.IP
		}
		fd.Status, fd.Err = fetchFPP(ctx, f.IP)
		if fd.Err == nil && strings.EqualFold(fd.Status.StatusName, "playing") {
			// Only playing FPPs need their outputs read.
			if fd.Outputs, fd.Err = fetchFPPOutputs(ctx, f.IP); fd.Err == nil {
				fd.Targets = fd.Outputs.targetsFor(ctx, boardAddrs)
			}
		}
		feeds = append(feeds, fd)
	}
	return feeds
}
