package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fppStatus is the part of FPP's /api/fppd/status the monitor cares about.
type fppStatus struct {
	StatusName       string  `json:"status_name"` // idle, playing, ...
	ModeName         string  `json:"mode_name"`   // player, remote, ...
	HostName         string  `json:"host_name"`
	Version          string  `json:"version"`
	Platform         string  `json:"platform"`
	CurrentSequence  string  `json:"current_sequence"`
	CurrentSong      string  `json:"current_song"`
	SecondsRemaining flexNum `json:"seconds_remaining"`
	OutputsEnabled   *bool   `json:"channelOutputsEnabled"`
	CurrentPlaylist  struct {
		Playlist string `json:"playlist"`
	} `json:"current_playlist"`
	Scheduler struct {
		NextPlaylist struct {
			Name     string `json:"playlistName"`
			Start    int64  `json:"scheduledStartTime"`
			StartStr string `json:"scheduledStartTimeStr"`
		} `json:"nextPlaylist"`
	} `json:"scheduler"`

	raw json.RawMessage // the whole document, for incident evidence
}

// flexNum reads FPP's numbers, some of which arrive as strings ("0").
type flexNum float64

func (n *flexNum) UnmarshalJSON(b []byte) error {
	var f float64
	if json.Unmarshal(b, &f) == nil {
		*n = flexNum(f)
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	f, _ = strconv.ParseFloat(s, 64)
	*n = flexNum(f)
	return nil
}

// active reports whether FPP is running something (playing, paused,
// stopping...); probes stand aside for all of it.
func (st *fppStatus) active() bool {
	s := strings.ToLower(st.StatusName)
	return s != "" && s != "idle"
}

func (st *fppStatus) playing() bool { return strings.EqualFold(st.StatusName, "playing") }

// what names what FPP is running.
func (st *fppStatus) what() string {
	if p := st.CurrentPlaylist.Playlist; p != "" {
		return p
	}
	if s := st.CurrentSequence; s != "" {
		return s
	}
	return "a show"
}

// nextShow is FPP's next scheduled playlist, if any.
func (st *fppStatus) nextShow() (string, time.Time) {
	n := st.Scheduler.NextPlaylist
	if n.Start <= 0 || n.Name == "" {
		return "", time.Time{}
	}
	return n.Name, time.Unix(n.Start, 0)
}

// showLeadTime is how long before a scheduled show probes stop.
const showLeadTime = 5 * time.Minute

var fppClient = &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
	DisableKeepAlives: true, DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext}}

// fppGet fetches one FPP API document. Only GETs are ever sent to FPP.
func fppGet(ctx context.Context, host, path string) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+path, nil)
	resp, err := fppClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

// fetchFPPStatus reads /api/fppd/status. A document without status_name
// isn't FPP.
func fetchFPPStatus(ctx context.Context, host string) (*fppStatus, error) {
	body, err := fppGet(ctx, host, "/api/fppd/status")
	if err != nil {
		return nil, err
	}
	var st fppStatus
	if err := json.Unmarshal(body, &st); err != nil {
		return nil, fmt.Errorf("not FPP: %w", err)
	}
	if st.StatusName == "" {
		return nil, errors.New("not FPP: no status_name in /api/fppd/status")
	}
	st.raw = body
	return &st, nil
}

// fetchFPPOutputs reads FPP's network channel outputs.
func fetchFPPOutputs(ctx context.Context, host string) (*fppOutputs, error) {
	body, err := fppGet(ctx, host, "/api/channel/output/universeOutputs")
	if err != nil {
		return nil, err
	}
	return parseFPPOutputs(body)
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

// fppOutputsTTL is how long an FPP's output config is trusted before it's
// read again; it only changes when someone edits it. A show starting also
// rereads it.
const fppOutputsTTL = 5 * time.Minute

var (
	resolveMu    sync.Mutex
	resolveCache = map[string]resolved{}
)

type resolved struct {
	at    time.Time
	addrs []string
}

// resolveHosts returns the IP addresses a host name or address stands for,
// remembered for a minute.
func resolveHosts(ctx context.Context, host string) []string {
	host = hostOnly(host)
	if net.ParseIP(host) != nil {
		return []string{host}
	}
	resolveMu.Lock()
	if c, ok := resolveCache[host]; ok && time.Since(c.at) < time.Minute {
		resolveMu.Unlock()
		return c.addrs
	}
	resolveMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	addrs, _ := net.DefaultResolver.LookupHost(ctx, host)
	resolveMu.Lock()
	resolveCache[host] = resolved{time.Now(), addrs}
	resolveMu.Unlock()
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
	FPP     FPPInstance // Hostname = registry name, IP = the address it streams from
	Name    string
	Err     error // status or outputs unknown: nothing can be concluded
	Status  *fppStatus
	Outputs *fppOutputs
	Targets []fppTarget // outputs aimed at this board
}

// Playing reports whether the FPP is running a show that drives the board.
func (f fppFeed) Playing() bool {
	return f.Err == nil && f.Status.playing() && len(f.Targets) > 0
}

// What names what the FPP is playing.
func (f fppFeed) What() string {
	if f.Status == nil {
		return ""
	}
	return f.Status.what()
}
