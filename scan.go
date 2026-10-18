package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sort"
	"sync"
	"time"
)

// Found describes a Baldrick board that answered at an address.
type Found struct {
	Host         string  `json:"host"`
	Hostname     string  `json:"hostname"`
	Manufacturer string  `json:"manufacturer"`
	Model        string  `json:"model"`
	BoardID      string  `json:"board_id"`
	Firmware     string  `json:"firmware"`
	Uptime       float64 `json:"uptime"`
	LatencyMs    float64 `json:"latency_ms"`
	Monitored    string  `json:"monitored,omitempty"` // board name if already monitored
}

var scanClient = &http.Client{Transport: &http.Transport{
	DisableKeepAlives: true,
	DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialBoard(ctx, network, addr, 800*time.Millisecond)
	},
}}

func scanGet(ctx context.Context, url string, v any) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := scanClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(v)
}

// inspect checks whether host is a Baldrick board and returns its identity.
func inspect(ctx context.Context, host string) (*Found, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	start := time.Now()
	var st SystemState
	if err := scanGet(ctx, "http://"+host+"/system_state", &st); err != nil {
		return nil, err
	}
	if st.BoardModel == "" && st.BoardID == "" {
		return nil, errors.New("answered HTTP but doesn't look like a Baldrick board")
	}
	fw, _, _ := st.firmware()
	f := &Found{Host: host, Manufacturer: st.BoardManufacturer, Model: st.BoardModel, BoardID: st.BoardID,
		Firmware: fw, Uptime: st.Uptime, LatencyMs: ms(time.Since(start))}
	var s Settings
	if scanGet(ctx, "http://"+host+"/settings", &s) == nil {
		f.Hostname = s.Hostname
	}
	return f, nil
}

// scanSubnet looks for Baldrick boards across a CIDR of at most 1024 hosts.
func scanSubnet(ctx context.Context, cidr string) ([]Found, error) {
	pfx, err := netip.ParsePrefix(cidr)
	if err != nil {
		return nil, fmt.Errorf("bad subnet %q (want e.g. 192.168.6.0/24)", cidr)
	}
	pfx = pfx.Masked()
	if !pfx.Addr().Is4() || pfx.Bits() < 22 {
		return nil, errors.New("scan an IPv4 subnet of /22 or smaller")
	}
	var hosts []netip.Addr
	for a := pfx.Addr(); pfx.Contains(a); a = a.Next() {
		hosts = append(hosts, a)
	}
	if pfx.Bits() <= 30 { // skip network and broadcast addresses
		hosts = hosts[1 : len(hosts)-1]
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var (
		mu    sync.Mutex
		found []Found
		wg    sync.WaitGroup
		sem   = make(chan struct{}, 64)
	)
	for _, a := range hosts {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			host := a.String()
			// Cheap TCP check first so dead addresses cost < 1s.
			c, err := dialBoard(ctx, "tcp", net.JoinHostPort(host, "80"), 700*time.Millisecond)
			if err != nil {
				return
			}
			c.Close()
			if f, err := inspect(ctx, host); err == nil {
				mu.Lock()
				found = append(found, *f)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Slice(found, func(i, j int) bool {
		return netip.MustParseAddr(found[i].Host).Less(netip.MustParseAddr(found[j].Host))
	})
	return found, nil
}
