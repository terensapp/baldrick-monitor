// Command fakeboard imitates a Baldrick board's HTTP/WebSocket/DDP surface so
// baldrick-monitor can be exercised without real hardware, including failure
// modes:
//
//	curl -XPOST localhost:8081/_sim/crash          # drop off the network for 3s, then reboot
//	curl -XPOST 'localhost:8081/_sim/hang?secs=30' # HTTP 503 + silent WebSocket, then recover
//	curl -XPOST 'localhost:8081/_sim/hang?secs=30&reboot=1'  # ...then reboot
//	curl -XPOST localhost:8081/_sim/leak           # heap drains ~3 KB/s until it crashes
//	curl -XPOST localhost:8081/_sim/hot            # temperature climbs to 82°C
//	curl -XPOST localhost:8081/_sim/ddpstall       # stop counting DDP packets
//	curl -XPOST 'localhost:8081/_sim/fpp?state=playing'  # its fake FPP plays a show
//	curl -XPOST 'localhost:8081/_sim/fpp?start_in=120'   # ...or schedules one in 2 min
//	curl -XPOST 'localhost:8081/_sim/fpp?state=playing&stream=0'  # plays, but its stream never arrives
//	curl -XPOST 'localhost:8081/_sim/fpp?stream=1'       # ...until it does
//	curl -XPOST 'localhost:8081/_sim/fpp?target=10.9.9.9' # point its outputs elsewhere
//	curl -XPOST 'localhost:8081/_sim/fpp?down=1'         # its FPP stops answering (down=0 to recover)
//	curl -XPOST localhost:8081/_sim/reset          # back to normal
package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type sim struct {
	mu         sync.Mutex
	hostname   string // identity, so a multi-board demo looks like a display
	model      string
	boardID    string
	fppAddr    string // the FPP this board reports (default: itself)
	fppName    string
	boot       time.Time
	ram, temp  float64
	leak, hot  bool
	ddpStall   bool
	downUntil  time.Time // crash: connections dropped
	hangUntil  time.Time // hang: 503s, WebSocket silent
	hangReboot bool
	ddpRecvd   float64
	ddpDropped float64
	ddpSrc     map[string]time.Time
	lastSeq    byte
	window     time.Time
	shownRecv  float64
	shownDrop  float64
	conns      map[net.Conn]bool
	fppState   string    // the fake FPP's status_name
	fppStart   time.Time // its next scheduled show
	fppStream  bool      // while playing, the board sees a stream from it
	fppTarget  string    // where its DDP output points
	fppDown    bool      // the fake FPP doesn't answer
	fppPlayAt  time.Time // when the current show started
	listen     string
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8081", "HTTP listen address")
	ddp := flag.String("ddp", "127.0.0.1:4048", "DDP UDP listen address (empty to disable)")
	hostname := flag.String("hostname", "fakeboard", "hostname the board reports")
	model := flag.String("model", "Baldrick Simulator", "board model it reports")
	id := flag.String("id", "fake0001", "board id it reports")
	ram := flag.Float64("ram", 115_000, "free heap it starts with, in bytes")
	temp := flag.Float64("temp", 30, "temperature it reports, in °C")
	fpp := flag.String("fpp", "", "address of the FPP it reports (default: its own, acting as FPP)")
	fppName := flag.String("fppname", "fakefpp", "hostname of the FPP it reports and plays")
	fppListen := flag.String("fpplisten", "", "also answer FPP's API on this address (pair with -fpp)")
	flag.Parse()
	s := &sim{boot: time.Now(), ram: *ram, temp: *temp, ddpSrc: map[string]time.Time{}, window: time.Now(), conns: map[net.Conn]bool{}, fppState: "idle", listen: *listen,
		hostname: *hostname, model: *model, boardID: *id, fppAddr: *fpp, fppName: *fppName}
	if s.fppAddr == "" {
		s.fppAddr = *listen
	}
	s.fppTarget = fppHost(*listen)
	go s.tick()
	if *ddp != "" {
		go s.ddpLoop(*ddp)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", s.guard(func(w http.ResponseWriter, r *http.Request) { s.json(w, map[string]any{}) }))
	mux.HandleFunc("/system_state", s.guard(func(w http.ResponseWriter, r *http.Request) { s.json(w, s.state()) }))
	mux.HandleFunc("/settings", s.guard(func(w http.ResponseWriter, r *http.Request) {
		s.json(w, map[string]any{"hostname": s.hostname, "ports": []any{
			map[string]any{"port_type": "pixel", "models": []any{map[string]any{"name": "Test", "num_channels": 300}}},
			map[string]any{"port_type": "pixel", "models": []any{}}}})
	}))
	mux.HandleFunc("/ws", s.guard(s.ws))
	mux.HandleFunc("/", s.guard(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, "<!doctype html><title>Baldrick Board</title><h1>Fake Baldrick</h1>%s", strings.Repeat("<!-- padding -->", 60))
	}))
	mux.HandleFunc("/_sim/", s.control)
	// The fake board doubles as the FPP it reports, so FPP-aware probing can
	// be exercised too.
	mux.HandleFunc("/api/fppd/status", s.fppGuard(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		st := map[string]any{"status_name": s.fppState, "current_sequence": "", "current_playlist": map[string]string{"playlist": ""},
			"host_name": s.fppName, "version": "9.5.3-sim", "platform": "Simulator", "mode_name": "player", "channelOutputsEnabled": true,
			"seconds_remaining": "0",
			"scheduler":         map[string]any{"nextPlaylist": map[string]any{"playlistName": "", "scheduledStartTime": 0}}}
		if s.fppState != "idle" {
			st["current_playlist"] = map[string]string{"playlist": "Halloween"}
			st["current_sequence"] = "Thriller.fseq"
			// Each fake sequence runs 4 minutes.
			st["seconds_remaining"] = strconv.Itoa(240 - int(time.Since(s.fppPlayAt).Seconds())%240)
		}
		if !s.fppStart.IsZero() {
			st["scheduler"] = map[string]any{"nextPlaylist": map[string]any{"playlistName": "Halloween", "scheduledStartTime": s.fppStart.Unix()}}
		}
		s.json(w, st)
	}))
	mux.HandleFunc("/api/channel/output/universeOutputs", s.fppGuard(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.json(w, map[string]any{"status": "OK", "channelOutputs": []any{map[string]any{
			"enabled": 1, "type": "universes", "interface": "eth0", "startChannel": 1, "channelCount": -1,
			"universes": []any{map[string]any{"active": 1, "address": s.fppTarget, "description": "fakeboard",
				"startChannel": 1, "channelCount": 300, "type": 4, "id": 1}}}}})
	}))
	log.Printf("fake board on http://%s (DDP %s)", *listen, *ddp)
	if *fppListen != "" {
		go func() { log.Fatal(http.ListenAndServe(*fppListen, mux)) }()
	}
	log.Fatal(http.ListenAndServe(*listen, mux))
}

// fppGuard makes the fake FPP's API refuse to answer while it's "down", and
// rejects anything but GET: the monitor must never change an FPP.
func (s *sim) fppGuard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			log.Printf("sim: FPP got a %s %s; the monitor should only GET", r.Method, r.URL.Path)
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		down := s.fppDown
		s.mu.Unlock()
		if down {
			if hj, ok := w.(http.Hijacker); ok {
				c, _, _ := hj.Hijack()
				c.Close()
			}
			return
		}
		h(w, r)
	}
}

func (s *sim) json(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// guard applies the simulated failure modes to every board endpoint.
func (s *sim) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		down, hang := time.Now().Before(s.downUntil), time.Now().Before(s.hangUntil)
		s.mu.Unlock()
		if down {
			if hj, ok := w.(http.Hijacker); ok {
				c, _, _ := hj.Hijack()
				c.Close()
			}
			return
		}
		if hang {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "<html><body><h1>503 Service Unavailable</h1>Server is busy</body></html>")
			return
		}
		h(w, r)
	}
}

func (s *sim) state() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var srcs []string
	for ip, t := range s.ddpSrc {
		if time.Since(t) < 6*time.Second {
			srcs = append(srcs, ip)
		}
	}
	if srcs == nil {
		srcs = []string{}
	}
	fps := 0.0
	if len(srcs) > 0 {
		fps = s.shownRecv / 5
	}
	return map[string]any{
		"board_manufacturer": "Simulated", "board_model": s.model, "board_id": s.boardID,
		"temperature": []float64{s.temp}, "last_settings_update": 1700000000,
		"ota": map[string]any{"updatable": []any{map[string]any{"current_firmware_version": "v3.8.9-sim",
			"current_factory_version": "sim", "available_firmware_version": "v3.8.9-sim"}}},
		"ram_available": s.ram, "ram_minimum": s.ram - 2000, "ram_largest_block": s.ram * 0.8,
		"uptime": int(time.Since(s.boot).Seconds()), "oom_count": 0, "ui_messages": []any{},
		"network_throughput": 0, "fpp": []any{map[string]any{"hostname": s.fppName, "ip": s.fppAddr, "sw_version": "9.5", "hardware": "simulator"}}, "frame_rate": fps, "test_mode_active": false, "fx_active": "",
		"streaming": map[string]any{"ddp_sources": srcs, "ddp_recvd": s.shownRecv, "ddp_dropped": s.shownDrop,
			"sacn_sources": []string{}, "sacn_recvd": 0, "sacn_dropped": 0, "artnet_sources": []string{}, "artnet_recvd": 0, "artnet_dropped": 0},
	}
}

func (s *sim) tick() {
	for range time.Tick(time.Second) {
		s.mu.Lock()
		now := time.Now()
		if s.leak {
			s.ram -= 3000
			if s.ram < 15_000 {
				log.Print("sim: out of heap, crashing")
				s.crashLocked(3 * time.Second)
			}
		}
		if s.hot && s.temp < 82 {
			s.temp += 2
		}
		if !s.hangUntil.IsZero() && now.After(s.hangUntil) {
			s.hangUntil = time.Time{}
			if s.hangReboot {
				s.boot, s.hangReboot = now, false
				log.Print("sim: hang over, rebooted")
			} else {
				log.Print("sim: hang over, recovered")
			}
		}
		if s.fppState == "playing" && s.fppStream && now.After(s.downUntil) && now.After(s.hangUntil) {
			s.ddpSrc[fppHost(s.fppAddr)] = now // the fake FPP's show stream, from the FPP it reports
			s.ddpRecvd += 40
		}
		if now.Sub(s.window) >= 5*time.Second { // the board's ~5s stats window
			s.shownRecv, s.shownDrop, s.ddpRecvd, s.ddpDropped, s.window = s.ddpRecvd, s.ddpDropped, 0, 0, now
		}
		s.mu.Unlock()
	}
}

// crashLocked drops every connection, stays dark for d, then "boots".
func (s *sim) crashLocked(d time.Duration) {
	s.downUntil = time.Now().Add(d)
	s.boot = s.downUntil
	s.ram, s.leak = 115_000, false
	for c := range s.conns {
		c.Close()
	}
}

func (s *sim) control(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch mode := strings.TrimPrefix(r.URL.Path, "/_sim/"); mode {
	case "crash":
		s.crashLocked(3 * time.Second)
	case "hang":
		secs, _ := strconv.Atoi(r.URL.Query().Get("secs"))
		if secs <= 0 {
			secs = 30
		}
		s.hangUntil, s.hangReboot = time.Now().Add(time.Duration(secs)*time.Second), r.URL.Query().Get("reboot") == "1"
	case "leak":
		s.leak = true
	case "hot":
		s.hot = true
	case "ddpstall":
		s.ddpStall = true
	case "fpp":
		q := r.URL.Query()
		if st := q.Get("state"); st != "" {
			if st != s.fppState {
				s.fppPlayAt = time.Now()
			}
			s.fppState = st
			s.fppStream = st == "playing" // a healthy show reaches the board...
		}
		if v := q.Get("stream"); v != "" {
			s.fppStream = v == "1" // ...unless told otherwise
		}
		if t := q.Get("target"); t != "" {
			s.fppTarget = t
		}
		if d := q.Get("down"); d != "" {
			s.fppDown = d == "1"
		}
		if in, _ := strconv.Atoi(r.URL.Query().Get("start_in")); in > 0 {
			s.fppStart = time.Now().Add(time.Duration(in) * time.Second)
		}
	case "reset":
		s.leak, s.hot, s.ddpStall, s.temp, s.ram, s.hangUntil = false, false, false, 30, 115_000, time.Time{}
		s.fppState, s.fppStart, s.fppStream, s.fppTarget, s.fppDown = "idle", time.Time{}, false, fppHost(s.listen), false
	default:
		http.Error(w, "unknown mode", http.StatusNotFound)
		return
	}
	log.Printf("sim: %s", r.URL.Path)
	fmt.Fprintln(w, "ok")
}

func (s *sim) ddpLoop(addr string) {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		log.Printf("DDP disabled: %v", err)
		return
	}
	buf := make([]byte, 1500)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil || n < 10 {
			continue
		}
		s.mu.Lock()
		if !s.ddpStall && time.Now().After(s.downUntil) && time.Now().After(s.hangUntil) {
			seq := buf[1] & 0x0f
			if s.lastSeq != 0 && seq != 0 && seq != s.lastSeq%15+1 {
				s.ddpDropped++
			}
			s.lastSeq = seq
			s.ddpRecvd++
			s.ddpSrc[from.(*net.UDPAddr).IP.String()] = time.Now()
		}
		s.mu.Unlock()
	}
}

// ws serves the "baldrick" WebSocket: a full snapshot, then periodic deltas,
// echoing the client's "{}" keepalives. It goes quiet during a hang.
func (s *sim) ws(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Sec-WebSocket-Key")
	hj, ok := w.(http.Hijacker)
	if !ok || key == "" {
		http.Error(w, "websocket only", http.StatusBadRequest)
		return
	}
	c, rw, err := hj.Hijack()
	if err != nil {
		return
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: baldrick\r\n\r\n",
		base64.StdEncoding.EncodeToString(h[:]))
	rw.Flush()
	s.mu.Lock()
	s.conns[c] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		c.Close()
	}()
	var wmu sync.Mutex
	send := func(v any) error {
		p, _ := json.Marshal(v)
		f := []byte{0x81}
		if len(p) < 126 {
			f = append(f, byte(len(p)))
		} else {
			f = append(f, 126, byte(len(p)>>8), byte(len(p)))
		}
		wmu.Lock()
		defer wmu.Unlock()
		_, err := c.Write(append(f, p...))
		return err
	}
	hanging := func() bool { s.mu.Lock(); defer s.mu.Unlock(); return time.Now().Before(s.hangUntil) }
	if send(s.state()) != nil {
		return
	}
	done := make(chan struct{})
	go func() { // read client frames; echo keepalives
		defer close(done)
		br := bufio.NewReader(c)
		for {
			var hd [2]byte
			if _, err := br.Read(hd[:1]); err != nil {
				return
			}
			if _, err := br.Read(hd[1:]); err != nil {
				return
			}
			n := int(hd[1] & 0x7f)
			mask := make([]byte, 4)
			if _, err := ioReadFull(br, mask); err != nil {
				return
			}
			p := make([]byte, n)
			if _, err := ioReadFull(br, p); err != nil {
				return
			}
			if hd[0]&0x0f == 0x8 {
				return
			}
			if !hanging() {
				send(map[string]any{})
			}
		}
	}()
	t := time.NewTicker(1500 * time.Millisecond)
	defer t.Stop()
	i := 0
	for {
		select {
		case <-done:
			return
		case <-t.C:
		}
		if hanging() {
			continue
		}
		i++
		st := s.state()
		var err error
		switch {
		case i%3 == 0:
			err = send(map[string]any{"ram_available": st["ram_available"], "ram_minimum": st["ram_minimum"],
				"ram_largest_block": st["ram_largest_block"], "uptime": st["uptime"], "oom_count": 0, "temperature": st["temperature"]})
		case i%3 == 1:
			err = send(map[string]any{"frame_rate": st["frame_rate"], "streaming": st["streaming"]})
		default:
			err = send(map[string]any{"network_throughput": 0})
		}
		if err != nil {
			return
		}
	}
}

func ioReadFull(r *bufio.Reader, p []byte) (int, error) {
	n := 0
	for n < len(p) {
		m, err := r.Read(p[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// fppHost is the address the fake FPP streams from and targets: the
// listen host, since the board and its FPP are the same process.
func fppHost(listen string) string {
	if h, _, err := net.SplitHostPort(listen); err == nil {
		return h
	}
	return listen
}
