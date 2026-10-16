package main

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// collector renders the Prometheus text format, keeping each metric family's
// samples together as the format requires.
type collector struct {
	order []string
	fams  map[string]*family
}

type family struct {
	typ, help string
	lines     []string
}

func newCollector() *collector { return &collector{fams: map[string]*family{}} }

func (c *collector) add(name, typ, help string, v float64, labels ...string) {
	f := c.fams[name]
	if f == nil {
		f = &family{typ: typ, help: help}
		c.fams[name] = f
		c.order = append(c.order, name)
	}
	var b strings.Builder
	b.WriteString(name)
	if len(labels) > 0 {
		b.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%s=%q", labels[i], labels[i+1])
		}
		b.WriteByte('}')
	}
	b.WriteByte(' ')
	switch {
	case math.IsNaN(v):
		b.WriteString("NaN")
	default:
		b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	}
	f.lines = append(f.lines, b.String())
}

func (c *collector) write(w io.Writer) {
	for _, name := range c.order {
		f := c.fams[name]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, f.help, name, f.typ)
		for _, l := range f.lines {
			fmt.Fprintln(w, l)
		}
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func writeMetrics(w io.Writer, views []BoardView) {
	c := newCollector()
	g := func(name, help string, v float64, bd string, l ...string) {
		c.add("baldrick_"+name, "gauge", help, v, append([]string{"board", bd}, l...)...)
	}
	ctr := func(name, help string, v float64, bd string, l ...string) {
		c.add("baldrick_"+name, "counter", help, v, append([]string{"board", bd}, l...)...)
	}
	for _, v := range views {
		n := v.Name
		g("up", "1 if the board answers HTTP /ping.", b2f(v.HTTPFails < 1 && v.HTTP.Count > 0), n)
		for _, s := range []string{"up", "degraded", "down", "unknown"} {
			g("health_status", "Overall health (one-hot).", b2f(v.Status == s), n, "status", s)
		}
		g("info", "Board identity.", 1, n, "host", v.Host, "hostname", v.Hostname, "model", v.Model,
			"manufacturer", v.Manufacturer, "board_id", v.BoardID, "firmware", v.Firmware, "factory_firmware", v.FactoryFirmware)
		g("update_available", "1 if the board reports newer firmware.", b2f(v.UpdateAvailable), n)

		if v.ICMPAvailable {
			ctr("icmp_sent_total", "ICMP echo requests sent.", float64(v.ICMPSent), n)
			ctr("icmp_lost_total", "ICMP echo requests without a reply.", float64(v.ICMPLost), n)
			addSummary(g, "icmp", "ICMP", n, v.ICMP)
		}
		addSummary(g, "http_ping", "HTTP /ping", n, v.HTTP)
		g("http_consecutive_failures", "Consecutive failed /ping checks.", float64(v.HTTPFails), n)
		for ep, e := range v.Endpoints {
			ctr("http_requests_total", "HTTP checks by endpoint and result.", float64(e.OK), n, "endpoint", ep, "result", "ok")
			ctr("http_requests_total", "HTTP checks by endpoint and result.", float64(e.Fail), n, "endpoint", ep, "result", "error")
			g("http_endpoint_ok", "1 if the last check of the endpoint passed.", b2f(e.LastOK), n, "endpoint", ep)
			g("http_last_duration_seconds", "Duration of the last check of the endpoint.", e.LastMs/1000, n, "endpoint", ep)
		}

		g("ws_connected", "1 while the WebSocket feed is connected.", b2f(v.WSConnected), n)
		g("ws_last_message_age_seconds", "Seconds since the last WebSocket message.", v.WSAge, n)
		ctr("ws_reconnects_total", "WebSocket reconnects.", float64(v.WSReconnects), n)
		ctr("ws_messages_total", "WebSocket messages received.", float64(v.WSMessages), n)

		if v.HaveState {
			g("state_age_seconds", "Seconds since board state was last updated.", v.StateAge, n)
			g("uptime_seconds", "Board uptime.", v.Uptime, n)
			for i, t := range v.Temperature {
				g("temperature_celsius", "Board temperature sensor.", t, n, "sensor", strconv.Itoa(i))
			}
			g("ram_available_bytes", "Free heap.", v.RAMAvailable, n)
			g("ram_minimum_bytes", "Lowest free heap since boot.", v.RAMMinimum, n)
			g("ram_largest_block_bytes", "Largest allocatable heap block.", v.RAMLargestBlock, n)
			g("oom_count", "Out-of-memory events reported by the board.", v.OOMCount, n)
			g("ui_messages", "Messages the board is showing in its UI.", float64(v.UIMessages), n)
			g("test_mode_active", "1 while a test pattern is running.", b2f(v.TestMode), n)
			g("frame_rate", "Output frame rate reported by the board.", v.FrameRate, n)
			g("network_throughput", "Network throughput reported by the board (board units).", v.NetworkThroughput, n)
			for proto, s := range map[string]StreamStats{"ddp": v.DDP, "sacn": v.SACN, "artnet": v.ArtNet} {
				help := "Packets in the board's current ~5s stats window."
				g("stream_packets", help, s.Recvd, n, "protocol", proto, "kind", "received")
				g("stream_packets", help, s.Dropped, n, "protocol", proto, "kind", "dropped")
				g("stream_sources", "Distinct sources streaming to the board.", float64(len(s.Sources)), n, "protocol", proto)
			}
		}
		ctr("reboots_detected_total", "Reboots detected from uptime going backwards.", float64(v.Reboots), n)

		open, ongoing := map[string]int{}, map[string]int{}
		for _, inc := range v.Incidents {
			open[inc.Type]++
			if inc.Ongoing {
				ongoing[inc.Type]++
			}
		}
		for _, t := range []string{"reboot", "lockup", "ddp"} {
			g("incidents_unacknowledged", "Incidents not yet acknowledged.", float64(open[t]), n, "type", t)
			g("incidents_ongoing", "Incidents still in progress (e.g. a lockup).", float64(ongoing[t]), n, "type", t)
		}
		ctr("ddp_probe_total", "DDP checks by result.", float64(v.ProbeCounts["ok"]), n, "result", "ok")
		ctr("ddp_probe_total", "DDP checks by result.", float64(v.ProbeCounts["fail"]), n, "result", "fail")
		if p := v.Probe; p != nil {
			g("ddp_probe_success", "1 if the last DDP check passed.", b2f(p.OK), n)
			g("ddp_probe_passive", "1 if the last DDP check judged a live stream instead of injecting.", b2f(p.Passive), n)
			g("ddp_probe_timestamp_seconds", "Unix time of the last DDP check.", float64(p.Time.Unix()), n)
			if p.OK && !p.Passive {
				g("ddp_probe_detect_seconds", "Time until the board reported our DDP packets.", p.DetectMs/1000, n)
			}
		}
	}
	c.write(w)
}

func writeMonitorMetrics(w io.Writer, u UpdateInfo) {
	c := newCollector()
	c.add("baldrick_monitor_info", "gauge", "This monitor's build.", 1, "version", u.Current.Version, "revision", u.Current.Revision)
	c.add("baldrick_monitor_update_available", "gauge", "1 if a newer monitor image is published for this channel.", b2f(u.UpdateAvailable))
	c.write(w)
}

func addSummary(g func(string, string, float64, string, ...string), prefix, what, board string, s Summary) {
	if s.Count == 0 {
		return
	}
	g(prefix+"_loss_ratio", what+" loss over the rollup window.", s.Loss, board)
	if s.Lost == s.Count {
		return
	}
	g(prefix+"_rtt_seconds", what+" round-trip time over the rollup window.", s.P50/1000, board, "quantile", "0.5")
	g(prefix+"_rtt_seconds", what+" round-trip time over the rollup window.", s.P95/1000, board, "quantile", "0.95")
	g(prefix+"_rtt_seconds", what+" round-trip time over the rollup window.", s.Max/1000, board, "quantile", "1")
	g(prefix+"_jitter_seconds", what+" jitter (mean change between consecutive replies).", s.Jitter/1000, board)
	if s.Last != nil {
		g(prefix+"_rtt_last_seconds", what+" most recent round-trip time.", *s.Last/1000, board)
	}
}
