package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

// universeOutputs as FPP 9.5.3 serves it, plus outputs that must not count.
const realOutputs = `{"channelOutputs":[{"channelCount":-1,"enabled":1,"interface":"eth0","startChannel":1,"threaded":1,"timeout":1000,"type":"universes","universes":[
	{"active":1,"address":"192.168.6.4","channelCount":3192,"deDuplicate":0,"description":"pixel1","id":1,"monitor":1,"priority":0,"startChannel":1,"type":4},
	{"active":0,"address":"192.168.6.5","channelCount":510,"description":"off","startChannel":3193,"type":4},
	{"active":1,"address":"","channelCount":510,"description":"multicast","startChannel":3703,"type":0},
	{"active":1,"address":"192.168.6.7","channelCount":510,"description":"arches","startChannel":4213,"type":1}]},
	{"enabled":0,"type":"universes","universes":[{"active":1,"address":"192.168.6.4","channelCount":9,"startChannel":1,"type":3}]}],"status":"OK"}`

func TestParseFPPOutputs(t *testing.T) {
	o, err := parseFPPOutputs([]byte(realOutputs))
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Targets) != 2 || o.Targets[0].Protocol != "ddp" || o.Targets[0].Address != "192.168.6.4" || o.Targets[0].ChannelCount != 3192 ||
		o.Targets[1].Protocol != "e131" {
		t.Fatalf("targets = %+v", o.Targets)
	}
	hit := o.targetsFor(context.Background(), []string{"192.168.6.4"})
	if len(hit) != 1 || hit[0].Description != "pixel1" {
		t.Errorf("targetsFor = %+v", hit)
	}
	if hit := o.targetsFor(context.Background(), []string{"192.168.6.9"}); len(hit) != 0 {
		t.Errorf("unrelated board matched %+v", hit)
	}
}

func showState(sources string) []byte {
	return []byte(`{"uptime":100,"fpp":[{"hostname":"fpp","ip":"10.0.0.100"}],"frame_rate":0,
		"streaming":{"ddp_sources":[` + sources + `],"ddp_recvd":0,"sacn_sources":[],"artnet_sources":[]}}`)
}

func playingFeed(state string, targets int) fppFeed {
	f := fppFeed{FPP: FPPInstance{Hostname: "fpp", IP: "10.0.0.100"}, Name: "fpp",
		Status:  &fppStatus{StatusName: state, raw: []byte(`{"status_name":"` + state + `"}`)},
		Outputs: &fppOutputs{Raw: []byte(realOutputs)}}
	f.Status.CurrentPlaylist.Playlist = "Halloween"
	for range targets {
		f.Targets = append(f.Targets, fppTarget{Protocol: "ddp", Address: "127.0.0.1", StartChannel: 1, ChannelCount: 300})
	}
	return f
}

func TestShowOutputIncident(t *testing.T) {
	b := testBoard(t)
	b.mergeState(showState(""), "poll")
	t0 := time.Now()
	note := func(at time.Duration, f fppFeed) {
		b.mu.Lock()
		b.noteShow([]fppFeed{f}, t0.Add(at))
		b.mu.Unlock()
	}
	note(0, playingFeed("playing", 1))
	note(15*time.Second, playingFeed("playing", 1))
	if len(b.incidents) != 0 {
		t.Fatalf("opened within the grace period: %+v", b.incidents[0])
	}
	note(25*time.Second, playingFeed("playing", 1))
	if len(b.incidents) != 1 {
		t.Fatalf("want 1 incident, got %d", len(b.incidents))
	}
	inc := b.incidents[0]
	if inc.Type != "show_output" || !inc.Ongoing || inc.FPP == nil || len(inc.FPP.Targets) != 1 || len(inc.FPP.Outputs) == 0 ||
		!strings.Contains(inc.Title, "FPP fpp is playing Halloween but t isn't receiving from it") {
		t.Fatalf("got %+v", inc)
	}
	if !inc.StartedAt.Equal(t0) {
		t.Errorf("started %v, want when the stream was first missed", inc.StartedAt)
	}
	note(30*time.Second, playingFeed("playing", 1))
	if len(b.incidents) != 1 {
		t.Fatal("a second incident opened for the same show")
	}
	b.mergeState(showState(`"10.0.0.100"`), "ws")
	note(35*time.Second, playingFeed("playing", 1))
	if inc.Ongoing || b.activeShow != nil || !strings.Contains(inc.Resolution, "receiving from FPP fpp") {
		t.Fatalf("not resolved by the stream: %+v", inc)
	}
	if v := b.View(); v.Status == "up" {
		t.Error("an unacknowledged show incident should keep the board degraded")
	}
}

func TestShowOutputQuietCases(t *testing.T) {
	cases := map[string]func(b *Board) fppFeed{
		"idle":         func(*Board) fppFeed { return playingFeed("idle", 1) },
		"not a target": func(*Board) fppFeed { return playingFeed("playing", 0) },
		"receiving": func(b *Board) fppFeed {
			b.mergeState(showState(`"10.0.0.100"`), "ws")
			return playingFeed("playing", 1)
		},
		"unreachable": func(*Board) fppFeed { return fppFeed{FPP: FPPInstance{IP: "10.0.0.100"}, Err: io.EOF} },
		"locked up": func(b *Board) fppFeed {
			b.pingFails = 3
			return playingFeed("playing", 1)
		},
	}
	for name, setup := range cases {
		b := testBoard(t)
		b.mergeState(showState(""), "poll")
		f := setup(b)
		t0 := time.Now()
		for i := range 10 {
			b.mu.Lock()
			b.noteShow([]fppFeed{f}, t0.Add(time.Duration(i)*5*time.Second))
			b.mu.Unlock()
		}
		if len(b.incidents) != 0 {
			t.Errorf("%s: opened %+v", name, b.incidents[0])
		}
	}
}

func TestShowOutputResolvesWhenFPPStops(t *testing.T) {
	b := testBoard(t)
	b.mergeState(showState(""), "poll")
	t0 := time.Now()
	for _, at := range []time.Duration{0, 25 * time.Second} {
		b.noteShow([]fppFeed{playingFeed("playing", 1)}, t0.Add(at))
	}
	// An FPP that can't be read leaves it open.
	b.noteShow([]fppFeed{{FPP: FPPInstance{IP: "10.0.0.100"}, Err: io.EOF}}, t0.Add(30*time.Second))
	if b.activeShow == nil {
		t.Fatal("no incident, or closed by an unreadable FPP")
	}
	inc := b.activeShow
	b.noteShow([]fppFeed{playingFeed("idle", 1)}, t0.Add(40*time.Second))
	if inc.Ongoing || !strings.Contains(inc.Resolution, "stopped playing") {
		t.Fatalf("got %+v", inc)
	}
}
