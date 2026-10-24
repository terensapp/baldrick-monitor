package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ======== remote-control commands ========
//
// The Discord bot turns text ("reboot pixel1") and slash commands into a
// botCmd and runs it here. Anything that interrupts the show comes back as
// a confirmation instead of running straight away.

// botBackend is everything the commands can see and do.
type botBackend interface {
	boards() []BoardView
	rebootBoard(ctx context.Context, name, by string) error
	setMaintenance(name string, on bool, d time.Duration, note, by string) (*Maintenance, error)
	ackAll(board, note, by string) int

	fpps() []FPPView
	fppPlaylists(ctx context.Context, fpp string) ([]string, error)
	fppStart(ctx context.Context, fpp, playlist string) error
	fppStop(ctx context.Context, fpp string, graceful bool) error
	fppVolume(ctx context.Context, fpp string) (int, error)
	fppSetVolume(ctx context.Context, fpp string, level int) error
}

type botCmd struct {
	Name     string // status, reboot, fpp, playlists, play, stop, volume, maint, ack, incidents, help
	Board    string
	FPP      string
	Playlist string
	When     string // stop: now (default) or after
	Level    *int   // volume
	Action   string // maint: on, off ("" = show)
	Duration string // maint
	Note     string
}

// botReply is what goes back to the channel. With Confirm set, Text is the
// question and nothing has happened yet.
type botReply struct {
	Text    string
	Confirm *botAction
}

// botAction is a command waiting for a yes.
type botAction struct {
	ID      string
	User    string
	Label   string // the confirm button
	Prompt  string
	Expires time.Time
	run     func(ctx context.Context) string
}

const botConfirmTTL = 2 * time.Minute

var botAliases = map[string]string{
	"s": "status", "st": "status", "boards": "status",
	"restart": "reboot",
	"show":    "fpp", "fpps": "fpp",
	"pl": "playlists", "playlist": "playlists", "list": "playlists",
	"start":       "play",
	"vol":         "volume",
	"maintenance": "maint", "mm": "maint",
	"acknowledge": "ack",
	"inc":         "incidents", "incident": "incidents",
	"?": "help", "commands": "help",
}

// parseBotText reads a typed command. Matching is case-insensitive;
// playlist names keep their case.
func parseBotText(text string) (botCmd, error) {
	text = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(text), "!/"))
	words := strings.Fields(text)
	if len(words) == 0 {
		return botCmd{}, errors.New("empty command")
	}
	verb, rest := strings.ToLower(words[0]), words[1:]
	if a, ok := botAliases[verb]; ok {
		verb = a
	}
	c := botCmd{Name: verb}
	arg := func(i int) string {
		if i < len(rest) {
			return rest[i]
		}
		return ""
	}
	switch verb {
	case "help", "incidents":
	case "status", "fpp", "playlists":
		if verb == "status" {
			c.Board = arg(0)
		} else {
			c.FPP = arg(0)
		}
	case "reboot":
		if c.Board = arg(0); c.Board == "" {
			return c, errors.New("which board? `reboot <board>`")
		}
	case "play":
		s := " " + strings.Join(rest, " ")
		if i := strings.LastIndex(strings.ToLower(s), " on "); i >= 0 {
			s, c.FPP = s[:i], strings.TrimSpace(s[i+4:])
		}
		if c.Playlist = strings.TrimSpace(s); c.Playlist == "" {
			return c, errors.New("which playlist? `play <playlist> on <fpp>`")
		}
	case "stop-after", "stopafter":
		c.Name, c.When = "stop", "after"
		c.FPP = arg(0)
	case "stop":
		c.When = "now"
		for _, w := range rest {
			switch lw := strings.ToLower(w); lw {
			case "now":
				c.When = "now"
			case "after", "after-current", "aftercurrent", "graceful", "gracefully", "later":
				c.When = "after"
			case "current", "song", "sequence":
			default:
				c.FPP = w
			}
		}
	case "volume":
		for _, w := range rest {
			if n, err := strconv.Atoi(strings.TrimSuffix(w, "%")); err == nil {
				c.Level = &n
			} else {
				c.FPP = w
			}
		}
	case "maint":
		c.Board = arg(0)
		i := 1
		switch a := strings.ToLower(arg(1)); a {
		case "on", "off":
			c.Action, i = a, 2
		}
		if d := arg(i); d != "" && c.Action != "off" {
			if _, err := time.ParseDuration(d); err == nil {
				c.Duration, c.Action, i = d, "on", i+1
			}
		}
		if i < len(rest) {
			c.Note = strings.Join(rest[i:], " ")
		}
	case "ack":
		c.Board = arg(0)
		if len(rest) > 1 {
			c.Note = strings.Join(rest[1:], " ")
		}
	default:
		return c, fmt.Errorf("unknown command `%s`. Type `help` for the list", words[0])
	}
	return c, nil
}

const botHelp = "**Baldrick Monitor commands**\n" +
	"`status [board]`: boards: up/down, uptime, OOMs, low memory\n" +
	"`reboot <board>`: reboot a board (asks first)\n" +
	"`fpp [fpp]`: what each FPP is playing, time left, next show, volume\n" +
	"`playlists [fpp]`: list playlists\n" +
	"`play <playlist> [on <fpp>]`: start a playlist (asks first if a show is running)\n" +
	"`stop [fpp]` / `stop [fpp] after`: stop now / after the current song (asks first)\n" +
	"`volume <fpp> [0-100]`: read or set volume\n" +
	"`maint <board> [on|off] [3h] [note]`: maintenance mode; `maint` lists boards in it\n" +
	"`ack [board|all] [note]`: acknowledge open incidents\n" +
	"`incidents`: incidents still alerting\n" +
	"Reply `yes` or `no` to a question, or use its buttons. Slash commands work too."

// botEngine runs commands against the monitor.
type botEngine struct {
	be  botBackend
	now func() time.Time

	mu      sync.Mutex
	pending map[string]*botAction
	seq     int
}

func newBotEngine(be botBackend) *botEngine {
	return &botEngine{be: be, now: time.Now, pending: map[string]*botAction{}}
}

// confirm parks an action until the user says yes.
func (e *botEngine) confirm(user, label, prompt string, run func(ctx context.Context) string) botReply {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	for id, a := range e.pending {
		if now.After(a.Expires) {
			delete(e.pending, id)
		}
	}
	e.seq++
	a := &botAction{ID: strconv.Itoa(e.seq), User: user, Label: label, Prompt: prompt, Expires: now.Add(botConfirmTTL), run: run}
	e.pending[a.ID] = a
	return botReply{Text: prompt, Confirm: a}
}

// take removes a pending action: by ID, or the user's latest when id is "".
func (e *botEngine) take(user, id string) *botAction {
	e.mu.Lock()
	defer e.mu.Unlock()
	var a *botAction
	if id != "" {
		a = e.pending[id]
	} else {
		for _, p := range e.pending {
			if p.User == user && (a == nil || p.Expires.After(a.Expires)) {
				a = p
			}
		}
	}
	if a == nil || a.User != user {
		return nil
	}
	delete(e.pending, a.ID)
	if e.now().After(a.Expires) {
		return nil
	}
	return a
}

// answer handles a yes/no reply; ok is false when the text isn't one.
func (e *botEngine) answer(ctx context.Context, user, text string) (string, bool) {
	var yes bool
	switch strings.ToLower(strings.Trim(strings.TrimSpace(text), "!.")) {
	case "yes", "y", "confirm", "ok", "do it", "go":
		yes = true
	case "no", "n", "cancel", "abort", "stop it":
	default:
		return "", false
	}
	a := e.take(user, "")
	switch {
	case a == nil:
		return "Nothing is waiting for an answer (questions expire after 2 minutes).", true
	case !yes:
		return "Cancelled: " + a.Prompt, true
	}
	return a.run(ctx), true
}

// resolve matches a name case-insensitively, then by unique prefix.
func resolve(kind, want string, names []string) (string, error) {
	if len(names) == 0 {
		return "", fmt.Errorf("no %ss are configured", kind)
	}
	if want == "" {
		if len(names) == 1 {
			return names[0], nil
		}
		return "", fmt.Errorf("which %s? %s", kind, strings.Join(names, ", "))
	}
	for _, n := range names {
		if strings.EqualFold(n, want) {
			return n, nil
		}
	}
	var hits []string
	for _, n := range names {
		if strings.HasPrefix(strings.ToLower(n), strings.ToLower(want)) {
			hits = append(hits, n)
		}
	}
	if len(hits) == 1 {
		return hits[0], nil
	}
	return "", fmt.Errorf("no %s called `%s`. Try: %s", kind, want, strings.Join(names, ", "))
}

func (e *botEngine) boardNames() []string {
	var out []string
	for _, b := range e.be.boards() {
		out = append(out, b.Name)
	}
	return out
}

func (e *botEngine) fppNames() []string {
	var out []string
	for _, f := range e.be.fpps() {
		if f.Enabled && !f.Ignored {
			out = append(out, f.Name)
		}
	}
	return out
}

func (e *botEngine) fppView(name string) (FPPView, bool) {
	for _, f := range e.be.fpps() {
		if f.Name == name {
			return f, true
		}
	}
	return FPPView{}, false
}

// run executes a command for user (a Discord user ID) shown as by.
func (e *botEngine) run(ctx context.Context, user, by string, c botCmd) botReply {
	txt := func(s string) botReply { return botReply{Text: s} }
	fail := func(err error) botReply { return txt("⚠️ " + err.Error()) }
	switch c.Name {
	case "help":
		return txt(botHelp)
	case "status":
		return txt(e.status(c.Board))
	case "incidents":
		return txt(e.incidents())
	case "reboot":
		name, err := resolve("board", c.Board, e.boardNames())
		if err != nil {
			return fail(err)
		}
		prompt := fmt.Sprintf("Reboot **%s**?", name)
		for _, b := range e.be.boards() {
			if b.Name == name && b.ShowRunning {
				prompt += " ⚠️ A show is running on it."
			}
		}
		return e.confirm(user, "Reboot", prompt, func(ctx context.Context) string {
			if err := e.be.rebootBoard(ctx, name, by); err != nil {
				return fmt.Sprintf("⚠️ Reboot %s: %v", name, err)
			}
			return fmt.Sprintf("🔄 Reboot sent to **%s**. It's usually back in about 10 seconds; check with `status %s`.", name, name)
		})
	case "fpp":
		return txt(e.fppStatus(ctx, c.FPP))
	case "playlists":
		return txt(e.playlists(ctx, c.FPP))
	case "play":
		return e.play(ctx, user, c)
	case "stop":
		name, err := e.stopTarget(c.FPP)
		if err != nil {
			return fail(err)
		}
		graceful := c.When == "after"
		what := "now"
		if graceful {
			what = "after the current song"
		}
		prompt := fmt.Sprintf("Stop the show on **%s** %s?", name, what)
		if v, ok := e.fppView(name); ok && v.Status != "" {
			prompt += fmt.Sprintf(" (%s: %s)", v.Status, orStr(v.Playlist, v.Sequence))
		}
		return e.confirm(user, "Stop", prompt, func(ctx context.Context) string {
			if err := e.be.fppStop(ctx, name, graceful); err != nil {
				return fmt.Sprintf("⚠️ Stop %s: %v", name, err)
			}
			if graceful {
				return fmt.Sprintf("⏹️ **%s** will stop after the current song.", name)
			}
			return fmt.Sprintf("⏹️ Stopped **%s**.", name)
		})
	case "volume":
		name, err := resolve("FPP", c.FPP, e.fppNames())
		if err != nil {
			return fail(err)
		}
		if c.Level == nil {
			v, err := e.be.fppVolume(ctx, name)
			if err != nil {
				return fail(err)
			}
			return txt(fmt.Sprintf("🔊 **%s** volume is %d%%.", name, v))
		}
		if *c.Level < 0 || *c.Level > 100 {
			return fail(errors.New("volume must be 0-100"))
		}
		if err := e.be.fppSetVolume(ctx, name, *c.Level); err != nil {
			return fail(err)
		}
		return txt(fmt.Sprintf("🔊 **%s** volume set to %d%%.", name, *c.Level))
	case "maint":
		return e.maint(by, c)
	case "ack":
		board := ""
		if c.Board != "" && !strings.EqualFold(c.Board, "all") {
			var err error
			if board, err = resolve("board", c.Board, e.boardNames()); err != nil {
				return fail(err)
			}
		}
		n := e.be.ackAll(board, c.Note, by)
		where := "on all boards"
		if board != "" {
			where = "on " + board
		}
		if n == 0 {
			return txt("Nothing to acknowledge " + where + ".")
		}
		return txt(fmt.Sprintf("✅ Acknowledged %d incident(s) %s.", n, where))
	}
	return fail(fmt.Errorf("unknown command `%s`", c.Name))
}

var statusIcon = map[string]string{"up": "🟢", "degraded": "🟠", "down": "🔴", "maintenance": "🔧", "unknown": "⚪"}

func (e *botEngine) status(only string) string {
	boards := e.be.boards()
	if only != "" {
		name, err := resolve("board", only, e.boardNames())
		if err != nil {
			return "⚠️ " + err.Error()
		}
		boards = slices.DeleteFunc(boards, func(b BoardView) bool { return b.Name != name })
	}
	if len(boards) == 0 {
		return "No boards are being monitored."
	}
	var sb strings.Builder
	for _, b := range boards {
		state := b.Status
		if b.Status == "down" {
			state = "DOWN"
			for _, inc := range b.Incidents {
				if inc.Ongoing && inc.Type == "lockup" {
					state = "LOCKED UP " + agoText(inc.StartedAt) + " ago"
				}
			}
		}
		fmt.Fprintf(&sb, "%s **%s** %s", statusIcon[b.Status], b.Name, state)
		if b.HaveState {
			fmt.Fprintf(&sb, " · up %s · OOM %.0f · low %s", (time.Duration(b.Uptime) * time.Second).String(), b.OOMCount, kb(b.RAMMinimum))
		}
		if b.ShowRunning {
			sb.WriteString(" · 🎶 show")
		}
		sb.WriteString("\n")
		if only != "" || b.Status != "up" {
			for _, r := range b.Reasons {
				sb.WriteString("  - " + r + "\n")
			}
		}
		if only != "" && b.HaveState {
			fmt.Fprintf(&sb, "  firmware %s · free %s (largest %s) · %d reboots seen\n", b.Firmware, kb(b.RAMAvailable), kb(b.RAMLargestBlock), b.Reboots)
		}
	}
	return strings.TrimRight(sb.String(), "\n")
}

func kb(bytes float64) string { return fmt.Sprintf("%.0fKB", bytes/1024) }

func (e *botEngine) incidents() string {
	var sb strings.Builder
	for _, b := range e.be.boards() {
		for _, inc := range b.Incidents {
			state := "not acknowledged"
			if inc.Ongoing {
				state = "ongoing"
			}
			fmt.Fprintf(&sb, "• **%s** %s, %s ago (%s)\n", b.Name, inc.Title, agoText(inc.StartedAt), state)
		}
	}
	if sb.Len() == 0 {
		return "✅ No incidents need attention."
	}
	return strings.TrimRight(sb.String(), "\n") + "\n`ack all` to acknowledge them."
}

func fmtSecs(s float64) string {
	d := time.Duration(s) * time.Second
	if d >= time.Hour {
		return fmt.Sprintf("%d:%02d:%02d", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
	}
	return fmt.Sprintf("%d:%02d", int(d.Minutes()), int(d.Seconds())%60)
}

func (e *botEngine) fppStatus(ctx context.Context, only string) string {
	names := e.fppNames()
	if only != "" {
		name, err := resolve("FPP", only, names)
		if err != nil {
			return "⚠️ " + err.Error()
		}
		names = []string{name}
	}
	if len(names) == 0 {
		return "No FPPs are being watched."
	}
	vols := make([]string, len(names))
	var wg sync.WaitGroup
	for i, n := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v, err := e.be.fppVolume(ctx, n); err == nil {
				vols[i] = fmt.Sprintf(" · 🔊 %d%%", v)
			}
		}()
	}
	wg.Wait()
	var sb strings.Builder
	for i, n := range names {
		v, _ := e.fppView(n)
		switch {
		case !v.Up && v.Checked:
			fmt.Fprintf(&sb, "🔴 **%s** unreachable: %s\n", n, v.Error)
			continue
		case v.Status == "playing":
			fmt.Fprintf(&sb, "🎶 **%s** playing **%s**", n, orStr(v.Playlist, v.Sequence))
			if v.Sequence != "" && v.Playlist != "" {
				fmt.Fprintf(&sb, " (%s)", v.Sequence)
			}
			if v.SecondsRemaining > 0 {
				fmt.Fprintf(&sb, " · %s left", fmtSecs(v.SecondsRemaining))
			}
		case v.Status == "":
			fmt.Fprintf(&sb, "⚪ **%s** not checked yet", n)
		default:
			fmt.Fprintf(&sb, "⏸️ **%s** %s", n, v.Status)
		}
		sb.WriteString(vols[i])
		if v.NextPlaylist != "" && v.NextStart != nil {
			fmt.Fprintf(&sb, "\n  next: %s at %s", v.NextPlaylist, v.NextStart.Local().Format("Mon 15:04"))
		}
		sb.WriteString("\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

func (e *botEngine) playlists(ctx context.Context, only string) string {
	names := e.fppNames()
	if only != "" {
		name, err := resolve("FPP", only, names)
		if err != nil {
			return "⚠️ " + err.Error()
		}
		names = []string{name}
	}
	if len(names) == 0 {
		return "No FPPs are being watched."
	}
	var sb strings.Builder
	for _, n := range names {
		pls, err := e.be.fppPlaylists(ctx, n)
		switch {
		case err != nil:
			fmt.Fprintf(&sb, "**%s**: ⚠️ %v\n", n, err)
		case len(pls) == 0:
			fmt.Fprintf(&sb, "**%s**: no playlists\n", n)
		default:
			fmt.Fprintf(&sb, "**%s**: %s\n", n, strings.Join(pls, ", "))
		}
	}
	return strings.TrimRight(sb.String(), "\n")
}

// findPlaylist finds a playlist by name on one FPP, or on whichever FPP
// has it when fpp is "".
func (e *botEngine) findPlaylist(ctx context.Context, fpp, want string) (string, string, error) {
	names := e.fppNames()
	if fpp != "" {
		name, err := resolve("FPP", fpp, names)
		if err != nil {
			return "", "", err
		}
		names = []string{name}
	}
	type hit struct{ fpp, pl string }
	var exact, prefix []hit
	var lastErr error
	for _, n := range names {
		pls, err := e.be.fppPlaylists(ctx, n)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", n, err)
			continue
		}
		for _, p := range pls {
			switch {
			case strings.EqualFold(p, want):
				exact = append(exact, hit{n, p})
			case strings.HasPrefix(strings.ToLower(p), strings.ToLower(want)):
				prefix = append(prefix, hit{n, p})
			}
		}
	}
	hits := exact
	if len(hits) == 0 {
		hits = prefix
	}
	switch {
	case len(hits) == 1:
		return hits[0].fpp, hits[0].pl, nil
	case len(hits) > 1:
		var where []string
		for _, h := range hits {
			where = append(where, h.pl+" on "+h.fpp)
		}
		return "", "", fmt.Errorf("`%s` is ambiguous: %s. Say `play <playlist> on <fpp>`", want, strings.Join(where, ", "))
	case lastErr != nil:
		return "", "", lastErr
	}
	return "", "", fmt.Errorf("no playlist called `%s`. `playlists` lists them", want)
}

func (e *botEngine) play(ctx context.Context, user string, c botCmd) botReply {
	fpp, pl, err := e.findPlaylist(ctx, c.FPP, c.Playlist)
	if err != nil {
		return botReply{Text: "⚠️ " + err.Error()}
	}
	start := func(ctx context.Context) string {
		if err := e.be.fppStart(ctx, fpp, pl); err != nil {
			return fmt.Sprintf("⚠️ Start %s on %s: %v", pl, fpp, err)
		}
		return fmt.Sprintf("▶️ Started **%s** on **%s**.", pl, fpp)
	}
	if v, ok := e.fppView(fpp); ok && v.Status != "" && v.Status != "idle" {
		return e.confirm(user, "Play", fmt.Sprintf("**%s** is %s **%s**. Replace it with **%s**?", fpp, v.Status, orStr(v.Playlist, v.Sequence), pl), start)
	}
	return botReply{Text: start(ctx)}
}

// stopTarget picks the FPP to stop: the named one, the only one, or the
// only one playing.
func (e *botEngine) stopTarget(want string) (string, error) {
	names := e.fppNames()
	if want != "" || len(names) <= 1 {
		return resolve("FPP", want, names)
	}
	var playing []string
	for _, n := range names {
		if v, ok := e.fppView(n); ok && v.Status != "" && v.Status != "idle" {
			playing = append(playing, n)
		}
	}
	if len(playing) == 1 {
		return playing[0], nil
	}
	return "", fmt.Errorf("which FPP? %s", strings.Join(names, ", "))
}

func (e *botEngine) maint(by string, c botCmd) botReply {
	if c.Board == "" {
		var sb strings.Builder
		for _, b := range e.be.boards() {
			if m := b.Maintenance; m != nil {
				until := "until turned off"
				if m.Until != nil {
					until = "until " + m.Until.Local().Format("Mon 15:04")
				}
				fmt.Fprintf(&sb, "🔧 **%s** %s%s\n", b.Name, until, noteSuffix(m.Note))
			}
		}
		if sb.Len() == 0 {
			return botReply{Text: "No boards are in maintenance. `maint <board> 2h` starts it."}
		}
		return botReply{Text: strings.TrimRight(sb.String(), "\n")}
	}
	name, err := resolve("board", c.Board, e.boardNames())
	if err != nil {
		return botReply{Text: "⚠️ " + err.Error()}
	}
	switch c.Action {
	case "off":
		if _, err := e.be.setMaintenance(name, false, 0, "", by); err != nil {
			return botReply{Text: "⚠️ " + err.Error()}
		}
		return botReply{Text: fmt.Sprintf("✅ **%s** is out of maintenance; alerts resume.", name)}
	case "on":
		var d time.Duration
		if c.Duration != "" {
			if d, err = time.ParseDuration(c.Duration); err != nil || d <= 0 {
				return botReply{Text: "⚠️ " + errBadMaintenance.Error()}
			}
		}
		m, err := e.be.setMaintenance(name, true, d, c.Note, by)
		if err != nil {
			return botReply{Text: "⚠️ " + err.Error()}
		}
		until := "until you turn it off"
		if m != nil && m.Until != nil {
			until = "until " + m.Until.Local().Format("Mon 15:04")
		}
		return botReply{Text: fmt.Sprintf("🔧 **%s** is in maintenance %s; its alerts are silenced.", name, until)}
	}
	for _, b := range e.be.boards() {
		if b.Name == name && b.Maintenance != nil {
			return botReply{Text: fmt.Sprintf("🔧 **%s** is in maintenance. `maint %s off` ends it.", name, name)}
		}
	}
	return botReply{Text: fmt.Sprintf("**%s** isn't in maintenance. `maint %s on` or `maint %s 2h` starts it.", name, name, name)}
}
