package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ======== Discord bot: remote control from a channel ========
//
// The bot keeps a gateway connection open (no public URL needed) and takes
// commands two ways: slash commands, and plain messages in the channel
// ("reboot pixel1"). Plain messages need the Message Content intent; without
// it the bot still answers slash commands and messages that @mention it.
// Only the allowed users, and only in the configured channel, are obeyed.

// DiscordConfig is the bot's part of the settings.
type DiscordConfig struct {
	Enabled      bool     `json:"enabled"`
	Token        string   `json:"token,omitempty"` // never sent to the browser
	ChannelID    string   `json:"channel_id"`
	AllowedUsers []string `json:"allowed_users"` // Discord user IDs
}

var snowflake = regexp.MustCompile(`^\d{5,25}$`)

func (c *DiscordConfig) normalize() error {
	c.Token, c.ChannelID = strings.TrimSpace(c.Token), strings.TrimSpace(c.ChannelID)
	var users []string
	for _, u := range c.AllowedUsers {
		if u = strings.TrimSpace(u); u != "" && !slices.Contains(users, u) {
			users = append(users, u)
		}
	}
	c.AllowedUsers = users
	if c.ChannelID != "" && !snowflake.MatchString(c.ChannelID) {
		return fmt.Errorf("discord: the channel ID must be a number (Developer Mode → right-click the channel → Copy Channel ID), got %q", c.ChannelID)
	}
	for _, u := range users {
		if !snowflake.MatchString(u) {
			return fmt.Errorf("discord: allowed users must be user IDs (right-click yourself → Copy User ID), got %q", u)
		}
	}
	if c.Enabled && (c.Token == "" || c.ChannelID == "" || len(users) == 0) {
		return errors.New("discord: the bot needs a token, a channel ID and at least one allowed user")
	}
	return nil
}

func (c DiscordConfig) allowed(user string) bool {
	return user != "" && slices.Contains(c.AllowedUsers, user)
}

// DiscordStatus is shown in the settings.
type DiscordStatus struct {
	State         string     `json:"state"` // off, connecting, connected, error
	Error         string     `json:"error,omitempty"`
	User          string     `json:"user,omitempty"` // the bot's own name
	TextCommands  bool       `json:"text_commands"`  // plain messages are read (Message Content intent)
	SlashCommands bool       `json:"slash_commands"` // registered in the channel's server
	Since         *time.Time `json:"since,omitempty"`
}

const (
	discordAPI     = "https://discord.com/api/v10"
	discordGateway = "wss://gateway.discord.gg/?v=10&encoding=json"

	intentGuildMessages  = 1 << 9
	intentMessageContent = 1 << 15
	discordIntents       = intentGuildMessages | intentMessageContent
)

type DiscordBot struct {
	api, gateway string
	client       *http.Client
	eng          *botEngine

	restart chan struct{}

	mu        sync.Mutex
	cfg       DiscordConfig
	status    DiscordStatus
	appID     string
	botID     string
	slashFor  string // app/guild the slash commands were registered for
	plCache   map[string]cachedPlaylists
	root      context.Context
	handlers  sync.WaitGroup
	sessionID string // owned by Run
	resumeURL string // owned by Run
	seq       atomic.Int64
}

type cachedPlaylists struct {
	at   time.Time
	list []string
}

func NewDiscordBot(be botBackend) *DiscordBot {
	return &DiscordBot{api: discordAPI, gateway: discordGateway, client: &http.Client{Timeout: 15 * time.Second},
		eng: newBotEngine(be), restart: make(chan struct{}, 1), status: DiscordStatus{State: "off"},
		plCache: map[string]cachedPlaylists{}, root: context.Background()}
}

// SetConfig applies new settings; the connection restarts only when the
// token or the on/off switch changes.
func (d *DiscordBot) SetConfig(c DiscordConfig) {
	c.AllowedUsers = slices.Clone(c.AllowedUsers)
	d.mu.Lock()
	old := d.cfg
	d.cfg = c
	d.mu.Unlock()
	if old.Enabled != c.Enabled || old.Token != c.Token {
		select {
		case d.restart <- struct{}{}:
		default:
		}
	}
}

func (d *DiscordBot) config() DiscordConfig {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cfg
}

func (d *DiscordBot) Status() DiscordStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status
}

func (d *DiscordBot) setState(state, errText string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.status.State != state {
		now := time.Now()
		d.status.Since = &now
	}
	d.status.State, d.status.Error = state, errText
	if state == "off" {
		d.status = DiscordStatus{State: "off"}
	}
}

// Run keeps the bot connected until ctx ends.
func (d *DiscordBot) Run(ctx context.Context) {
	d.mu.Lock()
	d.root = ctx
	d.mu.Unlock()
	defer d.handlers.Wait()
	backoff := time.Second
	intents := discordIntents
	wait := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-d.restart:
			return true
		}
	}
	for ctx.Err() == nil {
		// The config is read fresh below, so a restart signalled before now
		// is already covered; left queued, it would cancel the new session.
		select {
		case <-d.restart:
		default:
		}
		cfg := d.config()
		if !cfg.Enabled || cfg.Token == "" {
			d.setState("off", "")
			d.sessionID, d.resumeURL = "", ""
			if !wait() {
				return
			}
			intents, backoff = discordIntents, time.Second
			continue
		}
		d.setState("connecting", d.Status().Error)
		sctx, cancel := context.WithCancel(ctx)
		stop := make(chan struct{})
		var restarted atomic.Bool
		go func() {
			select {
			case <-d.restart:
				restarted.Store(true)
				cancel()
			case <-stop:
			}
		}()
		start := time.Now()
		err := d.session(sctx, cfg, intents)
		close(stop)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if restarted.Load() {
			d.sessionID, d.resumeURL = "", ""
			intents, backoff = discordIntents, time.Second
			continue
		}
		var ce *wsCloseError
		if errors.As(err, &ce) {
			switch ce.Code {
			case 4004:
				d.setState("error", "Discord rejected the bot token")
				log.Printf("discord: the bot token was rejected; fix it in the settings")
				d.sessionID, d.resumeURL = "", ""
				if !wait() {
					return
				}
				continue
			case 4014:
				if intents&intentMessageContent != 0 {
					log.Printf("discord: the Message Content intent isn't enabled for this bot; plain-text commands are off (slash commands and @mentions still work)")
					intents &^= intentMessageContent
					d.sessionID, d.resumeURL = "", ""
					continue
				}
				fallthrough
			case 4010, 4011, 4012, 4013:
				d.setState("error", err.Error())
				log.Printf("discord: %v", err)
				if !wait() {
					return
				}
				continue
			case 4007, 4009:
				d.sessionID, d.resumeURL = "", ""
			}
		}
		if time.Since(start) > time.Minute {
			backoff = time.Second
		}
		msg := "disconnected"
		if err != nil {
			msg = err.Error()
		}
		d.setState("connecting", msg)
		log.Printf("discord: %s; reconnecting in %s", msg, backoff)
		select {
		case <-ctx.Done():
			return
		case <-d.restart:
			d.sessionID, d.resumeURL = "", ""
			intents, backoff = discordIntents, time.Second
			continue
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Minute)
	}
}

type gwPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

var errGatewayReconnect = errors.New("gateway asked to reconnect")

// session runs one gateway connection.
func (d *DiscordBot) session(ctx context.Context, cfg DiscordConfig, intents int) error {
	u := d.gateway
	resume := d.sessionID != "" && d.resumeURL != ""
	if resume {
		u = strings.TrimRight(d.resumeURL, "/") + "/?v=10&encoding=json"
	}
	ws, err := wsDialURL(ctx, u, "Discord", 15*time.Second)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
		case <-done:
		}
		ws.Close()
	}()
	send := func(op int, data any) error {
		b, _ := json.Marshal(map[string]any{"op": op, "d": data})
		return ws.writeText(b)
	}

	raw, err := ws.read(30 * time.Second)
	if err != nil {
		return err
	}
	var hello struct {
		Op int `json:"op"`
		D  struct {
			Interval float64 `json:"heartbeat_interval"`
		} `json:"d"`
	}
	if json.Unmarshal(raw, &hello) != nil || hello.Op != 10 || hello.D.Interval <= 0 {
		return errors.New("gateway: no hello")
	}
	interval := time.Duration(hello.D.Interval) * time.Millisecond

	if resume {
		err = send(6, map[string]any{"token": cfg.Token, "session_id": d.sessionID, "seq": d.seq.Load()})
	} else {
		d.seq.Store(0)
		err = send(2, map[string]any{"token": cfg.Token, "intents": intents,
			"properties": map[string]string{"os": "linux", "browser": "baldrick-monitor", "device": "baldrick-monitor"}})
	}
	if err != nil {
		return err
	}

	heartbeat := func() error {
		var s any
		if n := d.seq.Load(); n > 0 {
			s = n
		}
		return send(1, s)
	}
	var acked atomic.Bool
	acked.Store(true)
	go func() {
		t := time.NewTimer(time.Duration(rand.Float64() * float64(interval)))
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			if !acked.Swap(false) {
				log.Printf("discord: no heartbeat ack; reconnecting")
				ws.Close()
				return
			}
			if heartbeat() != nil {
				return
			}
			t.Reset(interval)
		}
	}()

	for {
		raw, err := ws.read(interval + 30*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		var p gwPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			continue
		}
		if p.S != nil {
			d.seq.Store(*p.S)
		}
		switch p.Op {
		case 0:
			d.dispatch(p.T, p.D, intents)
		case 1:
			heartbeat()
		case 7:
			return errGatewayReconnect
		case 9:
			var resumable bool
			json.Unmarshal(p.D, &resumable)
			if !resumable {
				d.sessionID, d.resumeURL = "", ""
			}
			time.Sleep(time.Duration(1+rand.IntN(4)) * time.Second)
			return errors.New("gateway: invalid session")
		case 11:
			acked.Store(true)
		}
	}
}

type dUser struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Global   string `json:"global_name"`
	Bot      bool   `json:"bot"`
}

func (u dUser) by() string {
	return "Discord @" + orStr(u.Username, u.ID)
}

type dMessage struct {
	ID        string `json:"id"`
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
	Author    dUser  `json:"author"`
	Embeds    []struct {
		URL    string `json:"url"`
		Footer *struct {
			Text string `json:"text"`
		} `json:"footer"`
	} `json:"embeds"`
	Reference *struct {
		MessageID string `json:"message_id"`
	} `json:"message_reference"`
	Replied *dMessage `json:"referenced_message"`
}

// incidentIDs finds the incidents a monitor notice is about (its embed
// footer, or the dashboard link).
func (m *dMessage) incidentIDs() []string {
	var ids []string
	for _, e := range m.Embeds {
		s := e.URL
		if e.Footer != nil {
			s += " " + e.Footer.Text
		}
		for _, id := range incidentIDRe.FindAllString(s, -1) {
			if !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

type dOption struct {
	Name    string          `json:"name"`
	Value   json.RawMessage `json:"value"`
	Focused bool            `json:"focused"`
}

type dInteraction struct {
	ID            string `json:"id"`
	Token         string `json:"token"`
	Type          int    `json:"type"` // 2 command, 3 button, 4 autocomplete
	ApplicationID string `json:"application_id"`
	ChannelID     string `json:"channel_id"`
	Member        *struct {
		User dUser `json:"user"`
	} `json:"member"`
	User *dUser `json:"user"`
	Data struct {
		Name     string    `json:"name"`
		CustomID string    `json:"custom_id"`
		Options  []dOption `json:"options"`
	} `json:"data"`
	Message *struct {
		Content string `json:"content"`
	} `json:"message"`
}

func (in *dInteraction) user() dUser {
	if in.Member != nil {
		return in.Member.User
	}
	if in.User != nil {
		return *in.User
	}
	return dUser{}
}

// opts flattens the options to strings.
func (in *dInteraction) opts() (map[string]string, string) {
	out, focused := map[string]string{}, ""
	for _, o := range in.Data.Options {
		var s string
		if json.Unmarshal(o.Value, &s) != nil {
			s = string(o.Value)
		}
		out[o.Name] = s
		if o.Focused {
			focused = o.Name
		}
	}
	return out, focused
}

func (d *DiscordBot) dispatch(t string, data json.RawMessage, intents int) {
	switch t {
	case "READY":
		var r struct {
			SessionID string `json:"session_id"`
			ResumeURL string `json:"resume_gateway_url"`
			User      dUser  `json:"user"`
			App       struct {
				ID string `json:"id"`
			} `json:"application"`
		}
		json.Unmarshal(data, &r)
		d.sessionID, d.resumeURL = r.SessionID, r.ResumeURL
		d.mu.Lock()
		d.appID, d.botID = r.App.ID, r.User.ID
		d.status.User = orStr(r.User.Global, r.User.Username)
		d.status.TextCommands = intents&intentMessageContent != 0
		d.mu.Unlock()
		d.setState("connected", "")
		log.Printf("discord: connected as %s", r.User.Username)
		d.handle(d.registerCommands)
	case "RESUMED":
		d.setState("connected", "")
	case "MESSAGE_CREATE":
		var m dMessage
		if json.Unmarshal(data, &m) == nil {
			d.handle(func(ctx context.Context) { d.onMessage(ctx, m) })
		}
	case "INTERACTION_CREATE":
		var in dInteraction
		if json.Unmarshal(data, &in) == nil {
			d.handle(func(ctx context.Context) { d.onInteraction(ctx, &in) })
		}
	}
}

// handle runs f in the background; commands outlive a reconnect.
func (d *DiscordBot) handle(f func(ctx context.Context)) {
	d.mu.Lock()
	ctx := d.root
	d.mu.Unlock()
	d.handlers.Add(1)
	go func() {
		defer d.handlers.Done()
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		f(ctx)
	}()
}

var mentionRe = regexp.MustCompile(`<@!?(\d+)>`)

func (d *DiscordBot) onMessage(ctx context.Context, m dMessage) {
	cfg := d.config()
	d.mu.Lock()
	botID := d.botID
	d.mu.Unlock()
	if m.Author.Bot || m.Author.ID == botID || m.ChannelID != cfg.ChannelID {
		return
	}
	text := strings.TrimSpace(mentionRe.ReplaceAllStringFunc(m.Content, func(s string) string {
		if mentionRe.FindStringSubmatch(s)[1] == botID {
			return ""
		}
		return s
	}))
	if text == "" {
		return
	}
	if !cfg.allowed(m.Author.ID) {
		log.Printf("discord: ignored a message from user %s (%s), who isn't allowed", m.Author.ID, m.Author.Username)
		return
	}
	if m.Reference != nil && m.Reference.MessageID != "" {
		if note, ok := ackReply(text); ok {
			replied := m.Replied
			if replied == nil { // Discord leaves it out at times; ask for it
				var got dMessage
				if err := d.rest(ctx, http.MethodGet, "/channels/"+m.ChannelID+"/messages/"+m.Reference.MessageID, nil, &got); err != nil {
					d.postReply(ctx, m, botReply{Text: "⚠️ Couldn't read the message you replied to: " + err.Error()})
					return
				}
				replied = &got
			}
			log.Printf("discord: %s acknowledged by reply", m.Author.Username)
			d.postReply(ctx, m, botReply{Text: d.eng.ackIncidents(replied.incidentIDs(), note, m.Author.by())})
			return
		}
	}
	if out, ok := d.eng.answer(ctx, m.Author.ID, text); ok {
		d.postReply(ctx, m, botReply{Text: out})
		return
	}
	c, err := parseBotText(text)
	if err != nil {
		d.postReply(ctx, m, botReply{Text: "⚠️ " + err.Error()})
		return
	}
	log.Printf("discord: %s ran %q", m.Author.Username, truncate(text, 100))
	d.postReply(ctx, m, d.eng.run(ctx, m.Author.ID, m.Author.by(), c))
}

func (d *DiscordBot) postReply(ctx context.Context, m dMessage, r botReply) {
	body := messageBody(r)
	body["message_reference"] = map[string]any{"message_id": m.ID, "fail_if_not_exists": false}
	if err := d.rest(ctx, http.MethodPost, "/channels/"+m.ChannelID+"/messages", body, nil); err != nil {
		log.Printf("discord: reply: %v", err)
	}
}

// messageBody is a message with confirm buttons when one is needed.
func messageBody(r botReply) map[string]any {
	text := r.Text
	if r.Confirm != nil {
		text += "\n-# Reply `yes` or `no`, or use the buttons. Expires in 2 minutes."
	}
	body := map[string]any{"content": clip(text), "allowed_mentions": map[string]any{"parse": []string{}},
		"components": []any{}}
	if a := r.Confirm; a != nil {
		body["components"] = []any{map[string]any{"type": 1, "components": []any{
			map[string]any{"type": 2, "style": 4, "label": a.Label, "custom_id": "bm:yes:" + a.ID},
			map[string]any{"type": 2, "style": 2, "label": "Cancel", "custom_id": "bm:no:" + a.ID},
		}}}
	}
	return body
}

// clip keeps a message under Discord's 2000-character limit.
func clip(s string) string {
	if r := []rune(s); len(r) > 1990 {
		return string(r[:1990]) + "…"
	}
	return s
}

func (d *DiscordBot) onInteraction(ctx context.Context, in *dInteraction) {
	cfg := d.config()
	u := in.user()
	if !cfg.allowed(u.ID) || in.ChannelID != cfg.ChannelID {
		log.Printf("discord: refused an interaction from user %s (%s) in channel %s", u.ID, u.Username, in.ChannelID)
		if in.Type == 4 {
			d.callback(ctx, in, 8, map[string]any{"choices": []any{}})
		} else {
			d.callback(ctx, in, 4, map[string]any{"content": "You can't control Baldrick Monitor here.", "flags": 64})
		}
		return
	}
	switch in.Type {
	case 2:
		d.callback(ctx, in, 5, nil) // "thinking…" while FPPs and boards answer
		c := slashCmd(in)
		log.Printf("discord: %s ran /%s", u.Username, in.Data.Name)
		r := d.eng.run(ctx, u.ID, u.by(), c)
		if err := d.rest(ctx, http.MethodPatch, "/webhooks/"+in.ApplicationID+"/"+in.Token+"/messages/@original", messageBody(r), nil); err != nil {
			log.Printf("discord: reply: %v", err)
		}
	case 3:
		parts := strings.SplitN(in.Data.CustomID, ":", 3)
		if len(parts) != 3 || parts[0] != "bm" {
			return
		}
		prompt := ""
		if in.Message != nil {
			prompt, _, _ = strings.Cut(in.Message.Content, "\n-# ")
		}
		a := d.eng.take(u.ID, parts[2])
		switch {
		case a == nil:
			d.callback(ctx, in, 7, map[string]any{"content": clip(prompt + "\n_Already answered or expired._"), "components": []any{}})
		case parts[1] != "yes":
			d.callback(ctx, in, 7, map[string]any{"content": clip("~~" + a.Prompt + "~~ Cancelled."), "components": []any{}})
		default:
			d.callback(ctx, in, 7, map[string]any{"content": clip(a.Prompt + "\n⏳ Working…"), "components": []any{}})
			log.Printf("discord: %s confirmed %q", u.Username, a.Prompt)
			out := a.run(ctx)
			if err := d.rest(ctx, http.MethodPatch, "/webhooks/"+in.ApplicationID+"/"+in.Token+"/messages/@original",
				map[string]any{"content": clip(a.Prompt + "\n" + out)}, nil); err != nil {
				log.Printf("discord: reply: %v", err)
			}
		}
	case 4:
		d.callback(ctx, in, 8, map[string]any{"choices": d.autocomplete(ctx, in)})
	}
}

func (d *DiscordBot) callback(ctx context.Context, in *dInteraction, typ int, data map[string]any) {
	body := map[string]any{"type": typ}
	if data != nil {
		body["data"] = data
	}
	if err := d.rest(ctx, http.MethodPost, "/interactions/"+in.ID+"/"+in.Token+"/callback", body, nil); err != nil {
		log.Printf("discord: interaction response: %v", err)
	}
}

// slashCmd turns a slash command into the same command a message would.
func slashCmd(in *dInteraction) botCmd {
	o, _ := in.opts()
	c := botCmd{Name: in.Data.Name, Board: o["board"], FPP: o["fpp"], Playlist: o["playlist"],
		When: orStr(o["when"], "now"), Action: o["action"], Duration: o["duration"], Note: o["note"]}
	if v, ok := o["level"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.Level = &n
		}
	}
	return c
}

func (d *DiscordBot) autocomplete(ctx context.Context, in *dInteraction) []any {
	o, focused := in.opts()
	typed := strings.ToLower(o[focused])
	var names []string
	switch focused {
	case "board":
		names = d.eng.boardNames()
		if in.Data.Name == "ack" {
			names = append([]string{"all"}, names...)
		}
	case "fpp":
		names = d.eng.fppNames()
	case "playlist":
		fpps := d.eng.fppNames()
		if f, err := resolve("FPP", o["fpp"], fpps); err == nil && o["fpp"] != "" {
			fpps = []string{f}
		}
		for _, f := range fpps {
			for _, p := range d.playlists(ctx, f) {
				if !slices.Contains(names, p) {
					names = append(names, p)
				}
			}
		}
	}
	out := []any{}
	for _, n := range names {
		if strings.Contains(strings.ToLower(n), typed) && len(out) < 25 {
			out = append(out, map[string]string{"name": n, "value": n})
		}
	}
	return out
}

// playlists is an FPP's playlist names, remembered briefly for autocomplete.
func (d *DiscordBot) playlists(ctx context.Context, fpp string) []string {
	d.mu.Lock()
	c, ok := d.plCache[fpp]
	d.mu.Unlock()
	if ok && time.Since(c.at) < 30*time.Second {
		return c.list
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	list, err := d.eng.be.fppPlaylists(ctx, fpp)
	if err != nil {
		return c.list
	}
	d.mu.Lock()
	d.plCache[fpp] = cachedPlaylists{time.Now(), list}
	d.mu.Unlock()
	return list
}

// slashCommands are registered in the channel's server.
func slashCommands() []map[string]any {
	str := func(name, desc string, req, auto bool) map[string]any {
		return map[string]any{"type": 3, "name": name, "description": desc, "required": req, "autocomplete": auto}
	}
	board := func(req bool) map[string]any { return str("board", "Board name", req, true) }
	fpp := str("fpp", "FPP name", false, true)
	choices := func(o map[string]any, vals ...string) map[string]any {
		var cs []map[string]string
		for i := 0; i < len(vals); i += 2 {
			cs = append(cs, map[string]string{"name": vals[i+1], "value": vals[i]})
		}
		o["choices"], o["autocomplete"] = cs, false
		return o
	}
	cmd := func(name, desc string, opts ...map[string]any) map[string]any {
		if opts == nil {
			opts = []map[string]any{}
		}
		return map[string]any{"type": 1, "name": name, "description": desc, "options": opts}
	}
	return []map[string]any{
		cmd("status", "Boards: up/down, uptime, OOMs, low memory", board(false)),
		cmd("reboot", "Reboot a board (asks first)", board(true)),
		cmd("fpp", "What each FPP is playing, time left, next show", fpp),
		cmd("playlists", "List FPP playlists", fpp),
		cmd("play", "Start a playlist", str("playlist", "Playlist", true, true), fpp),
		cmd("stop", "Stop the show (asks first)", fpp, choices(str("when", "When to stop", false, false), "now", "Now", "after", "After the current song")),
		cmd("volume", "Read or set an FPP's volume", fpp, map[string]any{"type": 4, "name": "level", "description": "0-100", "min_value": 0, "max_value": 100}),
		cmd("maint", "Maintenance mode for a board", board(true), choices(str("action", "On or off", true, false), "on", "On", "off", "Off"),
			str("duration", "How long, e.g. 30m or 3h (empty: until turned off)", false, false), str("note", "Note", false, false)),
		cmd("ack", "Acknowledge open incidents", board(false), str("note", "Note", false, false)),
		cmd("incidents", "Incidents still alerting"),
		cmd("help", "List the commands"),
	}
}

// registerCommands installs the slash commands in the channel's server.
func (d *DiscordBot) registerCommands(ctx context.Context) {
	cfg := d.config()
	d.mu.Lock()
	app := d.appID
	d.mu.Unlock()
	var ch struct {
		GuildID string `json:"guild_id"`
	}
	if err := d.rest(ctx, http.MethodGet, "/channels/"+cfg.ChannelID, nil, &ch); err != nil {
		d.setSlash(false, "can't read the channel: "+err.Error())
		return
	}
	if ch.GuildID == "" {
		d.setSlash(false, "the channel isn't in a server, so slash commands are off")
		return
	}
	key := app + "/" + ch.GuildID
	d.mu.Lock()
	done := d.slashFor == key
	d.mu.Unlock()
	if done {
		d.setSlash(true, "")
		return
	}
	if err := d.rest(ctx, http.MethodPut, "/applications/"+app+"/guilds/"+ch.GuildID+"/commands", slashCommands(), nil); err != nil {
		d.setSlash(false, "registering slash commands: "+err.Error())
		return
	}
	d.mu.Lock()
	d.slashFor = key
	d.mu.Unlock()
	d.setSlash(true, "")
}

func (d *DiscordBot) setSlash(ok bool, problem string) {
	if problem != "" {
		log.Printf("discord: %s", problem)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.status.SlashCommands = ok
	if problem != "" {
		d.status.Error = problem
	}
}

// rest calls Discord's HTTP API, waiting out one rate limit.
func (d *DiscordBot) rest(ctx context.Context, method, path string, body, out any) error {
	token := d.config().Token
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	for try := 0; ; try++ {
		req, err := http.NewRequestWithContext(ctx, method, d.api+path, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bot "+token)
		req.Header.Set("User-Agent", "DiscordBot (https://github.com/terensapp/baldrick-monitor, "+version+")")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		seg := strings.SplitN(path, "/", 3)[1] // later segments can hold interaction tokens
		resp, err := d.client.Do(req)
		if err != nil {
			var ue *url.Error
			if errors.As(err, &ue) {
				err = ue.Err
			}
			return fmt.Errorf("%s %s: %v", method, seg, err)
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests && try == 0 {
			var rl struct {
				RetryAfter float64 `json:"retry_after"`
			}
			json.Unmarshal(b, &rl)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(min(max(rl.RetryAfter, 0.1), 10) * float64(time.Second))):
			}
			continue
		}
		if resp.StatusCode >= 300 {
			return fmt.Errorf("%s %s: HTTP %d %s", method, seg, resp.StatusCode, truncate(strings.TrimSpace(string(b)), 200))
		}
		if out != nil && len(b) > 0 {
			return json.Unmarshal(b, out)
		}
		return nil
	}
}
