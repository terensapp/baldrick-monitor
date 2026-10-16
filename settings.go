package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"time"
)

// UISettings is everything the dashboard can change. It's saved to
// <data_dir>/settings.json, which overrides the same settings in the
// config file.
type UISettings struct {
	TemperatureUnit string          `json:"temperature_unit"`
	DashboardURL    string          `json:"dashboard_url"`
	Notifications   []Channel       `json:"notifications"`
	Thresholds      Thresholds      `json:"thresholds"`
	Intervals       Intervals       `json:"intervals"`
	DDPProbe        ProbeConfig     `json:"ddp_probe"`
	HistoryDays     int             `json:"history_retention_days"`
	Resolution      Duration        `json:"history_resolution"`
	Incidents       IncidentsConfig `json:"incidents"`
	UpdateInterval  Duration        `json:"update_check_interval"`
	UpdatesDisabled bool            `json:"updates_disabled"`
	QuietHours      QuietHours      `json:"quiet_hours"`
	AdminToken      string          `json:"admin_token,omitempty"` // never sent to the browser
}

func (c *Config) uiSettings() UISettings {
	q := c.QuietHours
	q.Days = slices.Clone(q.Days)
	return UISettings{
		TemperatureUnit: c.TemperatureUnit, DashboardURL: c.DashboardURL,
		Notifications: slices.Clone(c.Notifications), Thresholds: c.Thresholds,
		Intervals: c.Intervals, DDPProbe: c.DDPProbe,
		HistoryDays: c.History.RetentionDays, Resolution: c.History.Resolution,
		Incidents: c.Incidents, UpdateInterval: c.Updates.CheckInterval, UpdatesDisabled: c.Updates.Disabled,
		QuietHours: q, AdminToken: c.AdminToken,
	}
}

func (s UISettings) applyTo(c *Config) {
	c.TemperatureUnit, c.DashboardURL = s.TemperatureUnit, s.DashboardURL
	c.Notifications, c.Thresholds = slices.Clone(s.Notifications), s.Thresholds
	c.Intervals, c.DDPProbe = s.Intervals, s.DDPProbe
	c.History.RetentionDays, c.History.Resolution = s.HistoryDays, s.Resolution
	c.Incidents = s.Incidents
	c.Updates.CheckInterval, c.Updates.Disabled = s.UpdateInterval, s.UpdatesDisabled
	c.AdminToken, c.QuietHours = s.AdminToken, s.QuietHours
	// The legacy alerts block has been folded into Notifications.
	c.Alerts = AlertsConfig{}
}

func (c *Config) settingsPath() string {
	if c.History.DataDir == "" {
		return ""
	}
	return filepath.Join(c.History.DataDir, "settings.json")
}

// loadSettingsFile overlays settings saved from the dashboard.
func loadSettingsFile(c *Config) error {
	p := c.settingsPath()
	if p == "" {
		return nil
	}
	data, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	var s UISettings
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("%s: %w", p, err)
	}
	s.applyTo(c)
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return fmt.Errorf("%s: %w", p, err)
	}
	c.publishLive()
	return nil
}

func (c *Config) saveSettings() error {
	p := c.settingsPath()
	if p == "" {
		return errors.New("settings can only be saved when the monitor has a data_dir")
	}
	data, _ := json.MarshalIndent(c.uiSettings(), "", "  ")
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

// ---- what the browser sees ----

// ChannelView is a channel with its secrets replaced by flags.
type ChannelView struct {
	Channel
	URLSet      bool   `json:"url_set"`
	URLHint     string `json:"url_hint,omitempty"`
	PasswordSet bool   `json:"password_set"`
	TokenSet    bool   `json:"token_set"`
}

type SettingsView struct {
	UISettings
	Notifications []ChannelView  `json:"notifications"`
	AuthRequired  bool           `json:"auth_required"`
	Events        []string       `json:"events"`
	ReadOnly      map[string]any `json:"read_only"`
}

func maskChannel(ch Channel) ChannelView {
	v := ChannelView{Channel: ch, URLSet: ch.URL != "", PasswordSet: ch.Password != "", TokenSet: ch.Token != ""}
	if u, err := url.Parse(ch.URL); err == nil && u.Host != "" {
		v.URLHint = u.Scheme + "://" + u.Host + "/…"
	}
	v.URL, v.Password, v.Token = "", "", ""
	return v
}

func (c *Config) settingsView(configPath string) SettingsView {
	s := c.uiSettings()
	s.AdminToken = ""
	v := SettingsView{UISettings: s, AuthRequired: c.token() != "", Events: AlertEvents}
	for _, ch := range c.Notifications {
		v.Notifications = append(v.Notifications, maskChannel(ch))
	}
	if v.Notifications == nil {
		v.Notifications = []ChannelView{}
	}
	v.ReadOnly = map[string]any{
		"listen": c.Listen, "data_dir": c.History.DataDir, "config_file": configPath,
		"settings_file": c.settingsPath(), "can_save": c.settingsPath() != "",
		"time_zone": time.Now().Format("MST, UTC-07:00"),
	}
	return v
}

// SettingsUpdate is what the dashboard sends back. Blank secrets on an
// existing channel mean "keep the saved value".
type SettingsUpdate struct {
	UISettings
	NewAdminToken   string `json:"new_admin_token"`
	ClearAdminToken bool   `json:"clear_admin_token"`
}

func (c *Config) keepSecrets(ch *Channel) {
	for _, old := range c.Notifications {
		if old.ID != ch.ID || ch.ID == "" {
			continue
		}
		if ch.URL == "" {
			ch.URL = old.URL
		}
		if ch.Password == "" {
			ch.Password = old.Password
		}
		if ch.Token == "" {
			ch.Token = old.Token
		}
	}
}

// withSettings returns a new Config with the update applied, and whether
// board monitoring has to restart to pick it up.
func (c *Config) withSettings(u SettingsUpdate) (*Config, bool, error) {
	s := u.UISettings
	for i := range s.Notifications {
		c.keepSecrets(&s.Notifications[i])
	}
	switch {
	case u.ClearAdminToken:
		s.AdminToken = ""
	case u.NewAdminToken != "":
		if len(u.NewAdminToken) < 8 {
			return nil, false, errors.New("the admin token must be at least 8 characters")
		}
		s.AdminToken = u.NewAdminToken
	default:
		s.AdminToken = c.AdminToken
	}
	n := *c // shares c.live, so publishing reaches running boards
	s.applyTo(&n)
	n.applyDefaults()
	if err := n.validate(); err != nil {
		return nil, false, err
	}
	return &n, !reflect.DeepEqual(restartScope(c), restartScope(&n)), nil
}

// restartScope is the part of the config that running board monitors read
// only when they start.
func restartScope(c *Config) any {
	return []any{c.Intervals, c.DDPProbe, c.History.RetentionDays, c.History.Resolution, c.Incidents}
}

// channelForTest resolves a channel as edited in the dashboard, filling in
// saved secrets, so it can be tested before saving.
func (c *Config) channelForTest(ch Channel) (Channel, error) {
	c.keepSecrets(&ch)
	err := ch.normalize()
	return ch, err
}
