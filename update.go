package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Set at build time with -ldflags "-X main.version=... -X main.revision=...".
var (
	version   = "dev"
	revision  = ""
	buildDate = ""
)

type UpdatesConfig struct {
	// Image reference the host updater pulls, e.g.
	// ghcr.io/terensapp/baldrick-monitor:main. The BALDRICK_IMAGE environment
	// variable (set by the Quadlet) overrides it.
	Image         string   `json:"image"`
	CheckInterval Duration `json:"check_interval"`
	Disabled      bool     `json:"disabled"`
}

const defaultImage = "ghcr.io/terensapp/baldrick-monitor:latest"

// Release describes one build of the monitor.
type Release struct {
	Version  string `json:"version"`
	Revision string `json:"revision"`
	Created  string `json:"created,omitempty"`
}

// UpdaterStatus is what the host-side updater reports in
// <data_dir>/update/status.json.
type UpdaterStatus struct {
	State   string `json:"state"` // running, done, failed
	Message string `json:"message"`
	Time    string `json:"time"`
	Image   string `json:"image"`
}

type UpdateInfo struct {
	Image           string         `json:"image"`
	Current         Release        `json:"current"`
	Available       *Release       `json:"available"`
	UpdateAvailable bool           `json:"update_available"`
	CheckedAt       time.Time      `json:"checked_at"`
	CheckError      string         `json:"check_error,omitempty"`
	Installed       bool           `json:"updater_installed"`
	Requested       *time.Time     `json:"requested_at,omitempty"`
	Status          *UpdaterStatus `json:"status,omitempty"`
	Disabled        bool           `json:"disabled"`
	RepoURL         string         `json:"repo_url,omitempty"`
}

type Updater struct {
	cfg    UpdatesConfig
	image  string
	dir    string // <data_dir>/update; "" = no host updater possible
	client *http.Client
	check  chan struct{}

	mu        sync.Mutex
	available *Release
	checkedAt time.Time
	checkErr  string
}

func NewUpdater(cfg *Config) *Updater {
	u := &Updater{cfg: cfg.Updates, image: cfg.Updates.Image, client: &http.Client{Timeout: 15 * time.Second}, check: make(chan struct{}, 1)}
	if env := os.Getenv("BALDRICK_IMAGE"); env != "" {
		u.image = env
	}
	if u.image == "" {
		u.image = defaultImage
	}
	if cfg.History.DataDir != "" {
		u.dir = filepath.Join(cfg.History.DataDir, "update")
	}
	return u
}

func (u *Updater) Run(ctx context.Context) {
	if u.cfg.Disabled {
		return
	}
	if !sleepCtx(ctx, 20*time.Second) {
		return
	}
	t := time.NewTicker(u.cfg.CheckInterval.Duration)
	defer t.Stop()
	for {
		u.refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-u.check:
		}
	}
}

// CheckNow asks for an immediate registry check and waits briefly for it.
func (u *Updater) CheckNow(ctx context.Context) {
	if u.cfg.Disabled {
		return
	}
	u.refresh(ctx)
}

func (u *Updater) refresh(ctx context.Context) {
	rel, err := latestFromRegistry(ctx, u.client, u.image)
	u.mu.Lock()
	defer u.mu.Unlock()
	u.checkedAt = time.Now()
	if err != nil {
		u.checkErr = err.Error()
		log.Printf("update check: %v", err)
		return
	}
	u.checkErr = ""
	if u.available == nil || u.available.Revision != rel.Revision {
		if rel.Revision != revision {
			log.Printf("update available: %s (%s)", rel.Version, short(rel.Revision))
		}
	}
	u.available = rel
}

func (u *Updater) Info() UpdateInfo {
	u.mu.Lock()
	info := UpdateInfo{
		Image: u.image, Current: Release{Version: version, Revision: revision, Created: buildDate},
		Available: u.available, CheckedAt: u.checkedAt, CheckError: u.checkErr, Disabled: u.cfg.Disabled,
	}
	u.mu.Unlock()
	if strings.HasPrefix(u.image, "ghcr.io/") {
		repo := strings.SplitN(strings.TrimPrefix(u.image, "ghcr.io/"), ":", 2)[0]
		info.RepoURL = "https://github.com/" + repo
	}
	if a := info.Available; a != nil && a.Revision != "" && a.Revision != revision {
		info.UpdateAvailable = true
	}
	if u.dir != "" {
		if _, err := os.Stat(filepath.Join(u.dir, "installed")); err == nil {
			info.Installed = true
		}
		if st, err := os.Stat(filepath.Join(u.dir, "request")); err == nil {
			t := st.ModTime()
			info.Requested = &t
		}
		if data, err := os.ReadFile(filepath.Join(u.dir, "status.json")); err == nil {
			var s UpdaterStatus
			if json.Unmarshal(data, &s) == nil {
				info.Status = &s
			}
		}
	}
	return info
}

var errNoUpdater = errors.New("the host updater isn't installed (see deploy/install.sh)")

// Request asks the host updater to pull and restart. The request carries no
// data: the host decides what image to pull.
func (u *Updater) Request() error {
	if u.dir == "" {
		return errors.New("updates need a data_dir shared with the host")
	}
	if _, err := os.Stat(filepath.Join(u.dir, "installed")); err != nil {
		return errNoUpdater
	}
	return os.WriteFile(filepath.Join(u.dir, "request"), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
}

func short(rev string) string {
	if len(rev) > 7 {
		return rev[:7]
	}
	return rev
}

// ---- registry ----

// latestFromRegistry reads the version and revision labels of the image an
// OCI registry currently serves for ref (e.g. ghcr.io/owner/name:tag), for
// this machine's architecture.
func latestFromRegistry(ctx context.Context, c *http.Client, ref string) (*Release, error) {
	host, repo, tag, err := parseRef(ref)
	if err != nil {
		return nil, err
	}
	token, err := registryToken(ctx, c, host, repo)
	if err != nil {
		return nil, fmt.Errorf("registry auth: %w", err)
	}
	get := func(path, accept string, v any) error {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+host+"/v2/"+repo+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		resp, err := c.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
			return fmt.Errorf("%s: HTTP %d %s", path, resp.StatusCode, strings.TrimSpace(string(b)))
		}
		return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(v)
	}
	const accept = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, " +
		"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"
	var m manifest
	if err := get("/manifests/"+tag, accept, &m); err != nil {
		return nil, err
	}
	if len(m.Manifests) > 0 { // multi-arch index
		digest := pickPlatform(m.Manifests, "linux", runtime.GOARCH)
		if digest == "" {
			return nil, fmt.Errorf("no linux/%s image under tag %q", runtime.GOARCH, tag)
		}
		m = manifest{}
		if err := get("/manifests/"+digest, accept, &m); err != nil {
			return nil, err
		}
	}
	if m.Config.Digest == "" {
		return nil, errors.New("manifest has no config")
	}
	var cfg struct {
		Created string `json:"created"`
		Config  struct {
			Labels map[string]string `json:"Labels"`
		} `json:"config"`
	}
	if err := get("/blobs/"+m.Config.Digest, "", &cfg); err != nil {
		return nil, err
	}
	l := cfg.Config.Labels
	rel := &Release{Version: l["org.opencontainers.image.version"], Revision: l["org.opencontainers.image.revision"], Created: cfg.Created}
	if rel.Revision == "" {
		return nil, errors.New("image has no org.opencontainers.image.revision label")
	}
	return rel, nil
}

type indexEntry struct {
	Digest   string `json:"digest"`
	Platform struct {
		OS           string `json:"os"`
		Architecture string `json:"architecture"`
	} `json:"platform"`
}

type manifest struct {
	Manifests []indexEntry `json:"manifests"`
	Config    struct {
		Digest string `json:"digest"`
	} `json:"config"`
}

func pickPlatform(ms []indexEntry, os, arch string) string {
	for _, m := range ms {
		if m.Platform.OS == os && m.Platform.Architecture == arch {
			return m.Digest
		}
	}
	return ""
}

func parseRef(ref string) (host, repo, tag string, err error) {
	host, rest, ok := strings.Cut(ref, "/")
	if !ok || !strings.Contains(host, ".") {
		return "", "", "", fmt.Errorf("image %q should look like registry.example/owner/name:tag", ref)
	}
	repo, tag = rest, "latest"
	if i := strings.LastIndex(rest, ":"); i > strings.LastIndex(rest, "/") {
		repo, tag = rest[:i], rest[i+1:]
	}
	return host, repo, tag, nil
}

// registryToken gets an anonymous pull token (public images).
func registryToken(ctx context.Context, c *http.Client, host, repo string) (string, error) {
	u := fmt.Sprintf("https://%s/token?service=%s&scope=%s", host, url.QueryEscape(host), url.QueryEscape("repository:"+repo+":pull"))
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d (is the package public?)", resp.StatusCode)
	}
	var t struct {
		Token string `json:"token"`
	}
	err = json.NewDecoder(resp.Body).Decode(&t)
	return t.Token, err
}
