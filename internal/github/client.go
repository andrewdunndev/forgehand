package github

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/actions/scaleset"

	"gitlab.com/dunn.dev/forgehand/internal/config"
)

var _ API = (*client)(nil)

type client struct {
	*scaleset.Client
}

// New returns the API backed by a GitHub App installation. The transport pings
// idle HTTP/2 connections so a dead one fails fast instead of stalling the
// listener's long poll.
func New(cfg config.ScaleSet, version string) (API, error) {
	key, err := os.ReadFile(cfg.GitHubApp.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read github_app.private_key_path: %w", err)
	}

	hc := scaleset.NewHTTPClient(scaleset.HTTPClientConfig{})
	hc.Transport.(*http.Transport).HTTP2 = &http.HTTP2Config{
		SendPingTimeout: 30 * time.Second,
		PingTimeout:     15 * time.Second,
	}

	c, err := scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
		GitHubConfigURL: cfg.URL,
		GitHubAppAuth: scaleset.GitHubAppAuth{
			ClientID:       cfg.GitHubApp.ClientID,
			InstallationID: cfg.GitHubApp.InstallationID,
			PrivateKey:     string(key),
		},
		SystemInfo: scaleset.SystemInfo{System: "forgehand", Version: version, Subsystem: "listener"},
	}, scaleset.WithHTTPClient(hc))
	if err != nil {
		return nil, fmt.Errorf("github client: %w", err)
	}
	return &client{c}, nil
}

func (c *client) MessageSessionClient(ctx context.Context, scaleSetID int, owner string) (Session, error) {
	return c.Client.MessageSessionClient(ctx, scaleSetID, owner)
}

func (c *client) GenerateJitRunnerConfig(ctx context.Context, name string, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	return c.Client.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: name}, scaleSetID)
}
