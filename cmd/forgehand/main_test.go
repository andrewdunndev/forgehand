package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-hclog"

	"gitlab.com/dunn.dev/forgehand/internal/config"
	"gitlab.com/dunn.dev/forgehand/internal/github"
)

func TestExecuteUsage(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "forgehand.toml")
	if err := os.WriteFile(cfg, []byte("log_level = \"info\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(t.TempDir(), "level.toml")
	if err := os.WriteFile(bad, []byte("log_level = \"loud\"\nshutdown_timeout = 30\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		args []string
		code int
		want string
	}{
		{"no command", nil, 2, "usage"},
		{"unknown command", []string{"serve"}, 2, "unknown command"},
		{"no config flag", []string{"run"}, 2, "-c is required"},
		{"missing file", []string{"check", "-c", filepath.Join(t.TempDir(), "none.toml")}, 1, "no such file"},
		{"invalid config", []string{"check", "-c", cfg}, 1, "scalesets"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := execute(tc.args, &stdout, &stderr); code != tc.code || !strings.Contains(stderr.String(), tc.want) {
				t.Errorf("code = %d, stderr = %q; want %d containing %q", code, stderr.String(), tc.code, tc.want)
			}
		})
	}
}

func checkDaemon(api github.API, err error) *daemon {
	return newDaemon(testConfig(5), deps{
		hostname: "testhost",
		newAPI:   func(config.ScaleSet) (github.API, error) { return api, err },
	}, hclog.NewNullLogger())
}

func TestCheck(t *testing.T) {
	ctx := context.Background()

	t.Run("new set", func(t *testing.T) {
		api := github.NewFake()
		var out bytes.Buffer
		if err := checkDaemon(api, nil).check(ctx, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "will be created") || len(api.CreatedSets()) != 0 {
			t.Errorf("out = %q, created = %d: check must not create", out.String(), len(api.CreatedSets()))
		}
	})

	t.Run("existing set", func(t *testing.T) {
		api := github.NewFake()
		api.Sets["example"] = &scaleset.RunnerScaleSet{ID: 9, Labels: []scaleset.Label{{Name: "self-hosted"}, {Name: "example"}}}
		var out bytes.Buffer
		if err := checkDaemon(api, nil).check(ctx, &out); err != nil || !strings.Contains(out.String(), "scale set 9 exists") {
			t.Errorf("err = %v, out = %q", err, out.String())
		}
	})

	t.Run("label drift", func(t *testing.T) {
		api := github.NewFake()
		api.Sets["example"] = &scaleset.RunnerScaleSet{ID: 9, Labels: []scaleset.Label{{Name: "example"}}}
		if err := checkDaemon(api, nil).check(ctx, &bytes.Buffer{}); !errors.Is(err, github.ErrLabelDrift) {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("auth failure names the next action", func(t *testing.T) {
		api := github.NewFake()
		api.GetScaleSetErr = errors.New("404 installation not found")
		err := checkDaemon(api, nil).check(ctx, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "App is installed on https://github.com/example/Example") {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("unreadable key", func(t *testing.T) {
		err := checkDaemon(nil, errors.New("read github_app.private_key_path: no such file")).check(ctx, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "private_key_path") {
			t.Errorf("err = %v", err)
		}
	})
}
