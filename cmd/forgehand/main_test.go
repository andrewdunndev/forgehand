package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
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

func TestExecuteHelpAndVersion(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"-h"}, {"--help"}, {"run", "-h"}, {"check", "--help"}} {
		var stdout, stderr bytes.Buffer
		code := execute(args, &stdout, &stderr)
		out := stdout.String()
		if code != 0 || stderr.Len() != 0 || !strings.Contains(out, "forgehand run -c") || !strings.Contains(out, "forgehand check -c") || !strings.Contains(out, "-c is the path") {
			t.Errorf("%v: code = %d, stdout = %q, stderr = %q", args, code, out, stderr.String())
		}
	}

	old := Version
	Version = "v1.2.3"
	t.Cleanup(func() { Version = old })
	for _, args := range [][]string{{"version"}, {"--version"}} {
		var stdout, stderr bytes.Buffer
		if code := execute(args, &stdout, &stderr); code != 0 || stdout.String() != "forgehand v1.2.3\n" {
			t.Errorf("%v: code = %d, stdout = %q", args, code, stdout.String())
		}
	}
}

func checkDaemon(t *testing.T, api github.API, err error, edit ...func(*config.ScaleSet)) *daemon {
	t.Helper()
	dir := t.TempDir()
	appKey := filepath.Join(dir, "app.pem")
	sshKey := filepath.Join(dir, "id")
	for _, f := range []string{appKey, sshKey} {
		if err := os.WriteFile(f, []byte("key"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := testConfig(5)
	cfg.ScaleSets[0].GitHubApp.PrivateKeyPath = appKey
	cfg.ScaleSets[0].Autoscaler.Plugin = "fleeting-plugin-example"
	cfg.ScaleSets[0].Autoscaler.ConnectorConfig.KeyPath = sshKey
	for _, e := range edit {
		e(&cfg.ScaleSets[0])
	}
	return newDaemon(cfg, deps{
		hostname:   "testhost",
		newAPI:     func(config.ScaleSet) (github.API, error) { return api, err },
		lookPlugin: func(p string) (string, error) { return "/plugins/" + p, nil },
	}, hclog.NewNullLogger())
}

func TestCheck(t *testing.T) {
	ctx := context.Background()

	t.Run("new set", func(t *testing.T) {
		api := github.NewFake()
		var out bytes.Buffer
		if err := checkDaemon(t, api, nil).check(ctx, &out); err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		if last := lines[len(lines)-1]; !strings.HasPrefix(last, "ok:") || !strings.Contains(last, "checked when run starts the plugin") {
			t.Errorf("last line = %q, want an ok line saying plugin_config is checked at run", last)
		}
		if !strings.Contains(out.String(), "plugin resolves to /plugins/fleeting-plugin-example") {
			t.Errorf("out = %q, want the resolved plugin path", out.String())
		}
		if !strings.Contains(out.String(), "will be created") || len(api.CreatedSets()) != 0 {
			t.Errorf("out = %q, created = %d: check must not create", out.String(), len(api.CreatedSets()))
		}
	})

	t.Run("existing set", func(t *testing.T) {
		api := github.NewFake()
		api.Sets["example"] = &scaleset.RunnerScaleSet{ID: 9, Labels: []scaleset.Label{{Name: "self-hosted"}, {Name: "example"}}}
		var out bytes.Buffer
		if err := checkDaemon(t, api, nil).check(ctx, &out); err != nil || !strings.Contains(out.String(), "scale set 9 exists") {
			t.Errorf("err = %v, out = %q", err, out.String())
		}
	})

	t.Run("keyless connector_config", func(t *testing.T) {
		noKey := func(s *config.ScaleSet) { s.Autoscaler.ConnectorConfig.KeyPath = "" }
		if err := checkDaemon(t, github.NewFake(), nil, noKey).check(ctx, &bytes.Buffer{}); err != nil {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("label drift", func(t *testing.T) {
		api := github.NewFake()
		api.Sets["example"] = &scaleset.RunnerScaleSet{ID: 9, Labels: []scaleset.Label{{Name: "example"}}}
		if err := checkDaemon(t, api, nil).check(ctx, &bytes.Buffer{}); !errors.Is(err, github.ErrLabelDrift) {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("auth failure names the next action", func(t *testing.T) {
		api := github.NewFake()
		api.GetScaleSetErr = errors.New("404 installation not found")
		err := checkDaemon(t, api, nil).check(ctx, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "App is installed on https://github.com/example/Example") {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("unreadable key", func(t *testing.T) {
		err := checkDaemon(t, nil, errors.New("read github_app.private_key_path: no such file")).check(ctx, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "private_key_path") {
			t.Errorf("err = %v", err)
		}
	})
	for _, tc := range []struct {
		name string
		edit func(*config.ScaleSet)
		want string
	}{
		{"missing app key", func(s *config.ScaleSet) { s.GitHubApp.PrivateKeyPath += ".gone" }, "github_app.private_key_path: "},
		{"missing connector key", func(s *config.ScaleSet) { s.Autoscaler.ConnectorConfig.KeyPath += ".gone" }, "connector_config.key_path: "},
		{"key is a directory", func(s *config.ScaleSet) {
			s.Autoscaler.ConnectorConfig.KeyPath = filepath.Dir(s.Autoscaler.ConnectorConfig.KeyPath)
		}, "is a directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := checkDaemon(t, github.NewFake(), nil, tc.edit).check(ctx, &out)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
			if strings.Contains(out.String(), "ok:") {
				t.Errorf("out = %q: a failed check must not say ok", out.String())
			}
		})
	}

	t.Run("plugin not found names the next action", func(t *testing.T) {
		d := checkDaemon(t, github.NewFake(), nil)
		d.deps.lookPlugin = func(string) (string, error) { return "", errors.New("not found") }
		var out bytes.Buffer
		err := d.check(ctx, &out)
		if err == nil || !strings.Contains(err.Error(), "autoscaler.plugin") || !strings.Contains(err.Error(), "install the plugin") || strings.Contains(out.String(), "ok:") {
			t.Errorf("err = %v, out = %q", err, out.String())
		}
	})
}

func TestResolveVersion(t *testing.T) {
	read := func(v string) func() (*debug.BuildInfo, bool) {
		return func() (*debug.BuildInfo, bool) { return &debug.BuildInfo{Main: debug.Module{Version: v}}, true }
	}
	for _, tt := range []struct {
		name, stamped string
		read          func() (*debug.BuildInfo, bool)
		want          string
	}{
		{"stamped wins", "v9", read("v0.1.0"), "v9"},
		{"module version", "dev", read("v0.1.0"), "v0.1.0"},
		{"devel build", "dev", read("(devel)"), "dev"},
		{"no build info", "dev", func() (*debug.BuildInfo, bool) { return nil, false }, "dev"},
	} {
		if got := resolveVersion(tt.stamped, tt.read); got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestVersionCommandUsesModuleVersion(t *testing.T) {
	old := readBuildInfo
	t.Cleanup(func() { readBuildInfo = old })
	readBuildInfo = func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Main: debug.Module{Version: "v0.1.0"}}, true
	}
	var out, errb bytes.Buffer
	if code := execute([]string{"version"}, &out, &errb); code != 0 || strings.TrimSpace(out.String()) != "forgehand v0.1.0" {
		t.Errorf("code %d, out %q", code, out.String())
	}
}
