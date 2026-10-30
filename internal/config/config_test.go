package config

import (
	"strings"
	"testing"
	"time"
)

const valid = `
log_level = "info"
listen_address = "127.0.0.1:9253"
shutdown_timeout = 30

[[scalesets]]
name = "example"
url = "https://github.com/owner/repo"
labels = ["self-hosted", "example"]
  [scalesets.github_app]
  client_id = "Iv1.example"
  installation_id = 12345678
  private_key_path = "/run/credentials/app.pem"
  [scalesets.runner]
  dir = "/opt/actions-runner"
  listen_timeout = "3m"
  idle_timeout = "5m"
  [scalesets.autoscaler]
  plugin = "fleeting-plugin-static"
  max_instances = 2
  instance_ready_command = "true"
  instance_acquire_timeout = "15m"
  failure_threshold = 3
    [scalesets.autoscaler.plugin_config]
    address = "10.0.0.1"
    pool = ["a", "b"]
    [scalesets.autoscaler.connector_config]
    username = "runner"
    key_path = "/run/credentials/guest-ssh"
    timeout = "10m"
  [[scalesets.autoscaler.policy]]
  idle_count = 0
`

func TestParseValid(t *testing.T) {
	cfg, err := Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.ScaleSets[0]
	if cfg.ShutdownDuration() != 30*time.Second {
		t.Errorf("shutdown = %v", cfg.ShutdownDuration())
	}
	if s.Runner.ListenTimeout != Duration(3*time.Minute) || s.Autoscaler.ConnectorConfig.Timeout != Duration(10*time.Minute) {
		t.Errorf("durations not parsed: %+v %+v", s.Runner, s.Autoscaler.ConnectorConfig)
	}
	if s.Autoscaler.MaxInstances != 2 || len(s.Autoscaler.Policy) != 1 {
		t.Errorf("autoscaler = %+v", s.Autoscaler)
	}
}

func TestParseDefaults(t *testing.T) {
	min := strings.NewReplacer(
		"log_level = \"info\"\n", "",
		"shutdown_timeout = 30\n", "",
		"  listen_timeout = \"3m\"\n", "",
		"  idle_timeout = \"5m\"\n", "",
		"  instance_acquire_timeout = \"15m\"\n", "",
	).Replace(valid)
	cfg, err := Parse([]byte(min))
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.ScaleSets[0]
	if cfg.ShutdownTimeout != 30 || cfg.LogLevel != "info" ||
		s.Runner.ListenTimeout != Duration(3*time.Minute) || s.Runner.IdleTimeout != Duration(5*time.Minute) ||
		s.Autoscaler.InstanceAcquireTimeout != Duration(15*time.Minute) {
		t.Errorf("defaults not applied: %+v", cfg)
	}
}

func TestBareIntegerDurationIsRejected(t *testing.T) {
	for _, key := range []string{"listen_timeout", "idle_timeout", "instance_acquire_timeout", "timeout"} {
		old := key + " = \""
		i := strings.Index(valid, old)
		if i < 0 {
			t.Fatalf("%s not in the fixture", key)
		}
		j := i + len(old) + strings.Index(valid[i+len(old):], "\"") + 1
		bad := valid[:i] + key + " = 180" + valid[j:]
		_, err := Parse([]byte(bad))
		if err == nil || !strings.Contains(err.Error(), `use a string such as "3m"`) {
			t.Errorf("%s = 180: err = %v", key, err)
		}
	}
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name string
		edit func(string) string
		want string
	}{
		{"capacity_per_instance", func(s string) string {
			return strings.Replace(s, "max_instances = 2", "max_instances = 2\n  capacity_per_instance = 2", 1)
		}, "scalesets.autoscaler.capacity_per_instance: forgehand runs one JIT runner per VM; remove this key"},
		{"max_use_count", func(s string) string {
			return strings.Replace(s, "max_instances = 2", "max_instances = 2\n  max_use_count = 1", 1)
		}, "scalesets.autoscaler.max_use_count: forgehand runs one JIT runner per VM; remove this key"},
		{"runner_group", func(s string) string {
			return strings.Replace(s, "labels = ", "runner_group = \"Default\"\nlabels = ", 1)
		}, "runner_group: unknown key"},
		{"delete_instances_on_shutdown", func(s string) string {
			return strings.Replace(s, "max_instances = 2", "max_instances = 2\n  delete_instances_on_shutdown = true", 1)
		}, "scalesets.autoscaler.delete_instances_on_shutdown: forgehand always deletes the VMs when it stops; remove this key"},
		{"idle_runner_timeout", func(s string) string {
			return strings.Replace(s, "idle_timeout", "idle_runner_timeout", 1)
		}, "idle_runner_timeout: unknown key"},
		{"unknown key", func(s string) string {
			return strings.Replace(s, "max_instances = 2", "max_instances = 2\n  state_storage = true", 1)
		}, "state_storage: unknown key"},
		{"negative listen_timeout", func(s string) string {
			return strings.Replace(s, `listen_timeout = "3m"`, `listen_timeout = "-3m"`, 1)
		}, "runner timeouts must be positive"},
		{"shutdown_timeout string", func(s string) string {
			return strings.Replace(s, "shutdown_timeout = 30", `shutdown_timeout = "30s"`, 1)
		}, "shutdown_timeout is integer seconds, for example 30; got 30s"},
		{"shutdown_timeout zero", func(s string) string { return strings.Replace(s, "shutdown_timeout = 30", "shutdown_timeout = 0", 1) }, "shutdown_timeout: must be positive"},
		{"no scalesets", func(string) string { return `log_level = "info"` }, "at least one [[scalesets]]"},
		{"no name", func(s string) string { return strings.Replace(s, `name = "example"`, "", 1) }, "name is required"},
		{"static without username", func(s string) string {
			return strings.Replace(s, `username = "runner"`, "use_static_credentials = true", 1)
		}, "use_static_credentials needs connector_config.username"},
		{"no labels", func(s string) string { return strings.Replace(s, `labels = ["self-hosted", "example"]`, "", 1) }, "labels is required"},
		{"empty label", func(s string) string { return strings.Replace(s, `"example"]`, `""]`, 1) }, "empty label"},
		{"no client_id", func(s string) string { return strings.Replace(s, `client_id = "Iv1.example"`, "", 1) }, "github_app.client_id"},
		{"no installation_id", func(s string) string { return strings.Replace(s, "installation_id = 12345678", "", 1) }, "github_app.installation_id"},
		{"no private_key_path", func(s string) string {
			return strings.Replace(s, `private_key_path = "/run/credentials/app.pem"`, "", 1)
		}, "github_app.private_key_path"},
		{"no runner dir", func(s string) string { return strings.Replace(s, `dir = "/opt/actions-runner"`, "", 1) }, "runner.dir"},
		{"no plugin", func(s string) string { return strings.Replace(s, `plugin = "fleeting-plugin-static"`, "", 1) }, "autoscaler.plugin is required"},
		{"no max_instances", func(s string) string { return strings.Replace(s, "max_instances = 2", "", 1) }, "max_instances"},
		{"static without key_path", func(s string) string {
			return strings.Replace(s, `key_path = "/run/credentials/guest-ssh"`, "use_static_credentials = true", 1)
		}, "use_static_credentials needs connector_config.key_path"},
		{"name dot", func(s string) string { return strings.Replace(s, `name = "example"`, `name = "."`, 1) }, "one path segment"},
		{"name dotdot", func(s string) string { return strings.Replace(s, `name = "example"`, `name = ".."`, 1) }, "one path segment"},
		{"name slash", func(s string) string { return strings.Replace(s, `name = "example"`, `name = "a/b"`, 1) }, "one path segment"},
		{"name traversal", func(s string) string { return strings.Replace(s, `name = "example"`, `name = "../x"`, 1) }, "one path segment"},
		{"name backslash", func(s string) string { return strings.Replace(s, `name = "example"`, `name = "a\\b"`, 1) }, "one path segment"},
		{"name newline", func(s string) string { return strings.Replace(s, `name = "example"`, `name = "a\nb"`, 1) }, "one path segment"},
		{"name empty", func(s string) string { return strings.Replace(s, `name = "example"`, `name = ""`, 1) }, "name is required"},
		{"duplicate name", func(s string) string { return s + valid[strings.Index(valid, "[[scalesets]]"):] }, "duplicate scale set name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.edit(valid)))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}

func TestPluginConfigIsOpaque(t *testing.T) {
	cfg, err := Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	pc := cfg.ScaleSets[0].Autoscaler.PluginConfig
	if pc["address"] != "10.0.0.1" || len(pc) != 2 {
		t.Errorf("plugin_config = %v", pc)
	}

	for name, table := range map[string]string{
		"header": "    [scalesets.autoscaler.plugin_config.instances.vm1]\n    address = \"10.0.0.2\"\n",
		"inline": "    instances = { vm1 = { address = \"10.0.0.2\" } }\n",
	} {
		nested := strings.Replace(valid, "    pool = [\"a\", \"b\"]\n", "    pool = [\"a\", \"b\"]\n"+table, 1)
		cfg, err := Parse([]byte(nested))
		if err != nil {
			t.Errorf("%s: nested plugin_config table rejected: %v", name, err)
			continue
		}
		inst, _ := cfg.ScaleSets[0].Autoscaler.PluginConfig["instances"].(map[string]any)
		vm, _ := inst["vm1"].(map[string]any)
		if vm["address"] != "10.0.0.2" {
			t.Errorf("%s: instances = %v", name, cfg.ScaleSets[0].Autoscaler.PluginConfig["instances"])
		}
	}
	if _, err := Parse([]byte(strings.Replace(valid, "  [scalesets.runner]\n", "  [scalesets.runner]\n  extra = { a = 1 }\n", 1))); err == nil {
		t.Error("a nested unknown key outside plugin_config was accepted")
	}

	bare := strings.Replace(valid, "    [scalesets.autoscaler.plugin_config]\n    address = \"10.0.0.1\"\n    pool = [\"a\", \"b\"]\n", "", 1)
	if bare == valid {
		t.Fatal("edit did not apply")
	}
	if _, err := Parse([]byte(bare)); err != nil {
		t.Errorf("a config without plugin_config keys is rejected: %v", err)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	cfg, err := Load("../../examples/forgehand.toml")
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.ScaleSets[0]
	if len(cfg.ScaleSets) != 1 || s.Autoscaler.ConnectorConfig.Protocol != "ssh" || len(s.Autoscaler.Policy) != 1 || cfg.ListenAddress != "127.0.0.1:9253" {
		t.Errorf("example config = %+v", cfg)
	}
}

func TestParseRejectsURLForms(t *testing.T) {
	const want = "url must be https://github.com/<owner>/<repo>, for example https://github.com/owner/repo"
	for _, u := range []string{
		"owner/repo",
		"",
		"http://github.com/owner/repo",
		"https://gitlab.com/owner/repo",
		"https://github.com",
		"https://github.com/",
		"https://github.com/owner",
		"https://github.com/owner/",
		"https://github.com/owner/repo/",
		"https://github.com/owner/repo.git",
		"https://github.com/owner/repo/issues",
		"https://github.com//repo",
		"https://user@github.com/owner/repo",
		"https://user:pass@github.com/owner/repo",
		"https://github.com/owner/repo?tab=readme",
		"https://github.com/owner/repo?",
		"https://github.com/owner/repo#readme",
		"https://github.com:8443/owner/repo",
	} {
		t.Run(u, func(t *testing.T) {
			_, err := Parse([]byte(strings.Replace(valid, "https://github.com/owner/repo", u, 1)))
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("err = %v, want it to contain %q", err, want)
			}
		})
	}
}
