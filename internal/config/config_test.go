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
runner_group = "Default"
  [scalesets.github_app]
  client_id = "Iv1.example"
  installation_id = 12345678
  private_key_path = "/run/credentials/app.pem"
  [scalesets.runner]
  dir = "/opt/actions-runner"
  listen_timeout = "3m"
  idle_runner_timeout = "5m"
  [scalesets.autoscaler]
  plugin = "fleeting-plugin-static"
  max_instances = 2
  instance_ready_command = "true"
  instance_acquire_timeout = "15m"
  failure_threshold = 3
  delete_instances_on_shutdown = true
    [scalesets.autoscaler.plugin_config]
    name_prefix = "job-example-"
    lock_file = "/var/lock/ledger"
    admit_command = "/usr/bin/admit"
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
	if s.Runner.ListenTimeout != 3*time.Minute || s.Autoscaler.ConnectorConfig.Timeout != 10*time.Minute {
		t.Errorf("durations not parsed: %+v %+v", s.Runner, s.Autoscaler.ConnectorConfig)
	}
	if s.Autoscaler.NamePrefix() != "job-example-" || s.Autoscaler.MaxInstances != 2 || len(s.Autoscaler.Policy) != 1 {
		t.Errorf("autoscaler = %+v", s.Autoscaler)
	}
}

func TestParseDefaults(t *testing.T) {
	min := strings.NewReplacer(
		"log_level = \"info\"\n", "",
		"shutdown_timeout = 30\n", "",
		"runner_group = \"Default\"\n", "",
		"  listen_timeout = \"3m\"\n", "",
		"  idle_runner_timeout = \"5m\"\n", "",
		"  instance_acquire_timeout = \"15m\"\n", "",
	).Replace(valid)
	cfg, err := Parse([]byte(min))
	if err != nil {
		t.Fatal(err)
	}
	s := cfg.ScaleSets[0]
	if cfg.ShutdownTimeout != 30 || cfg.LogLevel != "info" || s.RunnerGroup != "Default" ||
		s.Runner.ListenTimeout != 3*time.Minute || s.Runner.IdleRunnerTimeout != 5*time.Minute ||
		s.Autoscaler.InstanceAcquireTimeout != 15*time.Minute {
		t.Errorf("defaults not applied: %+v", cfg)
	}
}

func TestParseRejects(t *testing.T) {
	second := `
[[scalesets]]
name = "other"
url = "https://github.com/owner/other"
labels = ["other"]
  [scalesets.github_app]
  client_id = "c"
  installation_id = 1
  private_key_path = "/k"
  [scalesets.runner]
  dir = "/r"
  [scalesets.autoscaler]
  plugin = "p"
  max_instances = 1
    [scalesets.autoscaler.plugin_config]
    name_prefix = "job-example-"
    lock_file = "/l"
    admit_command = "/a"
`
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
		{"unknown key", func(s string) string {
			return strings.Replace(s, "max_instances = 2", "max_instances = 2\n  state_storage = true", 1)
		}, "state_storage: unknown key"},
		{"shutdown_timeout string", func(s string) string {
			return strings.Replace(s, "shutdown_timeout = 30", `shutdown_timeout = "30s"`, 1)
		}, "shutdown_timeout is integer seconds, for example 30; got 30s"},
		{"shutdown_timeout zero", func(s string) string { return strings.Replace(s, "shutdown_timeout = 30", "shutdown_timeout = 0", 1) }, "shutdown_timeout: must be positive"},
		{"no scalesets", func(string) string { return `log_level = "info"` }, "at least one [[scalesets]]"},
		{"no name", func(s string) string { return strings.Replace(s, `name = "example"`, "", 1) }, "name is required"},
		{"bad url", func(s string) string { return strings.Replace(s, "https://github.com/owner/repo", "owner/repo", 1) }, "url must be"},
		{"no labels", func(s string) string { return strings.Replace(s, `labels = ["self-hosted", "example"]`, "", 1) }, "labels is required"},
		{"empty label", func(s string) string { return strings.Replace(s, `"example"]`, `""]`, 1) }, "empty label"},
		{"runner group", func(s string) string {
			return strings.Replace(s, `runner_group = "Default"`, `runner_group = "Ops"`, 1)
		}, "runner_group"},
		{"no client_id", func(s string) string { return strings.Replace(s, `client_id = "Iv1.example"`, "", 1) }, "github_app.client_id"},
		{"no installation_id", func(s string) string { return strings.Replace(s, "installation_id = 12345678", "", 1) }, "github_app.installation_id"},
		{"no private_key_path", func(s string) string {
			return strings.Replace(s, `private_key_path = "/run/credentials/app.pem"`, "", 1)
		}, "github_app.private_key_path"},
		{"no runner dir", func(s string) string { return strings.Replace(s, `dir = "/opt/actions-runner"`, "", 1) }, "runner.dir"},
		{"no plugin", func(s string) string { return strings.Replace(s, `plugin = "fleeting-plugin-static"`, "", 1) }, "autoscaler.plugin is required"},
		{"no max_instances", func(s string) string { return strings.Replace(s, "max_instances = 2", "", 1) }, "max_instances"},
		{"no admit_command", func(s string) string { return strings.Replace(s, `admit_command = "/usr/bin/admit"`, "", 1) }, "plugin_config.admit_command is required"},
		{"empty admit_command", func(s string) string { return strings.Replace(s, `"/usr/bin/admit"`, `""`, 1) }, "plugin_config.admit_command is required"},
		{"no lock_file", func(s string) string { return strings.Replace(s, `lock_file = "/var/lock/ledger"`, "", 1) }, "plugin_config.lock_file is required"},
		{"no name_prefix", func(s string) string { return strings.Replace(s, `name_prefix = "job-example-"`, "", 1) }, "name_prefix is required"},
		{"prefix without dash", func(s string) string { return strings.Replace(s, `"job-example-"`, `"job-example"`, 1) }, `must end with "-"`},
		{"prefix is job-", func(s string) string { return strings.Replace(s, `"job-example-"`, `"job-"`, 1) }, "reserved"},
		{"duplicate prefix", func(s string) string { return s + second }, "already used by scalesets[0]"},
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

func TestDistinctPrefixesPass(t *testing.T) {
	second := strings.NewReplacer(`"example"`, `"other"`, "job-example-", "job-other-").Replace(valid[strings.Index(valid, "[[scalesets]]"):])
	if _, err := Parse([]byte(valid + second)); err != nil {
		t.Fatal(err)
	}
}
