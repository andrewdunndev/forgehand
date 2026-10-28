package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	defaultShutdownTimeout        = 30
	defaultRunnerGroup            = "Default"
	defaultListenTimeout          = Duration(3 * time.Minute)
	defaultIdleRunnerTimeout      = Duration(5 * time.Minute)
	defaultInstanceAcquireTimeout = Duration(15 * time.Minute)
	reservedNamePrefix            = "job-"
)

var rejectedKeys = map[string]bool{
	"capacity_per_instance": true,
	"max_use_count":         true,
}

type Config struct {
	LogLevel        string     `toml:"log_level"`
	ListenAddress   string     `toml:"listen_address"`
	ShutdownTimeout Seconds    `toml:"shutdown_timeout"`
	ScaleSets       []ScaleSet `toml:"scalesets"`
}

type Seconds int

func (s *Seconds) UnmarshalTOML(v any) error {
	n, ok := v.(int64)
	if !ok {
		return fmt.Errorf("shutdown_timeout is integer seconds, for example 30; got %v", v)
	}
	*s = Seconds(n)
	return nil
}

type Duration time.Duration

func (d *Duration) UnmarshalTOML(v any) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("got %v; use a string such as \"3m\"", v)
	}
	parsed, err := time.ParseDuration(str)
	if err != nil {
		return fmt.Errorf("invalid duration %q; use a string such as \"3m\"", str)
	}
	*d = Duration(parsed)
	return nil
}

type ScaleSet struct {
	Name        string     `toml:"name"`
	URL         string     `toml:"url"`
	Labels      []string   `toml:"labels"`
	RunnerGroup string     `toml:"runner_group"`
	GitHubApp   GitHubApp  `toml:"github_app"`
	Runner      Runner     `toml:"runner"`
	Autoscaler  Autoscaler `toml:"autoscaler"`
}

type GitHubApp struct {
	ClientID       string `toml:"client_id"`
	InstallationID int64  `toml:"installation_id"`
	PrivateKeyPath string `toml:"private_key_path"`
}

type Runner struct {
	Dir               string   `toml:"dir"`
	ListenTimeout     Duration `toml:"listen_timeout"`
	IdleRunnerTimeout Duration `toml:"idle_runner_timeout"`
}

type Autoscaler struct {
	Plugin                      string          `toml:"plugin"`
	PluginConfig                map[string]any  `toml:"plugin_config"`
	ConnectorConfig             ConnectorConfig `toml:"connector_config"`
	Policy                      []Policy        `toml:"policy"`
	MaxInstances                int             `toml:"max_instances"`
	InstanceReadyCommand        string          `toml:"instance_ready_command"`
	InstanceAcquireTimeout      Duration        `toml:"instance_acquire_timeout"`
	UpdateInterval              Duration        `toml:"update_interval"`
	UpdateIntervalWhenExpecting Duration        `toml:"update_interval_when_expecting"`
	DeletionRetryInterval       Duration        `toml:"deletion_retry_interval"`
	ShutdownDeletionInterval    Duration        `toml:"shutdown_deletion_interval"`
	ShutdownDeletionRetries     int             `toml:"shutdown_deletion_retries"`
	FailureThreshold            int             `toml:"failure_threshold"`
	ScaleThrottle               ScaleThrottle   `toml:"scale_throttle"`
	DeleteInstancesOnShutdown   bool            `toml:"delete_instances_on_shutdown"`
}

type ConnectorConfig struct {
	OS                   string   `toml:"os"`
	Arch                 string   `toml:"arch"`
	Protocol             string   `toml:"protocol"`
	ProtocolPort         int      `toml:"protocol_port"`
	Username             string   `toml:"username"`
	Password             string   `toml:"password"`
	KeyPath              string   `toml:"key_path"`
	UseStaticCredentials bool     `toml:"use_static_credentials"`
	Keepalive            Duration `toml:"keepalive"`
	Timeout              Duration `toml:"timeout"`
	UseExternalAddr      bool     `toml:"use_external_addr"`
}

type Policy struct {
	Periods          []string `toml:"periods"`
	Timezone         string   `toml:"timezone"`
	IdleCount        int      `toml:"idle_count"`
	IdleTime         Duration `toml:"idle_time"`
	ScaleFactor      float64  `toml:"scale_factor"`
	ScaleFactorLimit int      `toml:"scale_factor_limit"`
	PreemptiveMode   *bool    `toml:"preemptive_mode"`
}

type ScaleThrottle struct {
	Limit int `toml:"limit"`
	Burst int `toml:"burst"`
}

func (c *Config) ShutdownDuration() time.Duration {
	return time.Duration(c.ShutdownTimeout) * time.Second
}

func (p Policy) PreemptiveModeEnabled() bool {
	if p.PreemptiveMode == nil {
		return p.IdleCount > 0
	}
	return *p.PreemptiveMode
}

func (a Autoscaler) NamePrefix() string {
	s, _ := a.PluginConfig["name_prefix"].(string)
	return s
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func Parse(data []byte) (*Config, error) {
	cfg := &Config{
		LogLevel:        "info",
		ShutdownTimeout: defaultShutdownTimeout,
	}

	md, err := toml.Decode(string(data), cfg)
	if err != nil {
		return nil, err
	}
	if err := undecodedError(md.Undecoded()); err != nil {
		return nil, err
	}
	for i := range cfg.ScaleSets {
		cfg.ScaleSets[i].setDefaults()
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (s *ScaleSet) setDefaults() {
	if s.RunnerGroup == "" {
		s.RunnerGroup = defaultRunnerGroup
	}
	if s.Runner.ListenTimeout == 0 {
		s.Runner.ListenTimeout = defaultListenTimeout
	}
	if s.Runner.IdleRunnerTimeout == 0 {
		s.Runner.IdleRunnerTimeout = defaultIdleRunnerTimeout
	}
	if s.Autoscaler.InstanceAcquireTimeout == 0 {
		s.Autoscaler.InstanceAcquireTimeout = defaultInstanceAcquireTimeout
	}
}

func undecodedError(keys []toml.Key) error {
	var errs []error
	for _, k := range keys {
		if rejectedKeys[k[len(k)-1]] {
			errs = append(errs, fmt.Errorf("%s: forgehand runs one JIT runner per VM; remove this key", k))
			continue
		}
		errs = append(errs, fmt.Errorf("%s: unknown key; remove it", k))
	}
	return errors.Join(errs...)
}

func (c *Config) validate() error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if c.ShutdownTimeout <= 0 {
		fail("shutdown_timeout: must be positive integer seconds, for example 30")
	}
	if len(c.ScaleSets) == 0 {
		fail("scalesets: at least one [[scalesets]] entry is required")
	}

	prefixes := map[string]int{}
	for i := range c.ScaleSets {
		s := &c.ScaleSets[i]
		at := fmt.Sprintf("scalesets[%d]", i)
		if s.Name != "" {
			at = fmt.Sprintf("scalesets[%d] %q", i, s.Name)
		}
		s.validate(at, fail)

		prefix := s.Autoscaler.NamePrefix()
		if prefix == "" {
			continue
		}
		if j, ok := prefixes[prefix]; ok {
			fail("%s: plugin_config.name_prefix %q is already used by scalesets[%d]; each entry needs its own prefix", at, prefix, j)
		}
		prefixes[prefix] = i
	}
	return errors.Join(errs...)
}

func (s *ScaleSet) validate(at string, fail func(string, ...any)) {
	if s.Name == "" {
		fail("%s: name is required", at)
	}
	if u, err := url.Parse(s.URL); err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		fail("%s: url must be the repository URL, for example https://github.com/owner/repo", at)
	}
	if len(s.Labels) == 0 {
		fail("%s: labels is required, for example [\"self-hosted\", \"linux\"]", at)
	}
	for _, l := range s.Labels {
		if strings.TrimSpace(l) == "" {
			fail("%s: labels must not contain an empty label", at)
		}
	}
	if !strings.EqualFold(s.RunnerGroup, defaultRunnerGroup) {
		fail("%s: runner_group %q is not supported; use \"Default\"", at, s.RunnerGroup)
	}
	if s.GitHubApp.ClientID == "" {
		fail("%s: github_app.client_id is required", at)
	}
	if s.GitHubApp.InstallationID <= 0 {
		fail("%s: github_app.installation_id is required", at)
	}
	if s.GitHubApp.PrivateKeyPath == "" {
		fail("%s: github_app.private_key_path is required", at)
	}
	if s.Runner.Dir == "" {
		fail("%s: runner.dir is required: the directory of the actions runner in the guest image", at)
	}
	if s.Runner.ListenTimeout < 0 || s.Runner.IdleRunnerTimeout < 0 {
		fail("%s: runner timeouts must be positive durations such as \"3m\"", at)
	}

	a := s.Autoscaler
	if a.Plugin == "" {
		fail("%s: autoscaler.plugin is required: a fleeting plugin path, name or OCI reference", at)
	}
	if a.MaxInstances <= 0 {
		fail("%s: autoscaler.max_instances is required and must be positive", at)
	}
	for _, key := range []string{"admit_command", "lock_file"} {
		if v, _ := a.PluginConfig[key].(string); v == "" {
			fail("%s: autoscaler.plugin_config.%s is required: every VM start passes the host ledger", at, key)
		}
	}
	switch prefix := a.NamePrefix(); {
	case prefix == "":
		fail("%s: autoscaler.plugin_config.name_prefix is required, for example %q", at, "job-"+s.Name+"-")
	case !strings.HasSuffix(prefix, "-"):
		fail("%s: plugin_config.name_prefix %q must end with \"-\"", at, prefix)
	case prefix == reservedNamePrefix:
		fail("%s: plugin_config.name_prefix %q is reserved; add the scale set name, for example %q", at, prefix, "job-"+s.Name+"-")
	}
}
