package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/fleeting/fleeting"
	"gitlab.com/gitlab-org/fleeting/fleeting-artifact/pkg/installer"
	"gitlab.com/gitlab-org/fleeting/fleeting/connector"
	flprometheus "gitlab.com/gitlab-org/fleeting/fleeting/metrics/prometheus"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/taskscaler"
	tsprometheus "gitlab.com/gitlab-org/fleeting/taskscaler/metrics/prometheus"

	"gitlab.com/dunn.dev/forgehand/internal/config"
)

const initTimeout = time.Minute

// Instance is one VM held for one job. Releasing its acquisition key deletes it.
type Instance interface {
	InstanceID() string
	InstanceConnectInfo(ctx context.Context) (provider.ConnectInfo, error)
}

// Fleet hands out single-use VMs. Acquire blocks until one is ready, and
// blocked Acquire calls are the demand the underlying autoscaler grows on.
type Fleet interface {
	Acquire(ctx context.Context, key string) (Instance, error)
	Release(key string)
	Shutdown(ctx context.Context)
}

type Options struct {
	Logger hclog.Logger
	// Name labels the taskscaler and fleeting collectors registered with
	// Metrics, when Metrics is set.
	Name    string
	Metrics prometheus.Registerer
	// OnLeftover receives the id of an instance from an earlier daemon run
	// that was rejected and is being deleted, so its GitHub runner can be
	// removed. It runs on its own goroutine.
	OnLeftover func(id string)
}

type fleetImpl struct {
	ts     taskscaler.Taskscaler
	cancel context.CancelFunc
	kill   func()
}

// LookPath resolves a plugin path, name or OCI reference to its binary.
func LookPath(plugin string) (string, error) {
	return installer.LookPath(plugin, "")
}

// Start runs the plugin process named by cfg.Plugin and builds a Fleet on it.
func Start(cfg config.Autoscaler, opts Options) (Fleet, error) {
	pluginCfg, err := pluginConfig(cfg)
	if err != nil {
		return nil, err
	}
	path, err := LookPath(cfg.Plugin)
	if err != nil {
		return nil, fmt.Errorf("loading fleeting plugin: %w", err)
	}
	var pluginOpts []fleeting.PluginOption
	if opts.Logger != nil {
		pluginOpts = append(pluginOpts, fleeting.WithPluginLogger(opts.Logger.Named("fleeting-plugin")))
	}
	runner, err := fleeting.RunPlugin(path, pluginCfg, pluginOpts...)
	if err != nil {
		return nil, fmt.Errorf("running fleeting plugin: %w", err)
	}
	f, err := build(runner.InstanceGroup(), cfg, opts, runner.Kill)
	if err != nil {
		runner.Kill()
		return nil, err
	}
	return f, nil
}

// New builds a Fleet on an already-running instance group. extra options are
// applied after the mapped ones.
func New(group provider.InstanceGroup, cfg config.Autoscaler, opts Options, extra ...taskscaler.Option) (Fleet, error) {
	return build(group, cfg, opts, func() {}, extra...)
}

func build(group provider.InstanceGroup, cfg config.Autoscaler, opts Options, kill func(), extra ...taskscaler.Option) (*fleetImpl, error) {
	settings, err := settings(cfg)
	if err != nil {
		return nil, err
	}
	group = removeOnHeartbeatFailure{InstanceGroup: group, log: opts.Logger}
	ctx, cancel := context.WithCancel(context.Background())
	tsOpts := append(mapped(cfg, settings, upFunc(ctx, cfg, opts.OnLeftover)), extra...)
	if opts.Logger != nil {
		tsOpts = append(tsOpts, taskscaler.WithLogger(opts.Logger.Named("taskscaler")))
	}
	if opts.Metrics != nil {
		labels := prometheus.Labels{"scaleset": opts.Name}
		tsMetrics := tsprometheus.New(tsprometheus.WithConstLabels(labels))
		flMetrics := flprometheus.New(flprometheus.WithConstLabels(labels))
		if err := errors.Join(opts.Metrics.Register(tsMetrics), opts.Metrics.Register(flMetrics)); err != nil {
			cancel()
			return nil, fmt.Errorf("registering metrics: %w", err)
		}
		tsOpts = append(tsOpts, taskscaler.WithMetricsCollector(tsMetrics), taskscaler.WithFleetingMetricsCollector(flMetrics))
	}

	initCtx, initCancel := context.WithTimeout(ctx, initTimeout)
	defer initCancel()
	ts, err := taskscaler.New(initCtx, group, tsOpts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating taskscaler: %w", err)
	}

	if err := ts.ConfigureSchedule(schedules(cfg.Policy)...); err != nil {
		ts.Shutdown(ctx)
		cancel()
		return nil, fmt.Errorf("configuring taskscaler schedules: %w", err)
	}
	return &fleetImpl{ts: ts, cancel: cancel, kill: kill}, nil
}

func (f *fleetImpl) Acquire(ctx context.Context, key string) (Instance, error) {
	acq, err := f.ts.Acquire(ctx, key)
	if err != nil {
		return nil, err
	}
	return acq, nil
}

func (f *fleetImpl) Release(key string) { f.ts.Release(key) }

// Shutdown stops the autoscaler, which deletes the instances, then kills the
// plugin process.
func (f *fleetImpl) Shutdown(ctx context.Context) {
	f.cancel()
	f.ts.Shutdown(ctx)
	f.kill()
}

// mapped returns the taskscaler options derived from cfg. Capacity per
// instance and max use count are fixed at 1 so releasing an instance always
// deletes it, and instances are always deleted at shutdown; zero values
// elsewhere pass through for taskscaler to default.
func mapped(cfg config.Autoscaler, s provider.Settings, up taskscaler.UpFunc) []taskscaler.Option {
	opts := []taskscaler.Option{
		taskscaler.WithCapacityPerInstance(1),
		taskscaler.WithMaxUseCount(1),
		taskscaler.WithMaxInstances(cfg.MaxInstances),
		taskscaler.WithInstanceGroupSettings(s),
		taskscaler.WithInstanceUpFunc(up),
		taskscaler.WithHeartbeatFunc(heartbeat),
		taskscaler.WithUpdateInterval(time.Duration(cfg.UpdateInterval)),
		taskscaler.WithUpdateIntervalWhenExpecting(time.Duration(cfg.UpdateIntervalWhenExpecting)),
		taskscaler.WithDeletionRetryInterval(time.Duration(cfg.DeletionRetryInterval)),
		taskscaler.WithShutdownDeletionInterval(time.Duration(cfg.ShutdownDeletionInterval)),
		taskscaler.WithShutdownDeletionRetries(cfg.ShutdownDeletionRetries),
		taskscaler.WithFailureThreshold(cfg.FailureThreshold),
		taskscaler.WithScaleThrottle(cfg.ScaleThrottle.Limit, cfg.ScaleThrottle.Burst),
		taskscaler.WithDeleteInstancesOnShutdown(),
	}
	return opts
}

func schedules(policy []config.Policy) []taskscaler.Schedule {
	var out []taskscaler.Schedule
	for _, p := range policy {
		out = append(out, taskscaler.Schedule{
			Periods:          p.Periods,
			Timezone:         p.Timezone,
			IdleCount:        p.IdleCount,
			IdleTime:         time.Duration(p.IdleTime),
			ScaleFactor:      p.ScaleFactor,
			ScaleFactorLimit: p.ScaleFactorLimit,
			PreemptiveMode:   p.PreemptiveModeEnabled(),
		})
	}
	return out
}

// pluginConfig is exactly what the operator wrote under plugin_config.
func pluginConfig(cfg config.Autoscaler) ([]byte, error) {
	m := cfg.PluginConfig
	if m == nil {
		m = map[string]any{}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshaling plugin_config: %w", err)
	}
	return b, nil
}

func settings(cfg config.Autoscaler) (provider.Settings, error) {
	c := cfg.ConnectorConfig
	cc := provider.ConnectorConfig{
		OS:                   c.OS,
		Arch:                 c.Arch,
		Protocol:             provider.Protocol(c.Protocol),
		ProtocolPort:         c.ProtocolPort,
		Username:             c.Username,
		Password:             c.Password,
		UseStaticCredentials: c.UseStaticCredentials,
		Keepalive:            time.Duration(c.Keepalive),
		Timeout:              time.Duration(c.Timeout),
	}
	if c.KeyPath != "" {
		key, err := os.ReadFile(c.KeyPath)
		if err != nil {
			return provider.Settings{}, fmt.Errorf("reading connector_config.key_path: %w", err)
		}
		cc.Key = key
	}
	return provider.Settings{ConnectorConfig: cc}, nil
}

// upFunc runs when an instance is first running and before it can be acquired.
// Only an instance this run requested is usable: one that existed at start or
// appeared out of band carries an unknown runner and job, so it is rejected
// and deleted.
func upFunc(ctx context.Context, cfg config.Autoscaler, onLeftover func(string)) taskscaler.UpFunc {
	return func(_ taskscaler.Taskscaler, in taskscaler.UpFuncInstance) error {
		if in.Cause == fleeting.CausePreexisted || in.Cause == fleeting.CauseUnexpected || len(in.Acquisitions) > 0 {
			if onLeftover != nil {
				go onLeftover(in.ID)
			}
			return fmt.Errorf("instance %s is %s: removing it for safety", in.ID, in.Cause)
		}
		if cfg.InstanceReadyCommand == "" {
			return nil
		}
		err := connector.Run(ctx, in.Info, connector.ConnectorOptions{
			RunOptions:  connector.RunOptions{Command: cfg.InstanceReadyCommand},
			DialOptions: connector.DialOptions{UseExternalAddr: cfg.ConnectorConfig.UseExternalAddr},
		})
		if err != nil {
			return fmt.Errorf("instance_ready_command: %w", err)
		}
		return nil
	}
}
