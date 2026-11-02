package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gitlab.com/dunn.dev/forgehand/internal/config"
	"gitlab.com/dunn.dev/forgehand/internal/fleet"
	"gitlab.com/dunn.dev/forgehand/internal/github"
	"gitlab.com/dunn.dev/forgehand/internal/runner"
	"gitlab.com/dunn.dev/forgehand/internal/scaler"
)

const (
	reconnectMin  = 5 * time.Second
	reconnectMax  = 60 * time.Second
	closeTimeout  = 10 * time.Second
	sweepInterval = 10 * time.Second
	sweepAttempts = 30
)

// deps are the outside world the daemon is built on.
type deps struct {
	hostname   string
	newAPI     func(config.ScaleSet) (github.API, error)
	lookPlugin func(plugin string) (string, error)
	startFleet func(config.Autoscaler, fleet.Options) (fleet.Fleet, error)
	newRunner  func(cfg config.ScaleSet, api github.API, scaleSetID int, log hclog.Logger) scaler.Runner
}

func realDeps() deps {
	host, _ := os.Hostname()
	return deps{
		hostname:   host,
		newAPI:     func(c config.ScaleSet) (github.API, error) { return github.New(c, version()) },
		lookPlugin: fleet.LookPath,
		startFleet: fleet.Start,
		newRunner: func(c config.ScaleSet, api github.API, id int, log hclog.Logger) scaler.Runner {
			return runner.New(c.Runner, c.Autoscaler.ConnectorConfig.UseExternalAddr, api, id, log.Named("runner"))
		},
	}
}

type daemon struct {
	cfg  *config.Config
	deps deps
	log  hclog.Logger

	reconnectMin, reconnectMax time.Duration
	sweepInterval              time.Duration
	sweepAttempts              int
	metricsAddr                string
	metricsSrv                 *http.Server
	now                        func() time.Time
}

func newDaemon(cfg *config.Config, d deps, log hclog.Logger) *daemon {
	return &daemon{
		cfg: cfg, deps: d, log: log,
		reconnectMin: reconnectMin, reconnectMax: reconnectMax,
		sweepInterval: sweepInterval, sweepAttempts: sweepAttempts,
	}
}

// sessions hands the scaler whichever message session is current.
type sessions struct {
	mu  sync.Mutex
	cur github.Session
}

func (s *sessions) set(cur github.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur = cur
}

func (s *sessions) AcquireJobs(ctx context.Context, ids []int64) ([]int64, error) {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	if cur == nil {
		return nil, errors.New("no message session")
	}
	return cur.AcquireJobs(ctx, ids)
}

// unit is one scale set: its GitHub session, scaler and fleet.
type unit struct {
	name     string
	owner    string
	cfg      config.ScaleSet
	api      github.API
	set      *scaleset.RunnerScaleSet
	sessions *sessions
	fleet    fleet.Fleet
	scaler   *scaler.Scaler
	log      hclog.Logger
	d        *daemon
	listened chan struct{}
}

func (d *daemon) start(ctx context.Context, cfg config.ScaleSet, metrics prometheus.Registerer) (*unit, error) {
	log := d.log.With("scaleset", cfg.Name)
	api, err := d.deps.newAPI(cfg)
	if err != nil {
		return nil, err
	}
	set, err := github.EnsureScaleSet(ctx, api, cfg.Name, cfg.Labels)
	if err != nil {
		return nil, err
	}

	u := &unit{
		name:     cfg.Name,
		owner:    d.deps.hostname + "-" + cfg.Name,
		cfg:      cfg,
		api:      api,
		set:      set,
		sessions: &sessions{},
		log:      log,
		d:        d,
		listened: make(chan struct{}),
	}
	sw := sweeper{reg: api, log: log, interval: d.sweepInterval, attempts: d.sweepAttempts}
	opts := fleet.Options{
		Logger:     log,
		Name:       cfg.Name,
		Metrics:    metrics,
		OnLeftover: func(id string) { sw.remove(ctx, id) },
	}
	if cfg.Autoscaler.ConnectorConfig.UseStaticCredentials {
		opts.StateDir = filepath.Join(os.Getenv("STATE_DIRECTORY"), cfg.Name)
	}
	u.fleet, err = d.deps.startFleet(cfg.Autoscaler, opts)
	if err != nil {
		return nil, err
	}
	u.scaler = scaler.New(scaler.Config{
		MaxInstances:           cfg.Autoscaler.MaxInstances,
		ListenTimeout:          time.Duration(cfg.Runner.ListenTimeout),
		IdleTimeout:            time.Duration(cfg.Runner.IdleTimeout),
		InstanceAcquireTimeout: time.Duration(cfg.Autoscaler.InstanceAcquireTimeout),
		Logger:                 log,
		Now:                    d.now,
	}, u.sessions, api, u.fleet, d.deps.newRunner(cfg, api, set.ID, log))
	return u, nil
}

// listen keeps a message session open, recreating it with a doubling delay
// whenever the listener stops.
func (u *unit) listen(ctx context.Context) {
	defer close(u.listened)
	delay := u.d.reconnectMin
	for ctx.Err() == nil {
		began := time.Now()
		err := u.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(began) > u.d.reconnectMax {
			delay = u.d.reconnectMin
		}
		u.log.Warn("listener stopped, reconnecting", "error", err, "in", delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, u.d.reconnectMax)
	}
}

func (u *unit) listenOnce(ctx context.Context) error {
	sess, err := u.api.MessageSessionClient(ctx, u.set.ID, u.owner)
	if err != nil {
		return fmt.Errorf("creating the message session: %w", err)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancel()
		_ = sess.Close(cctx)
	}()
	u.sessions.set(sess)

	l, err := listener.New(sess, listener.Config{ScaleSetID: u.set.ID, MaxRunners: u.cfg.Autoscaler.MaxInstances})
	if err != nil {
		return err
	}
	return l.Run(ctx, u.scaler)
}

// run serves every scale set until a signal arrives or a scale set's failure
// breaker trips. SIGQUIT drains: idle runners go, running jobs finish. SIGTERM
// and SIGINT stop within shutdown_timeout, running jobs included.
func (d *daemon) run(ctx context.Context, sigs <-chan os.Signal) error {
	if os.Getenv("STATE_DIRECTORY") == "" {
		for _, s := range d.cfg.ScaleSets {
			if s.Autoscaler.ConnectorConfig.UseStaticCredentials {
				return fmt.Errorf("scaleset %q: STATE_DIRECTORY is not set: static guest credentials keep restart state there; run under systemd with StateDirectory=forgehand", s.Name)
			}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	metrics, closeMetrics, err := d.serveMetrics()
	if err != nil {
		return err
	}
	defer closeMetrics()

	var units []*unit
	fatal := make(chan error, len(d.cfg.ScaleSets))
	for _, s := range d.cfg.ScaleSets {
		u, interrupted, err := d.starting(ctx, s, metrics, sigs, cancel)
		if interrupted != nil {
			d.log.Info("signal received during startup", "signal", interrupted)
			if u != nil {
				units = append(units, u)
			}
			return d.stop(units, cancel)
		}
		if err != nil {
			if stopErr := d.stop(units, cancel); stopErr != nil {
				d.log.Warn("stopping after a failed start", "error", stopErr)
			}
			return fmt.Errorf("scaleset %q: %w", s.Name, err)
		}
		units = append(units, u)
		go u.listen(ctx)
		go func() {
			if err := u.scaler.Run(ctx); err != nil {
				fatal <- fmt.Errorf("scaleset %q: %w", u.name, err)
			}
		}()
	}

	handoff := false
wait:
	for {
		select {
		case err = <-fatal:
			d.log.Error("stopping", "error", err)
			break wait
		case sig := <-sigs:
			d.log.Info("signal received", "signal", sig)
			switch sig {
			case syscall.SIGQUIT:
				_, err = d.drain(units, sigs, fatal, false)
				break wait
			case syscall.SIGUSR1:
				if !d.canHandOff() {
					d.log.Error("ignoring SIGUSR1: handing off needs use_static_credentials on every scale set")
					continue
				}
				var forced bool
				forced, err = d.drain(units, sigs, fatal, true)
				handoff = !forced
				break wait
			default:
				break wait
			}
		}
	}
	if handoff {
		d.release(units, cancel)
		return err
	}
	if stopErr := d.stop(units, cancel); err == nil {
		err = stopErr
	}
	return err
}

// starting runs start while watching for a signal. A signal cancels the start
// and waits up to shutdown_timeout for it to return. A start that returns
// later has its fleet shut down.
func (d *daemon) starting(ctx context.Context, cfg config.ScaleSet, metrics prometheus.Registerer, sigs <-chan os.Signal, cancel context.CancelFunc) (*unit, os.Signal, error) {
	type result struct {
		u   *unit
		err error
	}
	res := make(chan result, 1)
	go func() {
		u, err := d.start(ctx, cfg, metrics)
		res <- result{u, err}
	}()
	for {
		select {
		case r := <-res:
			return r.u, nil, r.err
		case sig := <-sigs:
			if sig == syscall.SIGUSR1 {
				d.log.Error("ignoring SIGUSR1 during startup; send it again once every scale set has started")
				continue
			}
			cancel()
			select {
			case r := <-res:
				return r.u, sig, nil
			case <-time.After(d.cfg.ShutdownDuration()):
				go func() {
					if late := <-res; late.u != nil {
						ctx, stop := context.WithTimeout(context.Background(), d.cfg.ShutdownDuration())
						defer stop()
						late.u.fleet.Shutdown(ctx)
					}
				}()
				return nil, sig, nil
			}
		}
	}
}

// serveMetrics serves the taskscaler and fleeting collectors on listen_address
// and returns the registry they register with. With the key unset it returns
// a nil registry.
func (d *daemon) serveMetrics() (prometheus.Registerer, func(), error) {
	if d.cfg.ListenAddress == "" {
		return nil, func() {}, nil
	}
	ln, err := net.Listen("tcp", d.cfg.ListenAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("listen_address: %w", err)
	}
	d.metricsAddr = ln.Addr().String()
	reg := prometheus.NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	d.metricsSrv = srv
	go func() { _ = srv.Serve(ln) }()
	return reg, func() { _ = srv.Close() }, nil
}

// canHandOff reports whether every scale set keeps restart state, so a new
// process can adopt the VMs this one leaves.
func (d *daemon) canHandOff() bool {
	for _, s := range d.cfg.ScaleSets {
		if !s.Autoscaler.ConnectorConfig.UseStaticCredentials {
			return false
		}
	}
	return true
}

// drain returns once every scale set has no runner left, or with a handoff
// none but busy ones. A tripped breaker ends it with its error, and SIGTERM or
// SIGINT with forced set. SIGQUIT and SIGUSR1 are logged and ignored.
func (d *daemon) drain(units []*unit, sigs <-chan os.Signal, fatal <-chan error, handoff bool) (forced bool, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wait, mode := (*scaler.Scaler).Drain, "drain"
	if handoff {
		wait, mode = (*scaler.Scaler).Handoff, "handoff"
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for _, u := range units {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := wait(u.scaler, ctx); err != nil {
					u.log.Warn("draining", "error", err)
				}
			}()
		}
		wg.Wait()
	}()
	for {
		select {
		case <-done:
			return false, nil
		case err := <-fatal:
			d.log.Error("stopping", "error", err)
			return false, err
		case sig := <-sigs:
			if sig == syscall.SIGTERM || sig == syscall.SIGINT {
				d.log.Warn("forceful signal during drain", "signal", sig)
				return true, nil
			}
			d.log.Info(mode + " in progress; ignoring " + sigName(sig))
		}
	}
}

// stop cancels every slot, shuts the fleets down alongside and waits for the
// listeners, all within shutdown_timeout.
func (d *daemon) stop(units []*unit, stopListening context.CancelFunc) error {
	stopListening()
	timeout := d.cfg.ShutdownDuration()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for _, u := range units {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Slots blocked in Acquire wait on instances still being
				// prepared, and only the fleet's shutdown ends a preparation.
				shut := make(chan struct{})
				go func() {
					defer close(shut)
					u.fleet.Shutdown(ctx)
				}()
				if err := u.scaler.Stop(ctx); err != nil {
					u.log.Warn("stopping the scaler", "error", err)
				}
				<-shut
				select {
				case <-u.listened:
				case <-ctx.Done():
				}
			}()
		}
		wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown did not finish within shutdown_timeout (%s)", timeout)
	}
}

// check validates what can be proven without starting a VM: the plugin and
// key files resolve, the App authenticates and each scale set is absent or
// matches its labels. It ends with an ok line only when everything passed.
func (d *daemon) check(ctx context.Context, out io.Writer) error {
	var errs []error
	for _, s := range d.cfg.ScaleSets {
		if err := d.checkOne(ctx, s, out); err != nil {
			errs = append(errs, fmt.Errorf("scaleset %q: %w", s.Name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	_, _ = fmt.Fprintln(out, "ok: the configuration is valid; plugin_config is checked when run starts the plugin")
	return nil
}

func (d *daemon) checkLocal(cfg config.ScaleSet, out io.Writer) error {
	var errs []error
	path, err := d.deps.lookPlugin(cfg.Autoscaler.Plugin)
	if err != nil {
		errs = append(errs, fmt.Errorf("autoscaler.plugin %q: %w: install the plugin on this host or set the full path to its binary", cfg.Autoscaler.Plugin, err))
	} else {
		_, _ = fmt.Fprintf(out, "%s: plugin resolves to %s\n", cfg.Name, path)
	}
	for _, f := range []struct{ key, path, next string }{
		{"github_app.private_key_path", cfg.GitHubApp.PrivateKeyPath, "download the App's private key to that path"},
		{"autoscaler.connector_config.key_path", cfg.Autoscaler.ConnectorConfig.KeyPath, "point it at the SSH private key the guest image trusts"},
	} {
		if f.path == "" {
			continue
		}
		if info, err := os.Stat(f.path); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w: %s", f.key, err, f.next))
		} else if info.IsDir() {
			errs = append(errs, fmt.Errorf("%s: %s is a directory: %s", f.key, f.path, f.next))
		}
	}
	return errors.Join(errs...)
}

func (d *daemon) checkOne(ctx context.Context, cfg config.ScaleSet, out io.Writer) error {
	if err := d.checkLocal(cfg, out); err != nil {
		return err
	}
	api, err := d.deps.newAPI(cfg)
	if err != nil {
		return err
	}
	set, err := github.LookupScaleSet(ctx, api, cfg.Name, cfg.Labels)
	switch {
	case errors.Is(err, github.ErrLabelDrift):
		return err
	case err != nil:
		return fmt.Errorf("%w: check client_id, installation_id and the private key, and that the App is installed on %s with Administration read and write", err, cfg.URL)
	case set == nil:
		_, _ = fmt.Fprintf(out, "%s: authenticated; the scale set will be created at the first run\n", cfg.Name)
	default:
		_, _ = fmt.Fprintf(out, "%s: authenticated; scale set %d exists with the configured labels\n", cfg.Name, set.ID)
	}
	return nil
}

// sigName names a signal as the README and the unit do.
func sigName(sig os.Signal) string {
	switch sig {
	case syscall.SIGQUIT:
		return "SIGQUIT"
	case syscall.SIGUSR1:
		return "SIGUSR1"
	}
	return sig.String()
}

// release ends a handoff: the listeners stop and the plugins finish deleting
// the VMs of the removed idle runners, then the process may exit with the
// fleets untouched, so busy VMs and their runners keep running for the next
// process to adopt. A fleet that is slow to delete is given up on at
// shutdown_timeout.
func (d *daemon) release(units []*unit, stopListening context.CancelFunc) {
	stopListening()
	ctx, cancel := context.WithTimeout(context.Background(), d.cfg.ShutdownDuration())
	defer cancel()
	for _, u := range units {
		select {
		case <-u.listened:
		case <-ctx.Done():
			d.log.Warn("leaving with listeners still running", "error", ctx.Err())
			return
		}
	}
	for _, u := range units {
		if err := u.fleet.WaitDeleted(ctx); err != nil {
			u.log.Warn("leaving with VMs still being deleted", "error", err)
			return
		}
	}
}
