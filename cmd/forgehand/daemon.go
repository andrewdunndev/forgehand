package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
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
	startFleet func(config.Autoscaler, fleet.Options) (fleet.Fleet, error)
	newRunner  func(cfg config.ScaleSet, api github.API, scaleSetID int, log hclog.Logger) scaler.Runner
}

func realDeps() deps {
	host, _ := os.Hostname()
	return deps{
		hostname:   host,
		newAPI:     func(c config.ScaleSet) (github.API, error) { return github.New(c, Version) },
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
	u.fleet, err = d.deps.startFleet(cfg.Autoscaler, fleet.Options{
		Logger:     log,
		Name:       cfg.Name,
		Metrics:    metrics,
		OnLeftover: func(id string) { sw.remove(ctx, id) },
	})
	if err != nil {
		return nil, err
	}
	u.scaler = scaler.New(scaler.Config{
		MaxInstances:           cfg.Autoscaler.MaxInstances,
		ListenTimeout:          time.Duration(cfg.Runner.ListenTimeout),
		IdleRunnerTimeout:      time.Duration(cfg.Runner.IdleRunnerTimeout),
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

	select {
	case err = <-fatal:
		d.log.Error("stopping", "error", err)
	case sig := <-sigs:
		d.log.Info("signal received", "signal", sig)
		if sig == syscall.SIGQUIT {
			err = d.drain(units, sigs, fatal)
		}
	}
	if stopErr := d.stop(units, cancel); err == nil {
		err = stopErr
	}
	return err
}

// starting runs start while watching for a signal. A signal cancels the start
// and waits up to shutdown_timeout for it to return.
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
	select {
	case r := <-res:
		return r.u, nil, r.err
	case sig := <-sigs:
		cancel()
		select {
		case r := <-res:
			return r.u, sig, nil
		case <-time.After(d.cfg.ShutdownDuration()):
			return nil, sig, nil
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
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	return reg, func() { _ = srv.Close() }, nil
}

// drain returns once every scale set has no runner left, or sooner when a
// forceful signal or a tripped breaker asks for the exit.
func (d *daemon) drain(units []*unit, sigs <-chan os.Signal, fatal <-chan error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for _, u := range units {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := u.scaler.Drain(ctx); err != nil {
					u.log.Warn("draining", "error", err)
				}
			}()
		}
		wg.Wait()
	}()
	for {
		select {
		case <-done:
			return nil
		case err := <-fatal:
			return err
		case sig := <-sigs:
			if sig != syscall.SIGQUIT {
				d.log.Warn("forceful signal during drain", "signal", sig)
				return nil
			}
		}
	}
}

// stop cancels every slot, shuts the fleets down and waits for the listeners,
// all within shutdown_timeout.
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
				if err := u.scaler.Stop(ctx); err != nil {
					u.log.Warn("stopping the scaler", "error", err)
				}
				u.fleet.Shutdown(ctx)
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

// check validates what can be proven without starting a VM: the App
// authenticates and each scale set is absent or matches its labels.
func (d *daemon) check(ctx context.Context, out io.Writer) error {
	var errs []error
	for _, s := range d.cfg.ScaleSets {
		if err := d.checkOne(ctx, s, out); err != nil {
			errs = append(errs, fmt.Errorf("scaleset %q: %w", s.Name, err))
		}
	}
	return errors.Join(errs...)
}

func (d *daemon) checkOne(ctx context.Context, cfg config.ScaleSet, out io.Writer) error {
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
