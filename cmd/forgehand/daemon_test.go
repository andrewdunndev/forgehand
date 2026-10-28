package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/plugin/fleeting-plugin-dummy"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/taskscaler"

	"gitlab.com/dunn.dev/forgehand/internal/config"
	"gitlab.com/dunn.dev/forgehand/internal/fleet"
	"gitlab.com/dunn.dev/forgehand/internal/github"
	"gitlab.com/dunn.dev/forgehand/internal/scaler"
)

func testConfig(shutdown int) *config.Config {
	return &config.Config{
		ShutdownTimeout: config.Seconds(shutdown),
		ScaleSets: []config.ScaleSet{{
			Name:   "example",
			URL:    "https://github.com/example/Example",
			Labels: []string{"self-hosted", "example"},
			Runner: config.Runner{ListenTimeout: config.Duration(time.Minute), IdleRunnerTimeout: config.Duration(time.Minute)},
			Autoscaler: config.Autoscaler{
				MaxInstances:           2,
				InstanceAcquireTimeout: config.Duration(time.Minute),
			},
		}},
	}
}

// guest is a scaler.Runner the test drives by hand.
type guest struct {
	api *github.Fake
	mu  sync.Mutex
	run map[string]*guestRun
}

type guestRun struct {
	hooks scaler.Hooks
	exit  chan int
}

func (g *guest) Run(ctx context.Context, _ fleet.Instance, name string, h scaler.Hooks) (int, error) {
	if _, err := g.api.GenerateJitRunnerConfig(ctx, name, 1); err != nil {
		return 0, err
	}
	r := &guestRun{hooks: h, exit: make(chan int, 1)}
	g.mu.Lock()
	g.run[name] = r
	g.mu.Unlock()
	select {
	case code := <-r.exit:
		return code, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (g *guest) Diag(context.Context, fleet.Instance) string { return "" }

func (g *guest) get(name string) *guestRun {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.run[name]
}

// stuckFleet is a fleet whose Shutdown ignores its context.
type stuckFleet struct {
	*fleet.Fake
	release chan struct{}
}

func (f *stuckFleet) Shutdown(context.Context) { <-f.release }

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type rig struct {
	t     *testing.T
	d     *daemon
	api   *github.Fake
	fl    *fleet.Fake
	guest *guest
	sigs  chan os.Signal
	done  chan error
	out   *syncBuf

	mu      sync.Mutex
	started int
	opts    fleet.Options
}

func newRig(t *testing.T, cfg *config.Config) *rig {
	t.Helper()
	api := github.NewFake()
	r := &rig{
		t:     t,
		api:   api,
		fl:    fleet.NewFake(fleet.Options{}),
		guest: &guest{api: api, run: map[string]*guestRun{}},
		sigs:  make(chan os.Signal, 4),
		done:  make(chan error, 1),
		out:   &syncBuf{},
	}
	log := hclog.New(&hclog.LoggerOptions{Output: r.out, Level: hclog.Debug})
	r.d = newDaemon(cfg, deps{
		hostname: "testhost",
		newAPI:   func(config.ScaleSet) (github.API, error) { return api, nil },
		startFleet: func(_ config.Autoscaler, o fleet.Options) (fleet.Fleet, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.started++
			r.opts = o
			return r.fl, nil
		},
		newRunner: func(config.ScaleSet, github.API, int, hclog.Logger) scaler.Runner { return r.guest },
	}, log)
	r.d.reconnectMin, r.d.reconnectMax = time.Millisecond, 4*time.Millisecond
	r.d.sweepInterval = time.Millisecond
	return r
}

func (r *rig) fleetOptions() (int, fleet.Options) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started, r.opts
}

func (r *rig) start() {
	go func() { r.done <- r.d.run(context.Background(), r.sigs) }()
}

func (r *rig) wait(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s\nlog:\n%s", what, r.out.String())
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *rig) result(within time.Duration) (error, bool) {
	select {
	case err := <-r.done:
		return err, true
	case <-time.After(within):
		return nil, false
	}
}

func (r *rig) sessions() int {
	return len(r.api.OwnerList())
}

// boot grants the oldest blocked Acquire an instance called name and returns
// the slot's key with its runner.
func (r *rig) boot(name string) (string, *guestRun) {
	r.t.Helper()
	r.wait("a blocked Acquire", func() bool { return len(r.fl.Waiting()) > 0 })
	key := r.fl.Waiting()[0]
	if !r.fl.Grant(key, &fleet.FakeInstance{ID: name}) {
		r.t.Fatal("grant")
	}
	r.wait("runner "+name, func() bool { return r.guest.get(name) != nil })
	return key, r.guest.get(name)
}

func assigned(n int) *scaleset.RunnerScaleSetStatistic {
	return &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: n}
}

func TestExistingSetIsNotRecreatedAndStopDeletesNothing(t *testing.T) {
	r := newRig(t, testConfig(5))
	r.api.Sets["example"] = &scaleset.RunnerScaleSet{ID: 9, Name: "example", Labels: []scaleset.Label{{Name: "example"}, {Name: "self-hosted"}}}
	r.start()
	r.wait("the session", func() bool { return r.sessions() > 0 })

	r.sigs <- syscall.SIGTERM
	if err, ok := r.result(3 * time.Second); !ok || err != nil {
		t.Fatalf("run = %v (returned %v)", err, ok)
	}
	if n := len(r.api.CreatedSets()); n != 0 {
		t.Errorf("created %d scale sets over an existing one", n)
	}
	if owners := r.api.OwnerList(); owners[0] != "testhost-example" {
		t.Errorf("owners = %v, want hostname-name", owners)
	}
	if !r.api.Session.IsClosed() || !r.fl.IsShutdown() {
		t.Errorf("session closed = %v, fleet shut down = %v", r.api.Session.IsClosed(), r.fl.IsShutdown())
	}
}

func TestMissingSetIsCreatedWithLabelsAndUpdatesOff(t *testing.T) {
	r := newRig(t, testConfig(5))
	r.start()
	r.wait("the session", func() bool { return r.sessions() > 0 })
	r.sigs <- syscall.SIGTERM
	_, _ = r.result(3 * time.Second)

	created := r.api.CreatedSets()
	if len(created) != 1 {
		t.Fatalf("created %d scale sets", len(created))
	}
	if c := created[0]; len(c.Labels) != 2 || !c.RunnerSetting.DisableUpdate || c.RunnerGroupID != 1 {
		t.Errorf("created %+v", c)
	}
}

func TestLabelDriftRefusesToStartNamingTheRename(t *testing.T) {
	r := newRig(t, testConfig(5))
	r.api.Sets["example"] = &scaleset.RunnerScaleSet{ID: 9, Name: "example", Labels: []scaleset.Label{{Name: "example"}}}
	r.start()
	err, ok := r.result(3 * time.Second)
	if !ok || !errors.Is(err, github.ErrLabelDrift) || !strings.Contains(err.Error(), "rename the scale set") {
		t.Fatalf("run = %v (returned %v)", err, ok)
	}
	started, _ := r.fleetOptions()
	if started != 0 || len(r.api.CreatedSets()) != 0 || r.sessions() != 0 {
		t.Errorf("fleet started %d, created %d, sessions %d: drift must stop before anything starts", started, len(r.api.CreatedSets()), r.sessions())
	}
}

func TestSIGQUITRemovesIdleRunnerAtOnceAndWaitsForBusy(t *testing.T) {
	r := newRig(t, testConfig(5))
	r.api.Session.Info.Statistics = assigned(2)
	r.start()

	idle, a := r.boot("vm-a")
	_, b := r.boot("vm-b")
	a.hooks.Listening()
	b.hooks.Listening()
	b.hooks.RunningJob()

	r.sigs <- syscall.SIGQUIT
	r.wait("the idle runner released", func() bool { return len(r.fl.Released()) == 1 })
	if released := r.fl.Released(); released[0] != idle {
		t.Errorf("released %v, want only the idle %s", released, idle)
	}
	if err, ok := r.result(200 * time.Millisecond); ok {
		t.Fatalf("run returned (%v) while a job was running", err)
	}

	r.api.Session.Push(&scaleset.RunnerScaleSetMessage{
		MessageID:            1,
		JobCompletedMessages: []*scaleset.JobCompleted{{RunnerName: "vm-b", Result: "succeeded"}},
	})
	b.exit <- 0
	if err, ok := r.result(3 * time.Second); !ok || err != nil {
		t.Fatalf("run = %v (returned %v)", err, ok)
	}
	if !r.fl.IsShutdown() {
		t.Error("fleet not shut down after the drain")
	}
}

func TestSIGTERMStopsInsideShutdownTimeoutAndKillsJobs(t *testing.T) {
	r := newRig(t, testConfig(1))
	stuck := &stuckFleet{Fake: r.fl, release: make(chan struct{})}
	t.Cleanup(func() { close(stuck.release) })
	r.d.deps.startFleet = func(config.Autoscaler, fleet.Options) (fleet.Fleet, error) { return stuck, nil }
	r.api.Session.Info.Statistics = assigned(1)
	r.start()
	_, a := r.boot("vm-a")
	a.hooks.Listening()

	began := time.Now()
	r.sigs <- syscall.SIGTERM
	err, ok := r.result(5 * time.Second)
	if !ok || err == nil || !strings.Contains(err.Error(), "shutdown_timeout") {
		t.Fatalf("run = %v (returned %v)", err, ok)
	}
	if took := time.Since(began); took > 3*time.Second {
		t.Errorf("stop took %s with shutdown_timeout 1", took)
	}
	if released := r.fl.Released(); len(released) != 1 {
		t.Errorf("released %v, want the running slot", released)
	}
}

// hangingAPI blocks scale set lookup until its context is cancelled.
type hangingAPI struct{ *github.Fake }

func (hangingAPI) GetRunnerScaleSet(ctx context.Context, _ int, _ string) (*scaleset.RunnerScaleSet, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSIGTERMDuringStartupStopsInsideShutdownTimeout(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		r := newRig(t, testConfig(1))
		r.d.deps.newAPI = func(config.ScaleSet) (github.API, error) { return hangingAPI{r.api}, nil }
		r.start()
		time.Sleep(50 * time.Millisecond)

		began := time.Now()
		r.sigs <- sig
		err, ok := r.result(5 * time.Second)
		if !ok || err != nil {
			t.Fatalf("%v: run = %v (returned %v)\nlog:\n%s", sig, err, ok, r.out.String())
		}
		if took := time.Since(began); took > 2*time.Second {
			t.Errorf("%v: startup stop took %s with shutdown_timeout 1", sig, took)
		}
		if n, _ := r.fleetOptions(); n != 0 {
			t.Errorf("%v: fleet started %d times after the signal", sig, n)
		}
	}
}

func TestListenerReconnectsWithTheSameOwner(t *testing.T) {
	r := newRig(t, testConfig(5))
	r.api.Session.Errs = []error{errors.New("session evicted")}
	r.start()
	r.wait("a second session", func() bool { return r.sessions() >= 2 })
	if owners := r.api.OwnerList(); owners[0] != owners[1] {
		t.Errorf("owners = %v", owners)
	}
	r.sigs <- syscall.SIGTERM
	_, _ = r.result(3 * time.Second)
}

func TestLeftoverRunnerIsRemovedOnceItStopsReportingAJob(t *testing.T) {
	r := newRig(t, testConfig(5))
	r.start()
	r.wait("the fleet", func() bool { n, _ := r.fleetOptions(); return n == 1 })
	_, opts := r.fleetOptions()

	jit, _ := r.api.GenerateJitRunnerConfig(context.Background(), "old-vm", 1)
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	go opts.OnLeftover("old-vm")
	time.Sleep(20 * time.Millisecond)
	if n := len(r.api.RemovedRunners()); n != 0 {
		t.Fatal("removed a runner that still reports a job")
	}
	r.api.SetRemoveErr(nil)
	r.wait("the runner removed", func() bool {
		ref, _ := r.api.GetRunnerByName(context.Background(), "old-vm")
		return ref == nil
	})
	if got := r.api.RemovedRunners(); len(got) != 1 || got[0] != int64(jit.Runner.ID) {
		t.Errorf("removed %v", got)
	}
	r.sigs <- syscall.SIGTERM
	_, _ = r.result(3 * time.Second)
}

func TestPreexistingAndUnexpectedInstancesAreDeletedWithTheirRunners(t *testing.T) {
	r := newRig(t, testConfig(5))
	group := &dummy.InstanceGroup{MaxSize: 4}
	old := group.AddInstance(provider.StateRunning)
	_, _ = r.api.GenerateJitRunnerConfig(context.Background(), old, 1)
	r.d.deps.startFleet = func(_ config.Autoscaler, o fleet.Options) (fleet.Fleet, error) {
		return fleet.New(group, config.Autoscaler{
			MaxInstances:                2,
			UpdateInterval:              config.Duration(30 * time.Millisecond),
			UpdateIntervalWhenExpecting: config.Duration(10 * time.Millisecond),
			DeletionRetryInterval:       config.Duration(30 * time.Millisecond),
			ShutdownDeletionInterval:    config.Duration(10 * time.Millisecond),
			DeleteInstancesOnShutdown:   true,
		}, o, taskscaler.WithAcquireDelay(5*time.Millisecond))
	}
	r.start()
	gone := func(id string) func() bool {
		return func() bool {
			s := stateOf(group, id)
			ref, _ := r.api.GetRunnerByName(context.Background(), id)
			return (s == provider.StateDeleting || s == provider.StateDeleted) && ref == nil
		}
	}
	r.wait("the pre-existing instance and its runner gone", gone(old))

	stray := group.AddInstance(provider.StateRunning)
	_, _ = r.api.GenerateJitRunnerConfig(context.Background(), stray, 1)
	r.wait("the unexpected instance and its runner gone", gone(stray))

	r.sigs <- syscall.SIGTERM
	_, _ = r.result(3 * time.Second)
}

func stateOf(g *dummy.InstanceGroup, id string) provider.State {
	for _, i := range g.List() {
		if i.ID == id {
			return i.State
		}
	}
	return provider.StateDeleted
}

func TestMetricsEndpointServesTheFleetCollectors(t *testing.T) {
	cfg := testConfig(5)
	cfg.ListenAddress = "127.0.0.1:0"
	d := newDaemon(cfg, deps{}, hclog.NewNullLogger())
	reg, stop, err := d.serveMetrics()
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	group := &dummy.InstanceGroup{MaxSize: 2}
	fl, err := fleet.New(group, config.Autoscaler{
		MaxInstances:                2,
		UpdateInterval:              config.Duration(30 * time.Millisecond),
		UpdateIntervalWhenExpecting: config.Duration(10 * time.Millisecond),
		DeletionRetryInterval:       config.Duration(30 * time.Millisecond),
		ShutdownDeletionInterval:    config.Duration(10 * time.Millisecond),
		DeleteInstancesOnShutdown:   true,
	}, fleet.Options{Name: "example", Metrics: reg}, taskscaler.WithAcquireDelay(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer func() {
		fl.Release("a")
		fl.Shutdown(ctx)
	}()
	if _, err := fl.Acquire(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get("http://" + d.metricsAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"taskscaler_", `scaleset="example"`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics lack %q:\n%.400s", want, body)
		}
	}
}

func TestMetricsAddressInUseIsAnError(t *testing.T) {
	cfg := testConfig(5)
	cfg.ListenAddress = "127.0.0.1:0"
	first := newDaemon(cfg, deps{}, hclog.NewNullLogger())
	_, stop, err := first.serveMetrics()
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	cfg.ListenAddress = first.metricsAddr
	if _, _, err := newDaemon(cfg, deps{}, hclog.NewNullLogger()).serveMetrics(); err == nil || !strings.Contains(err.Error(), "listen_address") {
		t.Errorf("err = %v", err)
	}
}

func TestFiveRunnersExitingBeforeTheMarkerEndRunNamingTheCause(t *testing.T) {
	r := newRig(t, testConfig(1))
	var mu sync.Mutex
	var clock time.Time
	r.d.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		clock = clock.Add(time.Hour)
		return clock
	}
	r.api.Session.Info.Statistics = assigned(1)
	r.start()

	for i := 0; i < 5; i++ {
		_, g := r.boot(fmt.Sprintf("vm-%d", i))
		g.exit <- 1
	}
	err, ok := r.result(5 * time.Second)
	if !ok || err == nil {
		t.Fatalf("run = %v (returned %v), want the breaker error", err, ok)
	}
	if !strings.Contains(err.Error(), "5 consecutive slots failed") || !strings.Contains(err.Error(), `scaleset "example"`) {
		t.Fatalf("error does not name the cause: %v", err)
	}
}
