package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/fleeting/fleeting"
	"gitlab.com/gitlab-org/fleeting/fleeting/plugin/fleeting-plugin-dummy"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/taskscaler"

	"gitlab.com/dunn.dev/forgehand/internal/config"
)

func testConfig() config.Autoscaler {
	return config.Autoscaler{
		MaxInstances:                4,
		UpdateInterval:              config.Duration(30 * time.Millisecond),
		UpdateIntervalWhenExpecting: config.Duration(10 * time.Millisecond),
		DeletionRetryInterval:       config.Duration(30 * time.Millisecond),
		ShutdownDeletionInterval:    config.Duration(10 * time.Millisecond),
		DeleteInstancesOnShutdown:   true,
	}
}

func newFleet(t *testing.T, group *dummy.InstanceGroup, opts Options) Fleet {
	t.Helper()
	f, err := New(group, testConfig(), opts, taskscaler.WithAcquireDelay(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		f.Shutdown(ctx)
	})
	return f
}

func acquire(t *testing.T, f Fleet, key string) Instance {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inst, err := f.Acquire(ctx, key)
	if err != nil {
		t.Fatalf("acquire %s: %v", key, err)
	}
	return inst
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func states(g *dummy.InstanceGroup) map[string]provider.State {
	out := map[string]provider.State{}
	for _, i := range g.List() {
		out[i.ID] = i.State
	}
	return out
}

func TestInstancesAreSingleUse(t *testing.T) {
	group := &dummy.InstanceGroup{MaxSize: 4}
	f := newFleet(t, group, Options{})

	a := acquire(t, f, "a")
	b := acquire(t, f, "b")
	if a.InstanceID() == b.InstanceID() {
		t.Fatalf("capacity per instance is not 1: both acquisitions hold %s", a.InstanceID())
	}

	f.Release("a")
	eventually(t, "released instance deleted", func() bool {
		s := states(group)[a.InstanceID()]
		return s == provider.StateDeleting || s == provider.StateDeleted
	})
	if s := states(group)[b.InstanceID()]; s != provider.StateRunning {
		t.Errorf("unreleased instance state = %v", s)
	}
}

func TestCapacityPerInstanceIsOne(t *testing.T) {
	reg := prometheus.NewRegistry()
	newFleet(t, &dummy.InstanceGroup{MaxSize: 4}, Options{Name: "set", Metrics: reg})

	gauge := func() (float64, bool) {
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range families {
			if strings.HasSuffix(f.GetName(), "max_tasks_per_instance") && len(f.GetMetric()) == 1 {
				return f.GetMetric()[0].GetGauge().GetValue(), true
			}
		}
		return 0, false
	}
	eventually(t, "the capacity gauge", func() bool { _, ok := gauge(); return ok })
	if v, _ := gauge(); v != 1 {
		t.Errorf("max_tasks_per_instance = %v, want 1", v)
	}
}

func TestAcquireFollowsDemand(t *testing.T) {
	group := &dummy.InstanceGroup{MaxSize: 4}
	f := newFleet(t, group, Options{})

	var wg sync.WaitGroup
	for _, k := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			acquire(t, f, k)
		}()
	}
	wg.Wait()
	if n := len(group.List()); n != 3 {
		t.Errorf("3 blocked acquires made %d instances", n)
	}
}

func TestPreexistingInstanceIsRejectedAndReported(t *testing.T) {
	group := &dummy.InstanceGroup{MaxSize: 4}
	old := group.AddInstance(provider.StateRunning)

	left := make(chan string, 4)
	f := newFleet(t, group, Options{OnLeftover: func(id string) { left <- id }})
	_ = f

	select {
	case id := <-left:
		if id != old {
			t.Errorf("leftover = %s, want %s", id, old)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pre-existing instance not reported")
	}
	eventually(t, "pre-existing instance deleted", func() bool {
		s := states(group)[old]
		return s == provider.StateDeleting || s == provider.StateDeleted
	})
}

func TestUnexpectedInstanceIsRejectedAndReported(t *testing.T) {
	group := &dummy.InstanceGroup{MaxSize: 4}
	left := make(chan string, 4)
	newFleet(t, group, Options{OnLeftover: func(id string) { left <- id }})

	stray := group.AddInstance(provider.StateRunning)
	select {
	case id := <-left:
		if id != stray {
			t.Errorf("leftover = %s, want %s", id, stray)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unexpected instance not reported")
	}
}

func TestUpFunc(t *testing.T) {
	tests := []struct {
		name     string
		in       taskscaler.UpFuncInstance
		reject   bool
		leftover bool
	}{
		{"requested", taskscaler.UpFuncInstance{ID: "i", Cause: fleeting.CauseRequested}, false, false},
		{"preexisted", taskscaler.UpFuncInstance{ID: "i", Cause: fleeting.CausePreexisted}, true, true},
		{"unexpected", taskscaler.UpFuncInstance{ID: "i", Cause: fleeting.CauseUnexpected}, true, true},
		{"live acquisitions", taskscaler.UpFuncInstance{ID: "i", Cause: fleeting.CauseRequested, Acquisitions: []string{"k"}}, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := make(chan string, 1)
			up := upFunc(context.Background(), config.Autoscaler{}, func(id string) { got <- id })
			err := up(nil, tt.in)
			if (err != nil) != tt.reject {
				t.Fatalf("err = %v, reject = %v", err, tt.reject)
			}
			select {
			case id := <-got:
				if !tt.leftover || id != "i" {
					t.Errorf("leftover reported for %q", id)
				}
			case <-time.After(100 * time.Millisecond):
				if tt.leftover {
					t.Error("leftover not reported")
				}
			}
		})
	}
}

func TestPluginConfigIsPassedThroughUnchanged(t *testing.T) {
	decode := func(cfg config.Autoscaler) map[string]any {
		t.Helper()
		b, err := pluginConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	if got := decode(config.Autoscaler{}); len(got) != 0 {
		t.Errorf("empty plugin_config became %v", got)
	}
	got := decode(config.Autoscaler{PluginConfig: map[string]any{"address": "10.0.0.1", "owner": "someone"}})
	if len(got) != 2 || got["address"] != "10.0.0.1" || got["owner"] != "someone" {
		t.Errorf("plugin config = %v, want the operator's keys only", got)
	}
}

func TestFakeGrantAndCancel(t *testing.T) {
	f := NewFake(Options{})
	ctx, cancel := context.WithCancel(context.Background())

	type result struct {
		inst Instance
		err  error
	}
	res := make(chan result, 2)
	go func() { i, err := f.Acquire(context.Background(), "a"); res <- result{i, err} }()
	go func() { i, err := f.Acquire(ctx, "b"); res <- result{i, err} }()
	eventually(t, "two blocked acquires", func() bool { return len(f.Waiting()) == 2 })

	cancel()
	r := <-res
	if r.err == nil {
		t.Fatal("cancelled acquire returned an instance")
	}
	if got := f.Waiting(); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("waiting = %v", got)
	}
	if !f.Grant("a", &FakeInstance{ID: "vm1"}) {
		t.Fatal("no acquire waiting for a")
	}
	if r := <-res; r.err != nil || r.inst.InstanceID() != "vm1" {
		t.Fatalf("granted acquire = %+v", r)
	}
	f.Release("a")
	if got := f.Released(); !slices.Equal(got, []string{"a"}) {
		t.Errorf("released = %v", got)
	}
}

// heartbeatGroup fails Heartbeat for the first instance it is asked about.
type heartbeatGroup struct {
	provider.InstanceGroup
	mu  sync.Mutex
	bad string
}

func (g *heartbeatGroup) Heartbeat(ctx context.Context, id string) error {
	g.mu.Lock()
	if g.bad == "" {
		g.bad = id
	}
	bad := g.bad
	g.mu.Unlock()
	if id == bad {
		return errors.New("guest unhealthy")
	}
	return g.InstanceGroup.Heartbeat(ctx, id)
}

func (g *heartbeatGroup) badID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.bad
}

func TestFailedHeartbeatInstanceIsNeverAcquired(t *testing.T) {
	dg := &dummy.InstanceGroup{MaxSize: 4}
	group := &heartbeatGroup{InstanceGroup: dg}
	cfg := testConfig()
	f, err := New(group, cfg, Options{}, taskscaler.WithAcquireDelay(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		f.Shutdown(ctx)
	})

	got := acquire(t, f, "a")
	bad := group.badID()
	if bad == "" || got.InstanceID() == bad {
		t.Fatalf("acquired %q, heartbeat-failing instance is %q", got.InstanceID(), bad)
	}
	eventually(t, "unhealthy instance removed", func() bool {
		s := states(dg)[bad]
		return s == provider.StateDeleting || s == provider.StateDeleted
	})
}

func TestSettingsKeyIsOptional(t *testing.T) {
	var cfg config.Autoscaler
	cfg.ConnectorConfig.Username = "runner"
	got, err := settings(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != nil || got.UseStaticCredentials {
		t.Errorf("keyless settings = %+v", got.ConnectorConfig)
	}

	cfg.ConnectorConfig.UseStaticCredentials = true
	cfg.ConnectorConfig.KeyPath = filepath.Join(t.TempDir(), "id")
	if err := os.WriteFile(cfg.ConnectorConfig.KeyPath, []byte("pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = settings(cfg)
	if err != nil || string(got.Key) != "pem" {
		t.Errorf("static settings = %+v, err = %v", got.ConnectorConfig, err)
	}
	cfg.ConnectorConfig.KeyPath += ".gone"
	if _, err := settings(cfg); err == nil {
		t.Error("a configured key_path that cannot be read must fail")
	}
}

type blockingHeartbeatGroup struct {
	provider.InstanceGroup
	mu       sync.Mutex
	decrease []string
}

func (g *blockingHeartbeatGroup) Heartbeat(ctx context.Context, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func (g *blockingHeartbeatGroup) Decrease(_ context.Context, ids []string) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.decrease = append(g.decrease, ids...)
	return ids, nil
}

func TestHeartbeatCutShortByContextRemovesNothing(t *testing.T) {
	inner := &blockingHeartbeatGroup{}
	g := removeOnHeartbeatFailure{InstanceGroup: inner}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := g.Heartbeat(ctx, "vm1"); err == nil {
		t.Fatal("heartbeat error swallowed")
	}
	inner.mu.Lock()
	defer inner.mu.Unlock()
	if len(inner.decrease) != 0 {
		t.Fatalf("Decrease called for %v after the caller's context ended", inner.decrease)
	}
}
