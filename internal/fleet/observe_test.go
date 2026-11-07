package fleet

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/fleeting/fleeting"
	"gitlab.com/gitlab-org/fleeting/fleeting/plugin/fleeting-plugin-dummy"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/taskscaler"

	"gitlab.com/dunn.dev/forgehand/internal/config"
)

// metric returns the value of the series name{labels} in reg, or -1.
func metric(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue series
				}
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
			return m.GetGauge().GetValue()
		}
	}
	return -1
}

// connectGroup fails ConnectInfo for its first failures calls.
type connectGroup struct {
	provider.InstanceGroup
	failures int32
	calls    atomic.Int32
}

func (g *connectGroup) ConnectInfo(ctx context.Context, id string) (provider.ConnectInfo, error) {
	if g.calls.Add(1) <= g.failures {
		return provider.ConnectInfo{}, errors.New("no address")
	}
	return g.InstanceGroup.ConnectInfo(ctx, id)
}

func TestConnectInfoFailureIsCounted(t *testing.T) {
	reg := prometheus.NewRegistry()
	group := &connectGroup{InstanceGroup: &dummy.InstanceGroup{MaxSize: 4}, failures: 1}
	f := newFleet(t, group, Options{Name: "set", Metrics: reg})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _, _ = f.Acquire(ctx, "a") }()
	lbl := map[string]string{"scaleset": "set", "stage": "connect_info"}
	eventually(t, "the connect_info failure to be counted", func() bool {
		return metric(t, reg, "forgehand_prepare_failures_total", lbl) == 1
	})
	if got := metric(t, reg, "forgehand_prepare_consecutive_failures", nil); got != 1 {
		t.Errorf("consecutive failures = %v, want 1", got)
	}
}

func TestPreparedGuestResetsTheConsecutiveCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	obs, err := newObserved(reg, prometheus.Labels{"scaleset": "set"})
	if err != nil {
		t.Fatal(err)
	}
	obs.prepareFailed("connect_info")
	obs.prepareFailed("connect_info")
	up := upFunc(context.Background(), config.Autoscaler{}, nil, nil, obs)
	if err := up(nil, taskscaler.UpFuncInstance{ID: "i", Cause: fleeting.CauseRequested}); err != nil {
		t.Fatal(err)
	}
	if got := metric(t, reg, "forgehand_prepare_consecutive_failures", nil); got != 0 {
		t.Errorf("consecutive failures = %v after a guest prepared, want 0", got)
	}
}

func TestReadyCommandFailureIsCounted(t *testing.T) {
	reg := prometheus.NewRegistry()
	obs, err := newObserved(reg, prometheus.Labels{"scaleset": "set"})
	if err != nil {
		t.Fatal(err)
	}
	up := upFunc(context.Background(), config.Autoscaler{InstanceReadyCommand: "true"}, nil, nil, obs)
	in := taskscaler.UpFuncInstance{ID: "i", Cause: fleeting.CauseRequested}
	if err := up(nil, in); err == nil {
		t.Fatal("a ready command with no address to reach must fail")
	}
	if got := metric(t, reg, "forgehand_prepare_failures_total", map[string]string{"stage": "ready_command"}); got != 1 {
		t.Errorf("ready_command failures = %v, want 1", got)
	}
	if got := metric(t, reg, "forgehand_prepare_consecutive_failures", nil); got != 1 {
		t.Errorf("consecutive failures = %v, want 1", got)
	}
}

type increaseGroup struct {
	provider.InstanceGroup
	err error
}

func (g increaseGroup) Increase(context.Context, int) (int, error) { return 0, g.err }

func TestAdmissionRefusalsAreCounted(t *testing.T) {
	reg := prometheus.NewRegistry()
	obs, err := newObserved(reg, prometheus.Labels{"scaleset": "set"})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []error{
		errors.New("rpc error: admission refused for vm1: admit_command: no room"),
		errors.New("lock timed out"),
	} {
		_, _ = observedGroup{InstanceGroup: increaseGroup{err: e}, o: obs}.Increase(context.Background(), 1)
	}
	if got := metric(t, reg, "forgehand_admission_refusals_total", nil); got != 1 {
		t.Errorf("admission refusals = %v, want 1", got)
	}
}
