package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"gitlab.com/gitlab-org/fleeting/fleeting/connector"
	"gitlab.com/gitlab-org/fleeting/fleeting/plugin/fleeting-plugin-dummy"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/taskscaler"

	"gitlab.com/dunn.dev/forgehand/internal/config"
	"gitlab.com/dunn.dev/forgehand/internal/fleet"
	"gitlab.com/dunn.dev/forgehand/internal/github"
	"gitlab.com/dunn.dev/forgehand/internal/scaler"
)

// TestOneJobThroughTaskscalerAndTheDummyPlugin takes a job from an assigned
// message to a deleted instance: taskscaler boots a dummy instance for the
// blocked Acquire, the runner is started on it through a fake SSH target, and
// the completion releases the instance.
func TestOneJobThroughTaskscalerAndTheDummyPlugin(t *testing.T) {
	api := github.NewFake()
	group := &dummy.InstanceGroup{MaxSize: 2}
	fl, err := fleet.New(group, config.Autoscaler{
		MaxInstances:                2,
		UpdateInterval:              config.Duration(30 * time.Millisecond),
		UpdateIntervalWhenExpecting: config.Duration(10 * time.Millisecond),
		DeletionRetryInterval:       config.Duration(30 * time.Millisecond),
		ShutdownDeletionInterval:    config.Duration(10 * time.Millisecond),
	}, fleet.Options{}, taskscaler.WithAcquireDelay(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		fl.Shutdown(ctx)
	})

	running := make(chan struct{})
	finish := make(chan struct{})
	tg := useTarget(t, func(_ int, ctx context.Context, o connector.RunOptions) error {
		_, _ = o.Stdout.Write([]byte("2026-10-28 12:00:00Z: Listening for Jobs\n"))
		_, _ = o.Stdout.Write([]byte("2026-10-28 12:00:05Z: Running job: build\n"))
		close(running)
		select {
		case <-finish:
		case <-ctx.Done():
		}
		return nil
	})

	rn := New(config.Runner{Dir: "/opt/actions-runner"}, false, api, 1, nil)
	sc := scaler.New(scaler.Config{
		MaxInstances:           2,
		ListenTimeout:          time.Minute,
		IdleRunnerTimeout:      time.Minute,
		InstanceAcquireTimeout: time.Minute,
	}, api.Session, api, fl, rn)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = sc.Stop(ctx)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sc.Scale(ctx, &scaleset.RunnerScaleSetMessage{Statistics: &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "the runner to be running a job", running)

	instances := group.List()
	if len(instances) != 1 {
		t.Fatalf("%d instances for one assigned job", len(instances))
	}
	id := instances[0].ID
	if names := api.JitNames; len(names) != 1 || names[0] != id {
		t.Errorf("JIT configs minted for %v, want the instance %s", names, id)
	}
	if c := tg.all(); len(c) != 1 || !strings.Contains(c[0].command, "run --once") || c[0].stdin != "jit-"+id+"\n" {
		t.Errorf("runner start = %+v", c)
	}

	err = sc.Scale(ctx, &scaleset.RunnerScaleSetMessage{
		Statistics:           &scaleset.RunnerScaleSetStatistic{},
		JobCompletedMessages: []*scaleset.JobCompleted{{RunnerName: id, Result: "succeeded"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(finish)
	if err := sc.Drain(ctx); err != nil {
		t.Fatalf("slot never finished: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if s := stateOf(group, id); s == provider.StateDeleting || s == provider.StateDeleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("instance %s still %v after its job", id, stateOf(group, id))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := len(group.List()); n > 1 {
		t.Errorf("%d instances after the job, want the one being deleted", n)
	}
}

func waitFor(t *testing.T, ctx context.Context, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s", what)
	}
}

func stateOf(g *dummy.InstanceGroup, id string) provider.State {
	for _, i := range g.List() {
		if i.ID == id {
			return i.State
		}
	}
	return provider.StateDeleted
}
