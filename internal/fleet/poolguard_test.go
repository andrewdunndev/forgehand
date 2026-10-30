package fleet

import (
	"context"
	"slices"
	"testing"
	"time"

	"gitlab.com/gitlab-org/fleeting/fleeting/plugin/fleeting-plugin-dummy"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/taskscaler"

	"gitlab.com/dunn.dev/forgehand/internal/config"
)

// gatedGroup holds every deletion until open is closed, as a plugin that is
// slow to delete a VM does.
type gatedGroup struct {
	provider.InstanceGroup
	open chan struct{}
}

func (g gatedGroup) Decrease(ctx context.Context, ids []string) ([]string, error) {
	select {
	case <-g.open:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.InstanceGroup.Decrease(ctx, ids)
}

// poolFleet starts a fleet with an idle_count of one over group, whose
// deletions wait for the returned release function.
func poolFleet(t *testing.T, group *dummy.InstanceGroup, dir string) (*fleetImpl, func()) {
	t.Helper()
	gate := gatedGroup{InstanceGroup: group, open: make(chan struct{})}
	cfg := testConfig()
	cfg.Policy = []config.Policy{{IdleCount: 1}}
	f, err := New(gate, cfg, Options{StateDir: dir}, taskscaler.WithAcquireDelay(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	opened := false
	open := func() {
		if !opened {
			opened = true
			close(gate.open)
		}
	}
	t.Cleanup(func() {
		open()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		f.Shutdown(ctx)
	})
	return f.(*fleetImpl), open
}

// running returns the ids of the instances the plugin holds that are not
// deleted, sorted.
func running(g *dummy.InstanceGroup) []string {
	var ids []string
	for id, s := range states(g) {
		if s != provider.StateDeleted {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// settles waits for taskscaler to run its scaling loop twice more, which is
// once a second.
func settles() { time.Sleep(2300 * time.Millisecond) }

func wantInstances(t *testing.T, g *dummy.InstanceGroup, when string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if got := running(g); !slices.Equal(got, want) {
		t.Fatalf("%s: instances = %v, want %v", when, got, want)
	}
}

func TestRestartKeepsThePoolInstanceWhileARestoredIdleInstanceIsDeleted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	group := &dummy.InstanceGroup{MaxSize: 4}
	busy := group.AddInstance(provider.StateRunning)
	idle := group.AddInstance(provider.StateRunning)
	pool := group.AddInstance(provider.StateRunning)
	saveInstance(t, dir, busy, "old-1", false)
	saveInstance(t, dir, idle, "old-2", false)
	saveInstance(t, dir, pool, "", false)

	f, open := poolFleet(t, group, dir)
	if got := restored(t, f); len(got) != 2 {
		t.Fatalf("adopted %+v, want both acquisitions", got)
	}
	f.Release("old-2")
	settles()
	wantInstances(t, group, "while the idle instance is deleted", busy, idle, pool)
	if s := f.ts.Schedule(); s.IdleCount != 0 || s.IdleTime != noTrim {
		t.Errorf("schedule during the deletion = %+v, want no idle count and no trimming", s)
	}

	open()
	eventually(t, "the idle instance to be deleted", func() bool { return states(group)[idle] == provider.StateDeleted })
	settles()
	wantInstances(t, group, "after the idle instance is deleted", busy, pool)
	if s := f.ts.Schedule(); s.IdleCount != 1 || s.IdleTime != 0 {
		t.Errorf("schedule after the deletion = %+v, want the configured one", s)
	}
}

func TestReleaseOfTheBusyInstanceKeepsThePoolInstance(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	group := &dummy.InstanceGroup{MaxSize: 4}
	busy := group.AddInstance(provider.StateRunning)
	pool := group.AddInstance(provider.StateRunning)
	saveInstance(t, dir, busy, "old-1", false)
	saveInstance(t, dir, pool, "", false)

	f, open := poolFleet(t, group, dir)
	restored(t, f)
	f.Release("old-1")
	settles()
	open()
	eventually(t, "the released instance to be deleted", func() bool { return states(group)[busy] == provider.StateDeleted })
	settles()
	wantInstances(t, group, "after the job's instance is deleted", pool)
}

func TestAdjustedOffsetsTheIdleCountAndStopsTrimming(t *testing.T) {
	base := []taskscaler.Schedule{
		{IdleCount: 3, IdleTime: time.Minute, ScaleFactor: 1.5},
		{IdleCount: 1, Periods: []string{"* * * * *"}},
	}
	if got := adjusted(base, 0); !slices.EqualFunc(got, base, func(a, b taskscaler.Schedule) bool {
		return a.IdleCount == b.IdleCount && a.IdleTime == b.IdleTime && a.ScaleFactor == b.ScaleFactor
	}) {
		t.Errorf("adjusted(base, 0) = %+v, want base", got)
	}
	got := adjusted(base, 2)
	if got[0].IdleCount != 1 || got[1].IdleCount != 0 {
		t.Errorf("idle counts = %d, %d, want 1, 0", got[0].IdleCount, got[1].IdleCount)
	}
	if got[0].IdleTime != noTrim || got[1].IdleTime != noTrim {
		t.Errorf("idle times = %s, %s, want no trimming", got[0].IdleTime, got[1].IdleTime)
	}
	if got[0].ScaleFactor != 1.5 || len(got[1].Periods) != 1 {
		t.Errorf("adjusted dropped other fields: %+v", got)
	}
	if base[0].IdleCount != 3 || base[0].IdleTime != time.Minute {
		t.Errorf("adjusted changed its input: %+v", base[0])
	}
}
