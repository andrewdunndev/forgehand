package fleet

import (
	"context"
	"slices"
	"sync"
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

// scheduleRecorder records the schedules written to it, holding the first
// write after arm until let is closed.
type scheduleRecorder struct {
	taskscaler.Taskscaler
	mu      sync.Mutex
	idle    []int
	armed   bool
	entered chan struct{}
	let     chan struct{}
}

func (r *scheduleRecorder) ConfigureSchedule(s ...taskscaler.Schedule) error {
	r.mu.Lock()
	hold := r.armed
	r.armed = false
	r.mu.Unlock()
	if hold {
		close(r.entered)
		<-r.let
	}
	r.mu.Lock()
	r.idle = append(r.idle, s[0].IdleCount)
	r.mu.Unlock()
	return nil
}

func (r *scheduleRecorder) last() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.idle[len(r.idle)-1]
}

func TestOverlappingNotesLeaveTheLatestScheduleWritten(t *testing.T) {
	t.Parallel()
	rec := &scheduleRecorder{entered: make(chan struct{}), let: make(chan struct{})}
	g := newPoolGuard([]taskscaler.Schedule{{IdleCount: 2}})
	if err := g.start(rec); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	rec.armed = true
	rec.mu.Unlock()

	var wg sync.WaitGroup
	wg.Go(func() { g.note("a") })
	<-rec.entered
	wg.Go(func() { g.note("b") })
	eventually(t, "the second release to be recorded", func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return len(g.pending) == 2
	})
	time.Sleep(50 * time.Millisecond)
	close(rec.let)
	wg.Wait()
	if got := rec.last(); got != 0 {
		t.Errorf("last idle count written = %d, want 0 with two releases in flight", got)
	}
}

func TestFleetWithoutStorageKeepsTheConfiguredSchedule(t *testing.T) {
	t.Parallel()
	group := &dummy.InstanceGroup{MaxSize: 4}
	cfg := testConfig()
	cfg.Policy = []config.Policy{{IdleCount: 1}}
	f, err := New(group, cfg, Options{}, taskscaler.WithAcquireDelay(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		f.Shutdown(ctx)
	})
	acquire(t, f, "a")
	f.Release("a")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.WaitDeleted(ctx); err != nil {
		t.Fatal(err)
	}
	if s := f.(*fleetImpl).ts.Schedule(); s.IdleCount != 1 || s.IdleTime != 0 {
		t.Errorf("schedule = %+v, want the configured one", s)
	}
}

// scribbler writes into the schedules it is given, as taskscaler does.
type scribbler struct {
	taskscaler.Taskscaler
}

func (scribbler) ConfigureSchedule(s ...taskscaler.Schedule) error {
	for i := range s {
		s[i].IdleCount = -1
	}
	return nil
}

func TestGuardNeverLetsTaskscalerWriteItsBaseSchedules(t *testing.T) {
	for _, n := range []int{0, 2} {
		base := []taskscaler.Schedule{{IdleCount: 3}, {IdleCount: 1}}
		out := adjusted(base, n)
		out[0].IdleCount = -1
		if base[0].IdleCount != 3 {
			t.Errorf("adjusted(base, %d) shares base's backing array", n)
		}
	}

	g := newPoolGuard([]taskscaler.Schedule{{IdleCount: 3}, {IdleCount: 1}})
	if err := g.start(scribbler{}); err != nil {
		t.Fatal(err)
	}
	if g.base[0].IdleCount != 3 || g.base[1].IdleCount != 1 {
		t.Errorf("start let taskscaler write the base schedules: %+v", g.base)
	}
	g.note("a")
	if g.base[0].IdleCount != 3 || g.base[1].IdleCount != 1 {
		t.Errorf("note let taskscaler write the base schedules: %+v", g.base)
	}
}

// orderedRelease records whether the guard held the instance when taskscaler
// was told to release its key.
type orderedRelease struct {
	taskscaler.Taskscaler
	g       *poolGuard
	id      string
	mu      sync.Mutex
	noted   bool
	release int
}

func (o *orderedRelease) Release(key string) {
	o.g.mu.Lock()
	_, ok := o.g.pending[o.id]
	o.g.mu.Unlock()
	o.mu.Lock()
	o.noted = ok
	o.release++
	o.mu.Unlock()
	o.Taskscaler.Release(key)
}

func TestReleaseNotesTheInstanceBeforeTaskscalerMarksIt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	group := &dummy.InstanceGroup{MaxSize: 4}
	busy := group.AddInstance(provider.StateRunning)
	pool := group.AddInstance(provider.StateRunning)
	saveInstance(t, dir, busy, "old-1", false)
	saveInstance(t, dir, pool, "", false)

	f, _ := poolFleet(t, group, dir)
	restored(t, f)
	o := &orderedRelease{Taskscaler: f.ts, g: f.guard, id: busy}
	f.ts = o
	f.Release("old-1")
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.release != 1 || !o.noted {
		t.Errorf("releases = %d, guard held the instance at the release = %t, want 1 and true", o.release, o.noted)
	}
}
