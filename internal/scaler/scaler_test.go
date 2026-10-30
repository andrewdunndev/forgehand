package scaler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-hclog"

	"gitlab.com/dunn.dev/forgehand/internal/fleet"
	"gitlab.com/dunn.dev/forgehand/internal/github"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type eventLog struct {
	mu sync.Mutex
	ev []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ev = append(l.ev, e)
}

func (l *eventLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ev...)
}

type loggedRegistry struct {
	*github.Fake
	log *eventLog
}

func (r loggedRegistry) RemoveRunner(ctx context.Context, id int64) error {
	err := r.Fake.RemoveRunner(ctx, id)
	r.log.add("remove")
	return err
}

type loggedFleet struct {
	*fleet.Fake
	log *eventLog
}

func (f loggedFleet) Release(key string) {
	f.log.add("release")
	f.Fake.Release(key)
}

type result struct {
	code int
	err  error
}

type run struct {
	hooks Hooks
	exit  chan result
}

// fakeRunner mints a JIT config as the real runner does, then waits for the
// test to drive the process.
type fakeRunner struct {
	api  *github.Fake
	mu   sync.Mutex
	runs map[string]*run
	// attached holds the runs that followed an adopted runner.
	attached map[string]*run

	diag       string
	diagCalls  int
	releasedAt int
	fl         *fleet.Fake
}

func (f *fakeRunner) Run(ctx context.Context, _ fleet.Instance, name string, h Hooks) (int, error) {
	if _, err := f.api.GenerateJitRunnerConfig(ctx, name, 1); err != nil {
		return 0, err
	}
	r := &run{hooks: h, exit: make(chan result, 1)}
	f.mu.Lock()
	f.runs[name] = r
	f.mu.Unlock()
	select {
	case res := <-r.exit:
		return res.code, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Attach waits for the test to drive the adopted runner, as Run does.
func (f *fakeRunner) Attach(ctx context.Context, _ fleet.Instance, name string, h Hooks) (int, error) {
	r := &run{hooks: h, exit: make(chan result, 1)}
	f.mu.Lock()
	f.attached[name] = r
	f.mu.Unlock()
	select {
	case res := <-r.exit:
		return res.code, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (f *fakeRunner) Diag(context.Context, fleet.Instance) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diagCalls++
	f.releasedAt = len(f.fl.Released())
	return f.diag
}

func (f *fakeRunner) get(name string) *run {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs[name]
}

func (f *fakeRunner) follower(name string) *run {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attached[name]
}

type rig struct {
	t       *testing.T
	s       *Scaler
	clk     *clock
	api     *github.Fake
	session *github.FakeSession
	fl      *fleet.Fake
	rn      *fakeRunner
	log     *eventLog
	out     *syncWriter
}

func newRig(t *testing.T) *rig {
	t.Helper()
	api := github.NewFake()
	fl := fleet.NewFake(fleet.Options{})
	log := &eventLog{}
	out := &syncWriter{}
	rn := &fakeRunner{api: api, runs: map[string]*run{}, attached: map[string]*run{}, fl: fl}
	clk := &clock{t: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)}
	s := New(Config{
		MaxInstances:           2,
		Run:                    "slot",
		ListenTimeout:          3 * time.Minute,
		IdleTimeout:            5 * time.Minute,
		InstanceAcquireTimeout: 15 * time.Minute,
		Logger:                 hclog.New(&hclog.LoggerOptions{Output: out, Level: hclog.Debug}),
	}, api.Session, loggedRegistry{api, log}, loggedFleet{fl, log}, rn)
	s.now = clk.Now
	r := &rig{t: t, s: s, clk: clk, api: api, session: api.Session, fl: fl, rn: rn, log: log, out: out}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
	return r
}

type syncWriter struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func (r *rig) logged() string {
	return r.out.String()
}

func stats(assigned int) *scaleset.RunnerScaleSetStatistic {
	return &scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: assigned}
}

func (r *rig) scale(m *scaleset.RunnerScaleSetMessage) {
	r.t.Helper()
	if err := r.s.Scale(context.Background(), m); err != nil {
		r.t.Fatalf("Scale: %v", err)
	}
}

func (r *rig) assigned(n int) {
	r.t.Helper()
	r.scale(&scaleset.RunnerScaleSetMessage{Statistics: stats(n)})
}

// advance moves the clock and applies the timers.
func (r *rig) advance(d time.Duration) {
	r.clk.advance(d)
	r.s.mu.Lock()
	r.s.evaluate()
	r.s.mu.Unlock()
}

func (r *rig) waitFor(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s\nlog:\n%s", what, r.logged())
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *rig) slotCount() int {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.seq
}

func (r *rig) live() int {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.live()
}

func (r *rig) stateOf(key string) State {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.slots[key].state
}

func (r *rig) stateOfRunner(name string) State {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	sl := r.s.find(name)
	if sl == nil {
		return Acquiring
	}
	return sl.state
}

// boot grants the oldest blocked Acquire an instance named name and waits for
// the runner to be started on it.
func (r *rig) boot(name string) {
	r.t.Helper()
	r.waitFor("a blocked Acquire", func() bool { return len(r.fl.Waiting()) > 0 })
	key := r.fl.Waiting()[0]
	if !r.fl.Grant(key, &fleet.FakeInstance{ID: name}) {
		r.t.Fatalf("grant %s", key)
	}
	r.waitFor("runner "+name, func() bool { return r.rn.get(name) != nil })
}

func (r *rig) listening(name string) {
	r.t.Helper()
	r.boot(name)
	r.rn.get(name).hooks.Listening()
	if got := r.stateOfRunner(name); got != Listening {
		r.t.Fatalf("%s is %s, want LISTENING", name, got)
	}
}

func (r *rig) busy(name string) {
	r.t.Helper()
	r.listening(name)
	r.rn.get(name).hooks.RunningJob()
	if got := r.stateOfRunner(name); got != Busy {
		r.t.Fatalf("%s is %s, want BUSY", name, got)
	}
}

func (r *rig) exit(name string, code int) { r.rn.get(name).exit <- result{code: code} }

func (r *rig) wantState(name string, want State) {
	r.t.Helper()
	r.waitFor(fmt.Sprintf("%s to be %s", name, want), func() bool { return r.stateOfRunner(name) == want })
}

func (r *rig) wantReleased(n int) {
	r.t.Helper()
	r.waitFor(fmt.Sprintf("%d releases", n), func() bool { return len(r.fl.Released()) == n })
}

func completed(names ...string) *scaleset.RunnerScaleSetMessage {
	m := &scaleset.RunnerScaleSetMessage{}
	for _, n := range names {
		m.JobCompletedMessages = append(m.JobCompletedMessages, &scaleset.JobCompleted{RunnerName: n, Result: "succeeded"})
	}
	return m
}

func started(names ...string) *scaleset.RunnerScaleSetMessage {
	m := &scaleset.RunnerScaleSetMessage{}
	for _, n := range names {
		m.JobStartedMessages = append(m.JobStartedMessages, &scaleset.JobStarted{RunnerName: n})
	}
	return m
}

func TestAssignedOnlyMessageYieldsAcquires(t *testing.T) {
	r := newRig(t)
	r.scale(&scaleset.RunnerScaleSetMessage{
		Statistics:          stats(2),
		JobAssignedMessages: []*scaleset.JobAssigned{{}, {}},
	})
	r.waitFor("two blocked Acquires", func() bool { return len(r.fl.Waiting()) == 2 })
	if len(r.session.Acquired) != 0 {
		t.Fatalf("AcquireJobs called for assigned-only message: %v", r.session.Acquired)
	}
}

func TestMaxInstancesBoundsDemand(t *testing.T) {
	r := newRig(t)
	r.assigned(5)
	r.waitFor("two blocked Acquires", func() bool { return len(r.fl.Waiting()) == 2 })
	if got := r.slotCount(); got != 2 {
		t.Fatalf("slots = %d, want 2", got)
	}
}

func TestNilMessageAndNilStatisticsKeepCachedTarget(t *testing.T) {
	r := newRig(t)
	r.assigned(2)
	r.scale(nil)
	r.scale(&scaleset.RunnerScaleSetMessage{MessageID: 5})
	r.advance(time.Minute)
	if got := r.live(); got != 2 {
		t.Fatalf("live = %d, want the cached target 2", got)
	}
	if got := r.slotCount(); got != 2 {
		t.Fatalf("slots spawned = %d, want 2", got)
	}
}

func TestAcquireJobsErrorIsReturnedAndRedeliveryAcquiresOnce(t *testing.T) {
	r := newRig(t)
	fail := true
	r.session.AcquireFn = func(ids []int64) ([]int64, error) {
		if fail {
			fail = false
			return nil, errors.New("503")
		}
		return ids, nil
	}
	m := &scaleset.RunnerScaleSetMessage{
		MessageID:            1,
		Statistics:           stats(1),
		JobAvailableMessages: []*scaleset.JobAvailable{{JobMessageBase: scaleset.JobMessageBase{RunnerRequestID: 7}}},
	}
	if err := r.s.Scale(context.Background(), m); err == nil {
		t.Fatal("Scale swallowed the AcquireJobs error")
	}
	if got := r.slotCount(); got != 0 {
		t.Fatalf("slots after failed delivery = %d, want 0", got)
	}
	r.scale(m)
	r.waitFor("one blocked Acquire", func() bool { return len(r.fl.Waiting()) == 1 })
	if got := r.slotCount(); got != 1 {
		t.Fatalf("slots after redelivery = %d, want 1", got)
	}
	if len(r.session.Acquired) != 2 {
		t.Fatalf("AcquireJobs calls = %d, want 2", len(r.session.Acquired))
	}
}

func TestJobCompletedBeforeAnyRunnerListensCancelsBlockedSlot(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.waitFor("blocked Acquire", func() bool { return len(r.fl.Waiting()) == 1 })

	m := completed("")
	m.Statistics = stats(0)
	r.scale(m)
	r.advance(dropDebounce)

	if got := r.stateOf("slot-1"); got != Cancelled {
		t.Fatalf("slot is %s, want CANCELLED", got)
	}
	r.waitFor("Acquire to be cancelled", func() bool { return len(r.fl.Waiting()) == 0 })
	if len(r.api.JitNames) != 0 {
		t.Fatalf("JIT minted for %v", r.api.JitNames)
	}
}

func TestJobCompletedResultIsLogged(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.boot("vm-1")
	r.scale(completed("vm-1"))
	if !strings.Contains(r.logged(), "job completed") || !strings.Contains(r.logged(), "result=succeeded") {
		t.Fatalf("log lacks the job result:\n%s", r.logged())
	}
}

func TestDroppedDemandIsDebounced(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.waitFor("blocked Acquire", func() bool { return len(r.fl.Waiting()) == 1 })
	r.assigned(0)
	r.advance(5 * time.Second)
	r.assigned(1)
	r.advance(30 * time.Second)
	if got := r.slotCount(); got != 1 {
		t.Fatalf("Acquires = %d, want 1", got)
	}
	if got := r.stateOf("slot-1"); got != Acquiring {
		t.Fatalf("slot is %s, want ACQUIRING", got)
	}
}

func TestDropCancelsNewestAcquiringFirst(t *testing.T) {
	r := newRig(t)
	r.assigned(2)
	r.waitFor("two Acquires", func() bool { return len(r.fl.Waiting()) == 2 })
	r.assigned(1)
	r.advance(dropDebounce)
	if a, b := r.stateOf("slot-1"), r.stateOf("slot-2"); a != Acquiring || b != Cancelled {
		t.Fatalf("slot-1 %s, slot-2 %s; want ACQUIRING and CANCELLED", a, b)
	}
}

func TestRunnerExitBeforeMarkerFailsSlotAtOnce(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.boot("vm-1")
	r.exit("vm-1", 3)
	r.wantState("vm-1", Failed)
	r.wantReleased(1)
	if got := r.live(); got != 0 {
		t.Fatalf("live = %d, want 0", got)
	}
}

func TestRunnerExitWhileListeningFailsSlotAtOnce(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.listening("vm-1")
	r.exit("vm-1", 1)
	r.wantState("vm-1", Failed)
	r.wantReleased(1)
	if got := r.live(); got != 0 {
		t.Fatalf("live = %d, want 0", got)
	}
}

func TestRetiredRunnerNamesThePinInTheCause(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.listening("vm-1")
	r.exit("vm-1", 7)
	r.wantState("vm-1", Failed)
	if !strings.Contains(r.logged(), "runner retired: bump the golden's runner pin") {
		t.Fatalf("log lacks the retirement cause:\n%s", r.logged())
	}
}

func TestJitErrorFailsStartingSlot(t *testing.T) {
	r := newRig(t)
	r.api.SetJitErr(errors.New("404 installation: the App must be installed on the repo"))
	r.assigned(1)
	r.waitFor("a blocked Acquire", func() bool { return len(r.fl.Waiting()) > 0 })
	r.fl.Grant(r.fl.Waiting()[0], &fleet.FakeInstance{ID: "vm-1"})
	r.wantState("vm-1", Failed)
	r.wantReleased(1)
}

func TestFailedSlotRemovesItsRegistration(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.boot("vm-1")
	r.exit("vm-1", 1)
	r.wantState("vm-1", Failed)
	r.waitFor("the registration to be removed", func() bool { return len(r.api.RemovedRunners()) == 1 })
}

func TestBusyRunnerExitWithoutCompletionIsDoneAfterGrace(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.busy("vm-1")
	r.exit("vm-1", 0)
	r.waitFor("exit to be seen", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return r.s.find("vm-1").exited
	})
	r.advance(completionGrace - time.Second)
	if got := r.stateOfRunner("vm-1"); got != Busy {
		t.Fatalf("slot is %s before the grace ends, want BUSY", got)
	}
	r.advance(time.Second)
	if got := r.stateOfRunner("vm-1"); got != Done {
		t.Fatalf("slot is %s after the grace, want DONE", got)
	}
	r.wantReleased(1)
}

func TestBusyExitAndCompletionBothSeenIsDoneAtOnce(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.busy("vm-1")
	r.exit("vm-1", 0)
	r.waitFor("exit to be seen", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return r.s.find("vm-1").exited
	})
	r.scale(completed("vm-1"))
	if got := r.stateOfRunner("vm-1"); got != Done {
		t.Fatalf("slot is %s, want DONE", got)
	}
	r.wantReleased(1)
}

func TestRunnerExitWithAssignedStillOneAddsNoSlot(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.busy("vm-1")
	r.exit("vm-1", 0)
	r.waitFor("exit to be seen", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return r.s.find("vm-1").exited
	})
	r.assigned(1)
	if got := r.slotCount(); got != 1 {
		t.Fatalf("slots = %d, want 1: the BUSY slot must stay live", got)
	}
	if got := r.live(); got != 1 {
		t.Fatalf("live = %d, want 1", got)
	}
}

func TestRunningJobLineDisarmsIdleTimerWithoutJobStarted(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.busy("vm-1")
	r.advance(time.Hour)
	if got := r.stateOfRunner("vm-1"); got != Busy {
		t.Fatalf("slot is %s, want BUSY", got)
	}
	if len(r.api.RemovedRunners()) != 0 {
		t.Fatalf("runner removed under a running job: %v", r.api.RemovedRunners())
	}
}

func TestJobStartedMovesListeningToBusy(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.listening("vm-1")
	r.scale(started("vm-1"))
	r.advance(time.Hour)
	if got := r.stateOfRunner("vm-1"); got != Busy {
		t.Fatalf("slot is %s, want BUSY", got)
	}
}

func TestIdleListeningSlotRemovesRunnerThenReleases(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.listening("vm-1")
	r.advance(5 * time.Minute)
	r.wantState("vm-1", Done)
	r.wantReleased(1)
	if got := fmt.Sprint(r.log.list()); got != "[remove release]" {
		t.Fatalf("order = %s, want remove then release", got)
	}
}

func TestRemoveRunnerErrorClasses(t *testing.T) {
	t.Run("JobStillRunning keeps the VM and returns the slot to BUSY", func(t *testing.T) {
		r := newRig(t)
		r.assigned(1)
		r.listening("vm-1")
		r.api.SetRemoveErr(scaleset.JobStillRunningError)
		r.advance(5 * time.Minute)
		r.wantState("vm-1", Busy)
		r.advance(time.Hour)
		if n := len(r.fl.Released()); n != 0 {
			t.Fatalf("released %d times, want the VM kept", n)
		}
		if got := r.stateOfRunner("vm-1"); got != Busy {
			t.Fatalf("slot is %s, want BUSY", got)
		}
	})
	t.Run("RunnerNotFound releases", func(t *testing.T) {
		r := newRig(t)
		r.assigned(1)
		r.listening("vm-1")
		r.api.SetRemoveErr(scaleset.RunnerNotFoundError)
		r.advance(5 * time.Minute)
		r.wantState("vm-1", Done)
		r.wantReleased(1)
	})
	t.Run("another error retries on the next poll", func(t *testing.T) {
		r := newRig(t)
		r.assigned(1)
		r.listening("vm-1")
		r.api.SetRemoveErr(errors.New("502"))
		r.advance(5 * time.Minute)
		r.waitFor("a failed attempt", func() bool {
			r.s.mu.Lock()
			defer r.s.mu.Unlock()
			return strings.Contains(r.logged(), "retrying next poll") && !r.s.find("vm-1").reaping
		})
		if got := r.stateOfRunner("vm-1"); got != Reaping {
			t.Fatalf("slot is %s, want REAPING", got)
		}
		if n := len(r.fl.Released()); n != 0 {
			t.Fatalf("released %d times after a failed removal", n)
		}
		r.api.SetRemoveErr(nil)
		r.assigned(1)
		r.wantState("vm-1", Done)
		r.wantReleased(1)
	})
}

func TestLateEventsAreIgnored(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.busy("vm-1")
	r.exit("vm-1", 0)
	r.waitFor("exit to be seen", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return r.s.find("vm-1").exited
	})
	r.scale(completed("vm-1"))
	r.wantState("vm-1", Done)
	r.assigned(0)

	late := started("vm-1", "ghost", "")
	late.JobCompletedMessages = completed("vm-1", "ghost").JobCompletedMessages
	r.scale(late)
	r.rn.get("vm-1").hooks.Listening()
	r.rn.get("vm-1").hooks.RunningJob()
	if got := r.stateOfRunner("vm-1"); got != Done {
		t.Fatalf("slot is %s after late events, want DONE", got)
	}
	if got := len(r.fl.Released()); got != 1 {
		t.Fatalf("released %d times, want 1", got)
	}
}

func TestLateStartedAndHooksDoNotMoveReapingSlotUp(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.listening("vm-1")
	r.api.SetRemoveErr(errors.New("502"))
	r.advance(5 * time.Minute)
	r.waitFor("a failed attempt", func() bool { return strings.Contains(r.logged(), "retrying next poll") })
	r.scale(started("vm-1"))
	r.rn.get("vm-1").hooks.RunningJob()
	if got := r.stateOfRunner("vm-1"); got != Reaping {
		t.Fatalf("slot is %s, want REAPING", got)
	}
}

func TestLostRunnerKeepsVMUntilJobCompleted(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.busy("vm-1")
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.rn.get("vm-1").exit <- result{err: ErrLost}
	r.wantState("vm-1", Busy)
	r.waitFor("lost to be recorded", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		sl := r.s.find("vm-1")
		return sl.lost && !sl.reaping
	})
	r.advance(time.Hour)
	r.waitFor("the lost slot to settle back to BUSY", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		sl := r.s.find("vm-1")
		return sl.state == Busy && !sl.reaping
	})
	if n := len(r.fl.Released()); n != 0 {
		t.Fatalf("released %d times while the job may still run", n)
	}
	r.scale(completed("vm-1"))
	r.wantState("vm-1", Done)
	r.wantReleased(1)
}

func TestLostBusyRunnerIsReapedOnceGitHubForgetsIt(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.busy("vm-1")
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.rn.get("vm-1").exit <- result{err: ErrLost}
	r.waitFor("lost to be recorded", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		sl := r.s.find("vm-1")
		return sl.lost && !sl.reaping && sl.state == Busy
	})
	r.api.SetRemoveErr(scaleset.RunnerNotFoundError)
	r.advance(time.Minute)
	r.wantState("vm-1", Done)
	r.wantReleased(1)
}

func TestRunnerNeverListeningFailsWithinListenTimeoutAfterDiag(t *testing.T) {
	r := newRig(t)
	r.rn.diag = "Runner connect error: 401"
	r.assigned(1)
	r.boot("vm-1")
	r.advance(3*time.Minute - time.Second)
	if got := r.stateOfRunner("vm-1"); got != Starting {
		t.Fatalf("slot is %s before the timeout, want STARTING", got)
	}
	r.advance(time.Second)
	r.wantState("vm-1", Failed)
	r.wantReleased(1)
	if r.rn.diagCalls != 1 || r.rn.releasedAt != 0 {
		t.Fatalf("diag called %d times, with %d releases before it; want once, before the VM went", r.rn.diagCalls, r.rn.releasedAt)
	}
	if !strings.Contains(r.logged(), "Runner connect error: 401") {
		t.Fatalf("diag tail not logged:\n%s", r.logged())
	}
}

func TestAcquireTimeoutFailsSlot(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.waitFor("blocked Acquire", func() bool { return len(r.fl.Waiting()) == 1 })
	r.advance(15 * time.Minute)
	if got := r.stateOf("slot-1"); got != Failed {
		t.Fatalf("slot is %s, want FAILED", got)
	}
	r.waitFor("Acquire to be cancelled", func() bool { return len(r.fl.Waiting()) == 0 })
}

func TestFailureBackoffDoublesAndDoneOutOfBusyResets(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.boot("vm-1")
	r.exit("vm-1", 1)
	r.wantState("vm-1", Failed)
	r.advance(backoffBase - time.Second)
	if got := r.slotCount(); got != 1 {
		t.Fatalf("respawned inside the backoff: slots = %d", got)
	}
	r.advance(time.Second)
	r.boot("vm-2")
	r.exit("vm-2", 1)
	r.wantState("vm-2", Failed)
	r.advance(2*backoffBase - time.Second)
	if got := r.slotCount(); got != 2 {
		t.Fatalf("respawned inside the doubled backoff: slots = %d", got)
	}
	r.advance(time.Second)
	r.busy("vm-3")
	r.scale(completed("vm-3"))
	r.exit("vm-3", 0)
	r.wantState("vm-3", Done)
	r.assigned(1)
	r.boot("vm-4")
	r.exit("vm-4", 1)
	r.wantState("vm-4", Failed)
	r.advance(backoffBase)
	if got := r.slotCount(); got != 5 {
		t.Fatalf("slots = %d, want a respawn after one base backoff once DONE reset the count", got)
	}
}

func TestFiveFailuresInARowStopSpawningAndReturnTheCause(t *testing.T) {
	r := newRig(t)
	r.s.tick = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exit := make(chan error, 1)
	go func() { exit <- r.s.Run(ctx) }()

	r.assigned(1)
	for i := 1; i <= breakerLimit; i++ {
		name := fmt.Sprintf("vm-%d", i)
		r.boot(name)
		r.exit(name, 9)
		r.wantState(name, Failed)
		r.advance(15 * time.Minute)
	}
	select {
	case err := <-exit:
		if err == nil || !strings.Contains(err.Error(), "exit code 9") {
			t.Fatalf("Run = %v, want an error naming the last cause", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return")
	}
	if got := r.slotCount(); got != breakerLimit {
		t.Fatalf("slots = %d, want spawning stopped at %d", got, breakerLimit)
	}
}

func TestDrainRemovesListeningAtOnceAndWaitsForBusy(t *testing.T) {
	r := newRig(t)
	r.assigned(2)
	r.busy("vm-1")
	r.listening("vm-2")
	r.assigned(3)
	r.waitFor("nothing blocked", func() bool { return len(r.fl.Waiting()) == 0 })

	drained := make(chan error, 1)
	go func() { drained <- r.s.Drain(context.Background()) }()
	r.wantState("vm-2", Done)
	r.wantReleased(1)
	if got := r.stateOfRunner("vm-1"); got != Busy {
		t.Fatalf("busy slot is %s", got)
	}
	select {
	case <-drained:
		t.Fatal("Drain returned with a BUSY slot")
	case <-time.After(20 * time.Millisecond):
	}

	r.scale(completed("vm-1"))
	r.exit("vm-1", 0)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
	r.wantReleased(2)
}

func TestDrainCancelsAcquiringSlotsAndStopsSpawning(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.waitFor("blocked Acquire", func() bool { return len(r.fl.Waiting()) == 1 })
	if err := r.s.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.stateOf("slot-1"); got != Cancelled {
		t.Fatalf("slot is %s, want CANCELLED", got)
	}
	r.assigned(2)
	if got := r.slotCount(); got != 1 {
		t.Fatalf("spawned during drain: slots = %d", got)
	}
}

func TestStopCancelsEverySlotWithinTheBound(t *testing.T) {
	r := newRig(t)
	r.assigned(2)
	r.busy("vm-1")
	r.waitFor("second Acquire", func() bool { return len(r.fl.Waiting()) == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := r.s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("Stop took %s", time.Since(start))
	}
	if got := r.live(); got != 0 {
		t.Fatalf("live = %d after Stop", got)
	}
	if got := r.stateOfRunner("vm-1"); got != Cancelled {
		t.Fatalf("busy slot is %s, want CANCELLED", got)
	}
	r.wantReleased(1)
}

func TestStopRemovesRegistrationsOfRunnersThatAreNotBusy(t *testing.T) {
	r := newRig(t)
	r.assigned(2)
	r.busy("vm-1")
	r.waitFor("second Acquire", func() bool { return len(r.fl.Waiting()) == 1 })
	r.listening("vm-2")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if ref, _ := r.api.GetRunnerByName(ctx, "vm-2"); ref != nil {
		t.Error("idle runner vm-2 is still registered after Stop")
	}
	if ref, _ := r.api.GetRunnerByName(ctx, "vm-1"); ref == nil {
		t.Error("busy runner vm-1 was removed by Stop")
	}
}

func TestStopLogsRegistrationRemovalFailures(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.listening("vm-1")
	r.api.SetRemoveErr(errors.New("github down"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := r.logged(); !strings.Contains(got, "removing runner at stop failed") || !strings.Contains(got, "github down") {
		t.Errorf("failure not logged:\n%s", got)
	}
}

func TestDrainStopsAcquiringJobs(t *testing.T) {
	r := newRig(t)
	if err := r.s.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.scale(&scaleset.RunnerScaleSetMessage{
		MessageID:            1,
		Statistics:           stats(1),
		JobAvailableMessages: []*scaleset.JobAvailable{{JobMessageBase: scaleset.JobMessageBase{RunnerRequestID: 7}}},
	})
	if len(r.session.Acquired) != 0 {
		t.Fatalf("AcquireJobs called while draining: %v", r.session.Acquired)
	}
}

func (r *rig) grantOccupied(name string) {
	r.t.Helper()
	r.waitFor("a blocked Acquire", func() bool { return len(r.fl.Waiting()) > 0 })
	r.fl.Grant(r.fl.Waiting()[0], &fleet.FakeInstance{ID: name})
}

func TestRunnerExistsDoesNotCountTowardBreaker(t *testing.T) {
	r := newRig(t)
	r.s.tick = time.Millisecond
	r.api.Runners["vm-1"] = &scaleset.RunnerReference{ID: 1, Name: "vm-1"}
	r.api.RejectDuplicates = true
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	ctx, cancel := context.WithCancel(context.Background())
	exit := make(chan error, 1)
	go func() { exit <- r.s.Run(ctx) }()

	r.assigned(1)
	for i := 1; i <= 2*breakerLimit; i++ {
		r.grantOccupied("vm-1")
		r.wantReleased(i)
		r.advance(occupiedBackoff)
	}
	cancel()
	if err := <-exit; err != nil {
		t.Fatalf("Run = %v, want nil: an occupied name is not a slot failure", err)
	}
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if r.s.failures != 0 {
		t.Fatalf("failures = %d, want 0", r.s.failures)
	}
}

func TestRunnerExistsBackoffIsFlatAndRecovers(t *testing.T) {
	r := newRig(t)
	r.api.Runners["vm-1"] = &scaleset.RunnerReference{ID: 1, Name: "vm-1"}
	r.api.RejectDuplicates = true
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.assigned(1)
	for i := 1; i <= 3; i++ {
		r.grantOccupied("vm-1")
		r.wantReleased(i)
		r.advance(occupiedBackoff - time.Second)
		if got := r.slotCount(); got != i {
			t.Fatalf("respawned inside the flat backoff: slots = %d, want %d", got, i)
		}
		r.advance(time.Second)
	}
	r.api.SetRemoveErr(nil)
	r.grantOccupied("vm-1")
	r.wantReleased(4)
	r.waitFor("the idle survivor to be deregistered", func() bool { return len(r.api.RemovedRunners()) == 1 })
	r.advance(occupiedBackoff)
	r.listening("vm-1")
}

func TestNonConflictJitErrorStillCountsTowardBreaker(t *testing.T) {
	r := newRig(t)
	r.api.SetJitErr(errors.New("500 server error"))
	r.assigned(1)
	r.grantOccupied("vm-1")
	r.wantReleased(1)
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if r.s.failures != 1 {
		t.Fatalf("failures = %d, want 1", r.s.failures)
	}
}

// restore reports an acquisition from an earlier run for the instance name,
// with its runner registered at GitHub, and applies a poll so the scaler
// adopts it.
func (r *rig) restore(key, name string) {
	r.t.Helper()
	if _, err := r.api.GenerateJitRunnerConfig(context.Background(), name, 1); err != nil {
		r.t.Fatal(err)
	}
	r.restoreUnregistered(key, name)
}

func (r *rig) restoreUnregistered(key, name string) {
	r.t.Helper()
	r.fl.Restore(key, &fleet.FakeInstance{ID: name})
	r.scale(nil)
	r.waitFor("a follower on "+name, func() bool { return r.rn.follower(name) != nil })
}

func (r *rig) follow(name string, res result) { r.rn.follower(name).exit <- res }

// settledAdopting waits until no classification attempt is in flight.
func (r *rig) settledAdopting(name string) {
	r.t.Helper()
	r.waitFor(name+" to be idle", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return !r.s.find(name).reaping
	})
}

func TestAdoptedIdleRunnerIsRemovedAndItsVMReleased(t *testing.T) {
	r := newRig(t)
	r.restore("old-1", "vm-1")
	r.wantState("vm-1", Done)
	r.wantReleased(1)
	if got := fmt.Sprint(r.log.list()); got != "[remove release]" {
		t.Fatalf("order = %s, want remove then release", got)
	}
	if got := r.fl.Released()[0]; got != "old-1" {
		t.Fatalf("released %q, want the restored key", got)
	}
}

func TestAdoptedRunnerGitHubDoesNotKnowIsDone(t *testing.T) {
	r := newRig(t)
	r.restoreUnregistered("old-1", "vm-1")
	r.wantState("vm-1", Done)
	r.wantReleased(1)
}

func TestAdoptedRunnerWithAJobIsBusyUntilJobCompleted(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.restore("old-1", "vm-1")
	r.wantState("vm-1", Busy)
	r.advance(time.Hour)
	if n := len(r.fl.Released()); n != 0 {
		t.Fatalf("released %d times while the job runs", n)
	}

	r.scale(completed("vm-1"))
	r.follow("vm-1", result{})
	r.wantState("vm-1", Done)
	r.wantReleased(1)
}

func TestAdoptedBusyRunnerThatExitedWaitsForJobCompleted(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(errors.New("502"))
	r.restore("old-1", "vm-1")
	r.follow("vm-1", result{code: 0})
	r.waitFor("the exit to be seen", func() bool {
		r.s.mu.Lock()
		defer r.s.mu.Unlock()
		return r.s.find("vm-1").exited
	})
	r.settledAdopting("vm-1")
	if got := r.stateOfRunner("vm-1"); got != Adopting {
		t.Fatalf("slot is %s, want ADOPTING while RemoveRunner fails", got)
	}

	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.scale(nil)
	r.wantState("vm-1", Busy)
	r.advance(time.Hour)
	r.settledAdopting("vm-1")
	if got := r.stateOfRunner("vm-1"); got != Busy {
		t.Fatalf("slot is %s after the grace, want BUSY until JobCompleted", got)
	}
	if n := len(r.fl.Released()); n != 0 {
		t.Fatalf("released %d times while the job runs", n)
	}

	r.scale(completed("vm-1"))
	r.wantState("vm-1", Done)
	r.wantReleased(1)
}

func TestAdoptedRunnerNeverStartedIsDoneUnlessItHasAJob(t *testing.T) {
	never := result{err: errors.New("the runner was never started in the guest")}
	t.Run("no job", func(t *testing.T) {
		r := newRig(t)
		r.api.SetRemoveErr(errors.New("502"))
		r.restore("old-1", "vm-1")
		r.follow("vm-1", never)
		r.api.SetRemoveErr(nil)
		r.scale(nil)
		r.wantState("vm-1", Done)
		r.wantReleased(1)
	})
	t.Run("job running", func(t *testing.T) {
		r := newRig(t)
		r.api.SetRemoveErr(scaleset.JobStillRunningError)
		r.restore("old-1", "vm-1")
		r.follow("vm-1", never)
		r.wantState("vm-1", Busy)
		r.advance(time.Hour)
		r.settledAdopting("vm-1")
		if n := len(r.fl.Released()); n != 0 {
			t.Fatalf("released %d times while the job runs", n)
		}
	})
}

func TestAdoptedRemoveRunnerErrorRetriesOnTheNextPoll(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(errors.New("502"))
	r.restore("old-1", "vm-1")
	r.waitFor("a failed attempt", func() bool {
		return strings.Contains(r.logged(), "retrying next poll")
	})
	r.settledAdopting("vm-1")
	if got := r.stateOfRunner("vm-1"); got != Adopting {
		t.Fatalf("slot is %s, want ADOPTING", got)
	}
	if n := len(r.fl.Released()); n != 0 {
		t.Fatalf("released %d times after a failed removal", n)
	}
	r.api.SetRemoveErr(nil)
	r.scale(nil)
	r.wantState("vm-1", Done)
	r.wantReleased(1)
}

func TestAdoptedRunnerUnreachableIsReaped(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(errors.New("502"))
	r.restore("old-1", "vm-1")
	r.settledAdopting("vm-1")
	r.follow("vm-1", result{err: fmt.Errorf("%w: guest unreachable", ErrLost)})
	r.wantState("vm-1", Reaping)

	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.scale(nil)
	r.wantState("vm-1", Busy)
	r.api.SetRemoveErr(scaleset.RunnerNotFoundError)
	r.advance(time.Minute)
	r.wantState("vm-1", Done)
	r.wantReleased(1)
}

func TestAdoptedSlotsCountAsLiveAndSpawnNothing(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.restore("old-1", "vm-1")
	r.wantState("vm-1", Busy)
	r.assigned(1)
	r.advance(time.Minute)
	if got := r.live(); got != 1 {
		t.Fatalf("live = %d, want the adopted slot only", got)
	}
	if got := r.slotCount(); got != 0 {
		t.Fatalf("spawned %d slots for a job the adopted runner holds", got)
	}
	if w := r.fl.Waiting(); len(w) != 0 {
		t.Fatalf("blocked Acquires = %v", w)
	}
}

func TestNothingSpawnsUntilTheRestoreIsOver(t *testing.T) {
	r := newRig(t)
	r.fl.SetRestoring(true)
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.assigned(2)
	r.advance(time.Minute)
	if got := r.slotCount(); got != 0 {
		t.Fatalf("spawned %d slots during the restore", got)
	}

	r.restore("old-1", "vm-1")
	r.wantState("vm-1", Busy)
	r.fl.SetRestoring(false)
	r.assigned(2)
	r.waitFor("one blocked Acquire", func() bool { return len(r.fl.Waiting()) == 1 })
	if got := r.slotCount(); got != 1 {
		t.Fatalf("spawned %d slots, want one beside the adopted slot", got)
	}
}

func TestSlotKeysNeverMeetARestoredKey(t *testing.T) {
	spawn := func(restored string) string {
		api := github.NewFake()
		fl := fleet.NewFake(fleet.Options{})
		s := New(Config{MaxInstances: 1, InstanceAcquireTimeout: time.Hour}, api.Session, api, fl, &fakeRunner{api: api, runs: map[string]*run{}, attached: map[string]*run{}, fl: fl})
		t.Cleanup(func() { _ = s.Stop(context.Background()) })
		if restored != "" {
			fl.Restore(restored, &fleet.FakeInstance{ID: "vm-0"})
		}
		if err := s.Scale(context.Background(), &scaleset.RunnerScaleSetMessage{Statistics: stats(2)}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for len(fl.Waiting()) == 0 {
			if time.Now().After(deadline) {
				t.Fatal("no Acquire")
			}
			time.Sleep(time.Millisecond)
		}
		return fl.Waiting()[0]
	}
	first, second := spawn(""), spawn("")
	if first == second {
		t.Fatalf("two runs minted the key %q", first)
	}
	if ok, _ := regexp.MatchString(`^[0-9a-f]{8}-1$`, first); !ok {
		t.Fatalf("key %q is not <run>-<n> with an 8 hex digit run", first)
	}
	if got := spawn(first); got == first {
		t.Fatalf("a new run minted the restored key %q", first)
	}
}

func TestDrainWaitsForAdoptingSlotsToClassify(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(errors.New("502"))
	r.restore("old-1", "vm-1")
	r.settledAdopting("vm-1")

	drained := make(chan error, 1)
	go func() { drained <- r.s.Drain(context.Background()) }()
	select {
	case <-drained:
		t.Fatal("Drain returned with an ADOPTING slot")
	case <-time.After(20 * time.Millisecond):
	}
	if got := r.stateOfRunner("vm-1"); got != Adopting {
		t.Fatalf("slot is %s, want ADOPTING", got)
	}

	r.api.SetRemoveErr(nil)
	r.scale(nil)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return once the slot classified")
	}
	r.wantReleased(1)
}

func TestDrainWaitsForAdoptedBusyJob(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	r.restore("old-1", "vm-1")
	r.wantState("vm-1", Busy)

	drained := make(chan error, 1)
	go func() { drained <- r.s.Drain(context.Background()) }()
	select {
	case <-drained:
		t.Fatal("Drain returned with a BUSY adopted slot")
	case <-time.After(20 * time.Millisecond):
	}
	r.scale(completed("vm-1"))
	r.follow("vm-1", result{})
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
	r.wantReleased(1)
}

func TestDrainWaitsForTheRestoreAndAdoptsWhatItFinds(t *testing.T) {
	r := newRig(t)
	r.fl.SetRestoring(true)
	r.api.SetRemoveErr(scaleset.JobStillRunningError)

	drained := make(chan error, 1)
	go func() { drained <- r.s.Drain(context.Background()) }()
	select {
	case <-drained:
		t.Fatal("Drain returned while the restore was under way")
	case <-time.After(20 * time.Millisecond):
	}

	r.restore("old-1", "vm-1")
	r.wantState("vm-1", Busy)
	r.fl.SetRestoring(false)
	r.advance(time.Second)
	select {
	case <-drained:
		t.Fatal("Drain returned with the adopted job running")
	case <-time.After(20 * time.Millisecond):
	}
	r.scale(completed("vm-1"))
	r.follow("vm-1", result{})
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
}

func TestDrainWaitsForReapingSlots(t *testing.T) {
	r := newRig(t)
	r.assigned(1)
	r.listening("vm-1")
	r.api.SetRemoveErr(errors.New("502"))

	drained := make(chan error, 1)
	go func() { drained <- r.s.Drain(context.Background()) }()
	r.wantState("vm-1", Reaping)
	select {
	case <-drained:
		t.Fatal("Drain returned with a REAPING slot")
	case <-time.After(20 * time.Millisecond):
	}
	r.api.SetRemoveErr(nil)
	r.scale(nil)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return once the runner was removed")
	}
}

func TestStopCancelsAdoptedSlotsAndRemovesTheirRegistrations(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(errors.New("502"))
	r.restore("old-1", "vm-1")
	r.settledAdopting("vm-1")
	r.api.SetRemoveErr(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if got := r.stateOfRunner("vm-1"); got != Cancelled {
		t.Fatalf("slot is %s, want CANCELLED", got)
	}
	r.wantReleased(1)
	if n := len(r.api.RemovedRunners()); n != 1 {
		t.Fatalf("removed %d registrations, want 1", n)
	}
}

func TestStoppedScalerAdoptsNothing(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	r.fl.Restore("old-1", &fleet.FakeInstance{ID: "vm-1"})
	r.scale(nil)
	r.advance(time.Second)
	if got := r.live(); got != 0 {
		t.Fatalf("live = %d after Stop", got)
	}
}

// An adopted runner that exited or never started cannot say when its job is
// over, so GitHub is asked again every probe interval: a JobCompleted consumed
// before the restart must not strand the VM.
func TestAdoptedBusyRunnerThatEndedIsReapedOnceGitHubForgetsIt(t *testing.T) {
	cases := map[string]result{
		"exited":        {code: 0},
		"never started": {err: errors.New("the runner was never started in the guest")},
	}
	for name, res := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			r.api.SetRemoveErr(scaleset.JobStillRunningError)
			r.restore("old-1", "vm-1")
			r.wantState("vm-1", Busy)
			r.follow("vm-1", res)
			r.waitFor("the end to be recorded", func() bool {
				r.s.mu.Lock()
				defer r.s.mu.Unlock()
				return r.s.find("vm-1").lost
			})
			r.advance(time.Hour)
			r.settledAdopting("vm-1")
			if got := r.stateOfRunner("vm-1"); got != Busy {
				t.Fatalf("slot is %s while GitHub reports the job running, want BUSY", got)
			}

			r.api.SetRemoveErr(scaleset.RunnerNotFoundError)
			r.advance(time.Minute)
			r.wantState("vm-1", Done)
			r.wantReleased(1)
		})
	}
}

func TestDrainReturnsWhenTheRestoreEndsWithNothingToAdopt(t *testing.T) {
	r := newRig(t)
	r.fl.SetRestoring(true)

	drained := make(chan error, 1)
	go func() { drained <- r.s.Drain(context.Background()) }()
	select {
	case <-drained:
		t.Fatal("Drain returned while the restore was under way")
	case <-time.After(20 * time.Millisecond):
	}

	r.fl.SetRestoring(false)
	r.advance(time.Second)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return after the restore ended with nothing to adopt")
	}
}

func (r *rig) removes() int {
	n := 0
	for _, e := range r.log.list() {
		if e == "remove" {
			n++
		}
	}
	return n
}

// A BUSY slot whose runner is gone is asked about at GitHub no more often than
// 30 s, not on every tick.
func TestLostBusyRunnerAsksGitHubOncePerProbeInterval(t *testing.T) {
	lost := map[string]func(r *rig){
		"lost": func(r *rig) {
			r.assigned(1)
			r.busy("vm-1")
			r.api.SetRemoveErr(scaleset.JobStillRunningError)
			r.rn.get("vm-1").exit <- result{err: ErrLost}
		},
		"adopted and exited": func(r *rig) {
			r.api.SetRemoveErr(scaleset.JobStillRunningError)
			r.restore("old-1", "vm-1")
			r.wantState("vm-1", Busy)
			r.follow("vm-1", result{})
		},
	}
	for name, setup := range lost {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			setup(r)
			r.waitFor("the loss to be probed", func() bool {
				r.s.mu.Lock()
				defer r.s.mu.Unlock()
				sl := r.s.find("vm-1")
				return sl.lost && sl.state == Busy && !sl.reaping
			})
			for round := 1; round <= 2; round++ {
				before := r.removes()
				for range 29 {
					r.advance(time.Second)
					r.settledAdopting("vm-1")
				}
				if got := r.removes(); got != before {
					t.Fatalf("round %d: %d probes before the interval ran out", round, got-before)
				}
				r.advance(time.Second)
				r.waitFor("a probe once the interval ran out", func() bool { return r.removes() == before+1 })
				r.settledAdopting("vm-1")
				if got := r.stateOfRunner("vm-1"); got != Busy {
					t.Fatalf("round %d: slot is %s, want BUSY while the job runs", round, got)
				}
			}
		})
	}
}

func TestRepeatedRestoredKeyAdoptsOnlyTheFirst(t *testing.T) {
	r := newRig(t)
	r.api.SetRemoveErr(scaleset.JobStillRunningError)
	if _, err := r.api.GenerateJitRunnerConfig(context.Background(), "vm-1", 1); err != nil {
		t.Fatal(err)
	}
	r.fl.Restore("old-1", &fleet.FakeInstance{ID: "vm-1"})
	r.fl.Restore("old-1", &fleet.FakeInstance{ID: "vm-2"})
	r.scale(nil)
	r.waitFor("a follower on vm-1", func() bool { return r.rn.follower("vm-1") != nil })
	r.waitFor("the repeat to be logged", func() bool {
		return strings.Contains(r.logged(), "ignoring a repeated restored acquisition")
	})
	r.wantState("vm-1", Busy)
	if r.rn.follower("vm-2") != nil {
		t.Fatal("the repeated key started a second follower")
	}
	if n := r.live(); n != 1 {
		t.Fatalf("live = %d, want the one adopted slot", n)
	}
}
