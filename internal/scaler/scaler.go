// Package scaler turns scale set messages into single-use runner VMs and
// follows each one through its slot state machine.
package scaler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-hclog"

	"gitlab.com/dunn.dev/forgehand/internal/fleet"
)

const (
	dropDebounce    = 10 * time.Second
	completionGrace = 2 * time.Minute
	backoffBase     = 5 * time.Second
	occupiedBackoff = 30 * time.Second
	probeInterval   = 30 * time.Second
	breakerLimit    = 5
	reapTimeout     = 30 * time.Second
	diagTimeout     = 10 * time.Second
	slotHistory     = 256
	tickInterval    = time.Second
)

// ErrLost is returned by Runner.Run when the runner could no longer be
// reached and its fate is unknown.
var ErrLost = errors.New("runner unreachable")

// Hooks are called by Runner.Run as the runner prints its markers.
type Hooks struct {
	Listening  func()
	RunningJob func()
}

// Runner mints a JIT config, starts a runner on an instance and follows it.
type Runner interface {
	// Run blocks until the runner process exits and returns its exit code.
	// It returns ErrLost when the process cannot be reached any more, and
	// another error when the runner could not be started.
	Run(ctx context.Context, inst fleet.Instance, name string, hooks Hooks) (int, error)
	// Attach follows a runner an earlier daemon run started on inst, from the
	// start of its log, and returns as Run does. A runner that was never
	// started is an error.
	Attach(ctx context.Context, inst fleet.Instance, name string, hooks Hooks) (int, error)
	// Diag returns the tail of the runner's diagnostic log on inst.
	Diag(ctx context.Context, inst fleet.Instance) string
}

// Acquirer is the listener's session as far as the scaler needs it.
type Acquirer interface {
	AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error)
}

// Registry is GitHub's runner registry.
type Registry interface {
	GetRunnerByName(ctx context.Context, name string) (*scaleset.RunnerReference, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

type Config struct {
	MaxInstances           int
	ListenTimeout          time.Duration
	IdleTimeout            time.Duration
	InstanceAcquireTimeout time.Duration
	Logger                 hclog.Logger
	Now                    func() time.Time
	// Run prefixes the keys of the slots this run spawns, so they never equal
	// a restored acquisition's. Empty means a random value.
	Run string
}

// Scaler implements listener.Scaler. One slot holds one VM for one job.
type Scaler struct {
	cfg      Config
	client   Acquirer
	registry Registry
	fleet    fleet.Fleet
	runner   Runner
	log      hclog.Logger
	now      func() time.Time
	tick     time.Duration

	mu        sync.Mutex
	stats     scaleset.RunnerScaleSetStatistic
	slots     map[string]*slot
	seq       int
	overSince time.Time
	failures  int
	nextSpawn time.Time
	fatal     error
	restoring bool
	draining  bool
	stopped   bool
	drained   chan struct{}
	wg        sync.WaitGroup
}

func New(cfg Config, client Acquirer, registry Registry, fl fleet.Fleet, runner Runner) *Scaler {
	log := cfg.Logger
	if log == nil {
		log = hclog.NewNullLogger()
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	if cfg.Run == "" {
		var b [4]byte
		_, _ = rand.Read(b[:])
		cfg.Run = hex.EncodeToString(b[:])
	}
	return &Scaler{
		cfg:      cfg,
		client:   client,
		registry: registry,
		fleet:    fl,
		runner:   runner,
		log:      log,
		now:      now,
		tick:     tickInterval,
		slots:    map[string]*slot{},
		drained:  make(chan struct{}),
	}
}

// isDraining reports whether Drain has begun. A draining daemon leaves jobs
// unacquired so GitHub offers them elsewhere.
func (s *Scaler) isDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.draining
}

// Scale applies one poll. AcquireJobs failing is the only error it returns:
// the listener then leaves the message unacked and redelivers it, and nothing
// else has been touched.
func (s *Scaler) Scale(ctx context.Context, msg *scaleset.RunnerScaleSetMessage) error {
	if msg != nil {
		var ids []int64
		for _, j := range msg.JobAvailableMessages {
			ids = append(ids, j.RunnerRequestID)
		}
		if len(ids) > 0 && !s.isDraining() {
			if _, err := s.client.AcquireJobs(ctx, ids); err != nil {
				return fmt.Errorf("acquiring jobs: %w", err)
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	if msg != nil {
		if msg.Statistics != nil {
			s.stats = *msg.Statistics
		}
		for _, j := range msg.JobStartedMessages {
			s.jobStarted(j.RunnerName)
		}
		for _, j := range msg.JobCompletedMessages {
			s.jobCompleted(j)
		}
	}
	for _, sl := range s.slots {
		if sl.state == Reaping || sl.state == Adopting {
			s.reap(sl)
		}
	}
	s.evaluate()
	return nil
}

// Run applies the timers until ctx ends. It returns the error that tripped
// the failure breaker.
func (s *Scaler) Run(ctx context.Context) error {
	t := time.NewTicker(s.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.mu.Lock()
			s.evaluate()
			err := s.fatal
			s.mu.Unlock()
			if err != nil {
				return err
			}
		}
	}
}

// Drain stops spawning, cancels slots still waiting for a VM, reaps idle
// runners at once and returns when every slot is terminal.
func (s *Scaler) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	s.adoptRestored()
	for _, sl := range s.slots {
		switch sl.state {
		case Acquiring:
			s.terminate(sl, Cancelled)
		case Listening:
			s.toReaping(sl)
		}
	}
	s.settled()
	s.mu.Unlock()

	select {
	case <-s.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop cancels every slot, in-flight jobs included, removes the GitHub
// registrations of the runners that are not busy, and waits for the slot
// goroutines until ctx ends.
func (s *Scaler) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.stopped = true
	var registered []string
	for _, sl := range s.slots {
		switch sl.state {
		case Starting, Listening, Reaping, Adopting:
			registered = append(registered, sl.name)
		}
		s.terminate(sl, Cancelled)
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, name := range registered {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := RemoveRunner(ctx, s.registry, name)
				if err != nil && !errors.Is(err, scaleset.RunnerNotFoundError) && !errors.Is(err, scaleset.JobStillRunningError) {
					s.log.Warn("removing runner at stop failed", "runner", name, "error", err)
				}
			}()
		}
		wg.Wait()
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Scaler) evaluate() {
	now := s.now()
	if !s.stopped {
		s.adoptRestored()
	}
	for _, sl := range s.slots {
		switch sl.state {
		case Acquiring:
			if now.Sub(sl.since) >= s.cfg.InstanceAcquireTimeout {
				s.fail(sl, "no instance within instance_acquire_timeout: check the fleeting plugin and host capacity")
			}
		case Starting:
			if !sl.timedOut && now.Sub(sl.since) >= s.cfg.ListenTimeout {
				sl.timedOut = true
				s.wg.Add(1)
				go s.listenTimeout(sl)
			}
		case Listening:
			if now.Sub(sl.since) >= s.cfg.IdleTimeout {
				s.toReaping(sl)
			}
		case Busy:
			s.settle(sl, now)
			if sl.state == Busy && sl.lost && !now.Before(sl.probeAt) {
				s.toReaping(sl)
			}
		}
	}

	if s.stopped || s.draining {
		return
	}
	target := min(s.cfg.MaxInstances, s.stats.TotalAssignedJobs)
	live := s.live()
	if live > target {
		if s.overSince.IsZero() {
			s.overSince = now
		} else if now.Sub(s.overSince) >= dropDebounce {
			s.cancelNewest(live - target)
			s.overSince = time.Time{}
		}
	} else {
		s.overSince = time.Time{}
	}
	if s.fatal == nil && !s.restoring && !now.Before(s.nextSpawn) {
		for ; live < target; live++ {
			s.spawn(now)
		}
	}
}

func (s *Scaler) live() int {
	n := 0
	for _, sl := range s.slots {
		if !sl.state.Terminal() {
			n++
		}
	}
	return n
}

func (s *Scaler) cancelNewest(n int) {
	var waiting []*slot
	for _, sl := range s.slots {
		if sl.state == Acquiring {
			waiting = append(waiting, sl)
		}
	}
	slices.SortFunc(waiting, func(a, b *slot) int { return b.seq - a.seq })
	for _, sl := range waiting[:min(n, len(waiting))] {
		s.log.Info("demand dropped: cancelling slot", "slot", sl.key)
		s.terminate(sl, Cancelled)
	}
}

func (s *Scaler) spawn(now time.Time) {
	s.seq++
	ctx, cancel := context.WithCancel(context.Background())
	sl := &slot{key: fmt.Sprintf("%s-%d", s.cfg.Run, s.seq), seq: s.seq, since: now, cancel: cancel}
	s.slots[sl.key] = sl
	s.prune()
	s.wg.Add(1)
	go s.run(ctx, sl)
}

func (s *Scaler) prune() {
	if len(s.slots) <= slotHistory {
		return
	}
	var done []*slot
	for _, sl := range s.slots {
		if sl.state.Terminal() {
			done = append(done, sl)
		}
	}
	slices.SortFunc(done, func(a, b *slot) int { return a.seq - b.seq })
	for _, sl := range done[:len(done)/2] {
		delete(s.slots, sl.key)
	}
}

func (s *Scaler) run(ctx context.Context, sl *slot) {
	defer s.wg.Done()
	inst, err := s.fleet.Acquire(ctx, sl.key)

	s.mu.Lock()
	if sl.state != Acquiring {
		if err == nil {
			s.fleet.Release(sl.key)
		}
		s.mu.Unlock()
		return
	}
	if err != nil {
		s.fail(sl, fmt.Sprintf("acquiring an instance: %v: check the fleeting plugin", err))
		s.mu.Unlock()
		return
	}
	sl.inst = inst
	sl.name = inst.InstanceID()
	sl.acquired = true
	s.move(sl, Starting)
	s.mu.Unlock()

	code, err := s.runner.Run(ctx, inst, sl.name, s.hooks(sl))
	s.locked(func() {
		s.exited(sl, code, err)
		s.evaluate()
	})
}

func (s *Scaler) hooks(sl *slot) Hooks {
	return Hooks{
		Listening:  func() { s.locked(func() { s.listening(sl) }) },
		RunningJob: func() { s.locked(func() { s.running(sl) }) },
	}
}

func (s *Scaler) locked(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
}

func (s *Scaler) listening(sl *slot) {
	if sl.state != Starting || !s.move(sl, Listening) {
		return
	}
	if s.draining {
		s.toReaping(sl)
	}
}

func (s *Scaler) running(sl *slot) {
	if sl.state == Listening {
		s.move(sl, Busy)
		return
	}
	s.log.Debug("ignoring running-job marker", "slot", sl.key, "state", sl.state)
}

func (s *Scaler) exited(sl *slot, code int, err error) {
	if sl.state.Terminal() {
		return
	}
	if err != nil {
		if sl.state == Starting {
			if errors.Is(err, scaleset.RunnerExistsError) {
				s.occupied(sl)
				return
			}
			s.fail(sl, fmt.Sprintf("starting the runner: %v", err))
			return
		}
		s.log.Warn("runner lost", "slot", sl.key, "name", sl.name, "error", err)
		sl.lost = true
		if sl.state == Listening || sl.state == Busy {
			s.toReaping(sl)
		}
		return
	}
	sl.exited = true
	if sl.firstEnd.IsZero() {
		sl.firstEnd = s.now()
	}
	switch sl.state {
	case Starting:
		s.fail(sl, "runner exited before it was listening: "+exitCause(code))
	case Listening:
		s.fail(sl, "runner exited while idle: "+exitCause(code))
	}
}

// adoptRestored takes the acquisitions the fleet restored from an earlier run
// and gives each an ADOPTING slot, and notes whether the restore is under way:
// nothing spawns, and a drain does not settle, until it is over.
func (s *Scaler) adoptRestored() {
	adopted, restoring := s.fleet.Restored()
	for _, a := range adopted {
		s.adopt(a)
	}
	was := s.restoring
	s.restoring = restoring
	if was && !restoring {
		s.log.Info("restore finished")
		s.settled()
	}
}

// adopt starts following a restored acquisition. Its runner may be idle,
// running a job, or gone, which only GitHub can tell: RemoveRunner refuses to
// remove one that runs a job. The runner is followed from the start of its log
// meanwhile.
func (s *Scaler) adopt(a fleet.Adopted) {
	if _, dup := s.slots[a.Key]; dup {
		s.log.Warn("ignoring a repeated restored acquisition", "slot", a.Key)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	sl := &slot{key: a.Key, name: a.Instance.InstanceID(), state: Adopting, since: s.now(), cancel: cancel, inst: a.Instance, acquired: true}
	s.slots[sl.key] = sl
	s.prune()
	s.log.Info("adopting a restored instance", "slot", sl.key, "runner", sl.name)
	s.wg.Add(1)
	go s.attach(ctx, sl)
	s.reap(sl)
}

func (s *Scaler) attach(ctx context.Context, sl *slot) {
	defer s.wg.Done()
	code, err := s.runner.Attach(ctx, sl.inst, sl.name, s.hooks(sl))
	s.locked(func() {
		s.attached(sl, code, err)
		s.evaluate()
	})
}

// attached records how the followed runner ended. It never ends the slot: the
// runner's exit says nothing about its job, so GitHub's answer does. A runner
// that is gone is treated as lost, which asks GitHub again every probeInterval
// until the job is over.
func (s *Scaler) attached(sl *slot, code int, err error) {
	if sl.state.Terminal() {
		return
	}
	switch {
	case errors.Is(err, ErrLost):
		s.log.Warn("adopted runner lost", "slot", sl.key, "name", sl.name, "error", err)
		sl.lost = true
		if sl.state == Adopting || sl.state == Busy {
			s.toReaping(sl)
		}
	case err != nil:
		s.log.Warn("adopted runner not running", "slot", sl.key, "name", sl.name, "error", err)
		sl.lost = true
	default:
		s.log.Info("adopted runner exited", "slot", sl.key, "name", sl.name, "cause", exitCause(code))
		sl.exited = true
		sl.lost = true
	}
}

func exitCause(code int) string {
	if code == 7 {
		return "runner retired: bump the golden's runner pin"
	}
	return fmt.Sprintf("exit code %d: check the runner log in the guest", code)
}

func (s *Scaler) jobStarted(name string) {
	sl := s.find(name)
	if sl == nil || sl.state != Listening {
		s.log.Debug("ignoring JobStarted", "runner", name)
		return
	}
	s.move(sl, Busy)
}

func (s *Scaler) jobCompleted(j *scaleset.JobCompleted) {
	name := j.RunnerName
	s.log.Info("job completed", "runner", name, "result", j.Result)
	sl := s.find(name)
	if sl == nil || sl.state.Terminal() || sl.state == Starting {
		s.log.Debug("ignoring JobCompleted", "runner", name)
		return
	}
	sl.completed = true
	if sl.firstEnd.IsZero() {
		sl.firstEnd = s.now()
	}
	if sl.state == Listening {
		s.move(sl, Busy)
	}
}

func (s *Scaler) find(name string) *slot {
	if name == "" {
		return nil
	}
	var found *slot
	for _, sl := range s.slots {
		if sl.name == name && (found == nil || sl.seq > found.seq) {
			found = sl
		}
	}
	return found
}

// settle ends a BUSY slot once the runner and GitHub both report the job over,
// or the grace after the first report runs out. A lost runner never times out:
// only GitHub can say its job is over, so evaluate asks again every
// probeInterval.
func (s *Scaler) settle(sl *slot, now time.Time) {
	over := sl.completed && (sl.exited || sl.lost)
	if !over && !sl.firstEnd.IsZero() && now.Sub(sl.firstEnd) >= completionGrace {
		over = true
	}
	if over && s.terminate(sl, Done) {
		s.failures = 0
		s.nextSpawn = time.Time{}
	}
}

func (s *Scaler) toReaping(sl *slot) {
	if s.move(sl, Reaping) {
		s.reap(sl)
	}
}

// reap tries to remove the runner from GitHub. The slot learns the outcome
// when the attempt ends, and a failed attempt is repeated on the next poll.
func (s *Scaler) reap(sl *slot) {
	if sl.reaping || s.stopped {
		return
	}
	sl.reaping = true
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), reapTimeout)
		defer cancel()
		err := RemoveRunner(ctx, s.registry, sl.name)

		s.mu.Lock()
		defer s.mu.Unlock()
		sl.reaping = false
		if sl.state != Reaping && sl.state != Adopting {
			return
		}
		switch {
		case err == nil || errors.Is(err, scaleset.RunnerNotFoundError):
			s.terminate(sl, Done)
		case errors.Is(err, scaleset.JobStillRunningError):
			sl.probeAt = s.now().Add(probeInterval)
			s.move(sl, Busy)
		default:
			s.log.Warn("removing runner failed, retrying next poll", "runner", sl.name, "error", err)
		}
	}()
}

// RemoveRunner removes the runner called name from GitHub. A runner that is
// not registered yields scaleset.RunnerNotFoundError.
func RemoveRunner(ctx context.Context, reg Registry, name string) error {
	ref, err := reg.GetRunnerByName(ctx, name)
	if err != nil {
		return err
	}
	if ref == nil {
		return scaleset.RunnerNotFoundError
	}
	return reg.RemoveRunner(ctx, int64(ref.ID))
}

// listenTimeout fails a slot whose runner never printed its listening marker,
// after logging the runner's diagnostics while its VM still exists.
func (s *Scaler) listenTimeout(sl *slot) {
	defer s.wg.Done()
	ctx, cancel := context.WithTimeout(context.Background(), diagTimeout)
	defer cancel()
	diag := s.runner.Diag(ctx, sl.inst)
	s.log.Warn("runner never listened", "slot", sl.key, "runner", sl.name, "diag", diag)

	s.mu.Lock()
	defer s.mu.Unlock()
	if sl.state == Starting {
		s.fail(sl, "runner not listening within listen_timeout: check the runner log in the guest and the golden's runner pin")
		s.evaluate()
	}
}

func (s *Scaler) move(sl *slot, to State) bool {
	if !slices.Contains(allowed[sl.state], to) {
		s.log.Debug("ignoring transition", "slot", sl.key, "from", sl.state, "to", to)
		return false
	}
	s.log.Debug("slot moved", "slot", sl.key, "runner", sl.name, "from", sl.state, "to", to)
	sl.state = to
	sl.since = s.now()
	return true
}

// terminate ends a slot and releases its VM.
func (s *Scaler) terminate(sl *slot, to State) bool {
	if !s.move(sl, to) {
		return false
	}
	sl.cancel()
	if sl.acquired {
		s.fleet.Release(sl.key)
	}
	s.settled()
	return true
}

func (s *Scaler) settled() {
	if !s.draining || s.restoring || s.live() > 0 {
		return
	}
	select {
	case <-s.drained:
	default:
		close(s.drained)
	}
}

// fail ends a slot as FAILED and backs spawning off. The fifth failure in a
// row trips the breaker. A slot that reached an instance may have minted a JIT
// config, so its registration is removed on a best-effort basis.
func (s *Scaler) fail(sl *slot, cause string) {
	if !s.terminate(sl, Failed) {
		return
	}
	s.failures++
	s.log.Error("slot failed", "slot", sl.key, "runner", sl.name, "cause", cause)
	s.removeRegistration(sl)
	s.nextSpawn = s.now().Add(backoffBase << (s.failures - 1))
	if s.failures >= breakerLimit && s.fatal == nil {
		s.fatal = fmt.Errorf("%d consecutive slots failed, last: %s", s.failures, cause)
	}
}

// occupied ends a slot whose runner name is still registered, as after a
// daemon crash while a job ran on the instance. It releases the instance
// without counting toward the breaker and retries on a flat delay: the
// registration clears when its job ends or the idle runner is removed here.
func (s *Scaler) occupied(sl *slot) {
	if !s.terminate(sl, Failed) {
		return
	}
	s.log.Warn("runner name still registered, retrying", "slot", sl.key, "runner", sl.name, "retry_in", occupiedBackoff)
	s.removeRegistration(sl)
	s.nextSpawn = s.now().Add(occupiedBackoff)
}

// removeRegistration drops the slot's runner from GitHub in the background. A
// busy runner refuses, and an ephemeral one leaves when its job ends.
func (s *Scaler) removeRegistration(sl *slot) {
	if sl.name == "" {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), reapTimeout)
		defer cancel()
		err := RemoveRunner(ctx, s.registry, sl.name)
		if err != nil && !errors.Is(err, scaleset.RunnerNotFoundError) && !errors.Is(err, scaleset.JobStillRunningError) {
			s.log.Warn("removing failed slot's runner failed", "runner", sl.name, "error", err)
		}
	}()
}
