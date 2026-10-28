package github

import (
	"context"
	"sync"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// Fake is an in-memory API for tests. It has no delete call for scale sets, so
// a caller that tried to delete one would not compile against API.
type Fake struct {
	mu sync.Mutex

	Sets    map[string]*scaleset.RunnerScaleSet
	Runners map[string]*scaleset.RunnerReference
	Session *FakeSession

	GetScaleSetErr error
	RemoveErr      error
	JitErr         error

	Created  []*scaleset.RunnerScaleSet
	Removed  []int64
	JitNames []string
	Owners   []string
	nextID   int
}

var _ API = (*Fake)(nil)

func NewFake() *Fake {
	return &Fake{
		Sets:    map[string]*scaleset.RunnerScaleSet{},
		Runners: map[string]*scaleset.RunnerReference{},
		Session: NewFakeSession(),
		nextID:  100,
	}
}

func (f *Fake) GetRunnerScaleSet(_ context.Context, _ int, name string) (*scaleset.RunnerScaleSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.GetScaleSetErr != nil {
		return nil, f.GetScaleSetErr
	}
	return f.Sets[name], nil
}

func (f *Fake) CreateRunnerScaleSet(_ context.Context, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	created := *set
	f.nextID++
	created.ID = f.nextID
	f.Sets[set.Name] = &created
	f.Created = append(f.Created, set)
	return &created, nil
}

func (f *Fake) MessageSessionClient(_ context.Context, _ int, owner string) (Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Owners = append(f.Owners, owner)
	return f.Session, nil
}

func (f *Fake) GenerateJitRunnerConfig(_ context.Context, name string, _ int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.JitErr != nil {
		return nil, f.JitErr
	}
	f.JitNames = append(f.JitNames, name)
	f.nextID++
	ref := &scaleset.RunnerReference{ID: f.nextID, Name: name}
	f.Runners[name] = ref
	return &scaleset.RunnerScaleSetJitRunnerConfig{Runner: ref, EncodedJITConfig: "jit-" + name}, nil
}

func (f *Fake) GetRunnerByName(_ context.Context, name string) (*scaleset.RunnerReference, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Runners[name], nil
}

func (f *Fake) RemoveRunner(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.RemoveErr != nil {
		return f.RemoveErr
	}
	for name, r := range f.Runners {
		if int64(r.ID) == id {
			delete(f.Runners, name)
			f.Removed = append(f.Removed, id)
			return nil
		}
	}
	return scaleset.RunnerNotFoundError
}

func NewFakeSession() *FakeSession {
	return &FakeSession{
		Info: scaleset.RunnerScaleSetSession{
			SessionID:  uuid.New(),
			Statistics: &scaleset.RunnerScaleSetStatistic{},
		},
		inbox: make(chan *scaleset.RunnerScaleSetMessage, 16),
	}
}

// FakeSession records the calls a listener makes and replays scripted
// errors, then messages, then whatever Push delivers.
type FakeSession struct {
	mu sync.Mutex

	Info      scaleset.RunnerScaleSetSession
	Messages  []*scaleset.RunnerScaleSetMessage
	Errs      []error
	inbox     chan *scaleset.RunnerScaleSetMessage
	Acquired  [][]int64
	Deleted   []int
	AcquireFn func(ids []int64) ([]int64, error)
	Closed    bool
}

var _ Session = (*FakeSession)(nil)

func (s *FakeSession) GetMessage(ctx context.Context, _, _ int) (*scaleset.RunnerScaleSetMessage, error) {
	s.mu.Lock()
	if len(s.Errs) > 0 {
		err := s.Errs[0]
		s.Errs = s.Errs[1:]
		s.mu.Unlock()
		return nil, err
	}
	if len(s.Messages) > 0 {
		m := s.Messages[0]
		s.Messages = s.Messages[1:]
		s.mu.Unlock()
		return m, nil
	}
	s.mu.Unlock()
	select {
	case m := <-s.inbox:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Push delivers m to the listener's next GetMessage.
func (s *FakeSession) Push(m *scaleset.RunnerScaleSetMessage) { s.inbox <- m }

func (s *FakeSession) DeleteMessage(_ context.Context, id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Deleted = append(s.Deleted, id)
	return nil
}

func (s *FakeSession) AcquireJobs(_ context.Context, ids []int64) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Acquired = append(s.Acquired, ids)
	if s.AcquireFn != nil {
		return s.AcquireFn(ids)
	}
	return ids, nil
}

func (s *FakeSession) Session() scaleset.RunnerScaleSetSession { return s.Info }

func (s *FakeSession) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Closed = true
	return nil
}

// OwnerList returns the session owners seen so far.
func (f *Fake) OwnerList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.Owners...)
}

// CreatedSets returns the scale sets created so far.
func (f *Fake) CreatedSets() []*scaleset.RunnerScaleSet {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*scaleset.RunnerScaleSet(nil), f.Created...)
}

// RemovedRunners returns the ids passed to successful RemoveRunner calls.
func (f *Fake) RemovedRunners() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.Removed...)
}

// SetRemoveErr makes RemoveRunner fail with err until it is set to nil.
func (f *Fake) SetRemoveErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.RemoveErr = err
}

func (s *FakeSession) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Closed
}
