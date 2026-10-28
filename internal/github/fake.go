package github

import (
	"context"
	"sync"

	"github.com/actions/scaleset"
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
		Session: &FakeSession{},
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

// FakeSession records the calls a listener makes and replays scripted
// messages.
type FakeSession struct {
	mu sync.Mutex

	Messages  []*scaleset.RunnerScaleSetMessage
	Acquired  [][]int64
	Deleted   []int
	AcquireFn func(ids []int64) ([]int64, error)
	Closed    bool
}

var _ Session = (*FakeSession)(nil)

func (s *FakeSession) GetMessage(ctx context.Context, _, _ int) (*scaleset.RunnerScaleSetMessage, error) {
	s.mu.Lock()
	if len(s.Messages) > 0 {
		m := s.Messages[0]
		s.Messages = s.Messages[1:]
		s.mu.Unlock()
		return m, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

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

func (s *FakeSession) Session() scaleset.RunnerScaleSetSession {
	return scaleset.RunnerScaleSetSession{}
}

func (s *FakeSession) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Closed = true
	return nil
}
