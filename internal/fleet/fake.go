package fleet

import (
	"context"
	"sync"

	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

// FakeInstance is the Instance a Fake grants.
type FakeInstance struct {
	ID      string
	Info    provider.ConnectInfo
	InfoErr error
}

func (i *FakeInstance) InstanceID() string { return i.ID }

func (i *FakeInstance) InstanceConnectInfo(context.Context) (provider.ConnectInfo, error) {
	return i.Info, i.InfoErr
}

type pending struct {
	key string
	ch  chan Instance
}

// Fake is a Fleet whose Acquire calls block until a test grants them.
type Fake struct {
	opts Options

	mu       sync.Mutex
	waiting  []*pending
	held     map[string]Instance
	released []string
	down     bool

	restoring bool
	adopted   []Adopted
}

var _ Fleet = (*Fake)(nil)

func NewFake(opts Options) *Fake {
	return &Fake{opts: opts, held: map[string]Instance{}}
}

func (f *Fake) Acquire(ctx context.Context, key string) (Instance, error) {
	p := &pending{key: key, ch: make(chan Instance, 1)}
	f.mu.Lock()
	f.waiting = append(f.waiting, p)
	f.mu.Unlock()

	select {
	case inst := <-p.ch:
		return inst, nil
	case <-ctx.Done():
		f.mu.Lock()
		defer f.mu.Unlock()
		for i, w := range f.waiting {
			if w == p {
				f.waiting = append(f.waiting[:i], f.waiting[i+1:]...)
				return nil, ctx.Err()
			}
		}
		select {
		case inst := <-p.ch:
			return inst, nil
		default:
			return nil, ctx.Err()
		}
	}
}

// Waiting returns the keys of the blocked Acquire calls, oldest first.
func (f *Fake) Waiting() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, len(f.waiting))
	for i, w := range f.waiting {
		keys[i] = w.key
	}
	return keys
}

// Grant satisfies the blocked Acquire for key with inst. It reports whether
// one was waiting.
func (f *Fake) Grant(key string, inst Instance) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, w := range f.waiting {
		if w.key == key {
			f.waiting = append(f.waiting[:i], f.waiting[i+1:]...)
			f.held[key] = inst
			w.ch <- inst
			return true
		}
	}
	return false
}

// Held returns the instance currently granted for key.
func (f *Fake) Held(key string) (Instance, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	inst, ok := f.held[key]
	return inst, ok
}

func (f *Fake) Release(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.held, key)
	f.released = append(f.released, key)
}

// Released returns every key passed to Release, in order.
func (f *Fake) Released() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.released...)
}

func (f *Fake) Shutdown(context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = true
}

func (f *Fake) IsShutdown() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.down
}

// Leftover reports an instance from an earlier run, as the real fleet does
// when it rejects one at start.
func (f *Fake) Leftover(id string) {
	if f.opts.OnLeftover != nil {
		f.opts.OnLeftover(id)
	}
}

// SetRestoring makes Restored report a restore under way, or over.
func (f *Fake) SetRestoring(restoring bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoring = restoring
}

// Restore queues an acquisition from an earlier run for the next Restored
// call. The instance counts as held until it is released.
func (f *Fake) Restore(key string, inst Instance) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held[key] = inst
	f.adopted = append(f.adopted, Adopted{Key: key, Instance: inst})
}

func (f *Fake) Restored() ([]Adopted, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.adopted
	f.adopted = nil
	return out, f.restoring
}
