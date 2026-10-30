package fleet

import (
	"math"
	"sync"
	"time"

	"gitlab.com/gitlab-org/fleeting/taskscaler"
	"gitlab.com/gitlab-org/fleeting/taskscaler/storage"
)

// noTrim is an idle time no instance reaches.
const noTrim = time.Duration(math.MaxInt64)

// poolGuard keeps the idle pool whole while released instances are deleted.
// Taskscaler counts such an instance as unavailable capacity until the plugin
// has deleted it, but the provisioner stopped counting it as an instance at
// the release, so the demand for the idle pool reads one too high for each
// of them. Taskscaler asks for a replacement and, once the deletion is over,
// trims the oldest idle instance: the pool instance a restart kept. While
// releases are in flight the guard lowers every schedule's idle count by one
// for each and stops the trimming, until taskscaler drops the instance's
// state from storage.
type poolGuard struct {
	base []taskscaler.Schedule

	// applyMu orders the schedule writes so the last one carries the latest set.
	applyMu sync.Mutex
	mu      sync.Mutex
	ts      taskscaler.Taskscaler
	pending map[string]struct{}
}

func newPoolGuard(base []taskscaler.Schedule) *poolGuard {
	return &poolGuard{base: base, pending: map[string]struct{}{}}
}

// start configures the schedules on ts.
func (g *poolGuard) start(ts taskscaler.Taskscaler) error {
	g.applyMu.Lock()
	defer g.applyMu.Unlock()
	g.mu.Lock()
	g.ts = ts
	g.mu.Unlock()
	return ts.ConfigureSchedule(adjusted(g.base, 0)...)
}

// note records id as released, before taskscaler marks it.
func (g *poolGuard) note(id string) {
	g.mu.Lock()
	g.pending[id] = struct{}{}
	g.mu.Unlock()
	g.apply()
}

// dropped ends the guard for id, which taskscaler no longer holds. The write
// runs apart because taskscaler can call this with its own lock held.
func (g *poolGuard) dropped(id string) {
	g.mu.Lock()
	_, ok := g.pending[id]
	delete(g.pending, id)
	g.mu.Unlock()
	if ok {
		go g.apply()
	}
}

func (g *poolGuard) apply() {
	g.applyMu.Lock()
	defer g.applyMu.Unlock()
	g.mu.Lock()
	ts, n := g.ts, len(g.pending)
	g.mu.Unlock()
	if ts == nil {
		return
	}
	// The only error is a shutdown under way, which needs no schedule.
	_ = ts.ConfigureSchedule(adjusted(g.base, n)...)
}

// adjusted copies base for taskscaler, which writes into what it is given,
// with n releases in flight.
func adjusted(base []taskscaler.Schedule, n int) []taskscaler.Schedule {
	out := make([]taskscaler.Schedule, len(base))
	copy(out, base)
	if n == 0 {
		return out
	}
	for i := range out {
		out[i].IdleCount = max(0, out[i].IdleCount-n)
		out[i].IdleTime = noTrim
	}
	return out
}

// guardedStore tells g when taskscaler drops an instance's state.
type guardedStore struct {
	storage.Storage
	g *poolGuard
}

func (s guardedStore) DeleteInstanceState(id string) error {
	err := s.Storage.DeleteInstanceState(id)
	s.g.dropped(id)
	return err
}
