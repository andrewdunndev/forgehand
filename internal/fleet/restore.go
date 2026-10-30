package fleet

import (
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/taskscaler/storage"
)

// restoreCeiling bounds how long a restart waits for restored instances to
// come up: the ready wait plus the plugin's address and SSH wait.
const restoreCeiling = 3 * time.Minute

// Adopted is an acquisition restored from an earlier daemon run.
type Adopted struct {
	Key      string
	Instance Instance
}

// restore follows the instances taskscaler restores from storage. It knows
// which instance files to expect before taskscaler starts, collects the
// acquisitions the up function reports, and says when the restore is over.
type restore struct {
	store    storage.Storage
	log      hclog.Logger
	now      func() time.Time
	deadline time.Time

	mu      sync.Mutex
	waiting map[string]struct{}
	// keys holds every acquisition the state files hold, adopted or not.
	keys    []string
	adopted []Adopted
}

// newRestore expects every instance file in store that is not marked for
// removal: only those reach the up function.
func newRestore(store storage.Storage, log hclog.Logger, ceiling time.Duration) (*restore, error) {
	r := &restore{store: store, log: log, now: time.Now, waiting: map[string]struct{}{}}
	instances, err := r.restorable()
	if err != nil {
		return nil, err
	}
	for _, in := range instances {
		r.waiting[in.ID] = struct{}{}
		for _, a := range in.Slots {
			r.keys = append(r.keys, a.Key)
		}
	}
	r.deadline = r.now().Add(ceiling)
	return r, nil
}

func (r *restore) restorable() ([]storage.InstanceState, error) {
	state, err := r.store.Load()
	if err != nil {
		return nil, err
	}
	var out []storage.InstanceState
	for _, in := range state.Instances {
		if !in.Removing {
			out = append(out, in)
		}
	}
	return out, nil
}

// up records an instance's up function as returned, with the acquisitions it
// restored.
func (r *restore) up(id string, adopted []Adopted) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adopted = append(r.adopted, adopted...)
	delete(r.waiting, id)
}

// take returns the acquisitions reported since the last call and whether the
// restore is still under way. It is over when every expected instance has had
// its up function return, taskscaler has dropped or removed what it could not
// restore, or the ceiling has passed.
func (r *restore) take() ([]Adopted, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.adopted
	r.adopted = nil
	if len(r.waiting) == 0 {
		return out, false
	}
	if instances, err := r.restorable(); err == nil {
		live := map[string]struct{}{}
		for _, in := range instances {
			live[in.ID] = struct{}{}
		}
		for id := range r.waiting {
			if _, ok := live[id]; !ok {
				delete(r.waiting, id)
			}
		}
	}
	if len(r.waiting) > 0 && !r.now().Before(r.deadline) {
		r.log.Warn("restore did not finish in time: continuing", "instances", len(r.waiting))
		clear(r.waiting)
	}
	return out, len(r.waiting) > 0
}
