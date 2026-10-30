package fleet

import (
	"context"
	"sync"
)

// deletions follows the instances whose acquisition was released until the
// plugin has deleted them. Releasing only marks an instance for deletion; the
// plugin's Decrease runs later.
type deletions struct {
	mu      sync.Mutex
	pending map[string]struct{}
	changed chan struct{}
}

func newDeletions() *deletions {
	return &deletions{pending: map[string]struct{}{}, changed: make(chan struct{})}
}

func (d *deletions) expect(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending[id] = struct{}{}
}

func (d *deletions) done(ids []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ids {
		delete(d.pending, id)
	}
	close(d.changed)
	d.changed = make(chan struct{})
}

// wait returns nil once no expected instance is left, or ctx's error.
func (d *deletions) wait(ctx context.Context) error {
	for {
		d.mu.Lock()
		n, changed := len(d.pending), d.changed
		d.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
