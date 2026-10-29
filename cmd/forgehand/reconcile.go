package main

import (
	"context"
	"errors"
	"time"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-hclog"

	"gitlab.com/dunn.dev/forgehand/internal/github"
)

// reconciler removes the registrations of this scale set that no live slot
// holds: those of VMs a previous run left behind, and those a slot could not
// remove. A busy runner refuses removal until GitHub ends its job, so it is
// retried on every pass.
type reconciler struct {
	api   github.API
	setID int
	held  func() map[string]bool
	log   hclog.Logger
}

// pass lists the repository's runners before reading the held names. A slot
// sets its name before it mints the runner, so a runner that appears in the
// list is held by the time held is read.
func (r reconciler) pass(ctx context.Context) {
	names, err := r.api.ListRunnerNames(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("listing runners to reconcile failed", "error", err)
		}
		return
	}
	held := r.held()
	r.log.Debug("reconciling runners", "listed", len(names), "held", len(held))
	for _, name := range names {
		if held[name] || ctx.Err() != nil {
			continue
		}
		r.remove(ctx, name)
	}
}

func (r reconciler) remove(ctx context.Context, name string) {
	ref, err := r.api.GetRunnerByName(ctx, name)
	if err != nil {
		r.logFailure(ctx, name, err)
		return
	}
	if ref == nil {
		r.log.Debug("unheld runner not found by name", "runner", name)
		return
	}
	r.log.Debug("unheld runner", "runner", name, "id", ref.ID, "scale_set_id", ref.RunnerScaleSetID)
	if ref.RunnerScaleSetID == 0 || ref.RunnerScaleSetID != r.setID {
		return
	}
	err = r.api.RemoveRunner(ctx, int64(ref.ID))
	switch {
	case err == nil:
		r.log.Info("removed a runner no instance holds", "runner", name)
	case errors.Is(err, scaleset.RunnerNotFoundError), errors.Is(err, scaleset.JobStillRunningError):
	default:
		r.logFailure(ctx, name, err)
	}
}

func (r reconciler) logFailure(ctx context.Context, name string, err error) {
	if ctx.Err() == nil {
		r.log.Warn("reconciling a runner failed", "runner", name, "error", err)
	}
}

// run passes every interval until ctx ends, then closes done.
func (r reconciler) run(ctx context.Context, interval time.Duration, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.pass(ctx)
		}
	}
}
