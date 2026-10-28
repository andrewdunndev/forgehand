package main

import (
	"context"
	"errors"
	"time"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-hclog"

	"gitlab.com/dunn.dev/forgehand/internal/scaler"
)

// sweeper removes the GitHub runners of VMs a previous run left behind.
type sweeper struct {
	reg      scaler.Registry
	log      hclog.Logger
	interval time.Duration
	attempts int
}

// remove deletes the runner registered under id. A runner that still reports a
// job is retried, since its VM is already being deleted.
func (s sweeper) remove(ctx context.Context, id string) {
	var err error
	for i := 0; i < s.attempts; i++ {
		err = scaler.RemoveRunner(ctx, s.reg, id)
		switch {
		case err == nil:
			s.log.Info("removed the runner of a leftover instance", "instance", id)
			return
		case errors.Is(err, scaleset.RunnerNotFoundError):
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.interval):
		}
	}
	s.log.Error("could not remove the runner of a leftover instance: remove it in the repository's runner settings", "instance", id, "error", err)
}
