package fleet

import (
	"context"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

const removeTimeout = 30 * time.Second

// heartbeat makes taskscaler call the plugin's Heartbeat before each Acquire
// hands an instance out. The plugin check is the whole probe, so nothing more
// is done over the connection.
func heartbeat(context.Context, provider.ConnectInfo) error { return nil }

// removeOnHeartbeatFailure wraps an instance group so an instance whose
// Heartbeat fails is deleted. Taskscaler alone only refuses to hand it out and
// leaves it idle, because an unhealthy instance does not raise the demand for a
// replacement.
type removeOnHeartbeatFailure struct {
	provider.InstanceGroup
	log hclog.Logger
}

func (g removeOnHeartbeatFailure) Heartbeat(ctx context.Context, id string) error {
	err := g.InstanceGroup.Heartbeat(ctx, id)
	if err == nil {
		return nil
	}
	if g.log != nil {
		g.log.Warn("heartbeat failed, removing instance", "instance", id, "err", err)
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), removeTimeout)
	defer cancel()
	if _, derr := g.Decrease(rctx, []string{id}); derr != nil && g.log != nil {
		g.log.Error("removing instance after failed heartbeat", "instance", id, "err", derr)
	}
	return err
}
