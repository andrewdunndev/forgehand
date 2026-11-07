package fleet

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
)

// admissionRefused is what the plugin's Increase error says when its
// admit_command refuses a guest. Only the message crosses the plugin boundary.
const admissionRefused = "admission refused"

// observed counts what the plugin and the readiness gate report while a guest
// is prepared. Its methods tolerate a nil receiver, for a daemon with no
// metrics.
type observed struct {
	prepareFailures *prometheus.CounterVec
	consecutive     prometheus.Gauge
	refusals        prometheus.Counter
}

func newObserved(reg prometheus.Registerer, labels prometheus.Labels) (*observed, error) {
	o := &observed{
		prepareFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "forgehand_prepare_failures_total",
			Help:        "Guests that failed to prepare, by stage: connect_info or ready_command",
			ConstLabels: labels,
		}, []string{"stage"}),
		consecutive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "forgehand_prepare_consecutive_failures",
			Help:        "Guest preparations that failed since the last one that succeeded",
			ConstLabels: labels,
		}),
		refusals: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "forgehand_admission_refusals_total",
			Help:        "Guest creations the plugin's admission check refused",
			ConstLabels: labels,
		}),
	}
	for _, c := range []prometheus.Collector{o.prepareFailures, o.consecutive, o.refusals} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return o, nil
}

func (o *observed) prepareFailed(stage string) {
	if o == nil {
		return
	}
	o.prepareFailures.WithLabelValues(stage).Inc()
	o.consecutive.Inc()
}

func (o *observed) refused(err error) {
	if o != nil && strings.Contains(err.Error(), admissionRefused) {
		o.refusals.Inc()
	}
}

func (o *observed) prepared() {
	if o != nil {
		o.consecutive.Set(0)
	}
}

// observedGroup wraps an instance group to count the connect-info failures and
// admission refusals the plugin returns.
type observedGroup struct {
	provider.InstanceGroup
	o *observed
}

func (g observedGroup) ConnectInfo(ctx context.Context, id string) (provider.ConnectInfo, error) {
	info, err := g.InstanceGroup.ConnectInfo(ctx, id)
	if err != nil && ctx.Err() == nil {
		g.o.prepareFailed("connect_info")
	}
	return info, err
}

func (g observedGroup) Increase(ctx context.Context, n int) (int, error) {
	done, err := g.InstanceGroup.Increase(ctx, n)
	if err != nil {
		g.o.refused(err)
	}
	return done, err
}
