package github

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
)

// DefaultGroupID is the runner group every repository-level scale set lives in.
const DefaultGroupID = 1

var ErrLabelDrift = errors.New("labels are fixed at creation: rename the scale set in config")

var ErrUpdateEnabled = errors.New("runner self-update is fixed at creation: rename the scale set in config")

type Session interface {
	listener.Client
	Close(ctx context.Context) error
}

type API interface {
	GetRunnerScaleSet(ctx context.Context, groupID int, name string) (*scaleset.RunnerScaleSet, error)
	CreateRunnerScaleSet(ctx context.Context, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
	MessageSessionClient(ctx context.Context, scaleSetID int, owner string) (Session, error)
	GenerateJitRunnerConfig(ctx context.Context, name string, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	GetRunnerByName(ctx context.Context, name string) (*scaleset.RunnerReference, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
}

// LookupScaleSet returns the named scale set, or nil when it does not exist.
// An existing set whose labels differ from labels is an error: GitHub ignores
// label updates, and deleting the set would orphan queued jobs. One that allows
// runner self-update is an error for the same reason.
func LookupScaleSet(ctx context.Context, api API, name string, labels []string) (*scaleset.RunnerScaleSet, error) {
	set, err := api.GetRunnerScaleSet(ctx, DefaultGroupID, name)
	if err != nil {
		return nil, fmt.Errorf("get scale set %q: %w", name, err)
	}
	if set == nil {
		return nil, nil
	}
	have := labelNames(set.Labels)
	if !sameLabels(have, labels) {
		return nil, fmt.Errorf("scale set %q has labels %v, config wants %v: %w", name, have, labels, ErrLabelDrift)
	}
	if !set.RunnerSetting.DisableUpdate {
		return nil, fmt.Errorf("scale set %q allows runner self-update: %w", name, ErrUpdateEnabled)
	}
	return set, nil
}

// EnsureScaleSet returns the named scale set, creating it with the labels and
// the runner self-update switched off when absent.
func EnsureScaleSet(ctx context.Context, api API, name string, labels []string) (*scaleset.RunnerScaleSet, error) {
	set, err := LookupScaleSet(ctx, api, name, labels)
	if err != nil || set != nil {
		return set, err
	}
	want := &scaleset.RunnerScaleSet{
		Name:          name,
		RunnerGroupID: DefaultGroupID,
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	}
	for _, l := range labels {
		want.Labels = append(want.Labels, scaleset.Label{Name: l})
	}
	set, err = api.CreateRunnerScaleSet(ctx, want)
	if err != nil {
		return nil, fmt.Errorf("create scale set %q: %w", name, err)
	}
	return set, nil
}

func labelNames(labels []scaleset.Label) []string {
	names := make([]string, len(labels))
	for i, l := range labels {
		names[i] = l.Name
	}
	return names
}

func sameLabels(a, b []string) bool {
	norm := func(in []string) []string {
		out := make([]string, len(in))
		for i, s := range in {
			out[i] = strings.ToLower(s)
		}
		slices.Sort(out)
		return out
	}
	return slices.Equal(norm(a), norm(b))
}
