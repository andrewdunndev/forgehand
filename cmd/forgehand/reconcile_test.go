package main

import (
	"context"
	"slices"
	"testing"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-hclog"

	"gitlab.com/dunn.dev/forgehand/internal/github"
)

const ownSet = 9

func register(t *testing.T, f *github.Fake, name string, set int) {
	t.Helper()
	if _, err := f.GenerateJitRunnerConfig(context.Background(), name, set); err != nil {
		t.Fatal(err)
	}
}

func newReconciler(api github.API, held ...string) reconciler {
	return reconciler{
		api: api, setID: ownSet, log: hclog.NewNullLogger(),
		held: func() map[string]bool {
			m := map[string]bool{}
			for _, n := range held {
				m[n] = true
			}
			return m
		},
	}
}

func TestReconcilerRemovesOnlyUnheldRunnersOfItsOwnSet(t *testing.T) {
	f := github.NewFake()
	register(t, f, "orphan", ownSet)
	register(t, f, "held", ownSet)
	register(t, f, "other-set", ownSet+1)
	register(t, f, "unowned", 0)

	newReconciler(f, "held").pass(context.Background())

	got := f.RunnerNames()
	slices.Sort(got)
	if want := []string{"held", "other-set", "unowned"}; !slices.Equal(got, want) {
		t.Errorf("registered = %v, want %v", got, want)
	}
}

func TestReconcilerRetriesABusyRunnerUntilItsJobEnds(t *testing.T) {
	f := github.NewFake()
	register(t, f, "crashed", ownSet)
	f.SetBusy("crashed", true)
	r := newReconciler(f)

	for range 40 {
		r.pass(context.Background())
	}
	if len(f.RunnerNames()) != 1 {
		t.Fatal("removed a runner that still reports a job")
	}
	f.SetBusy("crashed", false)
	r.pass(context.Background())
	if n := len(f.RunnerNames()); n != 0 {
		t.Errorf("%d runners left after the job ended", n)
	}
}

// listFirst records the order of the list and held reads.
type listFirst struct {
	*github.Fake
	order *[]string
}

func (l listFirst) ListRunnerNames(ctx context.Context) ([]string, error) {
	*l.order = append(*l.order, "list")
	return l.Fake.ListRunnerNames(ctx)
}

func TestReconcilerReadsHeldNamesAfterListing(t *testing.T) {
	f := github.NewFake()
	register(t, f, "new-slot", ownSet)
	var order []string
	r := newReconciler(listFirst{f, &order})
	r.held = func() map[string]bool {
		order = append(order, "held")
		if !slices.Contains(order, "list") {
			return map[string]bool{}
		}
		return map[string]bool{"new-slot": true}
	}

	r.pass(context.Background())

	if !slices.Equal(order, []string{"list", "held"}) {
		t.Errorf("order = %v", order)
	}
	if len(f.RunnerNames()) != 1 {
		t.Error("removed the runner of a slot created during the pass")
	}
}

func TestReconcilerKeepsRunnersWhenListingFails(t *testing.T) {
	f := github.NewFake()
	register(t, f, "orphan", ownSet)
	f.ListErr = scaleset.RunnerNotFoundError
	newReconciler(f).pass(context.Background())
	if n := len(f.RemovedRunners()); n != 0 {
		t.Errorf("removed %d runners without a listing", n)
	}
}

func TestReconcilerNeverRemovesARunnerWithoutAScaleSet(t *testing.T) {
	f := github.NewFake()
	register(t, f, "unowned", 0)
	r := newReconciler(f)
	r.setID = 0
	r.pass(context.Background())
	if len(f.RunnerNames()) != 1 {
		t.Error("removed a runner that reports no scale set")
	}
}
