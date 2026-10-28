package github

import (
	"context"
	"errors"
	"testing"

	"github.com/actions/scaleset"
)

func labelled(names ...string) []scaleset.Label {
	var out []scaleset.Label
	for _, n := range names {
		out = append(out, scaleset.Label{Name: n, Type: "System"})
	}
	return out
}

func TestEnsureCreatesMissingSet(t *testing.T) {
	f := NewFake()
	set, err := EnsureScaleSet(context.Background(), f, "example", []string{"self-hosted", "example"})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Created) != 1 {
		t.Fatalf("created %d sets", len(f.Created))
	}
	c := f.Created[0]
	if c.Name != "example" || c.RunnerGroupID != DefaultGroupID || !c.RunnerSetting.DisableUpdate {
		t.Errorf("created set = %+v", c)
	}
	if got := labelNames(c.Labels); len(got) != 2 || got[0] != "self-hosted" || got[1] != "example" {
		t.Errorf("created labels = %v", got)
	}
	if set.ID == 0 {
		t.Error("returned set has no id")
	}
}

func TestEnsureReusesExistingSet(t *testing.T) {
	f := NewFake()
	f.Sets["example"] = &scaleset.RunnerScaleSet{ID: 7, Name: "example", Labels: labelled("example", "self-hosted")}
	set, err := EnsureScaleSet(context.Background(), f, "example", []string{"self-hosted", "example"})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Created) != 0 {
		t.Errorf("created %d sets over an existing one", len(f.Created))
	}
	if set.ID != 7 {
		t.Errorf("set id = %d", set.ID)
	}
}

func TestEnsureLabelDrift(t *testing.T) {
	tests := []struct {
		name string
		have []string
		want []string
	}{
		{"extra label in config", []string{"example"}, []string{"self-hosted", "example"}},
		{"label missing from config", []string{"self-hosted", "example"}, []string{"example"}},
		{"different label", []string{"self-hosted", "example"}, []string{"self-hosted", "other"}},
		{"set created with name-only label", []string{"example"}, []string{"self-hosted", "example"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFake()
			f.Sets["example"] = &scaleset.RunnerScaleSet{ID: 7, Name: "example", Labels: labelled(tt.have...)}
			_, err := EnsureScaleSet(context.Background(), f, "example", tt.want)
			if !errors.Is(err, ErrLabelDrift) {
				t.Fatalf("err = %v, want ErrLabelDrift", err)
			}
			if len(f.Created) != 0 {
				t.Error("drift must not create a second set")
			}
		})
	}
}

func TestEnsureLabelsMatchIgnoringOrderAndCase(t *testing.T) {
	f := NewFake()
	f.Sets["example"] = &scaleset.RunnerScaleSet{ID: 7, Name: "example", Labels: labelled("Self-Hosted", "example")}
	if _, err := EnsureScaleSet(context.Background(), f, "example", []string{"example", "self-hosted"}); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureLookupError(t *testing.T) {
	f := NewFake()
	f.GetScaleSetErr = errors.New("boom")
	_, err := EnsureScaleSet(context.Background(), f, "example", []string{"example"})
	if !errors.Is(err, f.GetScaleSetErr) {
		t.Fatalf("err = %v", err)
	}
	if len(f.Created) != 0 {
		t.Error("created a set after a failed lookup")
	}
}
