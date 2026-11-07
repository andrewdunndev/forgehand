package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting"
	"gitlab.com/gitlab-org/fleeting/fleeting/plugin/fleeting-plugin-dummy"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"gitlab.com/gitlab-org/fleeting/taskscaler"
	"gitlab.com/gitlab-org/fleeting/taskscaler/storage"

	"gitlab.com/dunn.dev/forgehand/internal/config"
)

// saveInstance writes the state file taskscaler would have left for id, held
// by key (none for an idle pool instance).
func saveInstance(t *testing.T, dir, id, key string, removing bool) {
	t.Helper()
	slots, used := "{}", 0
	if key != "" {
		slots, used = fmt.Sprintf(`{"0":{"Key":%q}}`, key), 1
	}
	var st storage.InstanceState
	raw := fmt.Sprintf(`{"ID":%q,"CapacityPerInstance":1,"MaxUseCount":1,"FailureThreshold":3,"Slots":%s,"UsedCount":%d,"Removing":%t}`, id, slots, used, removing)
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatal(err)
	}
	store, err := storage.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitInstanceState(st); err != nil {
		t.Fatal(err)
	}
}

func instanceFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "instances"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// restored polls f until the restore is over and returns what it adopted.
func restored(t *testing.T, f Fleet) []Adopted {
	t.Helper()
	var got []Adopted
	eventually(t, "the restore to finish", func() bool {
		a, restoring := f.Restored()
		got = append(got, a...)
		return !restoring
	})
	return got
}

func TestStateDirKeepsInstanceFilesPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "set")
	f := newFleet(t, &dummy.InstanceGroup{MaxSize: 4}, Options{StateDir: dir})
	acquire(t, f, "k")

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("state directory mode = %o, want 700", perm)
	}
	var files []string
	eventually(t, "the instance file", func() bool { files = instanceFiles(t, dir); return len(files) == 1 })
	st, err := os.Stat(filepath.Join(dir, "instances", files[0]))
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		t.Errorf("state file mode = %o, want 600", perm)
	}
	body, err := os.ReadFile(filepath.Join(dir, "instances", files[0]))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"k"`) {
		t.Errorf("state file does not hold the acquisition key: %s", body)
	}
}

func TestNoStateDirWritesNothing(t *testing.T) {
	t.Chdir(t.TempDir())
	f := newFleet(t, &dummy.InstanceGroup{MaxSize: 4}, Options{})
	acquire(t, f, "k")
	if a, restoring := f.Restored(); len(a) != 0 || restoring {
		t.Errorf("Restored = %v, %v on a fleet without storage", a, restoring)
	}
	if entries, err := os.ReadDir("."); err != nil || len(entries) != 0 {
		t.Errorf("a fleet without a state directory wrote %v, %v", entries, err)
	}
}

func TestStateDirThatCannotBeCreatedFailsStart(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := New(&dummy.InstanceGroup{MaxSize: 4}, testConfig(), Options{StateDir: filepath.Join(blocker, "set")})
	if err == nil || !strings.Contains(err.Error(), "state directory") {
		t.Fatalf("err = %v, want a state directory error", err)
	}
}

func TestRestartAdoptsTheAcquisitionAndKeepsTheInstance(t *testing.T) {
	dir := t.TempDir()
	group := &dummy.InstanceGroup{MaxSize: 4}
	id := group.AddInstance(provider.StateRunning)
	saveInstance(t, dir, id, "old-1", false)

	left := make(chan string, 4)
	f := newFleet(t, group, Options{StateDir: dir, OnLeftover: func(id string) { left <- id }})

	got := restored(t, f)
	if len(got) != 1 || got[0].Key != "old-1" || got[0].Instance.InstanceID() != id {
		t.Fatalf("adopted = %+v, want old-1 on %s", got, id)
	}
	select {
	case l := <-left:
		t.Fatalf("restored instance %s was reported as a leftover", l)
	case <-time.After(50 * time.Millisecond):
	}
	if s := states(group)[id]; s != provider.StateRunning {
		t.Fatalf("restored instance is %s", s)
	}

	f.Release("old-1")
	eventually(t, "released instance deleted", func() bool {
		s := states(group)[id]
		return s == provider.StateDeleting || s == provider.StateDeleted
	})
}

func TestRestartKeepsAnIdlePoolInstanceWithoutAdoptingIt(t *testing.T) {
	dir := t.TempDir()
	group := &dummy.InstanceGroup{MaxSize: 4}
	id := group.AddInstance(provider.StateRunning)
	saveInstance(t, dir, id, "", false)

	left := make(chan string, 4)
	f := newFleet(t, group, Options{StateDir: dir, OnLeftover: func(id string) { left <- id }})
	if got := restored(t, f); len(got) != 0 {
		t.Fatalf("adopted %+v for a pool instance", got)
	}
	select {
	case l := <-left:
		t.Fatalf("pool instance %s was reported as a leftover", l)
	case <-time.After(50 * time.Millisecond):
	}
	if s := states(group)[id]; s != provider.StateRunning {
		t.Fatalf("pool instance is %s", s)
	}
	if got := acquire(t, f, "new").InstanceID(); got != id {
		t.Errorf("acquire took %s, want the restored pool instance %s", got, id)
	}
}

func TestRestartDropsSavedStateForInstancesThatAreGone(t *testing.T) {
	dir := t.TempDir()
	group := &dummy.InstanceGroup{MaxSize: 4}
	live := group.AddInstance(provider.StateRunning)
	saveInstance(t, dir, live, "old-1", false)
	saveInstance(t, dir, "vanished", "old-2", false)

	f := newFleet(t, group, Options{StateDir: dir})
	got := restored(t, f)
	if len(got) != 1 || got[0].Key != "old-1" {
		t.Fatalf("adopted = %+v, want only old-1", got)
	}
	eventually(t, "the unrestorable file dropped", func() bool { return len(instanceFiles(t, dir)) == 1 })
}

func TestRestartRejectsAnInstanceWithoutSavedState(t *testing.T) {
	dir := t.TempDir()
	group := &dummy.InstanceGroup{MaxSize: 4}
	old := group.AddInstance(provider.StateRunning)

	left := make(chan string, 4)
	f := newFleet(t, group, Options{StateDir: dir, OnLeftover: func(id string) { left <- id }})
	if got := restored(t, f); len(got) != 0 {
		t.Fatalf("adopted %+v with no saved state", got)
	}
	select {
	case id := <-left:
		if id != old {
			t.Errorf("leftover = %s, want %s", id, old)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("instance without saved state not reported")
	}
}

func TestRestoreIsUnderWayUntilEveryRestorableInstanceHasComeUp(t *testing.T) {
	dir := t.TempDir()
	saveInstance(t, dir, "a", "old-1", false)
	saveInstance(t, dir, "b", "old-2", false)
	saveInstance(t, dir, "c", "old-3", true)
	store, err := storage.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := newRestore(store, hclog.NewNullLogger(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	if _, restoring := r.take(); !restoring {
		t.Fatal("restore over before any instance came up")
	}
	r.up("a", []Adopted{{Key: "old-1"}})
	adopted, restoring := r.take()
	if !restoring || len(adopted) != 1 || adopted[0].Key != "old-1" {
		t.Fatalf("after one instance: %v, restoring %v", adopted, restoring)
	}
	r.up("b", []Adopted{{Key: "old-2"}})
	adopted, restoring = r.take()
	if restoring || len(adopted) != 1 || adopted[0].Key != "old-2" {
		t.Fatalf("after both: %v, restoring %v; the removing instance must not be waited for", adopted, restoring)
	}
}

func TestRestoreEndsWhenTaskscalerRemovesAWaitedInstance(t *testing.T) {
	dir := t.TempDir()
	saveInstance(t, dir, "a", "old-1", false)
	store, err := storage.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := newRestore(store, hclog.NewNullLogger(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, restoring := r.take(); !restoring {
		t.Fatal("restore over before the instance came up")
	}
	if err := store.DeleteInstanceState("a"); err != nil {
		t.Fatal(err)
	}
	if _, restoring := r.take(); restoring {
		t.Fatal("still waiting for an instance taskscaler dropped")
	}
}

func TestRestoreCeilingEndsTheWait(t *testing.T) {
	dir := t.TempDir()
	saveInstance(t, dir, "a", "old-1", false)
	store, err := storage.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := newRestore(store, hclog.NewNullLogger(), 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := r.now()
	r.now = func() time.Time { return now.Add(3*time.Minute - time.Second) }
	if _, restoring := r.take(); !restoring {
		t.Fatal("restore over before the ceiling")
	}
	r.now = func() time.Time { return now.Add(3 * time.Minute) }
	if _, restoring := r.take(); restoring {
		t.Fatal("restore still under way at the ceiling")
	}
}

func TestRestoreCeilingDefaultsToThreeMinutesAndFollowsTheOption(t *testing.T) {
	for _, c := range []struct {
		name string
		set  time.Duration
		want time.Duration
	}{
		{"default", 0, 3 * time.Minute},
		{"option", 7 * time.Minute, 7 * time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := time.Now()
			f := newFleet(t, &dummy.InstanceGroup{MaxSize: 4}, Options{StateDir: t.TempDir(), RestoreCeiling: c.set})
			after := time.Now()
			deadline := f.(*fleetImpl).restore.deadline
			if deadline.Before(before.Add(c.want)) || deadline.After(after.Add(c.want)) {
				t.Fatalf("deadline %v after start is outside [%v, %v]", deadline.Sub(before), c.want, c.want+after.Sub(before))
			}
		})
	}
}

func TestRestoreWithNoFilesIsOverAtOnce(t *testing.T) {
	store, err := storage.NewFileStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r, err := newRestore(store, hclog.NewNullLogger(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, restoring := r.take(); restoring {
		t.Fatal("restore under way with nothing to restore")
	}
}

type stubAcquisition struct{ taskscaler.Acquisition }

type onlyGet struct {
	taskscaler.Taskscaler
	held map[string]taskscaler.Acquisition
}

func (g onlyGet) Get(key string) taskscaler.Acquisition { return g.held[key] }

func TestUpFuncRestoredInstance(t *testing.T) {
	dir := t.TempDir()
	saveInstance(t, dir, "i", "old-1", false)
	store, err := storage.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := newRestore(store, hclog.NewNullLogger(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	left := make(chan string, 1)
	cfg := config.Autoscaler{InstanceReadyCommand: "false"}
	up := upFunc(context.Background(), cfg, func(id string) { left <- id }, rs, nil)

	err = up(onlyGet{held: map[string]taskscaler.Acquisition{"old-1": stubAcquisition{}}}, taskscaler.UpFuncInstance{
		ID: "i", Cause: fleeting.CausePreexisted, Acquisitions: []string{"old-1"}, Restored: true,
	})
	if err != nil {
		t.Fatalf("restored instance rejected: %v (it must neither be rejected nor run instance_ready_command)", err)
	}
	select {
	case id := <-left:
		t.Errorf("restored instance %s reported as a leftover", id)
	default:
	}
	adopted, restoring := rs.take()
	if restoring {
		t.Error("the up function did not count the instance")
	}
	if len(adopted) != 1 || adopted[0].Key != "old-1" {
		t.Errorf("adopted = %+v, want old-1", adopted)
	}
}

// taskscaler's Get returns a non-nil interface around a nil pointer for a key
// it no longer holds.
func TestUpFuncSkipsAcquisitionsTaskscalerNoLongerHolds(t *testing.T) {
	store, err := storage.NewFileStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rs, err := newRestore(store, hclog.NewNullLogger(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	up := upFunc(context.Background(), config.Autoscaler{}, nil, rs, nil)
	held := map[string]taskscaler.Acquisition{
		"kept":    &stubAcquisition{},
		"typed":   (*stubAcquisition)(nil),
		"untyped": nil,
	}
	err = up(onlyGet{held: held}, taskscaler.UpFuncInstance{
		ID: "i", Acquisitions: []string{"typed", "kept", "untyped", "absent"}, Restored: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	adopted, _ := rs.take()
	if len(adopted) != 1 || adopted[0].Key != "kept" {
		t.Errorf("adopted = %+v, want only kept", adopted)
	}
}

func TestShutdownReleasesRestoredAcquisitionsNothingAdopted(t *testing.T) {
	dir := t.TempDir()
	group := &dummy.InstanceGroup{MaxSize: 4}
	id := group.AddInstance(provider.StateRunning)
	saveInstance(t, dir, id, "old-1", false)

	f, err := New(group, testConfig(), Options{StateDir: dir}, taskscaler.WithAcquireDelay(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	restored(t, f)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	f.Shutdown(ctx)
	if took := time.Since(start); took > time.Second {
		t.Errorf("Shutdown took %s waiting for a release nothing would make", took)
	}
	if st := states(group)[id]; st != provider.StateDeleted {
		t.Errorf("restored instance is %s after Shutdown", st)
	}
}
