package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gitlab.com/gitlab-org/fleeting/fleeting/connector"

	"gitlab.com/dunn.dev/forgehand/internal/github"
)

// shellGuest serves sessions by running the scripts with the local sh against
// a runner directory holding a scripted Runner.Listener.
type shellGuest struct {
	dir  string
	path string
}

func newShellGuest(t *testing.T, listener string) *shellGuest {
	t.Helper()
	g := &shellGuest{dir: filepath.Join(t.TempDir(), "actions-runner"), path: os.Getenv("PATH")}
	if err := os.MkdirAll(filepath.Join(g.dir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	g.write(t, filepath.Join("bin", "Runner.Listener"), "#!/bin/sh\n"+listener)

	if _, err := exec.LookPath("setsid"); err != nil {
		perl, err := exec.LookPath("perl")
		if err != nil {
			t.Skip("neither setsid nor perl to stand in for it")
		}
		shim := t.TempDir()
		body := fmt.Sprintf("#!%s\nuse POSIX; POSIX::setsid(); exec @ARGV;\n", perl)
		if err := os.WriteFile(filepath.Join(shim, "setsid"), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
		g.path = shim + string(os.PathListSeparator) + g.path
	}
	return g
}

func (g *shellGuest) write(t *testing.T, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(g.dir, name), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}

func (g *shellGuest) read(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(g.dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func (g *shellGuest) serve(_ int, ctx context.Context, opts connector.RunOptions) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", opts.Command)
	cmd.Env = append(os.Environ(), "PATH="+g.path)
	cmd.Stdin, cmd.Stdout = opts.Stdin, opts.Stdout
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 500 * time.Millisecond
	err := cmd.Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) && ee.Exited() {
		return &exitErr{code: ee.ExitCode()}
	}
	if err != nil {
		return errors.New(err.Error())
	}
	return nil
}

const listenerSource = `echo "$@" > args
echo "$ACTIONS_RUNNER_INPUT_JITCONFIG" > seen
echo "$ACTIONS_RUNNER_RETURN_VERSION_DEPRECATED_EXIT_CODE" > deprecated
echo "2026-10-28 12:00:00Z: Listening for Jobs"
sleep %s
echo "2026-10-28 12:00:01Z: Running job: build"
sleep %s
exit %d
`

func runShell(t *testing.T, g *shellGuest, hl *hookLog) (int, error) {
	t.Helper()
	useTarget(t, g.serve)
	r := newRunner(github.NewFake())
	r.dir = g.dir
	r.lostWindow = 5 * time.Second
	r.pollInterval = 50 * time.Millisecond
	return run(t, r, hl)
}

func TestScriptsStartTheListenerOnceWithTheBlobInItsEnvironment(t *testing.T) {
	g := newShellGuest(t, fmt.Sprintf(listenerSource, "0.2", "0.2", 0))
	hl := &hookLog{}
	code, err := runShell(t, g, hl)
	if code != 0 || err != nil {
		t.Fatalf("= %d, %v; want 0, nil", code, err)
	}
	if got := strings.Join(hl.list(), ","); got != "listening,running" {
		t.Errorf("hooks = %s", got)
	}
	if got := g.read(t, "args"); got != "run --once" {
		t.Errorf("listener args = %q", got)
	}
	if got := g.read(t, "seen"); got != "jit-vm-1" {
		t.Errorf("listener saw jit config %q", got)
	}
	if got := g.read(t, "deprecated"); got != "1" {
		t.Errorf("deprecated-exit env = %q", got)
	}
}

func TestScriptsReportTheListenerExitCode(t *testing.T) {
	g := newShellGuest(t, fmt.Sprintf(listenerSource, "0.1", "0.1", 7))
	code, err := runShell(t, g, &hookLog{})
	if code != 7 || err != nil {
		t.Errorf("= %d, %v; want 7, nil", code, err)
	}
}

func TestScriptsLetTheListenerOutliveADroppedSession(t *testing.T) {
	g := newShellGuest(t, fmt.Sprintf(listenerSource, "0.2", "1", 3))
	hl := &hookLog{}
	tg := useTarget(t, func(n int, ctx context.Context, opts connector.RunOptions) error {
		if n == 1 {
			ctx, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
			defer cancel()
			if err := g.serve(n, ctx, opts); err != nil {
				return err
			}
		}
		return g.serve(n, ctx, opts)
	})
	r := newRunner(github.NewFake())
	r.dir = g.dir
	r.lostWindow = 5 * time.Second
	r.pollInterval = 50 * time.Millisecond

	code, err := run(t, r, hl)
	if code != 3 || err != nil {
		t.Fatalf("= %d, %v; want 3, nil", code, err)
	}
	if n := len(tg.all()); n != 3 {
		t.Errorf("sessions = %d, want start, probe, follow", n)
	}
	if got := strings.Join(hl.list(), ","); !strings.HasSuffix(got, "running") {
		t.Errorf("hooks = %s", got)
	}
	if got := g.read(t, "runner.exit"); got != "3" {
		t.Errorf("runner.exit = %q", got)
	}
}
