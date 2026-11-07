package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/connector"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"

	"gitlab.com/dunn.dev/forgehand/internal/config"
	"gitlab.com/dunn.dev/forgehand/internal/fleet"
	"gitlab.com/dunn.dev/forgehand/internal/github"
	"gitlab.com/dunn.dev/forgehand/internal/scaler"
)

type exitErr struct {
	code int
	err  error
}

func (e *exitErr) Error() string { return "exit" }
func (e *exitErr) Unwrap() error { return e.err }
func (e *exitErr) ExitCode() int { return e.code }

type call struct {
	command string
	stdin   string
}

type fakeClient struct {
	run func(ctx context.Context, opts connector.RunOptions) error
}

func (c *fakeClient) Run(ctx context.Context, opts connector.RunOptions) error {
	return c.run(ctx, opts)
}
func (*fakeClient) Dial(string, string) (net.Conn, error) { return nil, errors.New("unused") }
func (*fakeClient) DialRun(context.Context, string) (net.Conn, error) {
	return nil, errors.New("unused")
}
func (*fakeClient) Close() error { return nil }

// target swaps connector.Dial for a fake whose sessions are served by serve.
type target struct {
	mu    sync.Mutex
	calls []call
	dials int
	serve func(n int, ctx context.Context, opts connector.RunOptions) error
}

func useTarget(t *testing.T, serve func(n int, ctx context.Context, opts connector.RunOptions) error) *target {
	t.Helper()
	tg := &target{serve: serve}
	old := connector.Dial
	connector.Dial = func(ctx context.Context, _ provider.ConnectInfo, _ connector.DialOptions) (connector.Client, error) {
		tg.mu.Lock()
		tg.dials++
		tg.mu.Unlock()
		return &fakeClient{run: func(ctx context.Context, opts connector.RunOptions) error {
			var stdin string
			if opts.Stdin != nil {
				b, _ := io.ReadAll(opts.Stdin)
				stdin = string(b)
				opts.Stdin = strings.NewReader(stdin)
			}
			tg.mu.Lock()
			tg.calls = append(tg.calls, call{opts.Command, stdin})
			n := len(tg.calls)
			tg.mu.Unlock()
			return tg.serve(n, ctx, opts)
		}}, nil
	}
	t.Cleanup(func() { connector.Dial = old })
	return tg
}

func (tg *target) all() []call {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	return append([]call(nil), tg.calls...)
}

type hookLog struct {
	mu sync.Mutex
	ev []string
}

func (h *hookLog) hooks() scaler.Hooks {
	return scaler.Hooks{
		Listening:  func() { h.add("listening") },
		RunningJob: func() { h.add("running") },
	}
}

func (h *hookLog) add(e string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ev = append(h.ev, e)
}

func (h *hookLog) list() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.ev...)
}

func newRunner(api *github.Fake) *Runner {
	r := New(config.Runner{Dir: "/opt/actions-runner"}, false, api, 7, hclog.NewNullLogger())
	r.lostWindow = 200 * time.Millisecond
	r.pollInterval = time.Millisecond
	return r
}

func run(t *testing.T, r *Runner, hl *hookLog) (int, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.Run(ctx, &fleet.FakeInstance{ID: "vm-1"}, "vm-1", hl.hooks())
}

func isProbe(opts connector.RunOptions) bool { return strings.Contains(opts.Command, "echo alive") }
func isFollow(opts connector.RunOptions) bool {
	return !strings.Contains(opts.Command, "read -r") && !isProbe(opts)
}

func TestStartCarriesOnceAndKeepsBlobOffArgv(t *testing.T) {
	api := github.NewFake()
	tg := useTarget(t, func(int, context.Context, connector.RunOptions) error { return nil })

	if _, err := run(t, newRunner(api), &hookLog{}); err != nil {
		t.Fatal(err)
	}
	c := tg.all()[0]
	if !strings.Contains(c.command, "bin/Runner.Listener run --once") {
		t.Errorf("runner command line lacks --once:\n%s", c.command)
	}
	if !strings.Contains(c.command, "setsid") || !strings.Contains(c.command, "RETURN_VERSION_DEPRECATED_EXIT_CODE=1") {
		t.Errorf("runner is not detached or does not report a retired version:\n%s", c.command)
	}
	if strings.Contains(c.command, "jit-vm-1") {
		t.Error("the JIT blob is on the command line")
	}
	if c.stdin != "jit-vm-1\n" {
		t.Errorf("stdin = %q, want the JIT blob and a newline", c.stdin)
	}
	if got := api.JitNames; len(got) != 1 || got[0] != "vm-1" {
		t.Errorf("JIT minted for %v, want the instance id", got)
	}
}

func TestMarkersFireHooksAcrossChunks(t *testing.T) {
	hl := &hookLog{}
	useTarget(t, func(_ int, _ context.Context, o connector.RunOptions) error {
		for _, chunk := range []string{"noise\n2026-10-28: Listen", "ing for Jobs\r\n", "x: Running ", "job: build\n"} {
			_, _ = o.Stdout.Write([]byte(chunk))
		}
		return nil
	})
	if _, err := run(t, newRunner(github.NewFake()), hl); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(hl.list(), ","); got != "listening,running" {
		t.Errorf("hooks = %s", got)
	}
}

func TestExitStatusIsTheRunnerExitCode(t *testing.T) {
	useTarget(t, func(int, context.Context, connector.RunOptions) error { return &exitErr{code: 7} })
	code, err := run(t, newRunner(github.NewFake()), &hookLog{})
	if code != 7 || err != nil {
		t.Errorf("= %d, %v; want 7, nil", code, err)
	}
}

func TestSessionDropWithRunnerAliveRedialsAndReattaches(t *testing.T) {
	hl := &hookLog{}
	tg := useTarget(t, func(n int, _ context.Context, o connector.RunOptions) error {
		switch {
		case n == 1:
			_, _ = o.Stdout.Write([]byte("Listening for Jobs\n"))
			return &exitErr{code: 0, err: io.ErrUnexpectedEOF}
		case isProbe(o):
			_, _ = o.Stdout.Write([]byte("alive\n"))
		default:
			_, _ = o.Stdout.Write([]byte("Running job: build\n"))
		}
		return nil
	})
	code, err := run(t, newRunner(github.NewFake()), hl)
	if code != 0 || err != nil {
		t.Fatalf("= %d, %v; want 0, nil", code, err)
	}
	calls := tg.all()
	if len(calls) != 3 || !isProbe(connector.RunOptions{Command: calls[1].command}) || !isFollow(connector.RunOptions{Command: calls[2].command}) {
		t.Errorf("sessions = %d, want start, probe, follow", len(calls))
	}
	if calls[2].stdin != "" {
		t.Error("the follow session was handed the JIT blob again")
	}
	if got := strings.Join(hl.list(), ","); got != "listening,running" {
		t.Errorf("hooks = %s", got)
	}
}

func TestSessionDropThenProbeFindsExit(t *testing.T) {
	useTarget(t, func(n int, _ context.Context, o connector.RunOptions) error {
		if n == 1 {
			return errors.New("connection reset")
		}
		_, _ = o.Stdout.Write([]byte("exited 3\n"))
		return nil
	})
	code, err := run(t, newRunner(github.NewFake()), &hookLog{})
	if code != 3 || err != nil {
		t.Errorf("= %d, %v; want 3, nil", code, err)
	}
}

func TestSessionDropBeforeStartIsNotARunner(t *testing.T) {
	useTarget(t, func(n int, _ context.Context, o connector.RunOptions) error {
		if n == 1 {
			return errors.New("connection reset")
		}
		_, _ = o.Stdout.Write([]byte("none\n"))
		return nil
	})
	_, err := run(t, newRunner(github.NewFake()), &hookLog{})
	if err == nil || errors.Is(err, scaler.ErrLost) {
		t.Errorf("err = %v, want a start failure", err)
	}
}

func TestUnreachableGuestIsLost(t *testing.T) {
	first := true
	var mu sync.Mutex
	old := connector.Dial
	t.Cleanup(func() { connector.Dial = old })
	connector.Dial = func(context.Context, provider.ConnectInfo, connector.DialOptions) (connector.Client, error) {
		mu.Lock()
		defer mu.Unlock()
		if !first {
			return nil, errors.New("no route to host")
		}
		first = false
		return &fakeClient{run: func(context.Context, connector.RunOptions) error { return io.ErrUnexpectedEOF }}, nil
	}
	_, err := run(t, newRunner(github.NewFake()), &hookLog{})
	if !errors.Is(err, scaler.ErrLost) {
		t.Errorf("err = %v, want ErrLost", err)
	}
}

func TestFirstDialFailureFailsTheStart(t *testing.T) {
	old := connector.Dial
	t.Cleanup(func() { connector.Dial = old })
	connector.Dial = func(context.Context, provider.ConnectInfo, connector.DialOptions) (connector.Client, error) {
		return nil, errors.New("dial ssh: timeout")
	}
	_, err := run(t, newRunner(github.NewFake()), &hookLog{})
	if err == nil || errors.Is(err, scaler.ErrLost) || !strings.Contains(err.Error(), "timeout") {
		t.Errorf("err = %v", err)
	}
}

func TestJitErrorDialsNothing(t *testing.T) {
	api := github.NewFake()
	api.JitErr = errors.New("403")
	tg := useTarget(t, func(int, context.Context, connector.RunOptions) error { return nil })
	if _, err := run(t, newRunner(api), &hookLog{}); err == nil {
		t.Fatal("no error")
	}
	if tg.dials != 0 {
		t.Errorf("dialed %d times after a JIT error", tg.dials)
	}
}

func TestCancelledContextEndsRun(t *testing.T) {
	useTarget(t, func(_ int, ctx context.Context, _ connector.RunOptions) error {
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	_, err := newRunner(github.NewFake()).Run(ctx, &fleet.FakeInstance{ID: "vm-1"}, "vm-1", scaler.Hooks{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}

func TestDiag(t *testing.T) {
	useTarget(t, func(_ int, _ context.Context, o connector.RunOptions) error {
		_, _ = o.Stdout.Write([]byte("Runner_20261028.log tail\n"))
		return nil
	})
	got := newRunner(github.NewFake()).Diag(context.Background(), &fleet.FakeInstance{ID: "vm-1"})
	if got != "Runner_20261028.log tail\n" {
		t.Errorf("diag = %q", got)
	}

	old := connector.Dial
	t.Cleanup(func() { connector.Dial = old })
	connector.Dial = func(context.Context, provider.ConnectInfo, connector.DialOptions) (connector.Client, error) {
		return nil, errors.New("refused")
	}
	if got := newRunner(github.NewFake()).Diag(context.Background(), &fleet.FakeInstance{ID: "vm-1"}); !strings.Contains(got, "refused") {
		t.Errorf("diag = %q", got)
	}
}

func TestQuote(t *testing.T) {
	if got := quote("/opt/it's"); got != `'/opt/it'\''s'` {
		t.Errorf("quote = %s", got)
	}
}

func attach(t *testing.T, r *Runner, hl *hookLog) (int, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.Attach(ctx, &fleet.FakeInstance{ID: "vm-1"}, "vm-1", hl.hooks())
}

func TestAttachProbesThenFollowsTheLogFromTheStart(t *testing.T) {
	api := github.NewFake()
	hl := &hookLog{}
	tg := useTarget(t, func(_ int, _ context.Context, o connector.RunOptions) error {
		if isProbe(o) {
			_, _ = o.Stdout.Write([]byte("alive\n"))
			return nil
		}
		_, _ = o.Stdout.Write([]byte("Listening for Jobs\nRunning job: build\n"))
		return &exitErr{code: 4}
	})
	code, err := attach(t, newRunner(api), hl)
	if code != 4 || err != nil {
		t.Fatalf("= %d, %v; want 4, nil", code, err)
	}
	calls := tg.all()
	if len(calls) != 2 || !isProbe(connector.RunOptions{Command: calls[0].command}) || !isFollow(connector.RunOptions{Command: calls[1].command}) {
		t.Fatalf("sessions = %v, want probe then follow", calls)
	}
	if !strings.Contains(calls[1].command, "off=0") {
		t.Errorf("the follow does not start at the beginning of the log:\n%s", calls[1].command)
	}
	for _, c := range calls {
		if c.stdin != "" || strings.Contains(c.command, "Runner.Listener") {
			t.Errorf("attach started a runner or sent input: %+v", c)
		}
	}
	if len(api.JitNames) != 0 {
		t.Errorf("attach minted a JIT config for %v", api.JitNames)
	}
	if got := strings.Join(hl.list(), ","); got != "listening,running" {
		t.Errorf("hooks = %s", got)
	}
}

func TestAttachToAnExitedRunnerReadsItsCode(t *testing.T) {
	tg := useTarget(t, func(_ int, _ context.Context, o connector.RunOptions) error {
		_, _ = o.Stdout.Write([]byte("exited 7\n"))
		return nil
	})
	code, err := attach(t, newRunner(github.NewFake()), &hookLog{})
	if code != 7 || err != nil {
		t.Errorf("= %d, %v; want 7, nil", code, err)
	}
	if n := len(tg.all()); n != 1 {
		t.Errorf("sessions = %d, want the probe only", n)
	}
}

func TestAttachToARunnerThatNeverStartedIsAnError(t *testing.T) {
	useTarget(t, func(_ int, _ context.Context, o connector.RunOptions) error {
		_, _ = o.Stdout.Write([]byte("none\n"))
		return nil
	})
	_, err := attach(t, newRunner(github.NewFake()), &hookLog{})
	if err == nil || errors.Is(err, scaler.ErrLost) || !strings.Contains(err.Error(), "never started") {
		t.Errorf("err = %v, want a never-started error", err)
	}
}

func TestAttachToAnUnreachableGuestIsLost(t *testing.T) {
	old := connector.Dial
	t.Cleanup(func() { connector.Dial = old })
	connector.Dial = func(context.Context, provider.ConnectInfo, connector.DialOptions) (connector.Client, error) {
		return nil, errors.New("no route to host")
	}
	_, err := attach(t, newRunner(github.NewFake()), &hookLog{})
	if !errors.Is(err, scaler.ErrLost) {
		t.Errorf("err = %v, want ErrLost", err)
	}
}

func TestAttachRedialsWhenTheSessionDrops(t *testing.T) {
	tg := useTarget(t, func(n int, _ context.Context, o connector.RunOptions) error {
		switch {
		case isProbe(o):
			_, _ = o.Stdout.Write([]byte("alive\n"))
			return nil
		case n == 2:
			return errors.New("connection reset")
		}
		return nil
	})
	code, err := attach(t, newRunner(github.NewFake()), &hookLog{})
	if code != 0 || err != nil {
		t.Fatalf("= %d, %v; want 0, nil", code, err)
	}
	if n := len(tg.all()); n != 4 {
		t.Errorf("sessions = %d, want probe, follow, probe, follow", n)
	}
}

func TestDiagKeepsTheDiagTailAtDebug(t *testing.T) {
	const marker = "AUTH-DETAIL-MARKER"
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "_diag"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"runner.log":            "listener started\n",
		"_diag/Runner_2026.log": "response header " + marker + "\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	useTarget(t, func(_ int, ctx context.Context, o connector.RunOptions) error {
		cmd := exec.CommandContext(ctx, "sh", "-c", o.Command)
		cmd.Stdout = o.Stdout
		return cmd.Run()
	})

	diag := func(level hclog.Level) (string, []map[string]any) {
		var buf bytes.Buffer
		log := hclog.New(&hclog.LoggerOptions{Output: &buf, Level: level, JSONFormat: true})
		r := New(config.Runner{Dir: dir}, false, github.NewFake(), 7, log)
		got := r.Diag(context.Background(), &fleet.FakeInstance{ID: "vm-1"})
		var lines []map[string]any
		for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if l == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(l), &m); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, m)
		}
		return got, lines
	}

	for _, level := range []hclog.Level{hclog.Info, hclog.Debug} {
		got, lines := diag(level)
		if !strings.Contains(got, "listener started") {
			t.Errorf("level %v: diag = %q, want the runner.log tail", level, got)
		}
		if strings.Contains(got, marker) {
			t.Errorf("level %v: diag returned for Warn and the fail cause carries the _diag tail: %q", level, got)
		}
		var atDebug bool
		for _, m := range lines {
			if !strings.Contains(fmt.Sprint(m), marker) {
				continue
			}
			if m["@level"] != "debug" {
				t.Errorf("level %v: _diag tail logged at %v", level, m["@level"])
			}
			atDebug = true
		}
		if want := level == hclog.Debug; atDebug != want {
			t.Errorf("level %v: _diag tail logged = %v, want %v", level, atDebug, want)
		}
	}
}
