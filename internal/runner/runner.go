// Package runner starts a just-in-time actions runner on a guest over SSH and
// follows it until it exits.
package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/connector"

	"gitlab.com/dunn.dev/forgehand/internal/config"
	"gitlab.com/dunn.dev/forgehand/internal/fleet"
	"gitlab.com/dunn.dev/forgehand/internal/scaler"
)

const (
	markerListening = "Listening for Jobs"
	markerRunning   = "Running job:"

	lostWindow   = 5 * time.Minute
	pollInterval = 5 * time.Second
	diagLimit    = 16 << 10
)

// JIT is the part of GitHub's API that mints a runner configuration.
type JIT interface {
	GenerateJitRunnerConfig(ctx context.Context, name string, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
}

var _ scaler.Runner = (*Runner)(nil)

// Runner implements scaler.Runner over fleeting's connector.
type Runner struct {
	dir      string
	setID    int
	external bool
	jit      JIT
	log      hclog.Logger

	lostWindow   time.Duration
	pollInterval time.Duration
}

func New(cfg config.Runner, useExternalAddr bool, jit JIT, scaleSetID int, log hclog.Logger) *Runner {
	if log == nil {
		log = hclog.NewNullLogger()
	}
	return &Runner{
		dir:          cfg.Dir,
		setID:        scaleSetID,
		external:     useExternalAddr,
		jit:          jit,
		log:          log,
		lostWindow:   lostWindow,
		pollInterval: pollInterval,
	}
}

// Run mints the JIT config for name, writes it to the guest on stdin, starts
// the runner there and returns its exit code. A session that drops while the
// runner lives is redialled and the runner re-attached; ErrLost is returned
// when the guest stays unreachable for the lost window.
func (r *Runner) Run(ctx context.Context, inst fleet.Instance, name string, hooks scaler.Hooks) (int, error) {
	jit, err := r.jit.GenerateJitRunnerConfig(ctx, name, r.setID)
	if err != nil {
		return 0, fmt.Errorf("generating the JIT config: %w", err)
	}

	script := startScript(r.dir)
	var stdin io.Reader = strings.NewReader(jit.EncodedJITConfig + "\n")
	for first := true; ; first = false {
		out := &lineWriter{fn: func(line string) { r.marker(line, hooks) }}
		dialed, err := r.session(ctx, inst, script, stdin, out)
		out.flush()
		if code, ok := exitStatus(err); ok {
			return code, nil
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if first && !dialed {
			return 0, err
		}
		r.log.Warn("runner session lost", "runner", name, "error", err)

		state, code, err := r.probe(ctx, inst)
		switch {
		case err != nil:
			return 0, fmt.Errorf("%w: %v", scaler.ErrLost, err)
		case state == probeExited:
			return code, nil
		case state == probeNone:
			return 0, errors.New("the runner was never started in the guest")
		}
		script, stdin = followScript(r.dir), nil
	}
}

// Diag returns the tail of the runner's logs on inst.
func (r *Runner) Diag(ctx context.Context, inst fleet.Instance) string {
	var out limitedBuffer
	if _, err := r.session(ctx, inst, diagScript(r.dir), nil, &out); err != nil {
		return fmt.Sprintf("diagnostics unavailable: %v", err)
	}
	return out.String()
}

func (r *Runner) marker(line string, hooks scaler.Hooks) {
	r.log.Debug("runner", "line", line)
	switch {
	case strings.Contains(line, markerListening):
		hooks.Listening()
	case strings.Contains(line, markerRunning):
		hooks.RunningJob()
	}
}

// session runs script on inst. dialed reports whether the SSH session came up.
func (r *Runner) session(ctx context.Context, inst fleet.Instance, script string, stdin io.Reader, stdout io.Writer) (dialed bool, err error) {
	info, err := inst.InstanceConnectInfo(ctx)
	if err != nil {
		return false, fmt.Errorf("connect info: %w", err)
	}
	client, err := connector.Dial(ctx, info, connector.DialOptions{UseExternalAddr: r.external})
	if err != nil {
		return false, err
	}
	defer func() { _ = client.Close() }()
	return true, client.Run(ctx, connector.RunOptions{Command: script, Stdin: stdin, Stdout: stdout})
}

type probeState int

const (
	probeAlive probeState = iota
	probeExited
	probeNone
)

// probe asks the guest whether the runner is still alive, redialling until the
// lost window ends.
func (r *Runner) probe(ctx context.Context, inst fleet.Instance) (probeState, int, error) {
	pctx, cancel := context.WithTimeout(ctx, r.lostWindow)
	defer cancel()
	for {
		var out limitedBuffer
		_, err := r.session(pctx, inst, probeScript(r.dir), nil, &out)
		if err == nil {
			return parseProbe(out.String())
		}
		select {
		case <-pctx.Done():
			return 0, 0, fmt.Errorf("guest unreachable for %s: %w", r.lostWindow, err)
		case <-time.After(r.pollInterval):
		}
	}
}

func parseProbe(out string) (probeState, int, error) {
	f := strings.Fields(out)
	switch {
	case len(f) == 1 && f[0] == "alive":
		return probeAlive, 0, nil
	case len(f) == 1 && f[0] == "none":
		return probeNone, 0, nil
	case len(f) == 2 && f[0] == "exited":
		if code, err := strconv.Atoi(f[1]); err == nil {
			return probeExited, code, nil
		}
	}
	return 0, 0, fmt.Errorf("unexpected probe output %q", strings.TrimSpace(out))
}

// exitStatus reports the runner's exit code if err is the guest command's own
// non-zero exit. A connector error without a status means the session broke.
func exitStatus(err error) (int, bool) {
	if err == nil {
		return 0, true
	}
	var e interface{ ExitCode() int }
	if errors.As(err, &e) && e.ExitCode() != 0 {
		return e.ExitCode(), true
	}
	return 0, false
}

// startScript reads the JIT blob from stdin, starts the listener detached in
// its own session, then follows it. The blob never reaches argv or disk.
func startScript(dir string) string {
	return fmt.Sprintf(`cd %s || exit 1
IFS= read -r ACTIONS_RUNNER_INPUT_JITCONFIG
export ACTIONS_RUNNER_INPUT_JITCONFIG
export ACTIONS_RUNNER_RETURN_VERSION_DEPRECATED_EXIT_CODE=1
rm -f runner.pid runner.exit
: > runner.log
(setsid sh -c 'echo $$ > runner.pid; bin/Runner.Listener run --once; echo $? > runner.exit' > runner.log 2>&1 < /dev/null &)
`, quote(dir)) + followBody
}

func followScript(dir string) string {
	return fmt.Sprintf("cd %s || exit 1\n", quote(dir)) + followBody
}

// followBody streams the log until the runner exits and exits with its code,
// or 255 when it died without leaving one.
const followBody = `while [ ! -s runner.pid ]; do sleep 0.1; done
p=$(cat runner.pid)
tail -n +1 -f runner.log &
t=$!
while [ ! -e runner.exit ] && kill -0 "$p" 2>/dev/null; do sleep 0.2; done
sleep 0.3
kill "$t" 2>/dev/null
wait "$t" 2>/dev/null
exit "$(cat runner.exit 2>/dev/null || echo 255)"
`

func probeScript(dir string) string {
	return fmt.Sprintf(`cd %s || { echo none; exit 0; }
p=$(cat runner.pid 2>/dev/null)
if [ -z "$p" ]; then echo none
elif [ -e runner.exit ]; then echo "exited $(cat runner.exit)"
elif kill -0 "$p" 2>/dev/null; then echo alive
else echo "exited 255"
fi
`, quote(dir))
}

func diagScript(dir string) string {
	return fmt.Sprintf(`cd %s || exit 1
tail -n 40 runner.log 2>/dev/null
for f in $(ls -t _diag/Runner_*.log 2>/dev/null | head -n 1); do tail -n 40 "$f"; done
`, quote(dir))
}

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// lineWriter calls fn once per complete line written to it.
type lineWriter struct {
	fn  func(string)
	mu  sync.Mutex
	buf []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			return len(p), nil
		}
		line := strings.TrimRight(string(w.buf[:i]), "\r")
		w.buf = w.buf[i+1:]
		w.fn(line)
	}
}

func (w *lineWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) > 0 {
		w.fn(string(w.buf))
		w.buf = nil
	}
}

// limitedBuffer keeps the first diagLimit bytes written to it.
type limitedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if room := diagLimit - l.b.Len(); room > 0 {
		l.b.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

func (l *limitedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
