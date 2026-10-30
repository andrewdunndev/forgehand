// Command forgehand runs GitHub Actions jobs on single-use virtual machines.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/hashicorp/go-hclog"

	"gitlab.com/dunn.dev/forgehand/internal/config"
)

// Version is stamped at build time with -ldflags "-X main.Version=...".
var Version = "dev"

var readBuildInfo = debug.ReadBuildInfo

// runSignals are the signals run handles.
var runSignals = []os.Signal{syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGTERM, syscall.SIGINT}

// version is the stamped Version, or the main module version of a go install build.
func version() string {
	return resolveVersion(Version, readBuildInfo)
}

func resolveVersion(stamped string, read func() (*debug.BuildInfo, bool)) string {
	if stamped != "dev" {
		return stamped
	}
	if bi, ok := read(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return stamped
}

const usage = `forgehand runs GitHub Actions jobs on single-use virtual machines.

usage:
  forgehand run -c config.toml     serve the configured scale sets until signalled
  forgehand check -c config.toml   validate the config and the App, and resolve the plugin
  forgehand version                print the version

-c is the path to the TOML config; see examples/forgehand.toml.`

func main() {
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr))
}

func execute(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	switch args[0] {
	case "help", "-h", "--help":
		_, _ = fmt.Fprintln(stdout, usage)
		return 0
	case "version", "--version":
		_, _ = fmt.Fprintln(stdout, "forgehand", version())
		return 0
	case "run", "check":
	default:
		_, _ = fmt.Fprintf(stderr, "forgehand: unknown command %q\n%s\n", args[0], usage)
		return 2
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {}
	path := fs.String("c", "", "path to the TOML config")
	switch err := fs.Parse(args[1:]); {
	case errors.Is(err, flag.ErrHelp):
		_, _ = fmt.Fprintln(stdout, usage)
		return 0
	case err != nil:
		return 2
	}
	if *path == "" {
		_, _ = fmt.Fprintf(stderr, "forgehand: -c is required\n%s\n", usage)
		return 2
	}
	cfg, err := config.Load(*path)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "forgehand: %v\n", err)
		return 1
	}

	level := hclog.LevelFromString(cfg.LogLevel)
	if level == hclog.NoLevel {
		_, _ = fmt.Fprintf(stderr, "forgehand: log_level %q is not one of trace, debug, info, warn, error\n", cfg.LogLevel)
		return 1
	}
	log := hclog.New(&hclog.LoggerOptions{Name: "forgehand", Level: level, Output: stderr})
	d := newDaemon(cfg, realDeps(), log)

	switch args[0] {
	case "check":
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		err = d.check(ctx, stdout)
	default:
		sigs := make(chan os.Signal, 4)
		signal.Notify(sigs, runSignals...)
		err = d.run(context.Background(), sigs)
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "forgehand: %v\n", err)
		return 1
	}
	return 0
}
