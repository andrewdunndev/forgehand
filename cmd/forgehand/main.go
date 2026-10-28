// Command forgehand runs GitHub Actions jobs on single-use virtual machines.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/hashicorp/go-hclog"

	"gitlab.com/dunn.dev/forgehand/internal/config"
)

var version = "dev"

const usage = `usage: forgehand run -c config.toml
       forgehand check -c config.toml`

func main() {
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr))
}

func execute(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("c", "", "path to the TOML config")
	if args[0] != "run" && args[0] != "check" {
		fmt.Fprintf(stderr, "forgehand: unknown command %q\n%s\n", args[0], usage)
		return 2
	}
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *path == "" {
		fmt.Fprintf(stderr, "forgehand: -c is required\n%s\n", usage)
		return 2
	}
	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintf(stderr, "forgehand: %v\n", err)
		return 1
	}

	level := hclog.LevelFromString(cfg.LogLevel)
	if level == hclog.NoLevel {
		fmt.Fprintf(stderr, "forgehand: log_level %q is not one of trace, debug, info, warn, error\n", cfg.LogLevel)
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
		signal.Notify(sigs, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
		err = d.run(context.Background(), sigs)
	}
	if err != nil {
		fmt.Fprintf(stderr, "forgehand: %v\n", err)
		return 1
	}
	return 0
}
