# forgehand

A daemon that runs GitHub Actions jobs on single-use virtual machines.

forgehand listens to a GitHub Actions scale set. For each assigned job it boots
one VM through a fleeting plugin, hands the VM a just-in-time runner
configuration over SSH, and deletes the VM when the job ends. Capacity is driven
by taskscaler, so the autoscaler configuration follows gitlab-runner's
`[runners.autoscaler]`.

One binary, one TOML file, no database, no webhooks.

## Usage

    forgehand check -c config.toml   # config, App auth and scale set labels; starts nothing
    forgehand run -c config.toml

`run` creates each scale set on first start and never deletes it. A scale set
whose labels differ from the config is refused: labels are fixed at creation,
so rename the set in the config.

SIGQUIT drains: idle runners are removed at once and running jobs finish.
SIGTERM and SIGINT stop within `shutdown_timeout` seconds and end running jobs.
`listen_address` serves the taskscaler and fleeting Prometheus metrics at
`/metrics`.

## Status

Pre-release. The configuration schema and daemon behavior are still changing.

## License

MIT
