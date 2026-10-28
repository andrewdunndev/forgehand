# forgehand

A daemon that runs GitHub Actions jobs on single-use virtual machines.

forgehand listens to a GitHub Actions scale set. For each assigned job it boots
one VM through a fleeting plugin, hands the VM a just-in-time runner
configuration over SSH, and deletes the VM when the job ends. Capacity is driven
by taskscaler, so the autoscaler configuration follows gitlab-runner's
`[runners.autoscaler]`.

One binary, one TOML file, no database, no webhooks.

## Status

Pre-release. The configuration schema and daemon behavior are still changing.

## License

MIT
