![hero](hero.svg)

# forgehand

[![pipeline](https://gitlab.com/dunn.dev/forgehand/badges/main/pipeline.svg)](https://gitlab.com/dunn.dev/forgehand/-/pipelines)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)
![built with GitLab](https://img.shields.io/badge/built_with-GitLab-FC6D26?logo=gitlab)

A daemon that runs each GitHub Actions job on a virtual machine made for that job alone.

forgehand listens to a GitHub Actions scale set as a GitHub App. For each job it
takes a fresh VM from a [fleeting](https://gitlab.com/gitlab-org/fleeting/fleeting)
plugin, starts a just-in-time runner on it over SSH, and releases the VM when the
job ends. [taskscaler](https://gitlab.com/gitlab-org/fleeting/taskscaler) drives
capacity, so the `[runners.autoscaler]` settings from gitlab-runner carry over
and any fleeting plugin should fit.

## Status

Pre-release. The tests take one job end to end against fleeting's dummy plugin,
but forgehand has not yet run against real GitHub and a real VM. The
configuration schema is still changing.

## Prerequisites

- A GitHub App installed on the repository or organization, with Administration
  read and write. You need its client ID, its installation ID and a private key.
- A fleeting plugin for your hypervisor or cloud, installed on the host.
- A guest image with:
  - POSIX `sh` and `setsid`
  - [actions/runner](https://github.com/actions/runner) unpacked at `runner.dir`
  - SSH reachable from the host as `connector_config.username`; the plugin
    generates the key pair itself. Set `key_path` and
    `use_static_credentials = true` only to use a key the image already trusts

## Install

```
go install gitlab.com/dunn.dev/forgehand/cmd/forgehand@latest
```

Or build from source with `mise install && just build`, which writes
`bin/forgehand`.

## Quickstart

```
curl -O https://gitlab.com/dunn.dev/forgehand/-/raw/main/examples/forgehand.toml
$EDITOR forgehand.toml
forgehand check -c forgehand.toml
forgehand run -c forgehand.toml
```

`check` resolves the plugin, finds the key files that are set and authenticates the App, then
prints `ok` when `run` will start. It creates nothing and starts no VM.
[`examples/forgehand.toml`](examples/forgehand.toml) comments each required key;
[`examples/forgehand.service`](examples/forgehand.service) is a hardened systemd unit.

## Signals

- `SIGQUIT` drains: idle runners are removed and running jobs finish before the
  daemon exits. `SIGTERM` or `SIGINT` during a drain ends it and stops within `shutdown_timeout`.
- `SIGTERM` and `SIGINT` stop within `shutdown_timeout` seconds, running jobs
  included.

## Metrics

With `listen_address` set, forgehand serves Prometheus `/metrics` there. The
endpoint has no authentication, so bind it to loopback or firewall it.

## Contributing

Issues and merge requests go to [GitLab](https://gitlab.com/dunn.dev/forgehand).
See [CONTRIBUTING.md](CONTRIBUTING.md); report vulnerabilities as
[SECURITY.md](SECURITY.md) describes.

## License

[MIT](LICENSE).

Built by Andrew Dunn, 2026.
