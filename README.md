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
capacity, so most `[runners.autoscaler]` settings from gitlab-runner carry over
and any fleeting plugin should fit. `capacity_per_instance` and `max_use_count` are
rejected, because each VM runs one job.

## Status

Pre-release. The tests take one job end to end against fleeting's dummy plugin,
but forgehand has not yet run against real GitHub and a real VM. The
configuration schema is still changing.

## Prerequisites

- A GitHub App installed on the repository. Each scale set serves one
  repository, set as `url = "https://github.com/<owner>/<repo>"`, so N
  repositories need N scale sets. [Create the
  App](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app)
  with the repository permission Administration read and write and the webhook
  off, then install it and generate a private key. `client_id` is the App's
  client ID, not its App ID; `installation_id` is the number at the end of the
  installation's settings URL. Do not point a scale set at a public repository
  whose fork pull requests can run without approval; see GitHub's
  [self-hosted runner security guidance](https://docs.github.com/en/actions/reference/security/secure-use#hardening-for-self-hosted-runners).
- A fleeting plugin for your hypervisor or cloud, installed on the host.
  [fleeting-plugin-libvirt](https://gitlab.com/dunn.dev/fleeting-plugin-libvirt) is the reference plugin.
- A guest image with:
  - POSIX `sh` and `setsid`
  - [actions/runner](https://github.com/actions/runner) unpacked at `runner.dir`, writable by `connector_config.username`
  - SSH reachable from the host as `connector_config.username`; the plugin
    generates the key pair itself. Set `key_path` and
    `use_static_credentials = true` only to use a key the image already trusts
  - `autoscaler.instance_ready_command` gates readiness: a VM takes a job only
    after it exits 0 over SSH, and the connection retries until `connector_config.timeout`.
    Set it (`true` suffices) unless the image brings SSH up before the plugin reports an address

## Install

```
go install gitlab.com/dunn.dev/forgehand/cmd/forgehand@latest
```

Release binaries for linux amd64 and arm64 are on the
[releases page](https://gitlab.com/dunn.dev/forgehand/-/releases).

Or build from source with `mise install && just build`, which writes
`bin/forgehand`. Install a plugin with `fleeting-artifact install <ref>` and `FLEETING_PLUGIN_PATH`
set to the directory the unit sets (the plugin's README has its install line),
or set `plugin` to a binary path.

## Quickstart

```
curl -O https://gitlab.com/dunn.dev/forgehand/-/raw/main/examples/forgehand.toml
$EDITOR forgehand.toml
forgehand check -c forgehand.toml
forgehand run -c forgehand.toml
```

`check` resolves the plugin, finds the key files that are set and authenticates the App, then prints `ok`. It does not start the plugin; `plugin_config` is validated when `run` starts it. It creates nothing and starts no VM.
[`examples/forgehand.toml`](examples/forgehand.toml) comments each required key;
[`examples/forgehand.service`](examples/forgehand.service) is a hardened systemd unit.

## Verify a release

Download a binary from the releases page and its `.bundle` from the package registry (`https://gitlab.com/api/v4/projects/dunn.dev%2Fforgehand/packages/generic/forgehand/<tag>/forgehand-linux-amd64.bundle`; substitute the platform), then:

```
cosign verify-blob forgehand-linux-amd64 --bundle forgehand-linux-amd64.bundle \
  --certificate-identity-regexp '^https://gitlab\.com/dunn\.dev/forgehand//\.gitlab-ci\.yml@refs/tags/.+$' \
  --certificate-oidc-issuer https://gitlab.com
```

## Signals

- `SIGQUIT` drains: idle runners are removed and running jobs finish before the
  daemon exits. `SIGTERM` or `SIGINT` during a drain ends it and stops within `shutdown_timeout`.
- `SIGTERM` and `SIGINT` stop within `shutdown_timeout` seconds, running jobs
  included.

## Instance health

A failed pre-acquire heartbeat removes the instance at once; `failure_threshold` does not govern that.

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
