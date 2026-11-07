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
and any fleeting plugin should fit. `capacity_per_instance`, `max_use_count` and
`delete_instances_on_shutdown` are rejected, because each VM runs one job and
forgehand deletes its VMs when it stops (the `SIGUSR1` handoff is the one exception).

## Status

forgehand has run end to end against real GitHub (a GitHub App on a user-owned
repository) and real libvirt VMs, including restart adoption, the upgrade
handoff, UEFI Secure Boot, a vTPM and PCI passthrough. Before 1.0 the
configuration may still change in a minor release; the release notes say how.

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
    generates the key pair itself. Set `use_static_credentials = true` with
    `key_path` (read at start) to survive a daemon restart: the image must trust the matching public key, because nothing is seeded
  - `autoscaler.instance_ready_command` gates readiness: a VM takes a job only
    after it exits 0 over SSH, and the connection retries until `connector_config.timeout`.
    Set it (`true` suffices) unless the image brings SSH up before the plugin reports an address
- Guest host keys are not verified, so guests need an isolated network; see the Guest network section of [SECURITY.md](SECURITY.md).
- The fleeting plugin runs as the same user as forgehand and is trusted code.

## Scale set names

Every scale set name must be one path segment: not `.` or `..`, with no `/` or
`\` and no control characters. With static credentials the name is also the
scale set's state directory.

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

A service user other than the plugin's installer needs `go+rX` on the plugin directory, since a restrictive umask leaves it `0700`. On an SELinux host a directory made by hand for the binary starts as `usr_t`; `restorecon -RF` on it gives `bin_t`.

## Quickstart

```
curl -O https://gitlab.com/dunn.dev/forgehand/-/raw/main/examples/forgehand.toml
$EDITOR forgehand.toml
forgehand check -c forgehand.toml
forgehand run -c forgehand.toml
```

`check` resolves the plugin, finds the set key files and authenticates the App, then prints `ok`; it creates nothing, starts no VM and leaves `plugin_config` to `run`.
[`examples/forgehand.service`](examples/forgehand.service) is the user unit of a lingering `forgehand` service user. Use a user unit, not a system unit: on an SELinux host the libvirt daemon (`virtproxyd_t`) is denied the /proc of a system service (`unconfined_service_t`) and polkit ignores `SupplementaryGroups`, so libvirt refuses it without local policy; the user manager runs it with the user's `libvirt` group. With the libvirt plugin:

```
sudo useradd --system --create-home --home-dir /var/srv/forgehand --groups libvirt forgehand
sudo loginctl enable-linger forgehand
sudo -u forgehand mkdir -p ~forgehand/.config/forgehand ~forgehand/.config/systemd/user
sudo install -o forgehand -m 0600 forgehand.toml app.pem ~forgehand/.config/forgehand/
sudo install -o forgehand -m 0644 forgehand.service ~forgehand/.config/systemd/user/
sudo -u forgehand /usr/local/bin/forgehand check -c ~forgehand/.config/forgehand/forgehand.toml
sudo -u forgehand XDG_RUNTIME_DIR=/run/user/$(id -u forgehand) systemctl --user enable --now forgehand
```

A user manager reads its user's groups when it starts. If `forgehand` joined `libvirt` after the manager was running, restart it with `systemctl restart user@$(id -u forgehand).service`; until then the plugin reports permission denied on the socket.

Run `systemctl --user` and `journalctl --user` with that `sudo -u forgehand XDG_RUNTIME_DIR=...` prefix. With `UMask=0077`, create a plugin `lock_file` shared with other accounts beforehand, with its mode.

Hardware is plugin config that forgehand passes through untouched: `tpm` and `pci_devices`
in `plugin_config`, one scale set per hardware class so `runs-on` selects it. A scale set with
`pci_devices` sets `max_instances` at most the number of addresses. Each `pci_devices` scale set needs its own pool, disjoint from every other scale set's; overlap makes
`max_instances` overstate capacity. The example config
shows both, and the [plugin README](https://gitlab.com/dunn.dev/fleeting-plugin-libvirt) defines the keys. A passed-through device may keep state between jobs, so serve trusted repositories only; see the plugin README.

## Configuration keys

[`examples/forgehand.toml`](examples/forgehand.toml) shows the required keys.
Keys whose defaults forgehand sets itself:

| Key | Default | Meaning |
|---|---|---|
| `runner.listen_timeout` | `3m` | how long a started runner may take to start listening for a job |
| `runner.idle_timeout` | `5m` | how long a listening runner waits for a job before its VM is released |
| `autoscaler.instance_acquire_timeout` | `15m` | how long a job waits for a VM |
| `shutdown_timeout` (top level) | `30` | seconds to drain on shutdown |

The remaining `[scalesets.autoscaler]` timing keys (`update_interval`,
`update_interval_when_expecting`, `deletion_retry_interval`,
`shutdown_deletion_interval`, `shutdown_deletion_retries`), the `scale_throttle`
table, the `[[scalesets.autoscaler.policy]]` tables (`periods`, `timezone`,
`idle_count`, `idle_time`, `scale_factor`, `scale_factor_limit`,
`preemptive_mode`) and the `connector_config` keys (`protocol_port`,
`keepalive`, `use_external_addr`, among others) take taskscaler's defaults and
mean what they do in GitLab's
[`[runners.autoscaler]`](https://docs.gitlab.com/runner/configuration/advanced-configuration/#the-runnersautoscaler-section)
documentation. Unknown keys are refused, so a typo fails at load, and so is `connector_config.password`: secrets reach forgehand only as key files.

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
- `SIGUSR1` hands off, only when every scale set sets `use_static_credentials`
  (otherwise it is ignored and logged as an error): idle runners are removed,
  nothing new starts, and once no runner is waiting for a VM, starting,
  listening, being removed or being adopted, and the VMs of the removed
  runners are deleted, the daemon exits 0 without
  deleting the VMs of running jobs. During a drain or a handoff only
  `SIGTERM` and `SIGINT` act; `SIGQUIT` and `SIGUSR1` are logged and ignored.
  To stop a handoff early, send SIGTERM (`systemctl --user kill --kill-whom=main -s SIGTERM forgehand`);
  that stops running jobs.
  A tripped failure breaker during a handoff exits 1 and still leaves the VMs
  of running jobs. `SIGUSR1` during startup is dropped: send it again once
  every scale set has started. See Upgrade.
- `SIGTERM` and `SIGINT` stop within `shutdown_timeout` seconds, running jobs
  included.
- A crash (`SIGKILL`) leaves the VMs and their runners running; see Restart.

Under `Type=notify`, as the example unit sets, forgehand sends `READY=1` once every
scale set holds a message session, and `STOPPING=1` when a drain, handoff or stop begins.
Without `NOTIFY_SOCKET` it sends nothing.

## Restart

With `use_static_credentials = true`, forgehand keeps its autoscaler state in
`$STATE_DIRECTORY/<scale set name>` (`~/.local/state/forgehand/` in the example unit). `run` refuses to start without `STATE_DIRECTORY`
when a scale set uses them. A restart adopts what an earlier run left:

- A VM whose runner is running a job keeps running; the new process follows
  the runner and holds the VM until GitHub reports the job complete.
  A runner that has already exited leaves the VM held the same way, and GitHub
  is asked about it every 30 seconds.
- A VM whose runner is idle or has not taken its job is deleted, and its
  runner is removed. GitHub offers a job assigned to it again to a new VM.
- A VM held warm by `idle_count` stays.
- Nothing starts a new VM for a job until the restored VMs have been counted.
  That wait ends at once when none are restorable, and after 3 minutes at most.
- `SIGTERM` during that wait deletes every VM but removes only the runners of
  slots already adopted; the others stay registered.

The state files hold no JIT config, but they hold each VM's connection
details, including the guest SSH private key. They are mode 0600 in a 0700
directory, readable only by the service user and root.

Without static credentials the plugin generates the guest key itself, and a
restart cannot reach the VMs an earlier run left. They are deleted and their
runners removed, and any job they ran is lost.

## Upgrade

With `use_static_credentials = true` on every scale set, a running job survives
an upgrade:

```
systemctl --user kill --kill-whom=main -s SIGUSR1 forgehand
# wait for the service to exit
# replace the binary
systemctl --user start forgehand
```

The old process removes idle runners and exits once the VMs of the removed runners are deleted, leaving the VMs of
running jobs for the new one to adopt as in Restart. `--kill-whom=main` keeps the signal off
the plugin child. With generated credentials `SIGUSR1` is ignored. Replace the binary, then
`systemctl --user restart --no-block forgehand`: the unit's `KillSignal=SIGQUIT` drains, so the new binary
starts once the jobs end. `--no-block` returns at once, and the unit reads `deactivating` until then.

## Instance health

A failed pre-acquire heartbeat removes the instance at once; `failure_threshold` does not govern that.

## Metrics

With `listen_address` set, forgehand serves Prometheus `/metrics` there. The
endpoint has no authentication, so bind it to loopback or firewall it.

forgehand adds these, each with a `scaleset` label:

- `forgehand_prepare_failures_total{stage}` counts guests that failed to prepare (`connect_info` or `ready_command`), and `forgehand_prepare_consecutive_failures` is the run since the last that succeeded.
- `forgehand_admission_refusals_total` counts guest creations the plugin's admission check refused.
- `forgehand_slot_failures_total{reason}` counts slots that failed: `acquire_timeout`, `acquire`, `runner_start`, `runner_exit` or `listen_timeout`.

## Contributing

Issues and changes are welcome on either forge:

- GitLab (canonical): https://gitlab.com/dunn.dev/forgehand
- GitHub (mirror): https://github.com/andrewdunndev/forgehand

GitHub pull requests land through GitLab; the mirror then marks them merged.

See [CONTRIBUTING.md](CONTRIBUTING.md); report vulnerabilities as
[SECURITY.md](SECURITY.md) describes.

## License

[MIT](LICENSE).

Built by Andrew Dunn, 2026.
