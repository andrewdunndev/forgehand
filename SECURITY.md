# Security

forgehand holds a GitHub App private key and the SSH key of the guest VMs, and
it starts code on those VMs. This document says how to report a problem and
what is in scope.

## Reporting a vulnerability

Email **andrew@dunn.dev** with the subject line `[SECURITY] forgehand`. Include:

- The version affected (commit SHA or tag).
- A description of the issue and its impact.
- Steps to reproduce, where applicable.
- Whether you intend to disclose publicly, and any timeline you prefer.

I will acknowledge receipt within seven days, then aim to ship a fix or agree a
disclosure date within thirty days. Please do not open a public issue first.

## In scope

- Leaking the App private key, the guest SSH key or a just-in-time runner
  configuration through logs, errors, metrics or process arguments.
- Deleting or reusing a VM or runner that does not belong to the daemon.
- Letting one job reach the VM, runner or credentials of another.
- Code execution on the host through guest output or crafted configuration.

## Out of scope

- The unauthenticated `/metrics` endpoint on a non-loopback `listen_address`.
  The README documents it; bind it to loopback or firewall it.
- Vulnerabilities in a fleeting plugin, GitHub or actions/runner. Report those
  to their maintainers.
- Issues that need an attacker who already controls the host or the config file.
- Theoretical issues without a concrete attack path.

## Guest network

Guest SSH host keys are not verified, because fleeting's connector ignores
them. The guest network must stop guests spoofing each other's IP or MAC: use
VF spoofchk plus port isolation.
