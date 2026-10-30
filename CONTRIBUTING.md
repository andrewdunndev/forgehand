# Contributing to forgehand

forgehand is a personal project; contributions are welcome, and the bar is
whether they make a single-use VM per job more reliable. Issues and merge
requests go to [GitLab](https://gitlab.com/dunn.dev/forgehand).

## Development environment

`mise install` provides Go, just and golangci-lint at the pinned versions. No
GitHub account or hypervisor is needed: tests run against fakes and fleeting's
dummy plugin.

```
just test    the test suite
just lint    golangci-lint and go vet
just build   bin/forgehand
```

Run `just lint` and `just test` before opening a merge request.

## Conventions

- Errors say what to do next, not only what went wrong.
- `plugin_config` belongs to the plugin: forgehand passes it through untouched
  and never requires or adds a key.
- Comments are sparse and explain why, never history.
- Tests assert something that fails when the behavior breaks; check a new test
  by breaking the code under it.
