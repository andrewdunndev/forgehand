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

Pre-release. The tests take one job end to end against fleeting's dummy plugin;
nothing has run on real hardware yet, and the configuration schema is still
changing. Issues and merge requests go to
[GitLab](https://gitlab.com/dunn.dev/forgehand); a GitHub mirror may come later.

Build with `mise install && just build`; test with `just test`.

## License

[MIT](LICENSE).
