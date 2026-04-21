# Cloud Attack Graph

A TurboLynx application that models cloud identities, workloads, secrets, and
resources as a privilege-escalation graph. The initial slice is intentionally
small: a committed fixture, one end-to-end scenario, and parity tests across
the Python API and CLI shell.

Status: **M1 — leaked-secret blast-radius scenario green on the committed fixture.**

## Scenario

`credential_blast_radius` answers a practical question:

> If a secret leaks from a workload, which sensitive resources become reachable
> through identity assumption chains?

The current query starts from a `Secret`, follows:

- `(:Secret)<-[:USES_SECRET]-(:Workload)`
- `(:Workload)-[:RUNS_AS]->(:Identity)`
- `(:Identity)-[:CAN_ASSUME*0..N]->(:Identity)`
- `(:Identity)-[:CAN_ACCESS]->(:Resource)`

and returns the reachable sensitive resources.

## Quickstart

```bash
python3 -m pip install --force-reinstall tools/pythonpkg/dist/turbolynx-*.whl
python3 -m pip install -e applications/cloud-attack-graph/app
TLX_BUILD_DIR=build-release pytest applications/cloud-attack-graph/tests -v
```

`TLX_BUILD_DIR` may also point to `build-portable/` on macOS.

## Layout

See [`../README.md`](../README.md) for the cross-application convention.
