# Bug Report

Use this file to capture TurboLynx issues found while expanding the cloud
attack-graph application.

## Current status

The initial Python packaging issue is fixed in the repo and no active app-local
engine bugs remain.

### 2026-04-21 — Installed Python wheel selected the wrong build artifacts

- Surface: wheel packaging path (`pip wheel tools/pythonpkg`) on a checkout with
  multiple CMake build directories
- Reproduction shape:
  1. keep both `build-portable/` and `build-release/` in the repo
  2. run `python3 -m pip wheel tools/pythonpkg -w dist`
- Observed result: `setup.py` picked `build-portable/` first and bundled stale
  binaries, even though the intended artifacts were in `build-release/`
- Expected result: the wheel should default to the active `build-release/`
  artifacts unless `TURBOLYNX_BUILD_DIR` explicitly overrides it
- Status: fixed by preferring `build-release/` in `tools/pythonpkg/setup.py`

## Entry template

### YYYY-MM-DD — Short title

- Surface:
- Reproduction query / import shape:
- Observed result:
- Expected result:
- Status:
