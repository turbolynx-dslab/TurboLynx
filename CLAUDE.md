# TurboLynx — Claude Agent Guidelines

## Before Starting Any Significant Work
1. Read `TECHSPEC.md` — architecture and subsystem overview
2. Read `PLAN.md` — current milestone and status

## Build & Test Environment

**Build:** Always inside the `turbograph-s62` Docker container.
Use the MCP tool `mcp__docker-turbograph__exec_in` (container: `turbograph-s62`):

```bash
cd /turbograph-v3/build-lwtest && ninja
```

**Test:** Run after every implementation — no exceptions.

```bash
cd /turbograph-v3/build-lwtest && ctest --output-on-failure
```

Specific module:

```bash
./test/unittest "[catalog]"
./test/unittest "[storage]"
./test/unittest "[common]"
./test/unittest "[execution]"
```

## Workflow Rules
- When in doubt about scope or approach, **ask — don't guess.**
- Keep changes small and focused. **One milestone at a time.**
- Before implementing anything, check if a utility or function already exists.
- Prefer editing existing files over creating new ones.
- Always run the relevant test suite after implementation.

## Code Standards
- All code and comments must be written in **English**.
- Follow existing C++17 patterns in the codebase.
- Do not add new dependencies without explicit discussion.
