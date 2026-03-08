# TurboLynx — Claude Agent Guidelines

## Before Starting Any Significant Work
1. Read `TECHSPEC.md` — architecture and subsystem overview
2. Read `PLAN.md` — current milestone and status

## Build & Test Environment

**Build:**

```bash
cd /turbograph-v3/build-lwtest && ninja
```

**Git commits:**

```bash
cd /turbograph-v3 && git add ... && git commit -m "..."
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
- **Never modify a test just because it fails.** Fix the implementation, not the test.

## Code Standards
- All code and comments must be written in **English**.
- Follow existing C++17 patterns in the codebase.
- Do not add new dependencies without explicit discussion.
