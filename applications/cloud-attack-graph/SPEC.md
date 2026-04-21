# SPEC — Cloud Attack Graph

Status: **Implemented M1 slice**

## Objective

Exercise TurboLynx on a small but realistic privilege graph where one leaked
secret can fan out into multiple reachable resources through chained identity
assumption.

## Current scope

- Tiny committed fixture only
- One scenario: `credential_blast_radius`
- Python API ↔ CLI differential coverage
- No live cloud ingestion yet

## Scenario S1 — Credential blast radius

Given a secret name and an assumption-hop budget, return the reachable
sensitive resources and the identity that can access them.

Acceptance in the current fixture:

- Python API result matches the committed golden file
- CLI result matches the same golden file
- Python API and CLI rowsets are identical after canonicalization
