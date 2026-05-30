# L1 — quarantined inputs

Inputs that surface a real ASAN/UBSAN finding but where the underlying
bug is **not yet fixed**.  They live here instead of `../corpus/` so the
smoke target stays green; once the underlying bug lands a fix, move the
file back into `corpus/regression_*.cypher` to lock the fix in.

## Currently empty

The first batch of quarantined inputs — three ORCA exception-unwind
heap-use-after-free reproducers (Bug A, `#181`, `#209`) — was closed by
issue `#232` ("ORCA heap-use-after-free in `~CAutoTaskProxy`
exception-unwind cleanup") and moved back into `../corpus/` as
`regression_*.cypher` once the fix landed.

Future findings that cannot be fixed in the same change-set should be
moved here with a short note about the open issue.
