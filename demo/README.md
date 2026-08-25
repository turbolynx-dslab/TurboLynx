# TurboLynx booth demo — the feature ladder

A five-click, browser-based demo for the VLDB booth. **One analytical query**,
run four times, with one more TurboLynx feature switched on each time. Same
query text, same 167,260-row answer, four very different execution times:

| # | click | what it turns on | execute (certified) | drop |
|---|-------|------------------|--------------------:|-----:|
| 1 | Run baseline | nothing | **4093 ms** | — |
| 2 | Prune schema | converter graphlet pruning (SI) | **2234 ms** | −45.4% |
| 3 | Split per district | GEM per-branch join ordering | **799 ms** | −64.3% |
| 4 | Pack rows | adaptive row format (SSRF) | **336 ms** | −58.0% |
| 5 | Verify results | — | | |

**End to end 12.2×**, and monotone on every one of the five certification
sessions individually. The fifth click is the point of the whole thing: it
shows that every rung returned *the same answer* — 33,452 venues, 167,260
result rows, sorted-CSV md5 `af57c00cbfe204ef665dfedebc64c2bb`, identical on
all four rungs.

## The one command

```bash
cd /TurboLynx/demo && ./run_demo.sh
```

That is the whole thing. It checks for the workspace (offering to build it if
it is not there), fetches the offline fonts, builds the served page, starts the
server, waits for the four warmup rungs, and prints the URL plus the SSH-tunnel
line. `Ctrl-C` stops it.

```bash
./run_demo.sh --regen        # force a full data rebuild (~10 min) first
./run_demo.sh --port 8600    # serve somewhere else
./run_demo.sh --no-fonts     # skip the font fetch; page then needs internet
```

Env overrides: `BIN` (binary, default `../build-release/tools/turbolynx`),
`SRC` (generated data, default `/data/ladder-v7-src`), `WS` (workspace,
default `/data/ladder-v7-ws`), `PORT` (default 8500).

From a laptop: `ssh -N -L 8500:localhost:8500 <user>@<host>`, then open
<http://localhost:8500/>. Add `?mock=1` for a design-only preview that makes
no engine calls.

## The hero query

Identical text on every rung — only planner flags and the `WHERE` form change.

```cypher
MATCH (a:`NODE`)-[:FOLLOWS]->(b:`NODE`)-[:RECOMMENDS]->(c:`NODE`),
      (a)-[:VISITS]->(c)
WHERE a.kind = 'person'          -- baseline rung uses: a.kind IS NOT NULL
WITH  c, count(*) AS reach       -- <- the GROUP BY: 172,000 -> 33,452 venues
MATCH (p:`PROFILE`)-[:PROFILE_OF]->(c)
RETURN c.title AS venue, reach, <200 p.* columns>;
```

"For every venue, how many follow → recommend → visit paths reach it, and its
full profile card." 172,000 traversal rows collapse 5.14× into 33,452 venues;
each venue then materializes its five heterogeneous profile records, giving
167,260 rows of 202 columns each of which only 5 of the 200 profile columns are
non-NULL.

The shape is load-bearing, not cosmetic. The aggregation must sit **below** the
wide profile seek: the binder rewrites any property of a `WITH`-visible
variable projected after the aggregation into a `first(...)` aggregate, which
drags the 200-column seek back underneath the HashAgg and re-flattens the row
format (measured −4.3% instead of −58%). Introducing `p` in a second `MATCH`
after the `WITH` is the only shape in the search space that keeps it above.
Six other shapes were tried and failed — `evidence/SHAPE_TRIALS.txt`.

## How long a click takes

| click | execute | wall clock |
|-------|--------:|-----------:|
| baseline | 3.9 s | **~14 s** |
| +SI | 2.2 s | ~10 s |
| +GEM | 0.79 s | **~25 s** |
| +SSRF | 0.34 s | ~25 s |

The GEM rungs are slow to *start*, not slow to run. GEM's join-order search on
a multi-part query is ~12× more expensive than the default planner and scales
~48 ms per projected column, so each planning pass costs ~8 s — and the
certification protocol (`-i 1 --warmup`) plans **twice**, once for the warmup
pass and once for the timed one. That is a documented planner defect and the
single highest-value follow-up; fixing it would make this demo pace like the
row-hero one. The page discloses it rather than hiding it: the hero KPI is
execute time, the compile+execute figure is printed beneath it *labelled as
such* alongside the measured end-to-end click time, and the run button reads
"Compiling plan… ~25 s".

Server startup warms all four rungs before anyone clicks (~2 min), so the first
booth click is already primed.

## Honesty notes — say these out loud at the booth

- **The dataset is a synthetic, illustrative sample**, built by `gen_data.py`
  specifically to make these four features visible in one query. It is not a
  benchmark. The page carries a "Mockup" chip and the graph visualization is a
  fixed sample, not a live render of the workspace. An LDBC/TPC-H rung is the
  honest answer to "is this a strawman?" and is a known, declined follow-up.
- **The baseline's `IS NOT NULL` form is a deliberate un-prunable spelling** of
  the same predicate, not a handicap: it returns the identical 167,260 rows
  (md5-identical), it just cannot drive converter graphlet pruning. The editor
  pane on the page shows `IS NOT NULL` on the baseline rung and `= 'person'`
  afterwards, so the page always displays the query it actually ran.
- **`TLX_GEM_SPLIT_TARGET=a` is a disclosed demo planner hint**, used on rungs
  3 and 4 to work around a documented zero-fanout-anchor costing defect. It
  picks the split target; it does not manufacture the speedup.
- **The SSRF rung here is time, not memory.** Peak RSS is indistinguishable
  between rungs 3 and 4 (2,111,072 vs 2,110,924 kB) because the result streams.
  For the memory half of the SSRF story quote the separate ssrf8 card
  (11.5×, 12.3 GB vs 731 MB), not this rung.
- **The "SI off" what-if is 1.20×, not 9.4×.** The row-hero demo's dramatic
  hidden card does not reproduce on this grouping shape. What survives is the
  memory half: without SI, GEM still buffers 5,961,518 peak rows against
  rung 3's 167,260 — 36× — so SI is what makes peak equal the answer. The page
  states it that way.
- **Numbers vary run to run.** The certified figures are quiet-machine medians
  of five interleaved sessions; individual runs were observed at
  3.7–5.3 / 2.1–2.9 / 0.73–1.00 / 0.32–0.38 s. On a busy host every rung slows,
  but the ladder stays monotone.

## Data and disk

| | size | time |
|---|---:|---:|
| generated source (`SRC`) | ~529 MB | ~4 min |
| loaded workspace (`WS`) | ~2.1 GB | ~6 min |
| offline fonts | ~3 MB | seconds |

**~2.6 GB total.** Both paths default under `/data`. The generator is
deterministic per `--seed`, and the certified v7 flags are baked in as
defaults, so a bare `python3 gen_data.py` reproduces exactly the source the
numbers were measured on. Totals: 450,000 NODE + 2,250,000 PROFILE nodes across
14 and 40 graphlets respectively, and 19,913,000 edges.

The workspace must be loaded with the **engine-default** clustering costs and
with the labels pinned to exactly `NODE` and `PROFILE` — `load_data.sh`
explains why both matter and warns if the environment would break them.

## Files

```
demo/
  run_demo.sh      THE one command (generate → load → fonts → build → serve → warm)
  gen_data.py      certified v7 generator; bare run reproduces the demo dataset
  load_data.sh     turbolynx import into a workspace (~6 min)
  server.py        4-mode JSON API (base|si|gem|ssrf) + static file server
  ui.html          the booth page (master; keeps CDN font links)
  fetch_fonts.py   downloads the woff2 set + writes fonts.css for offline use
  evidence/        the plan/attribution proof — read these before defending a number
  qa/              live QA, smoke test, screenshot harness
```

`run_demo.sh` builds the served web root into `.run/` (`index.html`, `fonts.css`,
`fonts/`) — generated, gitignored, and the *only* thing the HTTP server exposes,
so the repo tree it lives in is not browsable. `ui.html` is the master and
deliberately keeps its Google Fonts links so it also works as a standalone
artifact; the launcher swaps them for `fonts.css` in the served copy and fails
loudly if either half of that swap does not take.

### evidence/

- `FINAL_GROUPING_LADDER.txt` — the specification: protocol, the certified
  table with per-session samples, worst-case pairwise drops, results identity,
  peak-row and row-format evidence, plan shapes, every caveat.
- `SIDE_BY_SIDE_FOR_TSLEE_GROUPING.txt` — the reviewer-facing sheet answering
  "the paper says grouping but the hero returns raw columns".
- `SHAPE_TRIALS.txt` — the seven query shapes and five data levers that were
  tried, with numbers, and why six of the shapes fail.
- `UI_SERVER_CHANGES.txt` — what changed in the page and the API to move from
  the v6 row-hero to this grouping query, plus the four review rounds.

### qa/

- `qa_live_v7.py` — 18-check live QA of all six UI states against the real
  engine (~4 min; a GEM click is ~25 s).
- `smoke_stepper.py` — fast `?mock=1` smoke test: advance ×5, tabs, what-if,
  back ×5, re-advance, jump, reset, zero console errors.
- `shoot.py` — booth screenshots at four viewport sizes into `qa/shots/`.

All three drive a real browser and need Playwright plus its Chromium build:

```bash
pip install playwright && playwright install chromium
# or, if the system Python is managed:
python3 -m venv /tmp/pwenv && /tmp/pwenv/bin/pip install playwright \
  && /tmp/pwenv/bin/playwright install chromium
```

Start the server first; they all target `http://127.0.0.1:8500/`.

## Legacy demos

Other demos live alongside this one and are untouched by it:

- `legacy/gem-divergent-order/` — the focused CLI demo for GEM per-branch join
  ordering on the synth-y fixture: opposite-profile graphlets, the split, the
  divergent per-branch orders, and the ~1300× peak-intermediate payoff. Run
  `./legacy/gem-divergent-order/run_demo.sh`. This is the mechanism the booth
  ladder's third rung shows off, stripped to a terminal.
- `agentic-demo/`, `app/`, `api/`, `narration/`, `refs/`, `scripts/`,
  `DEMO_SCENARIO.md` — older demo material, unrelated to the booth ladder.
