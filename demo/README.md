# TurboLynx booth demo — the feature ladder

A six-step, browser-based demo for the VLDB booth. **One analytical query**,
run four times, with one more TurboLynx feature switched on each time. Same
query text, same 167,260-row answer, four very different execution times:

| # | click | what it turns on | reference execute | drop |
|---|-------|------------------|--------------------:|-----:|
| 1 | Run step · CGC | bulk-load illustration | — | — |
| 2 | Run step · Baseline | nothing | **4093 ms** | — |
| 3 | Run step · SI | converter graphlet pruning (SI) | **2234 ms** | −45.4% |
| 4 | Run step · GEM | GEM per-branch join ordering | **799 ms** | −64.3% |
| 5 | Run step · SSRF | adaptive row format (SSRF) | **336 ms** | −58.0% |
| 6 | Run step · Verify | — | | |

**End to end 12.2×**, and monotone on every one of the five certification
sessions individually. The final click is the point of the whole thing: it
shows that every rung returned *the same answer* — 33,452 venues, 167,260
result rows, with identical counts and top-10 rows across all four rungs.

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

On macOS, build natively with Ninja and put the generated data in the ignored
build directory instead of the Linux `/data` default. From the repository root:

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -S . -B build-release
ninja -C build-release turbolynx_bin
SRC="$PWD/build-release/demo-data/src" WS="$PWD/build-release/demo-data/ws" \
  ./demo/run_demo.sh --regen
```

Use the same `SRC` and `WS` values without `--regen` on subsequent launches.
The target above builds the demo CLI and its engine dependencies only. On this
branch, a default all-target build can fail in the unrelated LDBC query tests
because the default fixture lacks the referenced `IC10_*` / `IC11_*` constants.
The backend uses Python's built-in subprocess timeout; GNU `timeout` is not
required. Build dependency downloads and compilation add to the dataset setup
time above. The certified timings below are reference measurements, not a
promise of identical timings on another machine.

From a laptop: `ssh -N -L 8500:localhost:8500 <user>@<host>`, then open
<http://localhost:8500/>. Add `?mock=1` for a design-only preview that makes
no engine calls.

### Walk through the demo

The initial view explains the data semantically: people follow pages, pages
recommend venues, people visit venues, and a venue owns profile properties.
The first **Run Step** turns that diagram into mixed record nodes and draws the
relations between them, exposing the unorganized graph. The second **Run Step**
animates Cost-based Graphlet Chunking: compatible records gather by property
schema as the Graphlet view appears. Both are bulk-load illustrations over the
existing workspace, do not rebuild data, and are excluded from measured query
timings. Reduced-motion preferences skip movement. Reset cancels either
animation and restores the semantic view.

Continue with **Run Step** through **Baseline → SI → GEM → SSRF → Verify**.
The step strip names the current and next stages, so the button carries no
duplicate badge or keyboard hint. Back navigation preserves cached query
results; returning to the initial view lets you replay CGC.

The edge comparison uses readable relation rows and side-by-side district
counts. Desktop windows fit the dashboard and Run controls to the viewport;
short windows let the reference catalog scroll inside its card. Narrow windows
stack the cards and scroll vertically. The lower-right panel accumulates
Baseline, +SI, +GEM and +SSRF measurements in one table: Execute bars share a
scale, Optimize is a separate column, and speedups use Baseline as the reference.
Unrun steps stay blank; browsing a previous step preserves all measured rows.
Panel dimensions stay fixed across CGC, previews, execution and completed runs.
There is no End to End Time footer or separate metric-card collection.

Before each new SI/GEM/SSRF run, the center card shows a short guided
illustration. Touching any control pauses the guide; **Run query** executes the
unchanged engine query. **Return to graph** and **Explore** switch the center
without moving the surrounding panels. The illustrations are not live engine
traces and never update measurement rows:

- SI: compare reading all 14 candidate schemas with pruning to four for `a`.
  Select a schema and **Read anyway** to see the extra rejected work. This
  changes the illustration only; `b` and `c` still use their own candidates.
- GEM: compare shared FOLLOWS/VISITS starts with different orders per group.
  Plan search illustrates shared plans, full pushdown, equivalent rewrites and
  virtual grouping; displayed alternatives are representative, not plan counts.
- SSRF: the original graph stays visible. Select a venue and trace its five
  `VENUE_PROFILE` section records into the **Intermediate** tab. A dashed query
  outline marks the profile lookup before RETURN. Columnar shows the five
  semantic properties populated by each section (for example `venue_type`,
  `capacity`, and `rating`) beside `NULL × 195`. SSRF packs those same named
  properties without NULL padding. Values follow the generator for the
  selected graph node; the final RETURN stays unchanged.

SI/GEM support scrubbing; all three support replay, keyboard controls and reduced motion.
Music fans and Old Town residents are two `kind = 'person'` populations. Their
different property shapes become distinct graphlets; they are not names for
the page and venue subgraphs reached from those people.

The Plan tab reuses the SVG-card/subtree-layout approach from
`app/components/scenes/S3_Plan.tsx`, adapted in `plan-tree.js` for this console.
Its default overview groups consecutive operators into a shallow tree: GEM's
two real child branches spread sideways under UnionAll. Baseline/SI keep their
single path. Select a group to inspect every underlying operator and row count,
or choose **All operators** for the complete tree with zoom, pan and orientation
controls. Grouping preserves operator order, facts and accounting from the
trace-backed list; it does not invent branches. Plan navigation never resizes
the surrounding dashboard. The evidence panel has Results and Plan tabs, plus Intermediate during SSRF.

## The hero query

The presented query stays identical on every rung; only planner features change.

```cypher
MATCH (a:`NODE`)-[:FOLLOWS]->(b:`NODE`)-[:RECOMMENDS]->(c:`NODE`),
      (a)-[:VISITS]->(c)
WHERE a.kind = 'person'
WITH  c, count(*) AS reach       -- <- the GROUP BY: 172,000 -> 33,452 venues
MATCH (vp:`VENUE_PROFILE`)-[:PROFILE_OF]->(c)
RETURN c.title AS venue, reach, vp.*;  // 200 venue properties
```

"For every venue, how many follow → recommend → visit paths reach it, and its
full profile card." 172,000 traversal rows collapse 5.14× into 33,452 venues;
each venue then materializes five heterogeneous venue-profile sections, giving
167,260 rows of 202 columns. Each section fills 5 of the 200 named properties;
the remaining 195 are NULL in the columnar union.

The shape is load-bearing, not cosmetic. The aggregation must sit **below** the
wide profile seek: the binder rewrites any property of a `WITH`-visible
variable projected after the aggregation into a `first(...)` aggregate, which
drags the 200-column seek back underneath the HashAgg and re-flattens the row
format (measured −4.3% instead of −58%). Introducing `vp` in a second `MATCH`
after the `WITH` is the only shape in the search space that keeps it above.
Six other shapes were tried and failed — `evidence/SHAPE_TRIALS.txt`.

## How long a click takes

| click | execute | wall clock |
|-------|--------:|-----------:|
| baseline | 3.38 s | **~3.9 s** |
| +SI | 1.83 s | ~2.3 s |
| +GEM | 0.65 s | **~1.0 s** |
| +SSRF | 0.33 s | ~0.8 s |

GEM previously spent ~8 seconds compiling because the same expansion ran
2,305 extra times for one memo group. The xform now performs that expansion
once, bringing GEM planning to ~0.37 seconds. The server also keeps one warm
engine process per mode, so each click pays one compile and one warm execution;
nothing on the timed path is cached or replayed.

Server startup primes all four long-lived sessions (~80 s), keeping the
one-off process load out of booth clicks.

## Honesty notes — say these out loud at the booth

- **The dataset is a synthetic, illustrative sample**, built by `gen_data.py`
  specifically to make these four features visible in one query. It is not a
  benchmark. The page carries a "Mockup" chip and the graph visualization is a
  fixed sample, not a live render of the workspace. An LDBC/TPC-H rung is the
  honest answer to "is this a strawman?" and is a known, declined follow-up.
- **Baseline calibration uses an equivalent `IS NOT NULL` form internally** to
  keep converter graphlet pruning disabled. It returns the identical 167,260
  rows and grouping. The demo editor deliberately keeps the presented query
  fixed as `a.kind = 'person'`, so the audience compares planner mechanisms
  without the query appearing to change between rungs.
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
  of five interleaved sessions. The measured ladder is 3.38 / 1.83 / 0.65 /
  0.33 s execute with worst-case pairwise drops of 40.9% / 61.8% / 47.6%.
  On a busy host every rung slows, but the ladder stays monotone.

## Data and disk

| | size | time |
|---|---:|---:|
| generated source (`SRC`) | ~670 MB | host-dependent |
| loaded workspace (`WS`) | ~3.0 GB | host-dependent |
| offline fonts | ~3 MB | seconds |

**~3.7 GB total on the current macOS build.** Both paths default under `/data`.
Filesystem allocation can vary. The generator is
deterministic per `--seed`, and the certified v7 graph-shape flags are baked in
as defaults. The semantic `VENUE_PROFILE` rename preserves the 40 × 5 sparse
property shape, but the full workspace must be regenerated and the performance
figures re-certified before treating the historical timings as measurements of
this exact property schema. Totals: 450,000 NODE + 2,250,000 VENUE_PROFILE nodes
across 14 and 40 graphlets respectively, and 19,913,000 edges.

The workspace must be loaded with the **engine-default** clustering costs and
with the labels pinned to exactly `NODE` and `VENUE_PROFILE` — `load_data.sh`
explains why both matter and warns if the environment would break them.

## Files

```
demo/
  run_demo.sh      THE one command (generate → load → fonts → build → serve → warm)
  gen_data.py      deterministic graph-shape generator with semantic venue profiles
  load_data.sh     turbolynx import into a workspace (~6 min)
  server.py        4-mode JSON API (base|si|gem|ssrf) + static file server
  ui.html          the booth page (master; keeps CDN font links)
  fetch_fonts.py   downloads the woff2 set + writes fonts.css for offline use
  evidence/        the plan/attribution proof — read these before defending a number
  qa/              live QA, smoke test, screenshot harness
```

`run_demo.sh` builds the served web root into `.run/` (`index.html`, `fonts.css`,
`fonts/`, `app/public/logo.png`, `plan-tree.js`) — generated, gitignored, and the *only* thing the HTTP server exposes,
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
  engine (after the ~80 s server warmup, a GEM click is ~1 s).
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

The engine-launch regression tests need only Python's standard library and no
loaded dataset: `python3 -m unittest discover -s demo/qa -p test_server.py -v`
(run from the repository root).

The plan model/layout checks need only Node.js:
`node --test demo/qa/test_plan_tree.cjs`.

## Legacy demos

Other demos live alongside this one and are untouched by it:

- `legacy/gem-divergent-order/` — the focused CLI demo for GEM per-branch join
  ordering on the synth-y fixture: opposite-profile graphlets, the split, the
  divergent per-branch orders, and the ~1300× peak-intermediate payoff. Run
  `./legacy/gem-divergent-order/run_demo.sh`. This is the mechanism the booth
  ladder's third rung shows off, stripped to a terminal.
- `agentic-demo/`, `app/`, `api/`, `narration/`, `refs/`, `scripts/`,
  `DEMO_SCENARIO.md` — older demo material, unrelated to the booth ladder.
