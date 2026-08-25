#!/usr/bin/env python3
"""Booth-demo backend: serves the built web root + a JSON API that runs the
real TurboLynx engine on the ladder-v7 workspace (the one-query feature
ladder: base -> +SI -> +GEM -> +SSRF, evidence/FINAL_GROUPING_LADDER.txt).

Normally started by run_demo.sh, which builds the web root first.  Everything
is env-overridable:
  BIN      turbolynx binary   (default <repo>/build-release/tools/turbolynx)
  WS       workspace          (default /data/ladder-v7-ws)
  SRC      generated source   (default /data/ladder-v7-src; profile_cols.txt)
  PORT     listen port        (default 8500)
  WEBROOT  static dir         (default .run/ beside this file — run_demo.sh
                               builds index.html + fonts there, and nothing
                               else, so the repo tree is never exposed)

Endpoints:
  GET /api/health           -> {"ok": true}
  GET /api/run?mode=base    -> all features off   (SSRF gated off, a.kind IS NOT NULL)
  GET /api/run?mode=si      -> + schema pruning   (SSRF gated off, a.kind = 'person')
  GET /api/run?mode=gem     -> + per-branch order (-j gem, TLX_DIVERGE_ORDER=1,
                               TLX_GEM_SPLIT_TARGET=a, SSRF still gated off)
  GET /api/run?mode=ssrf    -> + row format       (same, TLX_SSRF_OFF dropped)

Every mode runs THE one GROUPING query: the follows -> recommends -> visits
triangle, aggregated per venue (WITH c, count(*) AS reach), and only THEN
joined to that venue's five PROFILE records and their 200-column union.

  172,000 traversal rows -> 33,452 venues (5.14x collapse) -> 167,260 wide
  result rows, 202 columns each, 5 of the 200 profile columns non-NULL.

The aggregation must sit BELOW the wide seek: the binder rewrites every
property of a WITH-visible variable projected after the aggregation into a
`first(...)` aggregate, which drags the 200-column seek back underneath the
HashAgg and re-flattens the row format (measured: -4.3% instead of -55%).
Introducing `p` in a second MATCH after the WITH is the only shape in the
search space that keeps it above.  See FINAL_GROUPING_LADDER.txt.

SI on/off is the predicate form: `a.kind IS NOT NULL` (un-prunable) vs
`a.kind = 'person'` (prunable) — both return the identical 167,260 rows
(sorted-CSV md5-identical on every rung, gladder_v7_evidence.txt A) and the
identical grouping (33,452 venues, sum(reach) = 172,000, section A2).
TLX_GEM_SPLIT_TARGET=a is the disclosed demo planner hint for the documented
zero-fanout-anchor costing defect (E2).

The timed run is `-i 1 --warmup -m trash` (results discarded): the engine
executes a warmup iteration, then reports the post-warmup `Time:` line — the
same protocol the ladder was certified with (quiet-machine medians
4092.7 / 2234.4 / 798.7 / 335.6 ms execute, g/gladder_v7_bench_quiet.txt).
Individual runs vary: observed 3.7-5.3 / 2.1-2.9 / 0.73-1.00 / 0.32-0.38 s
across the certification bench and the 5-cycle live soak.

PACING — the one honest wart, measured rather than estimated.  Rungs 2/3
(-j gem) spend ~8 s in COMPILE (rungs 0/1 take ~0.55 s): GEM's join-order
search on a MULTI-PART query is ~12x more expensive and scales ~48 ms per
projected column.  That is a documented planner defect, not a data or plan
problem.  On top of it, the certification protocol below (-i 1 --warmup)
PLANS TWICE — once for the warmup pass and once for the timed one — verified
directly: the same query at `-i 1` takes 15.0 s wall and at `-i 1 --warmup`
24.5 s, while the reported compile stays ~9.5 s.

So the real end-to-end click cost, measured through the actual page over a
5-cycle soak (medians): base 13.9 s, si 10.2 s, gem 25.2 s, ssrf 24.7 s —
for executes of 3.9 / 2.2 / 0.79 / 0.34 s.  Nearly all of the gem/ssrf wait
is two planner passes and none of it is query work.  The UI states this
rather than hiding it: the hero KPI is execute; directly beneath it the card
prints the compile+execute figure LABELLED AS SUCH plus the measured
end-to-end click time ("click 25.2 s"), because compile+execute alone still
understates a click by ~3x; the Verify screen carries the compile-inclusive
comparison next to the execute one; and the run button reads
"Compiling plan… ~25 s" while they run.

The `timeout 180` per-run guard is unchanged and still ample: the slowest
observed single engine invocation is a gem/ssrf timed run at ~25 s wall
(two ~8 s planner passes plus warm + timed execution).  The top-10 display rows
(venue, reach, pid ordered by venue/pid), the venue/path counts and the wide
result-row count are fetched once per mode under that mode's own config and
cached — plan-determined and identical across modes by the md5 identity.
"""
import json
import os
import re
import subprocess
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, ".."))

BIN = os.environ.get("BIN",
                     os.path.join(REPO_ROOT, "build-release/tools/turbolynx"))
WS = os.environ.get("WS", "/data/ladder-v7-ws")
SRC = os.environ.get("SRC", "/data/ladder-v7-src")
PORT = int(os.environ.get("PORT", "8500"))
WEBROOT = os.environ.get("WEBROOT", os.path.join(HERE, ".run"))

with open(os.path.join(SRC, "profile_cols.txt")) as _f:
    COLS = _f.read().strip()          # p.p00_0 AS p00_0, ... (200 cols)

# part 1: the triangle that gets aggregated.  `p` is deliberately ABSENT here.
TRI = ("MATCH (a:`NODE`)-[:FOLLOWS]->(b:`NODE`)-[:RECOMMENDS]->(c:`NODE`),"
       "(a)-[:VISITS]->(c)")
W_SI_OFF = " WHERE a.kind IS NOT NULL"    # un-prunable, same rows
W_SI_ON = " WHERE a.kind = 'person'"      # converter graphlet prune
# part 2: the GROUP BY, then the wide profile attach strictly above it.
GROUPBY = (" WITH c, count(*) AS reach"
           " MATCH (p:`PROFILE`)-[:PROFILE_OF]->(c)")

RET_FULL = GROUPBY + " RETURN c.title AS venue, reach, " + COLS + ";"
RET_T10 = GROUPBY + (" RETURN c.title AS venue, reach, p.id AS pid"
                     " ORDER BY venue ASC, pid ASC LIMIT 10;")
# two cardinalities, not one: venues are the GROUP BY output, paths are the
# traversal rows folded into them.
RET_CNT = (" WITH c, count(*) AS reach"
           " RETURN count(*) AS venues, sum(reach) AS paths;")
# and the wide result is a third: venues x 5 profile records each.
RET_ROWS = GROUPBY + " RETURN count(*) AS resultrows;"

MODES = ("base", "si", "gem", "ssrf")
GEM_ENV = {"TLX_DIVERGE_ORDER": "1", "TLX_GEM_SPLIT_TARGET": "a"}
# mode -> (planner argv, env, WHERE form)   [FINAL_GROUPING_LADDER.txt recipes]
RECIPES = {
    "base": ([], {"TLX_SSRF_OFF": "1"}, W_SI_OFF),
    "si":   ([], {"TLX_SSRF_OFF": "1"}, W_SI_ON),
    "gem":  (["-j", "gem"], dict(GEM_ENV, TLX_SSRF_OFF="1"), W_SI_ON),
    "ssrf": (["-j", "gem"], dict(GEM_ENV), W_SI_ON),
}

ENGINE_LOCK = threading.Lock()   # serialize all engine invocations
CACHE_LOCK = threading.Lock()
CACHE = {}                       # counts_<mode>, rows_<mode>


# ---------------------------------------------------------------- engine glue
def run_engine(args, env_extra=None):
    """Run the engine under `timeout 180`; NUL-stripped combined output."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("TLX_")}
    if env_extra:
        env.update(env_extra)
    cmd = ["timeout", "180", BIN, "--ws", WS] + args
    with ENGINE_LOCK:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           env=env, timeout=200)
    out = (p.stdout + b"\n" + p.stderr).replace(b"\x00", b"")
    text = out.decode("utf-8", "replace")
    if p.returncode == 124:
        raise RuntimeError("engine run timed out (180 s): %s" % " ".join(args))
    if p.returncode != 0:
        raise RuntimeError("engine exited %d: %s"
                           % (p.returncode, text[-500:].strip()))
    return text


def parse_time(text):
    """Last 'Time: compile X ms, execute Y ms, total Z ms' line."""
    m = re.findall(r"Time: compile ([\d.]+) ms, execute ([\d.]+) ms, "
                   r"total ([\d.]+) ms", text)
    if not m:
        raise RuntimeError("no timing line in engine output")
    c, e, t = (float(x) for x in m[-1])
    return c, e, t


def parse_count(text):
    """(venues, paths) from the `venues,paths` csv-mode output."""
    m = re.search(r"^venues,paths\s*\n(\d+),(\d+)\s*$", text, re.M)
    if not m:
        raise RuntimeError("no venues,paths result in engine output")
    return int(m.group(1)), int(m.group(2))


def parse_rowcount(text):
    """resultrows value from csv-mode output (the wide attach's row count)."""
    m = re.search(r"^resultrows\s*\n(\d+)\s*$", text, re.M)
    if not m:
        raise RuntimeError("no resultrows result in engine output")
    return int(m.group(1))


def parse_t10(text):
    """[[venue, reach, pid], ...] from csv-mode top-10 output."""
    m = re.search(r"^venue,reach,pid\s*\n", text, re.M)
    if not m:
        raise RuntimeError("no top-10 header in engine output")
    rows = []
    for line in text[m.end():].splitlines():
        cells = line.strip().split(",")
        if len(cells) != 3 or not cells[1].isdigit() or not cells[2].isdigit():
            break
        rows.append([cells[0], int(cells[1]), int(cells[2])])
    if len(rows) != 10:
        raise RuntimeError("expected 10 result rows, parsed %d" % len(rows))
    return rows


# ------------------------------------------------------------------- api core
def get_counts(mode):
    """Cached (venues, paths, result_rows) under this mode's own config —
    33,452 / 172,000 / 167,260 on every rung."""
    with CACHE_LOCK:
        if "counts_" + mode in CACHE:
            return CACHE["counts_" + mode]
    args, env, where = RECIPES[mode]
    venues, paths = parse_count(
        run_engine(args + ["-m", "csv", "-q", TRI + where + RET_CNT], env))
    rows = parse_rowcount(
        run_engine(args + ["-m", "csv", "-q", TRI + where + RET_ROWS], env))
    with CACHE_LOCK:
        CACHE["counts_" + mode] = (venues, paths, rows)
    return venues, paths, rows


def get_rows(mode):
    """Cached top-10 display rows under this mode's own config."""
    with CACHE_LOCK:
        if "rows_" + mode in CACHE:
            return CACHE["rows_" + mode], True
    args, env, where = RECIPES[mode]
    rows = parse_t10(run_engine(args + ["-m", "csv", "-q",
                                        TRI + where + RET_T10], env))
    with CACHE_LOCK:
        CACHE["rows_" + mode] = rows
    return rows, False


def api_run(mode):
    venues, paths, result_rows = get_counts(mode)
    rows, cached = get_rows(mode)
    args, env, where = RECIPES[mode]
    out = run_engine(args + ["-i", "1", "--warmup", "-m", "trash", "-q",
                             TRI + where + RET_FULL], env)
    c, e, t = parse_time(out)
    return {
        "ok": True, "mode": mode,
        "total_ms": round(t, 1), "compile_ms": round(c, 1),
        "execute_ms": round(e, 1),
        "venues": venues, "paths": paths, "result_rows": result_rows,
        "rows": rows, "row_count": len(rows),
        "rows_cached": cached,
    }


# -------------------------------------------------------------------- server
class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=WEBROOT, **kw)

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/api/health":
            return self._json(200, {"ok": True})
        if u.path == "/api/run":
            mode = parse_qs(u.query).get("mode", [""])[0]
            if mode not in MODES:
                return self._json(400, {"error": "mode must be "
                                                 "base|si|gem|ssrf"})
            try:
                t0 = time.time()
                res = api_run(mode)
                self.log_message("api_run mode=%s done in %.1fs: "
                                 "exec=%.1fms venues=%d rows=%d", mode,
                                 time.time() - t0, res["execute_ms"],
                                 res["venues"], res["result_rows"])
                return self._json(200, res)
            except Exception as exc:  # engine crash and parse errors -> 500
                self.log_message("api_run mode=%s FAILED: %s", mode, exc)
                return self._json(500, {"error": str(exc)})
        return super().do_GET()


def warmup():
    """Prime all four modes' caches + engine page cache before the booth.
    base costs ~25 s (its three cached fetches plus compile + warmup iter +
    timed iter on a ~4.1 s query); gem and ssrf each pay ~8 s of GEM compile
    per planning pass and cost ~40 s apiece.  The full pass is ~2 min."""
    for mode in MODES:
        try:
            t0 = time.time()
            res = api_run(mode)
            print("[warmup] %s done in %.1fs: exec=%.1fms compile=%.1fms "
                  "venues=%d paths=%d rows=%d top10=%d"
                  % (mode, time.time() - t0, res["execute_ms"],
                     res["compile_ms"], res["venues"], res["paths"],
                     res["result_rows"], res["row_count"]), flush=True)
        except Exception as exc:
            print("[warmup] %s failed: %s" % (mode, exc), flush=True)


def main():
    threading.Thread(target=warmup, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print("serving %s on 0.0.0.0:%d (ws=%s)" % (WEBROOT, PORT, WS),
          flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
