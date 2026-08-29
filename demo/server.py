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

The timed run goes to a LONG-LIVED engine process per mode (EngineSession):
one compile + one warm execution per click, results discarded (-m trash).
See EngineSession for why — briefly, the engine's first execution in a fresh
process pays a ~4.0-6.5 s per-process load whatever the rung, so a one-shot
`-i 1` reports a cold number and flattens the ladder outright, while the old
`-i 1 --warmup` reported an honest warm number but paid that load, plus a
second compile, on every click.  Keeping the process open pays it once at
server start.  The numbers are the same measurement the ladder is certified
with (5-rep interleaved medians, quiet machine, 2026-08-28):

    rung          execute ms   drop     compile ms   click wall
    0 base            3201.2     —          449.7      ~3.7 s
    1 +SI             1856.8   -42.0%       447.3      ~2.3 s
    2 +SI+GEM          612.8   -67.0%       366.9      ~1.0 s
    3 +SI+GEM+SSRF     299.2   -51.2%       367.1      ~0.7 s

Worst-case pairwise (slowest sample of the faster rung vs fastest of the
slower one): -40.9% / -61.8% / -47.6%, monotone on every rep.

COMPILE used to be the wart here: the two -j gem rungs took ~8 s to plan
because CXformExpandNAryJoinGEM re-ran GEM's whole join-order search once per
binding of its CPatternMultiTree children — 2,308 expansions of which 2,305
were the same memo group with the same child groups.  Fixed in the engine
(the xform now declines the un-expanded-NAry binding and stops at the first
binding that yields a result), so GEM now plans in ~0.37 s, BELOW the default
planner's ~0.45 s.  Nothing about the plan changed: same 167,260 rows, same
md5, same 2-branch split, zero HashJoin.

Nothing is cached or replayed on the timed path — every click really compiles
and runs the query.  Only the three plan-determined display fetches below are
cached, and they are identical across modes by the md5 identity.

The 180 s per-run guard is unchanged and now very generous: the slowest
observed click is base at ~3.7 s.  The top-10 display rows
(venue, reach, pid ordered by venue/pid), the venue/path counts and the wide
result-row count are fetched once per mode under that mode's own config and
cached — plan-determined and identical across modes by the md5 identity.
"""
import json
import os
import re
import select
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

TIME_RE = re.compile(rb"Time: compile ([\d.]+) ms, execute ([\d.]+) ms, "
                     rb"total ([\d.]+) ms")


class EngineSession:
    """One long-lived engine process per mode.

    The engine builds its per-process read structures on the FIRST execution in
    a process; that costs ~4.0-6.5 s on this workspace whatever the rung, which
    is why a one-shot `-i 1` reports a cold number that flattens the ladder
    (measured: 6540 / 4531 / 4586 / 3977 ms — rung 2 is SLOWER than rung 1).
    The old fix was `-i 1 --warmup`, which pays that cold pass and then reports
    the warm one — honest, but it plans and runs the query twice per click.

    Holding the process open instead pays the cold pass once, at server start,
    and every later click is a single compile + a single warm execution of the
    same plan.  The reported numbers are the same measurement the certified
    ladder uses (verified: 365.8 ms compile / 300.9 ms execute from a warm
    session vs 367.1 / 299.2 bench medians), for about a fifth of the wall
    clock.  Nothing is cached or replayed — every click really runs the query.
    """

    def __init__(self, mode):
        self.mode = mode
        self.proc = None
        args, env_extra, where = RECIPES[mode]
        self.args = args
        self.env = {k: v for k, v in os.environ.items()
                    if not k.startswith("TLX_")}
        self.env.update(env_extra)
        self.query = TRI + where + RET_FULL

    def _spawn(self):
        self.proc = subprocess.Popen(
            [BIN, "--ws", WS] + self.args + ["-m", "trash"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, env=self.env, bufsize=0)
        self._ask()          # burn the cold first execution here, not on a click

    def _ask(self, timeout=180):
        """Run the query once in this process; return (compile, execute, total)."""
        self.proc.stdin.write(self.query.encode() + b"\n")
        self.proc.stdin.flush()
        buf, deadline = b"", time.time() + timeout
        while True:
            if not select.select([self.proc.stdout], [], [],
                                 max(0.0, deadline - time.time()))[0]:
                raise RuntimeError("engine session timed out (%d s)" % timeout)
            ch = self.proc.stdout.read(1)
            if not ch:
                raise RuntimeError("engine session ended unexpectedly")
            buf += ch
            if ch == b"\n":
                m = TIME_RE.search(buf)
                if m:
                    return tuple(float(x) for x in m.groups())
                buf = buf[-4096:]

    def close(self):
        if self.proc is not None:
            try:
                self.proc.kill()
            except OSError:
                pass
            self.proc = None

    def run(self):
        """Timed run.  Restarts the process once if it died between clicks."""
        with ENGINE_LOCK:
            if self.proc is None or self.proc.poll() is not None:
                self.close()
                self._spawn()
            try:
                return self._ask()
            except RuntimeError:
                # a dead or wedged session must not report a cold number:
                # respawn (which re-warms) and take the next run instead
                self.close()
                self._spawn()
                return self._ask()


SESSIONS = {m: EngineSession(m) for m in MODES}


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
    c, e, t = SESSIONS[mode].run()
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
    """Prime every mode before the booth opens: the three cached display
    fetches, and — the point — each mode's long-lived engine process, whose
    first execution pays the ~4.0-6.5 s per-process load.  After this pass
    every click is one compile + one warm execution (~0.7-3.7 s)."""
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
