"""Live QA for the 6-state booth GROUPING ladder (rest/base/si/gem/ssrf/verify)
at 1920x1080 against the real engine backend (server.py modes base|si|gem|ssrf
on /data/ladder-v7-ws).

Covers: advance / back / step-jump / reset, the double-click guard, live
parity (page numbers & grids == the /api/run JSON the page itself received,
merged with the measured plan-shape constants), wrong-state protection
(back at rest, run at verify, clicks while running), the "Compiling plan…"
running label on the two GEM rungs, auto-tab selection, what-if toggles,
stats log, and in-cycle monotonicity + venues/paths/rows identity.

Run: qa_live_v7.py     (one full live ladder; a GEM rung click is ~25 s wall —
                        ~8 s of planner time paid TWICE by `-i 1 --warmup` —
                        so a full pass is ~4 min)
"""
import asyncio, os, json, math
from playwright.async_api import async_playwright

PORT = os.environ.get("PORT", "8500")
URL = "http://127.0.0.1:%s/" % PORT
GUARD_MS = 420
API_T = 240000

# measured plan-shape constants (must mirror MEASURED in the UI)
PEAK = {"base": 65751448, "si": 27466010, "gem": 167260, "ssrf": 167260}
GRAPH = {"base": 14, "si": 4, "gem": 4, "ssrf": 4}
SHAPE = {"base": "1 · 1", "si": "1 · 1", "gem": "2 · 2", "ssrf": "2 · 2"}
FMTROW = {"base": 0, "si": 0, "gem": 0, "ssrf": 164}
VENUES, PATHS, RESULT_ROWS = 33452, 172000, 167260
SLOWCOMPILE = {"gem", "ssrf"}
# measured end-to-end click wall clock (5-cycle soak medians) — must match WALL
# in the UI; compile+execute understates a GEM click by ~3x.
WALL = {"base": "13.9 s", "si": "10.2 s", "gem": "25.2 s", "ssrf": "24.7 s"}

NEXTTXT = {"rest": "Run baseline", "base": "Prune schema",
           "si": "Split per district", "gem": "Pack rows",
           "ssrf": "Verify results", "verify": "Restart demo"}
# the evidence panel auto-selects what the rung is about: the two single-order
# rungs are about the answer, the two GEM rungs about the plan shape that
# changed.  Both other tabs stay reachable — this pins the DEFAULT, not access.
AUTOTAB = {"base": "results", "si": "results", "gem": "plan", "ssrf": "plan",
           "verify": "results"}

FAILS, PASSES, CONSOLE = [], [], []

def fail(t, m): FAILS.append((t, m)); print(f"FAIL [{t}] {m}")
def ok(t): PASSES.append(t); print(f"PASS [{t}]")
def fmt(n): return f"{n:,}"
def kfmt(n):
    if n >= 1e6: return f"{n/1e6:.1f}M"
    if n >= 1e4: return f"{round(n/1e3)}K"
    if n >= 1e3: return f"{n/1e3:.1f}K"
    return fmt(n)
def rnd(x): return math.floor(x + 0.5)
def sig2(x):
    p = 10 ** (math.floor(math.log10(x)) - 1)
    v = rnd(x / p) * p
    if v >= 10: return str(int(rnd(v)))
    return str(round(v * 10) / 10).rstrip("0").rstrip(".")
def msS(x):
    if x >= 1000:
        return (f"{x/1000:.0f} s" if x >= 9500 else f"{x/1000:.1f} s")
    return f"{rnd(x)} ms"
def catOf(pid):
    """five records per node, pid = nid*5 + k, record k -> cat (nid+13k)%40"""
    return "p%02d" % ((pid // 5 + (pid % 5) * 13) % 40)

class Ctx:
    def __init__(self, t): self.t, self.p = t, []
    def eq(self, n, got, want):
        if got != want: self.p.append(f"{n}: got={got!r} want={want!r}")
    def true(self, n, c, d=""):
        if not c: self.p.append(f"{n}: expected true ({d})")
    def flush(self):
        (fail(self.t, "; ".join(self.p)) if self.p else ok(self.t))

SNAP = """
() => { const t=id=>{const e=document.getElementById(id);return e?e.textContent.trim():null};
  const $=id=>document.getElementById(id);
  const gridRows=id=>{const g=$(id); if(!g) return null;
    return [...g.querySelectorAll('tbody tr')].map(tr=>{
      const td=tr.querySelectorAll('td');
      return [td[1].textContent.trim(), td[2].textContent.trim(),
              td[3].textContent.trim()];});};
  const gridHead=id=>{const g=$(id); if(!g) return null;
    return [...g.querySelectorAll('thead th')].map(e=>e.textContent.trim());};
  const tabs={}; ['results','plan','stats'].forEach(x=>
    tabs[x]=$('tabb-'+x).classList.contains('active'));
  return { chip:t('livechip-t'),
    k:{total:t('k-total'),compile:t('k-compile'),exec:t('k-exec'),exec2:t('k-exec2'),
       peak:t('k-peak'),count:t('k-count'),rows:t('k-rows'),graph:t('k-graph'),
       shape:t('k-shape'),fmt:t('k-fmt')},
    subs:{exec:t('s-exec'),peak:t('s-peak'),graph:t('s-graph'),
          shape:t('s-shape'),fmt:t('s-fmt'),total:t('s-total')},
    rowsP:{t:t('rows-p'),cls:$('rows-p').className},
    rowsQ:{t:t('rows-q'),cls:$('rows-q').className},
    okSame:t('ok-same'), okCount:t('ok-count'), okRows:t('ok-rows'),
    grids:{single:gridRows('g-single'),base:gridRows('g-base'),turbo:gridRows('g-turbo')},
    heads:{single:gridHead('g-single'),base:gridHead('g-base')},
    v:{same:t('v-same'),cbase:t('v-cbase'),cturbo:t('v-cturbo'),eq:t('v-eq'),
       rows:t('v-rows'),rowschip:t('v-rowschip'),exec:t('v-exec'),execchip:t('v-execchip')},
    tabs, tabbody:t('tabbody').slice(0,600),
    wpanel:(()=>{const w=document.querySelector('.wpanel');return w?w.textContent.trim():null})(),
    whatifBtns:[...document.querySelectorAll('.whatif')].map(b=>b.textContent.trim()),
    whatifCap:(()=>{const c=document.querySelector('.whatifcap');return c?c.textContent.trim():null})(),
    statsRows:[...document.querySelectorAll('.logtable tbody tr')].map(
      tr=>[...tr.querySelectorAll('td')].map(td=>td.textContent.trim())),
    statsHead:[...document.querySelectorAll('.logtable thead th')].map(e=>e.textContent.trim()),
    hook:t('hook'), apierr:t('apierr'),
    si:t('st-si'), gem:t('st-gem'), ssrf:t('st-ssrf'),
    steps:[...document.querySelectorAll('#steps .step')].map(e=>({
      cls:e.className, go:e.dataset.go||null})),
    next:{text:t('navnext'),dis:$('navnext').disabled},
    back:{dis:$('navback').disabled}, run:{dis:$('btn-run').disabled},
    stage:document.getElementById('stage').className };
}
"""

def grid_want(rows):
    """expected grid cells for api rows [venue, reach, pid]"""
    return [[r[0], fmt(r[1]), f"{catOf(r[2])}_* · 5/200"] for r in rows]

async def new_page(browser):
    page = await browser.new_page(viewport={"width": 1920, "height": 1080})
    page.on("console", lambda m: CONSOLE.append(("console-" + m.type, m.text))
            if m.type == "error" else None)
    page.on("pageerror", lambda e: CONSOLE.append(("pageerror", str(e))))
    api_log = []
    async def on_resp(r):
        if "/api/run" in r.url:
            try:
                api_log.append((r.url.split("mode=")[1], await r.json()))
            except Exception as exc:
                CONSOLE.append(("api-parse", f"{r.url}: {exc}"))
    page.on("response", on_resp)
    await page.goto(URL)
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1200)
    return page, api_log

async def wait_state(page, st):
    await page.wait_for_function(
        "(t)=>{const e=document.getElementById('navnext');"
        "return e&&!e.disabled&&e.textContent.includes(t)}",
        arg=NEXTTXT[st], timeout=API_T)
    await page.wait_for_timeout(GUARD_MS)

def api_of(api_log, mode):
    return next((j for m, j in api_log if m == mode), None)

def check_rung(c, s, mode, d):
    # the hero KPI is EXECUTE (the number the ladder moves); the wall-clock
    # total lives in the sub line and must still be exact and visible.
    c.eq("k-total (in sub line)", s["k"]["total"],
         fmt(rnd(d["compile_ms"]) + rnd(d["execute_ms"])))
    c.eq("k-compile", s["k"]["compile"], fmt(rnd(d["compile_ms"])))
    c.eq("k-exec (hero)", s["k"]["exec"], fmt(rnd(d["execute_ms"])))
    c.eq("k-exec2 (bar legend)", s["k"]["exec2"], fmt(rnd(d["execute_ms"])))
    c.eq("k-count(venues)", s["k"]["count"], fmt(d["venues"]))
    c.eq("k-rows(result rows)", s["k"]["rows"], fmt(d["result_rows"]))
    c.eq("k-peak", s["k"]["peak"], kfmt(PEAK[mode]))
    c.eq("k-graph", s["k"]["graph"], str(GRAPH[mode]))
    c.eq("k-shape", s["k"]["shape"], SHAPE[mode])
    c.eq("k-fmt", s["k"]["fmt"], str(FMTROW[mode]))
    c.eq("rows-p", s["rowsP"]["t"], fmt(PEAK[mode]))
    c.eq("rows-q", s["rowsQ"]["t"], fmt(d["result_rows"]))
    c.eq("rows-p cls", s["rowsP"]["cls"],
         "rv bad" if PEAK[mode] > d["result_rows"] else "rv ok")
    c.eq("rows-q cls", s["rowsQ"]["cls"], "rv ok")
    c.eq("venues is 33452", d["venues"], VENUES)
    c.eq("paths is 172000", d["paths"], PATHS)
    c.eq("result rows is 167260", d["result_rows"], RESULT_ROWS)
    c.eq("result_rows == venues*5", d["result_rows"], d["venues"] * 5)
    c.eq("SI chip", s["si"], "off" if mode == "base" else "on")
    c.eq("GEM chip", s["gem"], "on" if mode in ("gem", "ssrf") else "off")
    c.eq("SSRF chip", s["ssrf"], "on" if mode == "ssrf" else "off")
    # compile+execute is spelled out under the hero AND labelled as such — it is
    # NOT the click cost, so the measured end-to-end wall clock sits beside it.
    sub = s["subs"]["total"] or ""
    c.true("s-total labels compile+execute honestly",
           f"compile + execute {fmt(rnd(d['compile_ms']) + rnd(d['execute_ms']))} ms"
           in sub, repr(sub))
    c.true("s-total carries the measured click wall clock",
           f"click {WALL[mode]}" in sub, repr(sub))
    c.true("s-total never calls compile+execute the total",
           not sub.startswith("total"), repr(sub))
    tab = AUTOTAB[mode]
    c.true(f"auto-tab {tab}", s["tabs"][tab], json.dumps(s["tabs"]))

async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        page, api_log = await new_page(browser)

        s = await page.evaluate(SNAP)
        c = Ctx("0.initial-rest")
        c.eq("chip", s["chip"], "Live")
        c.true("back disabled at rest", s["back"]["dis"])
        c.true("no-run empty state", "Run baseline" in (s["tabbody"] or ""))
        c.true("rest state leads with the payoff",
               "in five clicks" in (s["tabbody"] or ""), (s["tabbody"] or "")[:200])
        c.eq("next", True, "Run baseline" in (s["next"]["text"] or ""))
        c.true("rest hook is the grouping story",
               "per venue" in (s["hook"] or ""), repr(s["hook"]))
        c.flush()

        # ---- advance the four live rungs
        for mode, st in (("base", "base"), ("si", "si"),
                         ("gem", "gem"), ("ssrf", "ssrf")):
            n_before = len(api_log)
            await page.click("#navnext")
            if mode == "si":
                # double-click guard: an immediate second click must be a
                # no-op (programmatic — page.click would wait for re-enable
                # and fire a real second advance after the run).
                await page.evaluate("document.getElementById('navnext').click()")
            c = Ctx(f"1.{mode}.running-state")
            want = ("Compiling plan… ~25 s" if mode in SLOWCOMPILE
                    else "Running query")
            try:
                await page.wait_for_function(
                    "(t)=>{const e=document.getElementById('navnext');"
                    "return e && e.textContent.includes(t) && e.disabled}",
                    arg=want, timeout=6000)
            except Exception:
                cur = await page.evaluate(
                    "()=>document.getElementById('navnext').textContent")
                c.p.append(f"navnext never showed disabled {want!r} (was {cur!r})")
            # wrong-state protection: clicks while running are ignored
            # (buttons are disabled; programmatic clicks must be no-ops too)
            await page.evaluate("document.getElementById('navback').click();"
                                "document.getElementById('btn-reset').click();")
            c.flush()
            await wait_state(page, st)
            s = await page.evaluate(SNAP)
            c = Ctx(f"1.{mode}.parity")
            c.eq("exactly one api call this step", len(api_log) - n_before, 1)
            d = api_of(api_log, mode)
            if d is None:
                c.p.append(f"no /api/run?mode={mode} captured")
            else:
                check_rung(c, s, mode, d)
                if mode != "ssrf":     # single-rung results grid states
                    if s["tabs"]["results"]:
                        c.eq("grid == api rows", s["grids"]["single"],
                             grid_want(d["rows"]))
                        c.eq("grid header", s["heads"]["single"],
                             ["#", "venue", "reach", "profile"])
            c.flush()

        # in-cycle sanity: monotonic + identical answer
        c = Ctx("2.ladder-sanity")
        mods = {m: api_of(api_log, m) for m in ("base", "si", "gem", "ssrf")}
        if all(mods.values()):
            e = {m: mods[m]["execute_ms"] for m in mods}
            c.true("monotonic execute",
                   e["base"] > e["si"] > e["gem"] > e["ssrf"], json.dumps(e))
            c.true("venues identical 33,452",
                   all(mods[m]["venues"] == VENUES for m in mods))
            c.true("paths identical 172,000",
                   all(mods[m]["paths"] == PATHS for m in mods))
            c.true("result rows identical 167,260",
                   all(mods[m]["result_rows"] == RESULT_ROWS for m in mods))
            c.true("top-10 identical across modes",
                   len({json.dumps(mods[m]["rows"]) for m in mods}) == 1)
            c.true("top-10 ordered by venue",
                   [r[0] for r in mods["base"]["rows"]]
                   == sorted(r[0] for r in mods["base"]["rows"]),
                   json.dumps([r[0] for r in mods["base"]["rows"]]))
            c.true("GEM rungs really do pay ~9 s of compile",
                   all(mods[m]["compile_ms"] > 4000 for m in ("gem", "ssrf")),
                   json.dumps({m: mods[m]["compile_ms"] for m in mods}))
        else:
            c.p.append(f"missing api runs: {[m for m in mods if not mods[m]]}")
        c.flush()

        # ---- verify state (no api call)
        n_before = len(api_log)
        await page.click("#navnext"); await wait_state(page, "verify")
        s = await page.evaluate(SNAP)
        c = Ctx("3.verify")
        b, d = mods["base"], mods["ssrf"]
        c.eq("no api call to verify", len(api_log) - n_before, 0)
        c.true("run btn disabled at verify", s["run"]["dis"])
        c.eq("v-cbase", s["v"]["cbase"], fmt(b["venues"]))
        c.eq("v-cturbo", s["v"]["cturbo"], fmt(d["venues"]))
        c.eq("v-eq", s["v"]["eq"], "=")
        c.eq("v-same", s["v"]["same"], "check_circleidentical top-10")
        c.eq("base grid == api", s["grids"]["base"], grid_want(b["rows"]))
        c.eq("turbo grid == api", s["grids"]["turbo"], grid_want(d["rows"]))
        c.eq("base grid header", s["heads"]["base"],
             ["#", "venue", "reach", "profile"])
        c.true("verify caption carries the v7 md5",
               "af57c00c" in (s["tabbody"] or ""), (s["tabbody"] or "")[:300])
        c.true("verify caption carries the result-row count",
               fmt(RESULT_ROWS) in (s["tabbody"] or ""))
        rr = sig2(PEAK["base"] / PEAK["ssrf"])
        er = sig2(rnd(b["execute_ms"]) / max(1, rnd(d["execute_ms"])))
        c.eq("v-rows", s["v"]["rows"], f"{kfmt(PEAK['base'])} → {kfmt(PEAK['ssrf'])}")
        c.eq("v-rowschip", s["v"]["rowschip"], f"arrow_downward{rr}×")
        c.eq("v-execchip", s["v"]["execchip"], f"arrow_downward{er}×")
        c.eq("v-exec", s["v"]["exec"],
             f"{msS(b['execute_ms'])} → {msS(d['execute_ms'])}")
        c.flush()
        await page.keyboard.press("r")
        await page.wait_for_timeout(500)
        s = await page.evaluate(SNAP)
        c = Ctx("3.verify-R-inert")
        c.true("still verify", "Restart demo" in (s["next"]["text"] or ""))
        c.flush()

        # what-if toggles — the si_off card must be HONEST about 1.35x, and the
        # ssrf_off card must quote the +GEM rung the visitor just ran.
        c = Ctx("4.whatif")
        gem_ms = fmt(rnd(mods["gem"]["execute_ms"]))
        ssrf_ratio = sig2(mods["gem"]["execute_ms"] / mods["ssrf"]["execute_ms"])
        si_ratio = sig2(955.4 / mods["gem"]["execute_ms"])
        # every what-if chip must carry its DENOMINATOR, or a visitor divides
        # by the hero number and reads a ratio that was never measured
        c.true("ssrf_off chip quotes the live +GEM rung AND its denominator",
               any(t == f"SSRF off → {gem_ms} ms ({ssrf_ratio}× vs +SSRF)"
                   for t in s["whatifBtns"]), json.dumps(s["whatifBtns"]))
        c.true("si_off chip carries its denominator too",
               any(t.startswith("SI off →") and f"({si_ratio}× vs +GEM)" in t
                   for t in s["whatifBtns"]), json.dumps(s["whatifBtns"]))
        c.true("what-if group is captioned as a control",
               "switch one back off" in (s["whatifCap"] or ""),
               repr(s["whatifCap"]))
        await page.click(".whatif[data-w='si_off']"); await page.wait_for_timeout(150)
        s = await page.evaluate(SNAP)
        w = s["wpanel"] or ""
        # the ratio must be recomputed against the +GEM rung this page actually
        # ran, never a number carried over from the bench
        want_ratio = si_ratio + "×"
        c.true("si_off panel does NOT quote the dead 7,953 ms card",
               "7,953" not in w, repr(w))
        c.true(f"si_off ratio recomputed live ({want_ratio})", want_ratio in w, repr(w))
        c.true("si_off quotes 9.4x only as the superseded row-hero card",
               ("9.4×" not in w) or ("was 9.4×" in w and "It is not" in w), repr(w))
        c.true("si_off keeps the memory half (36x peak)", "36×" in w, repr(w))
        c.true("si_off keeps the SI rung's own quiet-machine credit",
               "45.4%" in w, repr(w))
        await page.click(".whatif[data-w='ssrf_off']"); await page.wait_for_timeout(150)
        s = await page.evaluate(SNAP)
        w = s["wpanel"] or ""
        c.true("ssrf_off panel shows 12.3 GB", "12.3 GB" in w, repr(w))
        c.true("ssrf8 honestly labeled", "ssrf8" in w, repr(w))
        c.true("ssrf_off panel is honest that this rung is time, not memory",
               "time" in w.lower(), repr(w))
        await page.click(".whatif[data-w='ssrf_off']"); await page.wait_for_timeout(150)
        s = await page.evaluate(SNAP)
        c.true("panel toggles off", s["wpanel"] is None, repr(s["wpanel"]))
        c.flush()

        # stats log: 4 live rows
        await page.click("#tabb-stats"); await page.wait_for_timeout(200)
        s = await page.evaluate(SNAP)
        c = Ctx("5.stats-log")
        c.eq("4 runs logged", len(s["statsRows"]), 4)
        c.true("provenance stated once for a uniform log",
               "live" in (s["tabbody"] or ""), (s["tabbody"] or "")[:300])
        # column positions are header-driven: the `source` column is dropped
        # when every logged run shares a source, so never hard-code an index
        H = s["statsHead"]
        c.eq("stats header tail", H[-3:],
             ["peak intermediate rows", "venues", "result rows"])
        ip, iv, ir = H.index("peak intermediate rows"), H.index("venues"), \
            H.index("result rows")
        c.true("peak col measured", s["statsRows"][0][ip] == fmt(PEAK["base"]),
               json.dumps(s["statsRows"][0]))
        c.true("venues col", all(r[iv] == fmt(VENUES) for r in s["statsRows"]),
               json.dumps([r[iv] for r in s["statsRows"]]))
        c.true("result-rows col",
               all(r[ir] == fmt(RESULT_ROWS) for r in s["statsRows"]),
               json.dumps([r[ir] for r in s["statsRows"]]))
        c.true("ms columns carry thousands separators",
               all("," in r[H.index("compile ms")] or
                   int(r[H.index("compile ms")].replace(",", "")) < 1000
                   for r in s["statsRows"]),
               json.dumps([r[H.index("compile ms")] for r in s["statsRows"]]))
        c.true("plan configuration labels are cumulative",
               [r[2] for r in s["statsRows"]]
               == ["all off", "SI", "SI + GEM", "SI + GEM + SSRF"],
               json.dumps([r[2] for r in s["statsRows"]]))
        c.flush()

        # ---- back-nav: verify -> ssrf -> gem (cached, no api calls)
        n_before = len(api_log)
        await page.click("#navback"); await wait_state(page, "ssrf")
        await page.click("#navback"); await wait_state(page, "gem")
        s = await page.evaluate(SNAP)
        c = Ctx("6.back-nav")
        c.eq("no api calls on back", len(api_log) - n_before, 0)
        c.eq("k-exec still gem", s["k"]["exec"], fmt(rnd(mods["gem"]["execute_ms"])))
        c.eq("GEM chip on", s["gem"], "on")
        c.eq("SSRF chip off", s["ssrf"], "off")
        c.flush()

        # ---- step-jump: gem -> Baseline chip, then -> Verify chip (cached)
        n_before = len(api_log)
        await page.click(".step[data-go='1']"); await wait_state(page, "base")
        s = await page.evaluate(SNAP)
        c = Ctx("7.step-jump")
        c.eq("no api call on jump", len(api_log) - n_before, 0)
        c.eq("k-exec is base", s["k"]["exec"], fmt(rnd(mods["base"]["execute_ms"])))
        cur = [x["cls"] for x in s["steps"]]
        c.true("baseline chip current", "cur" in cur[0], json.dumps(cur))
        await page.click(".step[data-go='5']"); await wait_state(page, "verify")
        c.eq("still no api call", len(api_log) - n_before, 0)
        c.flush()

        # ---- back to rest via Back x5, then re-advance cached
        for st in ("ssrf", "gem", "si", "base", "rest"):
            await page.click("#navback"); await wait_state(page, st)
        s = await page.evaluate(SNAP)
        c = Ctx("8.back-to-rest")
        c.true("back disabled at rest", s["back"]["dis"])
        c.eq("k-exec cleared", s["k"]["exec"], "—")
        c.eq("k-total gone with the sub line", s["k"]["total"], None)
        c.eq("k-rows cleared", s["k"]["rows"], "—")
        n_before = len(api_log)
        # rapid double-click on a CACHED advance: qaLock must swallow the
        # second click — we land on base, not si.
        await page.evaluate("const b=document.getElementById('navnext');"
                            "b.click(); b.click();")
        await wait_state(page, "base")
        s = await page.evaluate(SNAP)
        c.eq("re-advance cached (no api)", len(api_log) - n_before, 0)
        c.true("double-click landed on base only",
               "Prune schema" in (s["next"]["text"] or ""), s["next"]["text"])
        c.flush()

        # ---- reset clears everything
        await page.click("#btn-reset"); await wait_state(page, "rest")
        s = await page.evaluate(SNAP)
        c = Ctx("9.reset")
        c.true("empty state back", "Run baseline" in (s["tabbody"] or ""))
        c.true("back disabled", s["back"]["dis"])
        c.true("steps not clickable",
               all(x["go"] is None for x in s["steps"]), json.dumps(s["steps"]))
        c.flush()

        await page.close()
        await browser.close()
    print("\n========== SUMMARY ==========")
    print(f"passes: {len(PASSES)}  failures: {len(FAILS)}")
    for t, m in FAILS: print(f"  FAIL {t}: {m}")
    print("---- console/page events ----")
    for kind, text in CONSOLE: print(f"  {kind}: {text[:200]}")
    if not CONSOLE: print("  (none)")
    if FAILS or any(k == "pageerror" for k, _ in CONSOLE):
        raise SystemExit(1)
    print("QA GREEN")

asyncio.run(main())
