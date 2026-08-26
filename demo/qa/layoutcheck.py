"""Layout self-check across a RESOLUTION MATRIX, all 6 states.

Asserts, at every size and every state:
  * no clipping container overflows its own box (scrollH/W <= clientH/W + 1)
  * no text is ellipsised or cut
  * no element reaches past its card's padding box
  * no dense panel leaves more than VOID_MAX of its height empty at the foot
  * row-1 / row-2 siblings do not overlap, and the page never scrolls

Usage: layoutcheck.py [--only 1920x1080,...] [--states rest,base,...]
"""
import asyncio, os, sys
from playwright.async_api import async_playwright

PORT = os.environ.get("PORT", "8500")
URL = "http://127.0.0.1:%s/?mock=1" % PORT
NEXTTXT = {"base": "Prune schema", "si": "Split per district",
           "gem": "Pack rows", "ssrf": "Verify results",
           "verify": "Restart demo"}
STATES = ["rest", "base", "si", "gem", "ssrf", "verify"]
MATRIX = [(1600, 900), (1680, 1050), (1920, 1080), (1990, 1037),
          (2200, 1200), (2560, 1440), (3840, 2160)]

PROBE = r"""() => {
  const out = {over: [], boxes: {}};
  const vis = e => { const r = e.getBoundingClientRect();
    return r.width > 0.5 && r.height > 0.5 &&
           getComputedStyle(e).visibility !== 'hidden'; };

  /* ---- 1. every clipping container must actually contain its content ---- */
  const CONT = ['card-editor','card-graphlets','stage','tabpanel','rail','story',
                'kpi-total','kpi-answer','tabbody','pgrid','hook',
                'kpi-peak','kpi-graph','kpi-shape','kpi-fmt','steps'];
  for (const id of CONT) {
    const e = document.getElementById(id); if (!e || !vis(e)) continue;
    const dw = e.scrollWidth - e.clientWidth, dh = e.scrollHeight - e.clientHeight;
    if (dw > 1 || dh > 1) out.over.push(`${id} overflows: +${dw}w +${dh}h`);
  }
  /* class-level: every card, kpi tile and inner scroller on the page */
  document.querySelectorAll('.card,.kpi,.levers,.gtable,.mirror,.edbody,.code,.tabbody')
    .forEach(e => {
      if (!vis(e)) return;
      const dw = e.scrollWidth - e.clientWidth, dh = e.scrollHeight - e.clientHeight;
      if (dw > 1 || dh > 1) out.over.push(
        `${e.id || e.className} overflows: +${dw}w +${dh}h`);
    });

  /* ---- 2. nothing may reach past the box that CLIPS it ------------------
     The general form of "is it cut off": for every visible element, find the
     nearest ancestor that does not paint its overflow, and compare the
     element's border box to that ancestor's PADDING box (inside the border —
     content is allowed to sit in the padding, a 2em dot centred in a 1.6em
     track does, but the border is where the clip happens).
     Popovers are display:none until hovered; stage overlays are absolutely
     positioned decoration over a canvas. */
  const SKIP = el => el.classList.contains('pop') || el.closest('.pop') ||
                     el.classList.contains('ovl') || el.closest('.ovl') ||
                     el.closest('#stage') ||
                     /* width:0 right-aligned column labels overhang by design */
                     el.classList.contains('cg') || el.classList.contains('cn');
  const clips = e => { const s = getComputedStyle(e);
    return s.overflowX !== 'visible' || s.overflowY !== 'visible'; };
  const pad = el => { const r = el.getBoundingClientRect(), s = getComputedStyle(el);
    return {t: r.top + parseFloat(s.borderTopWidth),
            b: r.bottom - parseFloat(s.borderBottomWidth),
            l: r.left + parseFloat(s.borderLeftWidth),
            r: r.right - parseFloat(s.borderRightWidth)}; };
  document.querySelectorAll('.main *, .bottom *').forEach(el => {
    if (!vis(el) || SKIP(el)) return;
    let c = el.parentElement;
    while (c && c !== document.body && !clips(c)) c = c.parentElement;
    if (!c || c === document.body) return;
    const p = pad(c), r = el.getBoundingClientRect(), why = [];
    if (r.bottom > p.b + 1) why.push(`bottom +${(r.bottom - p.b).toFixed(1)}`);
    if (r.top < p.t - 1) why.push(`top -${(p.t - r.top).toFixed(1)}`);
    if (r.right > p.r + 1) why.push(`right +${(r.right - p.r).toFixed(1)}`);
    if (r.left < p.l - 1) why.push(`left -${(p.l - r.left).toFixed(1)}`);
    if (why.length) out.over.push(
      `clipped by #${c.id || c.className}: [${el.className || el.tagName} ` +
      `"${el.textContent.trim().slice(0,28)}"] ${why.join(' ')}`);
  });

  /* ---- 3. ellipsised text ---------------------------------------------- */
  document.querySelectorAll('.main *, .bottom *').forEach(el => {
    if (!vis(el) || SKIP(el)) return;
    const s = getComputedStyle(el);
    if (s.textOverflow === 'ellipsis' && el.scrollWidth > el.clientWidth + 1)
      out.over.push(`ellipsised [${el.className}] "` +
        `${el.textContent.trim().slice(0,40)}": +${el.scrollWidth - el.clientWidth}w`);
  });
  /* a nowrap schema is an INLINE span: it never reports its own overflow, it
     just runs out of its grid track and over the count column beside it. */
  document.querySelectorAll('.glrow').forEach(r => {
    const gs = r.querySelector('.gs'), gg = r.querySelector('.gg');
    if (!gs || !gg || !vis(gs) || !vis(gg)) return;
    if (gs.getBoundingClientRect().right > gg.getBoundingClientRect().left - 0.5)
      out.over.push(`schema overruns its column: "${gs.textContent.trim()}"`);
  });

  /* ---- 4. the other half of "fits": a panel must not be mostly EMPTY -----
     Clipping is only one failure mode of a per-panel fit, and for two years it
     was the only one checked — so a panel whose content stopped well short of
     its box passed while reading, from the aisle, as a card with a hole in it
     (the graphlet view at 1600x900 ran 19% empty under the edge matrix).
     The measure is the gap between the deepest INK in the panel and the
     panel's own content box, MINUS the matching gap above it: several of these
     panels centre their content (every .kpi, the code block), so air below a
     centred block is balanced by the same air above it and reads as margin,
     not as a hole. What the report was about is the EXCESS at the foot — the
     graphlet card ran 0% above and 19% below. Generated ::after content is
     real ink with no DOM node — the rest rail's four lever captions are
     ::after — so it is measured off the last visible child plus the pseudo's
     own used height. */
  const VOID_MAX = 0.10;
  const FITP = ['card-editor','card-graphlets','story','kpi-total','kpi-peak',
                'kpi-graph','kpi-shape','kpi-fmt','kpi-answer'];
  out.voids = {};
  for (const id of FITP) {
    const el = document.getElementById(id); if (!el || !vis(el)) continue;
    const r = el.getBoundingClientRect(), s = getComputedStyle(el);
    const ctop = r.top + parseFloat(s.borderTopWidth) + parseFloat(s.paddingTop);
    const cbot = r.bottom - parseFloat(s.borderBottomWidth) - parseFloat(s.paddingBottom);
    let ink = ctop, inkTop = cbot;
    el.querySelectorAll('*').forEach(e => {
      if (e.classList.contains('pop') || e.closest('.pop') || !vis(e)) return;
      const b = e.getBoundingClientRect();
      ink = Math.max(ink, b.bottom); inkTop = Math.min(inkTop, b.top);
    });
    [el, ...el.querySelectorAll('*')].forEach(e => {
      const a = getComputedStyle(e, '::after');
      const ah = parseFloat(a.height) || 0;
      if (!a.content || a.content === 'none' || a.content === 'normal') return;
      if (ah <= 0 || a.display === 'inline') return;
      const kids = [...e.children].filter(vis);
      const base = kids.length ? kids[kids.length - 1].getBoundingClientRect().bottom
                               : e.getBoundingClientRect().top;
      ink = Math.max(ink, base + (parseFloat(a.marginTop) || 0) + ah);
    });
    const H = el.clientHeight || 1;
    const below = cbot - ink, above = Math.max(0, inkTop - ctop);
    const frac = Math.max(0, below - above) / H;
    out.voids[id] = +(frac * 100).toFixed(1);
    if (frac > VOID_MAX) out.over.push(
      `#${id} is ${(frac * 100).toFixed(1)}% empty at the bottom ` +
      `(${Math.round(below)}px below vs ${Math.round(above)}px above, ` +
      `of ${H}px, --fit ${el.style.getPropertyValue('--fit') || '1'})`);
  }

  /* ---- 5. siblings must not overlap, page must not scroll --------------- */
  const row = ['card-editor','stage','card-graphlets'].map(
    id => [id, document.getElementById(id).getBoundingClientRect()]);
  for (let i = 0; i < row.length; i++) for (let j = i+1; j < row.length; j++) {
    const a = row[i][1], b = row[j][1];
    if (a.left < b.right - 0.5 && b.left < a.right - 0.5 &&
        a.top < b.bottom - 0.5 && b.top < a.bottom - 0.5)
      out.over.push(`overlap: ${row[i][0]} x ${row[j][0]}`);
  }
  const p2 = document.getElementById('tabpanel').getBoundingClientRect();
  const q2 = document.getElementById('rail').getBoundingClientRect();
  if (p2.right > q2.left + 0.5) out.over.push('overlap: tabpanel x rail');
  for (const id of ['navnext','navback']) {
    const e = document.getElementById(id); if (!e) continue;
    const r = e.getBoundingClientRect(), lh = parseFloat(getComputedStyle(e).fontSize) * 1.6;
    if (e.scrollHeight > e.clientHeight + 1 || e.scrollWidth > e.clientWidth + 1)
      out.over.push(`${id} overflows: ${e.scrollWidth}x${e.scrollHeight} in ` +
                    `${Math.round(r.width)}x${Math.round(r.height)}`);
    const inner = [...e.childNodes].reduce((m, n) => n.nodeType !== 1 ? m :
      Math.max(m, n.getBoundingClientRect().height), 0);
    if (inner > lh + 1) out.over.push(`${id} label wraps: ${Math.round(inner)} > ${Math.round(lh)}`);
  }
  if (document.documentElement.scrollHeight > innerHeight + 1)
    out.over.push(`body scrolls: ${document.documentElement.scrollHeight} > ${innerHeight}`);
  if (document.documentElement.scrollWidth > innerWidth + 1)
    out.over.push(`body scrolls sideways: ${document.documentElement.scrollWidth}`);

  const g = window.__gfit || {}, f = window.__fit || {};
  const card = document.getElementById('card-graphlets');
  out.boxes = {
    row1: Math.round(document.getElementById('stage').getBoundingClientRect().height),
    gletw: Math.round(card.getBoundingClientRect().width),
    gleth: Math.round(card.getBoundingClientRect().height),
    fit: f,
    gfx: g.sc,
    voidpct: out.voids,
    glrow: getComputedStyle(document.querySelector('.glrow')).fontSize,
    code: getComputedStyle(document.querySelector('.edbody')).fontSize,
    q: getComputedStyle(document.querySelector('.edq')).fontSize,
    hook: getComputedStyle(document.querySelector('.hook')).fontSize,
  };
  return out;
}"""


async def run_size(pw, w, h, states, verbose):
    bad = 0
    b = await pw.chromium.launch()
    page = await b.new_page(viewport={"width": w, "height": h}, color_scheme="light")
    errs = []
    page.on("pageerror", lambda e: errs.append(str(e)))
    page.on("console", lambda m: errs.append(m.text) if m.type == "error" else None)
    await page.goto(URL)
    await page.wait_for_load_state("load")
    await page.evaluate("document.fonts.ready")
    await page.wait_for_timeout(700)
    for st in STATES:
        if st != "rest":
            await page.click("#navnext")
            await page.wait_for_function(
                "(t)=>{const e=document.getElementById('navnext');"
                "return e&&!e.disabled&&e.textContent.includes(t)}",
                arg=NEXTTXT[st], timeout=240000)
            await page.wait_for_timeout(420)
        if st not in states:
            continue
        # A hover disclosure (the ⓘ popover, a KPI tile's tip) is positioned
        # OUTSIDE its card on purpose, so a cursor left resting on a tile makes
        # every card under it look like it overflows. Park the pointer first.
        await page.mouse.move(2, 2)
        await page.wait_for_timeout(120)
        r = await page.evaluate(PROBE)
        n = len(r["over"])
        bad += n
        tag = "ok  " if not n else "FAIL"
        if verbose or n:
            print(f"  [{tag}] {st:6s} {r['boxes']}")
        for o in r["over"]:
            print("        !", o)
    await b.close()
    if errs:
        print("  CONSOLE ERRORS:", errs)
        bad += len(errs)
    return bad


async def main():
    only = None
    states = STATES
    verbose = "-v" in sys.argv
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1].split(",")
    if "--states" in sys.argv:
        states = sys.argv[sys.argv.index("--states") + 1].split(",")
    sizes = [(w, h) for (w, h) in MATRIX
             if not only or f"{w}x{h}" in only]
    total = 0
    async with async_playwright() as pw:
        for w, h in sizes:
            print(f"=== {w}x{h} ===")
            n = await run_size(pw, w, h, states, verbose)
            total += n
            print(f"    -> {'PASS' if not n else str(n) + ' FAILURES'}")
    print("\nLAYOUT MATRIX:", "FAIL (%d)" % total if total else "PASS")
    sys.exit(1 if total else 0)

if __name__ == "__main__":
    asyncio.run(main())
