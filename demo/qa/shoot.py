"""Booth screenshots for the DB-executor redesign: feature-ladder flow.
Usage: shoot.py [light] [dark]   (default light) -> shots/{26in}-N-state[-dark].png
Shoots ?mock=1 so the states show the PLACEHOLDER ladder numbers, not the
old live-API contract's numbers.
"""
import asyncio, os, sys
from playwright.async_api import async_playwright

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shots")
PORT = os.environ.get("PORT", "8500")
URL = "http://127.0.0.1:%s/?mock=1" % PORT
# the booth sizes AND the odd ones the auto-fit has to hold: 1990x1037 is a
# real presenter window, 1680x1050 and 2200x1200 are the aspect ratios either
# side of it. layoutcheck.py asserts the same matrix.
RES = [(1600, 900, "16in"), (1680, 1050, "16-10"), (1920, 1080, "26in"),
       (1990, 1037, "win"), (2200, 1200, "22in"), (2560, 1440, "32in"),
       (3840, 2160, "4k")]
if os.environ.get("ONLY"):
    RES = [r for r in RES if r[2] in os.environ["ONLY"].split(",")]
GUARD = 420
API_T = 240000
# state reached -> text the forward button shows once that state is live
NEXTTXT = {"base": "Prune schema", "si": "Split per district",
           "gem": "Pack rows", "ssrf": "Verify results",
           "verify": "Restart demo"}

async def snap(page, name):
    await page.wait_for_timeout(250)
    await page.screenshot(path=os.path.join(OUT, name))
    print("shot", name)

async def advance(page, to):
    await page.click("#navnext")
    await page.wait_for_function(
        "(t)=>{const e=document.getElementById('navnext');"
        "return e&&!e.disabled&&e.textContent.includes(t)}",
        arg=NEXTTXT[to], timeout=API_T)
    await page.wait_for_timeout(GUARD)

async def run_flow(pw, w, h, tag, scheme="light"):
    browser = await pw.chromium.launch()
    page = await browser.new_page(viewport={"width": w, "height": h},
                                  color_scheme=scheme)
    await page.goto(URL)
    await page.wait_for_load_state("load")
    await page.evaluate("document.fonts.ready")
    await page.wait_for_timeout(700)
    sfx = "" if scheme == "light" else "-dark"
    await snap(page, f"{tag}-1-rest{sfx}.png")
    await advance(page, "base")     # rung 1: all off -> Results grid
    await snap(page, f"{tag}-2-base{sfx}.png")
    await advance(page, "si")       # rung 2: +SI -> Plan tab
    await snap(page, f"{tag}-3-si{sfx}.png")
    await advance(page, "gem")      # rung 3: +GEM -> Plan tab (2 branches)
    await snap(page, f"{tag}-4-gem{sfx}.png")
    await advance(page, "ssrf")     # rung 4: +SSRF -> Plan tab (fmt=ROW)
    await snap(page, f"{tag}-5-ssrf{sfx}.png")
    await advance(page, "verify")   # verify -> side-by-side grids
    await snap(page, f"{tag}-6-verify{sfx}.png")
    await browser.close()

async def main():
    os.makedirs(OUT, exist_ok=True)
    schemes = sys.argv[1:] or ["light"]
    async with async_playwright() as pw:
        for w, h, tag in RES:
            for s in schemes:
                await run_flow(pw, w, h, tag, s)

asyncio.run(main())
