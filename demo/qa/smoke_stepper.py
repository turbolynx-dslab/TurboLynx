"""Smoke: full advance -> back to rest -> re-advance -> reset on the stepper UI.
Fails loudly on any console error or pageerror. Uses ?mock=1 (no live engine)."""
import asyncio, os
from playwright.async_api import async_playwright

PORT = os.environ.get("PORT", "8500")
URL = "http://127.0.0.1:%s/?mock=1" % PORT
NEXTTXT = ["Run baseline", "Prune schema", "Split per district", "Pack rows",
           "Verify results", "Restart demo"]
ERRS = []

async def wait_next(page, txt):
    await page.wait_for_function(
        "(t)=>{const e=document.getElementById('navnext');"
        "return e&&!e.disabled&&e.textContent.includes(t)}", arg=txt, timeout=20000)
    await page.wait_for_timeout(400)  # > qaLock guard (350ms)

async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        page = await browser.new_page(viewport={"width": 1920, "height": 1080})
        page.on("pageerror", lambda e: ERRS.append("pageerror: " + str(e)))
        page.on("console", lambda m: ERRS.append("console: " + m.text)
                if m.type == "error" else None)
        await page.goto(URL)
        await page.wait_for_load_state("load")
        await page.evaluate("document.fonts.ready")
        await wait_next(page, NEXTTXT[0])
        # full advance rest -> verify
        for i in range(1, 6):
            await page.click("#navnext")
            await wait_next(page, NEXTTXT[i])
        # tabs on verify
        for t in ("tabb-plan", "tabb-stats", "tabb-results"):
            await page.click("#" + t)
            await page.wait_for_timeout(150)
        # what-if toggle on verify
        await page.click(".whatif[data-w='si_off']")
        await page.wait_for_timeout(150)
        await page.click(".whatif[data-w='si_off']")
        await page.wait_for_timeout(150)
        # back all the way to rest
        for i in range(4, -1, -1):
            await page.click("#navback")
            await wait_next(page, NEXTTXT[i])
        assert await page.is_disabled("#navback"), "back not disabled at rest"
        # re-advance two rungs (cached, no re-run) + step-jump to Baseline
        await page.click("#navnext"); await wait_next(page, NEXTTXT[1])
        await page.click("#navnext"); await wait_next(page, NEXTTXT[2])
        await page.click(".step[data-go='1']")
        await wait_next(page, NEXTTXT[1])
        # reset
        await page.click("#btn-reset")
        await wait_next(page, NEXTTXT[0])
        empty = await page.eval_on_selector("#tabbody", "e=>e.textContent")
        assert "Run baseline" in empty, "reset did not clear results"
        assert await page.query_selector("#tabbody #g-preview"), \
            "reset did not restore the rest-state preview grid"
        assert not await page.query_selector("#tabbody #g-single"), \
            "reset left a live result grid behind"
        await browser.close()
    if ERRS:
        print("SMOKE FAIL:")
        for e in ERRS:
            print(" ", e)
        raise SystemExit(1)
    print("SMOKE PASS: advance x5, tabs, what-if, back x5, re-advance, jump, reset — zero console errors")

asyncio.run(main())
