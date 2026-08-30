"""Smoke: full advance -> back to rest -> re-advance -> reset on the stepper UI.
Fails loudly on any console error or pageerror. Uses ?mock=1 (no live engine)."""
import asyncio, os
from playwright.async_api import async_playwright

PORT = os.environ.get("PORT", "8500")
URL = "http://127.0.0.1:%s/?mock=1" % PORT
NEXTTXT = ["Run step"] * 7 + ["Restart demo"]
ERRS = []

async def wait_next(page, txt):
    await page.wait_for_function(
        "(t)=>{const e=document.getElementById('navnext');"
        "return e&&!e.disabled&&e.getAttribute('aria-label')===t}", arg=txt, timeout=20000)
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
        assert await page.query_selector("#card-graphlets.unbuilt"), \
            "initial graphlet view must be empty before CGC"
        assert await page.locator('.semantic-meaning').is_visible(), \
            "initial stage must explain the data meaning before showing the graph"
        assert "What the data means" in await page.locator('.semantic-meaning').inner_text()
        raw_scene = await page.evaluate("window.__rawScene")
        assert raw_scene == {"profiles": 40, "profileEdges": 40}, \
            "raw graph must include VENUE_PROFILE nodes and PROFILE_OF edges"
        assert await page.locator('#run-comparison tr').count() == 4
        assert await page.locator('#run-comparison .run-execute span').all_text_contents() == ['—'] * 4
        frame_selectors = ['#card-editor', '#stage', '#card-graphlets',
                           '#rail', '#kpi-total', '#navnext']
        initial_boxes = {s: await page.locator(s).bounding_box() for s in frame_selectors}
        # full advance meaning -> mixed graph -> CGC -> verify
        for i in range(1, 8):
            await page.click("#navnext")
            await page.wait_for_timeout(150)
            for selector, before in initial_boxes.items():
                during = await page.locator(selector).bounding_box()
                assert all(abs(during[k]-before[k]) <= 2 for k in before), \
                    f"panel moved while running: {selector}"
            await wait_next(page, NEXTTXT[i])
            for selector, before in initial_boxes.items():
                after = await page.locator(selector).bounding_box()
                assert all(abs(after[k]-before[k]) <= 2 for k in before), \
                    f"panel moved after step {i}: {selector}"
            if i == 1:
                assert await page.query_selector('#stage.graph'), \
                    "semantic view did not resolve into the mixed raw graph"
                assert not await page.locator('.semantic-meaning').is_visible(), \
                    "semantic cards remained over the raw graph"
                assert await page.query_selector("#card-graphlets.unbuilt"), \
                    "graphlets must stay empty until the second click runs CGC"
            if i == 2:
                assert not await page.query_selector("#card-graphlets.unbuilt"), \
                    "CGC did not reveal the graphlets"
                assert not await page.query_selector("#card-graphlets.building"), \
                    "CGC animation did not finish"
            if 3 <= i <= 6:
                measured = await page.locator('#run-comparison .run-execute span').all_text_contents()
                assert sum(v != '—' for v in measured) == i - 2, 'measurement rows did not accumulate'
            if i == 5:
                assert await page.locator(".mirror .led").count() == 2, \
                    "GEM must highlight one start edge per district"
            if i in (3, 4, 5, 6):
                await page.click('#tabb-plan')
                expected = 10 if i < 5 else 17
                assert await page.locator('.plan-summary').count() == (3 if i < 5 else 4)
                await page.get_by_role('button', name='Show all operators', exact=True).click()
                assert await page.locator('.plan-node').count() == expected
                assert await page.locator('.plan-link').count() == expected - 1
                assert await page.locator('.plan-view').is_visible()
                await page.get_by_role('button', name='Fit entire plan', exact=True).click()
                await page.get_by_role('button', name='Show vertical tree', exact=True).click()
                assert await page.locator('.plan-node').count() == expected
                await page.get_by_role('button', name='Show horizontal tree', exact=True).click()
                await page.get_by_role('button', name='Show grouped overview', exact=True).click()
            if i == 3:
                assert await page.locator('#run-comparison tr').count() == 4
                assert await page.locator('#s-total').inner_text() == '', \
                    "completed runs should not show an End to End Time footer"
                assert not await page.locator('#s-total .info').count(), \
                    "the removed timing footer should not leave an info icon"
        # The same graph feeds intermediate rows; presentation never changes timings.
        measured = await page.locator('#run-comparison .run-execute span').all_text_contents()
        await page.click('.step[data-go="4"]')
        await page.click('#tabb-intermediate')
        await page.locator('[data-profile-format="column"]').click()
        assert await page.locator('#gfx').is_visible()
        assert await page.locator('#card-editor.profile-query').count() == 1
        wide = page.locator('.intermediate-wide')
        packed = page.locator('.intermediate-packed')
        assert await wide.locator('.ivalue').count() == 25
        assert await wide.locator('.inull:not(.imore)').count() == 100
        values = await wide.locator('.ivalue').all_text_contents()
        await page.locator('[data-profile-format="row"]').click()
        assert await page.locator('.intermediate.is-packed').count() == 1
        assert await packed.locator('.inull').count() == 0
        packed_values = await packed.locator('.ivalue').evaluate_all("els=>els.map(e=>e.lastChild.textContent)")
        assert packed_values == values
        await page.locator('#feature-explorer [data-record="2"]').click()
        assert await packed.locator('tr.selected').count() == 1
        assert await page.locator('#run-comparison .run-execute span').all_text_contents() == measured
        await page.get_by_role('button', name='Return to graph').click()
        assert not await page.locator('#feature-explorer').is_visible()
        await page.get_by_role('button', name='Explore SSRF').click()
        assert await page.locator('#feature-explorer').is_visible()
        await page.click('#navnext')
        await wait_next(page, NEXTTXT[7])
        # tabs on verify
        assert await page.locator('#tabb-stats').count() == 0
        for t in ("tabb-plan", "tabb-results"):
            await page.click("#" + t)
            await page.wait_for_timeout(150)
        # what-if toggle on verify
        await page.click(".whatif[data-w='si_off']")
        await page.wait_for_timeout(150)
        await page.click(".whatif[data-w='si_off']")
        await page.wait_for_timeout(150)
        # back all the way to rest
        for i in range(6, -1, -1):
            await page.click("#navback")
            await wait_next(page, NEXTTXT[i])
        assert await page.is_disabled("#navback"), "back not disabled at semantic start"
        assert all(v != '—' for v in await page.locator('#run-comparison .run-execute span').all_text_contents()), 'back cleared measured rows'
        # replay graph reveal + CGC, re-enter cached query rungs, then jump to Baseline
        await page.click("#navnext"); await wait_next(page, NEXTTXT[1])
        await page.click("#navnext"); await wait_next(page, NEXTTXT[2])
        await page.click("#navnext"); await wait_next(page, NEXTTXT[3])
        await page.click("#navnext"); await wait_next(page, NEXTTXT[4])
        await page.click(".step[data-go='1']")
        await wait_next(page, NEXTTXT[3])
        # reset
        await page.click("#btn-reset")
        await wait_next(page, NEXTTXT[0])
        assert await page.query_selector("#card-graphlets.unbuilt"), \
            "reset did not restore the empty graphlet view"
        # Reset preserves metric slots without showing sample result values.
        assert await page.query_selector("#tabbody .emptystate.esrich"), \
            "reset did not restore the rest-state panel"
        assert await page.query_selector("#tabbody .empty-result"), \
            "reset did not restore the empty result state"
        assert not await page.query_selector("#tabbody #g-preview"), \
            "reset shows sample results before execution"
        assert await page.locator('#run-comparison .run-execute span').all_text_contents() == ['—'] * 4
        assert not await page.query_selector("#tabbody #g-single"), \
            "reset left a live result grid behind"
        # Reset must cancel the scheduled CGC frames, not just clear the UI.
        await page.click("#navnext")
        await wait_next(page, NEXTTXT[1])
        await page.click("#navnext")
        await page.wait_for_timeout(450)  # clear the shared click guard mid-animation
        assert await page.query_selector("#card-graphlets.building")
        await page.click("#btn-reset")
        await wait_next(page, NEXTTXT[0])
        await page.wait_for_timeout(5000)
        assert await page.query_selector("#card-graphlets.unbuilt"), \
            "a cancelled CGC frame restored the graphlets"
        # Reduced motion still reaches the same completed CGC state.
        await page.emulate_media(reduced_motion="reduce")
        await page.click("#navnext")
        await wait_next(page, NEXTTXT[1])
        await page.click("#navnext")
        await wait_next(page, NEXTTXT[2])
        assert not await page.query_selector("#card-graphlets.building")
        await browser.close()
    if ERRS:
        print("SMOKE FAIL:")
        for e in ERRS:
            print(" ", e)
        raise SystemExit(1)
    print("SMOKE PASS: semantic reveal + six steps, fixed geometry, SVG trees, tabs, back, reset — zero console errors")

asyncio.run(main())
