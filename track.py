#!/usr/bin/env python3
"""Record StrikeZone's BUY signals into signals.json — the app's track record.

The nightly job runs this after fetch.py. It opens the published StrikeZone app in a headless browser, feeds it the
freshly built data (out/ folder, before it is published), and asks the app itself to find today's BUY signals and to
update the results of earlier ones. Using the app's own code means the log always matches what users were shown.
Recorded entries are never edited; closed results are frozen.

    python track.py --out out --state state --markets US,CA
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

log = logging.getLogger("track")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

APP = os.getenv("STRIKEZONE_APP_URL", "https://amodkk1.github.io/stockscanner/")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="out"); ap.add_argument("--state", default="state")
    ap.add_argument("--markets", default="US,CA,IN")
    ap.add_argument("--backfill", type=int, default=60, help="sessions to replay when a market has no history yet")
    a = ap.parse_args()
    out, state = Path(a.out), Path(a.state)
    markets = [m.strip().upper() for m in a.markets.split(",") if m.strip()]
    prev_p = out / "signals.json" if (out / "signals.json").exists() else state / "signals.json"
    prev = json.loads(prev_p.read_text()) if prev_p.exists() else None
    log.info("Previous log: %s signals", len(prev["signals"]) if prev else 0)

    from playwright.sync_api import sync_playwright

    def serve(route):
        url = route.request.url
        if "raw.githubusercontent.com" in url and "/data/" in url:          # the data feed → today's fresh files
            name = url.split("/data/")[-1].split("?")[0]
            for folder in (out, state):
                f = folder / name
                if f.exists():
                    return route.fulfill(status=200, body=f.read_bytes(), content_type="application/json",
                                         headers={"Access-Control-Allow-Origin": "*"})
            return route.fulfill(status=404, body="", headers={"Access-Control-Allow-Origin": "*"})
        if any(h in url for h in ("firebase", "googleapis.com/identitytoolkit", "firestore.googleapis", "gstatic.com/firebasejs")):
            return route.abort()                                             # no accounts / visitor logging
        return route.continue_()

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context(viewport={"width": 1280, "height": 800}).new_page()
        page.on("console", lambda m: m.type == "error" and log.warning("app: %s", m.text[:200]))
        page.route("**/*", serve)
        page.goto(APP + ("&" if "?" in APP else "?") + "recorder=1", timeout=120_000)
        page.wait_for_function("() => window.StrikeZone && window.StrikeZone.ready === true", timeout=180_000)
        log.info("App %s loaded", page.evaluate("() => window.StrikeZone.version"))
        res = page.evaluate("async ([prev, opts]) => await window.StrikeZone.track(prev, opts)",
                            [prev, {"markets": markets, "backfill": a.backfill, "live": True}])
        browser.close()
    for n in res.get("notes", []):
        log.info("%s", n)
    res.pop("notes", None)
    out.mkdir(parents=True, exist_ok=True)
    (out / "signals.json").write_text(json.dumps(res, separators=(",", ":")))
    log.info("signals.json written: %d signals", len(res["signals"]))


if __name__ == "__main__":
    main()
