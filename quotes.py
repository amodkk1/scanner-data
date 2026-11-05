#!/usr/bin/env python3
"""Intraday price snapshot for StrikeZone (quotes.json).

Runs every 15 minutes while a market is open (see .github/workflows/intraday-quotes.yml) and writes today's
still-forming daily bar (open, high, low, latest price, volume) for every stock in the published universe, plus the
market indices, gold, silver, Bitcoin and Ethereum. Source: Yahoo Finance via yfinance — usually about 15 minutes
behind the exchange. The nightly job (fetch.py) still produces the official end-of-day data; the app only uses these
quotes for the current, unfinished day.

    python quotes.py --state state --out qout            # only the markets that are open right now
    python quotes.py --state state --out qout --force US # ignore the clock (testing)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import fetch  # same folder: reuses the symbol mapping and the index list

log = logging.getLogger("quotes")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

# Exchange hours (local time) + 20 minutes so the last snapshot after the close is captured
HOURS = {"US": ("America/New_York", (9, 30), (16, 20)), "CA": ("America/New_York", (9, 30), (16, 20)),
         "IN": ("Asia/Kolkata", (9, 15), (15, 50))}


def open_now(m: str, now: dt.datetime | None = None) -> bool:
    tz, a, b = HOURS[m]
    t = (now or dt.datetime.now(dt.timezone.utc)).astimezone(ZoneInfo(tz))
    return t.weekday() < 5 and a <= (t.hour, t.minute) <= b


def snap(pairs: list[tuple[str, str]]) -> dict[str, list]:
    """pairs = [(our symbol, Yahoo symbol)] → {our symbol: [date, open, high, low, last, volume]} for today's bar."""
    import yfinance as yf
    out: dict[str, list] = {}
    for i in range(0, len(pairs), 200):
        chunk = pairs[i:i + 200]
        ys = [y for _, y in chunk]
        try:
            df = yf.download(ys, period="5d", interval="1d", auto_adjust=False, group_by="ticker", threads=True, progress=False)
        except Exception as e:  # noqa: BLE001
            log.warning("chunk %d failed: %s", i, e); continue
        if df is None or df.empty:
            continue
        multi = hasattr(df.columns, "levels")
        for s, y in chunk:
            try:
                sub = df[y] if multi else df
            except KeyError:
                continue
            sub = sub.dropna(subset=["Close"]) if "Close" in sub else None
            if sub is None or sub.empty:
                continue
            idx, row = sub.index[-1], sub.iloc[-1]
            out[s] = [idx.date().isoformat(), fetch.rnd(row.get("Open")), fetch.rnd(row.get("High")), fetch.rnd(row.get("Low")),
                      fetch.rnd(row.get("Close")), int(row.get("Volume") or 0)]
        time.sleep(1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default="state"); ap.add_argument("--out", default="qout")
    ap.add_argument("--force", default="", help="comma-separated markets to update regardless of the clock")
    a = ap.parse_args()
    state, out = Path(a.state), Path(a.out)
    forced = {x.strip().upper() for x in a.force.split(",") if x.strip()}
    todo = [m for m in ("US", "CA", "IN") if m in forced or open_now(m)]
    if not todo:
        log.info("No market is open — nothing to do."); return
    old = {}
    if (state / "quotes.json").exists():
        try:
            old = json.loads((state / "quotes.json").read_text())
        except Exception:  # noqa: BLE001
            old = {}
    res = {"v": 1, "source": "Yahoo Finance (about 15 minutes delayed)", "markets": old.get("markets", {}), "idx": old.get("idx", {})}
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    for m in todo:
        p = state / f"{m}.json"
        if not p.exists():
            log.warning("[%s] no published universe yet", m); continue
        syms = [t["s"] for t in json.loads(p.read_text())["tickers"]]
        q = snap([(s, fetch.Yahoo.ysym(m, s)) for s in syms])
        res["markets"][m] = {"at": now, "q": q}
        log.info("[%s] %d of %d quotes", m, len(q), len(syms))
        groups = [m] + (["GL"] if m == "US" else [])
        idx = snap([(s, s) for g in groups for s, _ in fetch.INDICES.get(g, [])])
        for s, v in idx.items():
            res["idx"][s] = v + [now]
    res["generated"] = now
    out.mkdir(parents=True, exist_ok=True)
    (out / "quotes.json").write_text(json.dumps(res, separators=(",", ":")))
    log.info("quotes.json written (%s)", ", ".join(todo))


if __name__ == "__main__":
    main()
