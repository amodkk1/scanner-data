#!/usr/bin/env python3
"""
Opportunity Scanner — nightly market-data fetcher.

Builds one compact JSON file per market (US, CA, IN) with ~20 months of daily
OHLCV bars, basic fundamentals and next earnings dates, in the format the
Opportunity Scanner app reads as a "Live feed".

It is incremental: each run starts from the previously published files
(--state), downloads only the sessions that are missing, trims to HISTORY
sessions and writes the result to --out. The first run backfills.

Data sources (first one that is configured / works wins):

  Market  Primary                                   Fallback
  ------  ----------------------------------------  ---------------------------
  US      EODHD bulk (if EODHD_API_KEY)             Massive grouped daily (free key)
  CA      EODHD bulk (if EODHD_API_KEY)             Yahoo Finance via yfinance
  IN      EODHD bulk (if EODHD_API_KEY)             NSE bhavcopy (official), then yfinance
  Fundamentals / earnings dates: yfinance, refreshed on a rolling basis.

Environment variables:
  MASSIVE_API_KEY   free key from massive.com (US daily bars)
  EODHD_API_KEY     optional paid key (one bulk call per exchange per day)
  HISTORY           sessions to keep (default 420 ≈ 20 months)
  UNIVERSE_US/CA/IN max tickers kept per market, ranked by traded value
  FUND_PER_RUN      tickers per market whose fundamentals are refreshed per run (default 150)
  FEED_MOCK=1       generate synthetic data instead of calling any API (for testing)

Usage:
  python fetch.py --markets US,CA,IN --state state --out out
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import logging
import math
import os
import random
import sys
import time
import zipfile
from pathlib import Path

import requests

log = logging.getLogger("fetch")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

HISTORY = int(os.getenv("HISTORY", "420"))
UNIVERSE = {"US": int(os.getenv("UNIVERSE_US", "1000")), "CA": int(os.getenv("UNIVERSE_CA", "300")), "IN": int(os.getenv("UNIVERSE_IN", "1000"))}
MIN_PRICE = {"US": 3.0, "CA": 1.0, "IN": 20.0}
INDEX_SYMS = {"US": ["SPY"], "CA": ["XIU", "XIC"], "IN": ["NIFTYBEES"]}   # always kept for the regime filter
FUND_PER_RUN = int(os.getenv("FUND_PER_RUN", "150"))
MOCK = os.getenv("FEED_MOCK") == "1"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36"

SECTOR_MAP = {  # yfinance / EODHD sector names -> app sectors
    "Technology": "Technology", "Information Technology": "Technology", "Healthcare": "Healthcare", "Health Care": "Healthcare",
    "Financial Services": "Financials", "Financials": "Financials", "Financial": "Financials",
    "Consumer Cyclical": "Consumer Discretionary", "Consumer Discretionary": "Consumer Discretionary",
    "Industrials": "Industrials", "Communication Services": "Communication Services", "Energy": "Energy",
    "Consumer Defensive": "Consumer Staples", "Consumer Staples": "Consumer Staples",
    "Basic Materials": "Materials", "Materials": "Materials", "Utilities": "Utilities", "Real Estate": "Real Estate",
}

# Fallback Canadian universe if the S&P/TSX Composite list can't be downloaded.
CA_SEED = ("RY TD ENB SHOP CNR CP BNS BMO CM TRP SU CNQ BN BAM ATD MFC SLF NTR ABX AEM WCN CSU TRI FTS BCE T RCI-B QSR L DOL "
           "MRU WPM FNV TECK-B CCO IMO CVE PPL GIB-A OTEX POW IFC GWO FFH EMA H CU MG WSP STN TFII CTC-A DOO X IAG NA "
           "BIP-UN BEP-UN K LUN FM IVN TOU ARX CPX ALA KEY GFL XIU").split()


# --------------------------------------------------------------------------- helpers
def weekdays_between(start: dt.date, end: dt.date) -> list[dt.date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def rnd(x: float | None) -> float | None:
    if x is None or not math.isfinite(x):
        return None
    return round(x, 2) if abs(x) >= 1 else round(x, 4)


def http_get(url: str, *, session: requests.Session | None = None, params=None, headers=None, retries=3, timeout=60) -> requests.Response:
    s = session or requests
    for a in range(1, retries + 1):
        try:
            r = s.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            return r
        except (requests.RequestException,) as e:
            if a == retries:
                raise
            wait = 2 ** a + random.random()
            log.warning("GET %s failed (%s); retry %d in %.1fs", url.split("?")[0], e, a, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


Bars = dict  # {symbol: {date_iso: [o, h, l, c, v]}}


def put(bars: Bars, sym: str, day: str, o, h, l, c, v):
    try:
        o, h, l, c, v = float(o), float(h), float(l), float(c), float(v or 0)
    except (TypeError, ValueError):
        return
    if min(o, h, l, c) <= 0 or not all(map(math.isfinite, (o, h, l, c, v))):
        return
    bars.setdefault(sym, {})[day] = [o, h, l, c, v]


# --------------------------------------------------------------------------- sources
class EODHD:
    """Paid, all three markets: one bulk call (100 API credits) per exchange per date. Server-side only (no CORS)."""
    EXCH = {"US": "US", "CA": "TO", "IN": "NSE"}

    def __init__(self, key: str):
        self.key = key

    def fetch(self, market: str, days: list[dt.date], universe: set[str] | None) -> tuple[Bars, list[str]]:
        bars: Bars = {}
        for d in days:
            r = http_get(f"https://eodhd.com/api/eod-bulk-last-day/{self.EXCH[market]}",
                         params={"api_token": self.key, "fmt": "json", "date": d.isoformat()})
            if r.status_code == 404:
                raise RuntimeError(f"EODHD exchange {self.EXCH[market]} not found for {market}")
            r.raise_for_status()
            for row in r.json():
                if row.get("date") != d.isoformat():
                    continue  # holiday: EODHD returns the previous session
                sym = row["code"]
                # use adjusted close ratio to adjust OHLC for splits/dividends
                adj = (row.get("adjusted_close") or row["close"]) / row["close"] if row.get("close") else 1
                put(bars, sym, row["date"], row["open"] * adj, row["high"] * adj, row["low"] * adj, row["close"] * adj, row.get("volume"))
        return bars, [f"EODHD bulk {self.EXCH[market]}"]


class Massive:
    """US only. Free 'Stocks Basic' key: 5 calls/min, previous session available after midnight ET."""

    def __init__(self, key: str):
        self.key = key

    def fetch(self, days: list[dt.date]) -> tuple[Bars, list[str]]:
        bars: Bars = {}
        for k, d in enumerate(days):
            if k:
                time.sleep(12.5)  # stay under 5 requests/minute
            r = http_get(f"https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{d.isoformat()}",
                         params={"adjusted": "true", "apiKey": self.key})
            if r.status_code == 403:
                log.warning("Massive: %s not available yet on this plan (403)", d)
                continue
            r.raise_for_status()
            for row in r.json().get("results") or []:
                put(bars, row["T"], d.isoformat(), row.get("o"), row.get("h"), row.get("l"), row.get("c"), row.get("v"))
            log.info("Massive %s: %d tickers", d, len(r.json().get("results") or []))
        return bars, ["Massive grouped daily"]


class NSE:
    """India: official NSE capital-market bhavcopy (UDiFF format, one zip per session, all symbols)."""
    URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{ymd}_F_0000.csv.zip"

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.nseindia.com/"})
        try:
            self.s.get("https://www.nseindia.com/", timeout=20)  # cookie warm-up
        except requests.RequestException as e:
            log.warning("NSE warm-up failed: %s", e)

    def fetch(self, days: list[dt.date]) -> tuple[Bars, list[str]]:
        import csv
        bars: Bars = {}
        misses = 0
        for d in days:
            r = http_get(self.URL.format(ymd=d.strftime("%Y%m%d")), session=self.s, retries=2, timeout=30)
            if r.status_code == 404:
                continue  # holiday
            if r.status_code != 200 or not r.content.startswith(b"PK"):
                misses += 1
                if misses >= 3:
                    raise RuntimeError(f"NSE archive blocked or unavailable (HTTP {r.status_code})")
                continue
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                text = z.read(z.namelist()[0]).decode("utf-8", "replace")
            rd = csv.DictReader(io.StringIO(text))
            n = 0
            for row in rd:
                row = {k.strip(): (v or "").strip() for k, v in row.items() if k}
                series = row.get("SctySrs") or row.get("SERIES")
                if series not in ("EQ", "BE"):
                    continue
                sym = row.get("TckrSymb") or row.get("SYMBOL")
                put(bars, sym, d.isoformat(), row.get("OpnPric") or row.get("OPEN"), row.get("HghPric") or row.get("HIGH"),
                    row.get("LwPric") or row.get("LOW"), row.get("ClsPric") or row.get("CLOSE"), row.get("TtlTradgVol") or row.get("TOTTRDQTY"))
                n += 1
            log.info("NSE %s: %d symbols", d, n)
            time.sleep(0.4)
        return bars, ["NSE bhavcopy (unadjusted for splits)"]


class Yahoo:
    """yfinance (unofficial Yahoo endpoints; personal use only per Yahoo's terms)."""
    SUFFIX = {"US": "", "CA": ".TO", "IN": ".NS"}

    @staticmethod
    def ysym(market: str, sym: str) -> str:
        if market == "US":
            return sym.replace(".", "-")
        if market == "CA":
            return sym.replace(".", "-") + ".TO"
        return sym + ".NS"

    def fetch(self, market: str, syms: list[str], start: dt.date) -> tuple[Bars, list[str]]:
        import yfinance as yf
        bars: Bars = {}
        for i in range(0, len(syms), 100):
            chunk = syms[i:i + 100]
            ys = [self.ysym(market, s) for s in chunk]
            df = yf.download(ys, start=start.isoformat(), auto_adjust=True, group_by="ticker", threads=True, progress=False)
            for s, y in zip(chunk, ys):
                try:
                    sub = df[y] if len(ys) > 1 else df
                except KeyError:
                    continue
                for idx, row in sub.dropna(how="all").iterrows():
                    put(bars, s, idx.date().isoformat(), row.get("Open"), row.get("High"), row.get("Low"), row.get("Close"), row.get("Volume"))
            time.sleep(2)
        return bars, ["Yahoo Finance (yfinance)"]

    def fundamentals(self, market: str, syms: list[str]) -> dict:
        import yfinance as yf
        out = {}
        for s in syms:
            try:
                t = yf.Ticker(self.ysym(market, s))
                info = t.info or {}
                ocf = info.get("operatingCashflow")
                de = info.get("debtToEquity")
                earn = None
                ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
                if ts:
                    earn = dt.datetime.fromtimestamp(ts, dt.timezone.utc).date().isoformat()
                out[s] = {
                    "n": info.get("longName") or info.get("shortName") or s,
                    "x": SECTOR_MAP.get(info.get("sector") or "", "Unclassified"),
                    "cap": info.get("marketCap"),
                    "f": {"rg": info.get("revenueGrowth"), "eg": info.get("earningsGrowth"), "om": info.get("operatingMargins"),
                          "ocf": None if ocf is None else int(ocf > 0), "de": None if de is None else de / 100,
                          "pe": info.get("trailingPE") if (info.get("trailingPE") or 0) > 0 else None,
                          "peg": info.get("trailingPegRatio") or info.get("pegRatio")},
                    "e": earn, "u": dt.date.today().isoformat(),
                }
            except Exception as e:  # noqa: BLE001 — keep going on any single-ticker failure
                log.warning("fundamentals %s failed: %s", s, e)
            time.sleep(0.6)
        return out


class Mock:
    """Synthetic random-walk data so the whole pipeline and app can be tested offline."""

    def __init__(self, market: str):
        self.m = market
        self.r = random.Random({"US": 1, "CA": 2, "IN": 3}[market])
        n = {"US": 600, "CA": 200, "IN": 400}[market]
        self.syms = INDEX_SYMS[market][:1] + [self._sym() for _ in range(n)]
        self.px = {s: math.exp(self.r.uniform(math.log(MIN_PRICE[market] * 2), math.log(MIN_PRICE[market] * 120))) for s in self.syms}
        self.vol = {s: self.r.uniform(.01, .03) for s in self.syms}

    def _sym(self):
        return "".join(self.r.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(self.r.choice([3, 4, 5 if self.m == "IN" else 4])))

    def fetch(self, days):
        bars: Bars = {}
        for d in days:
            mkt = self.r.gauss(.0004, .009)
            for s in self.syms:
                p0 = self.px[s]
                p1 = p0 * math.exp(mkt + self.r.gauss(.0002, self.vol[s]))
                o = p0 * math.exp(self.r.gauss(0, self.vol[s] * .3))
                put(bars, s, d.isoformat(), o, max(o, p1) * (1 + abs(self.r.gauss(0, .006))), min(o, p1) * (1 - abs(self.r.gauss(0, .006))), p1,
                    self.r.lognormvariate(math.log(2e6 / max(p1, 1) * 50), .4))
                self.px[s] = p1
        return bars, ["MOCK synthetic data"]

    def fundamentals(self, syms):
        secs = sorted(set(SECTOR_MAP.values()))
        return {s: {"n": f"{s} Mock Corp", "x": self.r.choice(secs), "cap": 10 ** self.r.uniform(8.5, 12),
                    "f": {"rg": self.r.gauss(.08, .15), "eg": self.r.gauss(.1, .25), "om": self.r.gauss(.12, .1), "ocf": int(self.r.random() > .2),
                          "de": self.r.lognormvariate(-.4, .7), "pe": self.r.uniform(8, 45), "peg": self.r.uniform(.5, 3)},
                    "e": (dt.date.today() + dt.timedelta(days=self.r.randint(1, 90))).isoformat(), "u": dt.date.today().isoformat()} for s in syms}


# --------------------------------------------------------------------------- state & output
def load_state(path: Path) -> dict:
    """Read a previously published feed file back into {sym: {date: [o,h,l,c,v]}} plus metadata."""
    if not path.exists():
        return {"bars": {}, "meta": {}}
    j = json.loads(path.read_text())
    dates = j["dates"]
    bars: Bars = {}
    meta = {}
    for t in j["tickers"]:
        st = t.get("st", 0)
        for k in range(len(t["c"])):
            c = t["c"][k]
            if c is None:
                continue
            bars.setdefault(t["s"], {})[dates[st + k]] = [t["o"][k], t["h"][k], t["l"][k], c, t["v"][k] or 0]
        meta[t["s"]] = {k: t[k] for k in ("n", "x", "cap", "f", "e", "u") if k in t}
    return {"bars": bars, "meta": meta, "fund_cache": j.get("fund_cache", {})}


def select_universe(market: str, bars: Bars, dates: list[str]) -> list[str]:
    recent = dates[-50:]
    ranked = []
    for s, by in bars.items():
        vals = [by[d][3] * by[d][4] for d in recent if d in by]
        if len(vals) < 20:
            continue
        last = by[max(by)][3]
        if last < MIN_PRICE[market]:
            continue
        ranked.append((sorted(vals)[len(vals) // 2], s))
    ranked.sort(reverse=True)
    keep = [s for _, s in ranked[:UNIVERSE[market]]]
    for s in INDEX_SYMS[market]:
        if s in bars and s not in keep:
            keep.append(s)
    return keep


def build_output(market: str, bars: Bars, meta: dict, sources: list[str], warnings: list[str]) -> dict:
    all_dates = sorted({d for by in bars.values() for d in by})
    # a session counts only if a meaningful share of symbols traded (drops stray holiday rows)
    counts = {d: 0 for d in all_dates}
    for by in bars.values():
        for d in by:
            counts[d] += 1
    peak = max(counts.values()) if counts else 0
    dates = [d for d in all_dates if counts[d] >= peak * .3][-HISTORY:]
    syms = select_universe(market, bars, dates)
    idx = {d: i for i, d in enumerate(dates)}
    tickers = []
    for s in syms:
        by = bars[s]
        present = [idx[d] for d in by if d in idx]
        if not present:
            continue
        st = min(present)
        cols = {k: [None] * (len(dates) - st) for k in "ohlcv"}
        for d, row in by.items():
            if d in idx:
                k = idx[d] - st
                for j, key in enumerate("ohlcv"):
                    cols[key][k] = rnd(row[j]) if key != "v" else int(row[4])
        t = {"s": s, "st": st, **cols}
        m = meta.get(s, {})
        for k in ("n", "x", "cap", "f", "e", "u"):
            if m.get(k) is not None:
                t[k] = m[k]
        tickers.append(t)
    return {"v": 1, "market": market, "asOf": dates[-1] if dates else None, "generated": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "source": " + ".join(dict.fromkeys(sources)), "warnings": warnings, "dates": dates, "tickers": tickers}


# --------------------------------------------------------------------------- main
def run_market(market: str, state_dir: Path, out_dir: Path, backfill: bool) -> dict:
    st = load_state(state_dir / f"{market}.json")
    bars, meta = st["bars"], st["meta"]
    today = dt.date.today()
    last = max((d for by in bars.values() for d in by), default=None)
    start = today - dt.timedelta(days=int(HISTORY * 1.45) + 10) if (backfill or not last) else dt.date.fromisoformat(last) + dt.timedelta(days=1)
    days = weekdays_between(start, today)
    log.info("[%s] %s: fetching %d candidate session(s) from %s", market, "backfill" if (backfill or not last) else "update", len(days), start)
    sources, warnings = [], []
    new: Bars = {}

    def merge(b: Bars):
        for s, by in b.items():
            new.setdefault(s, {}).update(by)

    eod_key, massive_key = os.getenv("EODHD_API_KEY"), os.getenv("MASSIVE_API_KEY")
    if days:
        try:
            if MOCK:
                mk = Mock(market)
                for s in mk.syms:  # continue the random walk from the stored closes
                    if s in bars:
                        mk.px[s] = bars[s][max(bars[s])][3]
                b, src = mk.fetch(days)
            elif eod_key:
                b, src = EODHD(eod_key).fetch(market, days, None)
            elif market == "US":
                if not massive_key:
                    raise RuntimeError("Set MASSIVE_API_KEY (free at massive.com) or EODHD_API_KEY for US data")
                b, src = Massive(massive_key).fetch(days)
            elif market == "IN":
                try:
                    b, src = NSE().fetch(days)
                except Exception as e:  # noqa: BLE001
                    warnings.append(f"NSE archive failed ({e}); used Yahoo Finance instead")
                    log.warning(warnings[-1])
                    syms = sorted(bars) or nifty500()
                    b, src = Yahoo().fetch("IN", syms, start)
            else:  # CA
                syms = sorted(set(bars) | set(tsx_universe()))
                b, src = Yahoo().fetch("CA", syms, start)
            merge(b); sources += src
        except Exception as e:  # noqa: BLE001
            warnings.append(f"Price update failed: {e}")
            log.error("[%s] %s", market, e)
    for s, by in new.items():
        bars.setdefault(s, {}).update(by)
    if not bars:
        raise RuntimeError(f"[{market}] no data at all — check API keys / network")

    # Fundamentals: refresh the stalest FUND_PER_RUN tickers in the (new) universe
    all_dates = sorted({d for by in bars.values() for d in by})[-HISTORY:]
    uni = select_universe(market, bars, all_dates)
    stale = sorted(uni, key=lambda s: meta.get(s, {}).get("u", ""))[:FUND_PER_RUN]
    try:
        fresh = Mock(market).fundamentals(stale) if MOCK else Yahoo().fundamentals(market, stale)
        for s, m in fresh.items():
            meta[s] = {**meta.get(s, {}), **m}
        log.info("[%s] fundamentals refreshed for %d ticker(s)", market, len(fresh))
    except Exception as e:  # noqa: BLE001
        warnings.append(f"Fundamentals refresh skipped: {e}")
    if not sources:
        sources = ["no new sessions"]
    out = build_output(market, bars, meta, sources, warnings)
    (out_dir / f"{market}.json").write_text(json.dumps(out, separators=(",", ":")))
    log.info("[%s] wrote %d tickers × %d sessions, as of %s", market, len(out["tickers"]), len(out["dates"]), out["asOf"])
    return {"asOf": out["asOf"], "tickers": len(out["tickers"]), "sessions": len(out["dates"]), "source": out["source"], "warnings": warnings}


def tsx_universe() -> list[str]:
    """S&P/TSX Composite constituents from Wikipedia; falls back to a seed list."""
    try:
        import pandas as pd
        html = http_get("https://en.wikipedia.org/wiki/S%26P/TSX_Composite_Index", headers={"User-Agent": UA}).text
        for t in pd.read_html(io.StringIO(html)):
            col = next((c for c in t.columns if str(c).lower() in ("symbol", "ticker")), None)
            if col is not None and len(t) > 150:
                syms = [str(x).replace(".TO", "").replace(".", "-").strip() for x in t[col]]
                return sorted(set(syms) | {"XIU"})
    except Exception as e:  # noqa: BLE001
        log.warning("TSX constituent list failed (%s); using seed list", e)
    return CA_SEED


def nifty500() -> list[str]:
    try:
        r = http_get("https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv", headers={"User-Agent": UA})
        import csv
        return [row["Symbol"] for row in csv.DictReader(io.StringIO(r.text))] + ["NIFTYBEES"]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"no Indian universe available ({e})") from e


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--markets", default="US,CA,IN")
    ap.add_argument("--state", default="state", help="folder holding the previously published feed files")
    ap.add_argument("--out", default="out")
    ap.add_argument("--backfill", action="store_true", help="re-download the full history")
    a = ap.parse_args()
    state, out = Path(a.state), Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    meta_path = state / "meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {"markets": {}}
    failed = []
    for m in [x.strip().upper() for x in a.markets.split(",") if x.strip()]:
        try:
            meta["markets"][m] = run_market(m, state, out, a.backfill)
        except Exception as e:  # noqa: BLE001
            log.error("%s", e)
            failed.append(m)
            if (state / f"{m}.json").exists():  # keep publishing yesterday's file
                (out / f"{m}.json").write_bytes((state / f"{m}.json").read_bytes())
    # carry over files for markets not processed this run
    for p in state.glob("*.json"):
        if p.name != "meta.json" and not (out / p.name).exists():
            (out / p.name).write_bytes(p.read_bytes())
    meta["generated"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    meta["v"] = 1
    (out / "meta.json").write_text(json.dumps(meta, indent=1))
    if failed:
        log.error("Markets failed: %s", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
