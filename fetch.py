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
  HISTORY           sessions to keep (default 760 ≈ 3 years)
  UNIVERSE_US/CA/IN max tickers kept per market, ranked by traded value
  FUND_PER_RUN      tickers per market whose fundamentals are refreshed per run (default 150)
  FEED_MOCK=1       generate synthetic data instead of calling any API (for testing)

My stocks:
  Tickers listed in my-tickers.json (repo root, written by the app's "My stocks" panel) are always
  kept in the feed, whatever their liquidity. A newly added ticker gets its full history downloaded
  on the next run; tickers that can't be found are reported in meta.json ("notFound").
  Format: {"US": ["AAPL"], "CA": ["SHOP"], "IN": ["TCS"]}

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

HISTORY = int(os.getenv("HISTORY", "760"))
UNIVERSE = {"US": int(os.getenv("UNIVERSE_US", "1000")), "CA": int(os.getenv("UNIVERSE_CA", "1500")), "IN": int(os.getenv("UNIVERSE_IN", "1000"))}
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

MY: dict[str, list[str]] = {"US": [], "CA": [], "IN": []}   # user's own tickers, loaded in main()
SUFFIXES = (".TO", ".TSX", ".V", ".NS", ".NSE", ".BO", ".BSE", ".US")


def load_my_tickers() -> dict[str, list[str]]:
    """Read my-tickers.json from the repo root (or next to this script). Invalid entries are skipped."""
    import re
    here = Path(__file__).resolve().parent
    for p in (Path.cwd() / "my-tickers.json", here / "my-tickers.json", here.parent / "my-tickers.json"):
        if not p.exists():
            continue
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except (ValueError, OSError) as e:
            log.warning("my-tickers.json is not valid JSON (%s); ignoring it", e)
            return {"US": [], "CA": [], "IN": []}
        out = {}
        for m in ("US", "CA", "IN"):
            seen = []
            for x in raw.get(m, []) if isinstance(raw, dict) else []:
                sym = str(x).strip().upper()
                for suf in SUFFIXES:
                    if sym.endswith(suf):
                        sym = sym[: -len(suf)]
                if re.fullmatch(r"[A-Z0-9][A-Z0-9.&\-]{0,19}", sym) and sym not in seen:
                    seen.append(sym)
            out[m] = seen[:200]
        log.info("My stocks: %s", {m: len(v) for m, v in out.items()})
        return out
    return {"US": [], "CA": [], "IN": []}


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
        refused, last_msg = 0, ""
        for k, d in enumerate(days):
            if k:
                time.sleep(12.5)  # stay under 5 requests/minute
            r = http_get(f"https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{d.isoformat()}",
                         params={"adjusted": "true", "apiKey": self.key})
            if r.status_code == 401:
                raise RuntimeError("Massive rejected the API key (401). Check the MASSIVE_API_KEY secret has the exact key from your Massive dashboard.")
            if r.status_code == 403:
                refused += 1
                try:
                    last_msg = r.json().get("message", "")
                except ValueError:
                    last_msg = ""
                log.warning("Massive: %s refused (403) %s", d, last_msg)
                if refused >= 3 and not bars:
                    raise RuntimeError("Massive refused the stock data (403) — the key isn't on a Stocks plan. "
                                       "On massive.com subscribe to 'Stocks Basic' (free). " + (f"Massive said: {last_msg}" if last_msg else ""))
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
                sym = row.get("TckrSymb") or row.get("SYMBOL")
                if series not in ("EQ", "BE") and sym not in MY["IN"]:
                    continue
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
            if df is None or df.empty:
                continue
            multi = hasattr(df.columns, "levels")
            for s, y in zip(chunk, ys):
                try:
                    sub = df[y] if multi and y in df.columns.get_level_values(0) else (None if multi else df)
                except KeyError:
                    continue
                if sub is None:
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


def fetch_history(market: str, syms: list[str], start: dt.date) -> Bars:
    """Full daily history for a few individual tickers (used when the user adds a stock)."""
    bars: Bars = {}
    if MOCK:
        mk = Mock(market)
        real = [s for s in syms if not s.startswith("ZZ")]           # ZZ… = 'unknown ticker' in tests
        mk.syms = real
        mk.px = {s: mk.r.uniform(MIN_PRICE[market] * 3, MIN_PRICE[market] * 80) for s in real}
        mk.vol = {s: mk.r.uniform(.01, .03) for s in real}
        return mk.fetch(weekdays_between(start, dt.date.today()))[0] if real else {}
    eod_key, massive_key = os.getenv("EODHD_API_KEY"), os.getenv("MASSIVE_API_KEY")
    for k, s in enumerate(syms):
        try:
            if eod_key:
                r = http_get(f"https://eodhd.com/api/eod/{s}.{EODHD.EXCH[market]}",
                             params={"api_token": eod_key, "fmt": "json", "from": start.isoformat()})
                if r.status_code == 200:
                    for row in r.json():
                        adj = (row.get("adjusted_close") or row["close"]) / row["close"] if row.get("close") else 1
                        put(bars, s, row["date"], row["open"] * adj, row["high"] * adj, row["low"] * adj, row["close"] * adj, row.get("volume"))
            elif market == "US" and massive_key:
                if k:
                    time.sleep(12.5)  # free plan: 5 requests/minute
                from zoneinfo import ZoneInfo
                r = http_get(f"https://api.massive.com/v2/aggs/ticker/{s}/range/1/day/{start.isoformat()}/{dt.date.today().isoformat()}",
                             params={"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": massive_key})
                if r.status_code == 200:
                    for row in r.json().get("results") or []:
                        d = dt.datetime.fromtimestamp(row["t"] / 1000, ZoneInfo("America/New_York")).date().isoformat()
                        put(bars, s, d, row.get("o"), row.get("h"), row.get("l"), row.get("c"), row.get("v"))
        except Exception as e:  # noqa: BLE001
            log.warning("history for %s failed: %s", s, e)
    left = [s for s in syms if s not in bars]
    if left:
        try:
            b, _ = Yahoo().fetch(market, left, start)
            bars.update(b)
        except Exception as e:  # noqa: BLE001
            log.warning("Yahoo history for %s failed: %s", ", ".join(left), e)
    for s in syms:
        log.info("[%s] my stock %s: %s", market, s, f"{len(bars[s])} sessions downloaded" if s in bars else "NOT FOUND")
    return bars


# --------------------------------------------------------------------------- state & output
def load_state(path: Path) -> dict:
    """Read a previously published feed file back into {sym: {date: [o,h,l,c,v]}} plus metadata."""
    if not path.exists():
        return {"bars": {}, "meta": {}, "noData": {}}
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
    return {"bars": bars, "meta": meta, "noData": j.get("noData", {})}


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
    keep = [s for _, s in ranked if s not in MY[market]][:UNIVERSE[market]]
    for s in INDEX_SYMS[market] + MY[market]:
        if s in bars and s not in keep:
            keep.append(s)
    return keep


def build_output(market: str, bars: Bars, meta: dict, sources: list[str], warnings: list[str]) -> dict:
    all_dates = sorted({d for by in bars.values() for d in by})
    # Pick the stocks first, then keep a session only if a meaningful share of THOSE stocks traded that day (drops stray
    # holiday rows). Counting all symbols instead wrongly dropped older days in India, where each new NSE file brings
    # ~2,500 symbols while the stored history holds only the chosen ~1,000.
    syms = select_universe(market, bars, all_dates[-HISTORY:])
    counts = {d: 0 for d in all_dates}
    for s in syms:
        for d in bars[s]:
            counts[d] += 1
    peak = max(counts.values()) if counts else 0
    dates = [d for d in all_dates if counts[d] >= peak * .3][-HISTORY:]
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
    prior = set(bars)
    no_data: dict[str, str] = st.get("noData", {})
    mine = MY[market]
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
                b, src, massive_ok = {}, [], False
                if massive_key:
                    try:
                        b, src = Massive(massive_key).fetch(days)
                        massive_ok = bool(b) or bool(bars)   # nothing new is fine once we have history
                    except Exception as e:  # noqa: BLE001
                        warnings.append(f"Massive: {e} Used Yahoo Finance instead.")
                        log.warning("[US] %s", warnings[-1])
                        b = {}
                else:
                    warnings.append("No MASSIVE_API_KEY secret found, so US prices came from Yahoo Finance.")
                    log.warning("[US] %s", warnings[-1])
                if not massive_ok:
                    universe, usrc = us_universe()
                    log.info("[US] stock list: %s", usrc)
                    if "unavailable" in usrc:
                        warnings.append(f"US stock list: {usrc}")
                    b = yahoo_update("US", universe, bars, mine, start, today, no_data)
                    src = [f"Yahoo Finance (yfinance) · {usrc}"]
            elif market == "IN":
                try:
                    b, src = NSE().fetch(days)
                except Exception as e:  # noqa: BLE001
                    warnings.append(f"NSE archive failed ({e}); used Yahoo Finance instead")
                    log.warning(warnings[-1])
                    syms = sorted(set(bars) | set(mine)) or nifty500()
                    b, src = Yahoo().fetch("IN", syms, start)
            else:  # CA
                universe, usrc = tsx_universe()
                log.info("[CA] stock list: %s", usrc)
                if "unavailable" in usrc:
                    warnings.append(f"Canada stock list: {usrc}")
                b = yahoo_update("CA", universe, bars, mine, start, today, no_data)
                src = [f"Yahoo Finance (yfinance) · {usrc}"]
            merge(b); sources += src
        except Exception as e:  # noqa: BLE001
            warnings.append(f"Price update failed: {e}")
            log.error("[%s] %s", market, e)
    for s, by in new.items():
        bars.setdefault(s, {}).update(by)
    # India: NSE's archive only keeps about a year of daily files (older days answer 404, which looks like a
    # holiday). Fill the older part of the history from Yahoo Finance, scaled to join NSE's prices seamlessly.
    if market == "IN" and bars and not MOCK:
        try:
            added = india_extend(bars, mine, today - dt.timedelta(days=int(HISTORY * 1.45) + 10))
            if added:
                sources.append(f"Yahoo Finance for sessions before {added}")
        except Exception as e:  # noqa: BLE001
            warnings.append(f"Couldn’t extend India history before NSE’s archive: {e}")
            log.warning("[IN] %s", warnings[-1])
    # My stocks: download full history for any newly added ticker
    need = [s for s in mine if s not in prior and len(bars.get(s, {})) < 60]
    if need:
        log.info("[%s] downloading history for newly added stock(s): %s", market, ", ".join(need))
        hist_start = today - dt.timedelta(days=int(HISTORY * 1.45) + 10)
        for s, by in fetch_history(market, need, hist_start).items():
            bars.setdefault(s, {}).update(by)
    not_found = [s for s in mine if s not in bars]
    if not_found:
        warnings.append(f"My stocks not found: {', '.join(not_found)} — check the symbol")
    if not bars:
        raise RuntimeError(f"No {market} prices could be downloaded. " + " ".join(warnings[-3:]))

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
    if no_data:
        out["noData"] = no_data
    (out_dir / f"{market}.json").write_text(json.dumps(out, separators=(",", ":")))
    log.info("[%s] wrote %d tickers × %d sessions, as of %s", market, len(out["tickers"]), len(out["dates"]), out["asOf"])
    return {"asOf": out["asOf"], "generated": out["generated"], "tickers": len(out["tickers"]), "sessions": len(out["dates"]),
            "source": out["source"], "warnings": warnings, "mine": [s for s in mine if s in bars], "notFound": not_found}


def yahoo_update(market: str, universe: list[str], bars: Bars, mine: list[str], start: dt.date, today: dt.date,
                 no_data: dict) -> Bars:
    """Yahoo prices for a stock list: new sessions for stocks we already have, full history for new ones."""
    known = sorted(set(bars) | set(mine))
    b, _ = Yahoo().fetch(market, known, start) if known else ({}, [])
    hist_start = today - dt.timedelta(days=int(HISTORY * 1.45) + 10)
    retry_after = (today - dt.timedelta(days=30)).isoformat()
    fresh = [x for x in universe if x not in bars and x not in mine and no_data.get(x, "") < retry_after]
    if fresh:
        log.info("[%s] downloading history for %d stock(s) new to the list", market, len(fresh))
        b2, _ = Yahoo().fetch(market, fresh, hist_start)
        for x, by in b2.items():
            b.setdefault(x, {}).update(by)
        for x in fresh:
            if x not in b2:
                no_data[x] = today.isoformat()   # no prices on Yahoo; try again in 30 days
    return b


US_SEED = ("AAPL MSFT NVDA AMZN GOOGL META AVGO TSLA BRK.B JPM LLY V UNH XOM MA COST HD PG JNJ WMT NFLX ABBV BAC CRM ORCL KO CVX MRK "
           "AMD PEP ADBE TMO LIN ACN MCD CSCO WFC ABT IBM GE DHR PM INTU QCOM TXN CAT AMGN VZ NOW ISRG GS SPGI PFE UBER AMAT "
           "BKNG NEE RTX T LOW HON UNP BLK ELV SYK PLD MS PGR SCHW TJX C BSX VRTX LMT ADP MDT REGN CB MMC DE PANW ADI SBUX "
           "MU KLAC LRCX ANET GILD BMY SO CI FI ICE DUK SHW MO ZTS CME SPY").split()


def us_universe() -> tuple[list[str], str]:
    """S&P 500 + S&P MidCap 400 + S&P SmallCap 600 (about 1,500 US stocks) from Wikipedia; seed list if unavailable."""
    import pandas as pd
    found: set[str] = set()
    for page in ("List_of_S%26P_500_companies", "List_of_S%26P_400_companies", "List_of_S%26P_600_companies"):
        try:
            html = http_get(f"https://en.wikipedia.org/wiki/{page}", headers={"User-Agent": UA}).text
            for t in pd.read_html(io.StringIO(html)):
                col = next((c for c in t.columns if str(c).lower() in ("symbol", "ticker", "ticker symbol")), None)
                if col is not None and len(t) > 300:
                    found |= {str(x).strip().upper() for x in t[col] if str(x).strip()}
                    break
        except Exception as e:  # noqa: BLE001
            log.warning("US stock list %s failed (%s)", page, e)
    if len(found) > 400:
        return sorted(found | {"SPY"}), f"S&P 500/400/600, {len(found)} stocks"
    return sorted(set(US_SEED) | found), "built-in list of large US stocks — S&P lists unavailable today"


def clean_tsx(sym) -> str | None:
    """TSX symbol -> our form (class shares and trust units kept, e.g. BAM.A -> BAM-A, REI.UN -> REI-UN).
    Preferred shares, debentures, warrants, notes, rights and US-dollar lines are dropped."""
    import re
    sym = str(sym or "").strip().upper().replace(".TO", "")
    if not sym or not re.fullmatch(r"[A-Z0-9]{1,6}([.\-][A-Z]{1,2})?", sym):
        return None
    parts = re.split(r"[.\-]", sym)
    if len(parts) == 2 and not (parts[1] == "UN" or (len(parts[1]) == 1 and parts[1] not in ("U", "R"))):
        return None
    return "-".join(parts)


def tsx_universe() -> tuple[list[str], str]:
    """Every TSX-listed stock. Tries, in order: TMX's official listed-company directory, EODHD's symbol
    list (if EODHD_API_KEY is set), the S&P/TSX Composite list on Wikipedia, then a built-in seed list."""
    found: set[str] = set()
    try:  # 1) TMX directory (all TSX issuers and their share classes)
        r = http_get("https://www.tsx.com/json/company-directory/search/tsx/%5E*",
                     headers={"User-Agent": UA, "Accept": "application/json", "Referer": "https://www.tsx.com/"}, retries=2)
        for co in (r.json().get("results") or []):
            for ins in (co.get("instruments") or [co]):
                c = clean_tsx(ins.get("symbol"))
                if c:
                    found.add(c)
    except Exception as e:  # noqa: BLE001
        log.warning("TMX company directory failed (%s)", e)
    if len(found) > 400:
        return sorted(found | {"XIU"}), f"TMX directory, {len(found)} TSX symbols"
    key = os.getenv("EODHD_API_KEY")
    if key:  # 2) EODHD symbol list (1 API call; works on its free plan too)
        try:
            r = http_get("https://eodhd.com/api/exchange-symbol-list/TO", params={"api_token": key, "fmt": "json"})
            for row in r.json():
                if row.get("Type") in ("Common Stock", "ETF", "FUND"):
                    c = clean_tsx(row.get("Code"))
                    if c:
                        found.add(c)
        except Exception as e:  # noqa: BLE001
            log.warning("EODHD symbol list failed (%s)", e)
        if len(found) > 400:
            return sorted(found | {"XIU"}), f"EODHD symbol list, {len(found)} TSX symbols"
    try:  # 3) S&P/TSX Composite (~220 largest companies)
        import pandas as pd
        html = http_get("https://en.wikipedia.org/wiki/S%26P/TSX_Composite_Index", headers={"User-Agent": UA}).text
        for t in pd.read_html(io.StringIO(html)):
            col = next((c for c in t.columns if str(c).lower() in ("symbol", "ticker")), None)
            if col is not None and len(t) > 150:
                found |= {c for c in (clean_tsx(x) for x in t[col]) if c}
                return sorted(found | {"XIU"}), f"S&P/TSX Composite only ({len(found)} symbols) — full TSX list unavailable today"
    except Exception as e:  # noqa: BLE001
        log.warning("TSX Composite list failed (%s)", e)
    return sorted(set(CA_SEED) | found), "built-in list of large Canadian stocks — full TSX list unavailable today"


def india_extend(bars: Bars, mine: list[str], want_start: dt.date) -> str | None:
    """NSE's archive keeps only about a year of daily files, so fill each stock's older history from Yahoo Finance.
    Works per stock: a stock whose own history starts late gets Yahoo bars for the missing earlier days, multiplied by
    NSE close / Yahoo close on the first day both have, so the two sources join without a jump."""
    dates = sorted({d for by in bars.values() for d in by})
    syms = select_universe("IN", bars, dates[-HISTORY:])
    syms = [s for s in dict.fromkeys(syms + INDEX_SYMS["IN"] + list(mine)) if s in bars]
    def run_start(by):   # first day of the unbroken recent run of data (a stray old bar must not hide a long gap)
        ds = sorted(by)
        for k in range(len(ds) - 1, 0, -1):
            if (dt.date.fromisoformat(ds[k]) - dt.date.fromisoformat(ds[k - 1])).days > 30:
                return ds[k]
        return ds[0]
    limit = (want_start + dt.timedelta(days=21)).isoformat()
    firsts = {s: run_start(bars[s]) for s in syms}
    need = [s for s in syms if firsts[s] > limit]
    if not need:
        return None
    firsts = {s: firsts[s] for s in need}
    log.info("[IN] %d stocks have history starting after %s; filling from %s with Yahoo", len(need), limit, want_start)
    import yfinance as yf
    end = (dt.date.fromisoformat(max(firsts.values())) + dt.timedelta(days=10)).isoformat()
    filled, earliest = 0, None
    for i in range(0, len(need), 100):
        chunk = need[i:i + 100]
        ys = [Yahoo.ysym("IN", x) for x in chunk]
        df = yf.download(ys, start=want_start.isoformat(), end=end, auto_adjust=False, group_by="ticker", threads=True, progress=False)
        if df is None or df.empty:
            continue
        multi = hasattr(df.columns, "levels")
        for s, y in zip(chunk, ys):
            try:
                sub = df[y] if multi and y in df.columns.get_level_values(0) else (None if multi else df)
            except KeyError:
                continue
            if sub is None:
                continue
            rows = {idx.date().isoformat(): row for idx, row in sub.dropna(how="all").iterrows()}
            by, first = bars[s], firsts[s]
            shared = sorted(d for d in rows if d in by and d >= first)
            if not shared:
                continue
            yc = rows[shared[0]].get("Close")
            if not yc or yc != yc or yc <= 0:
                continue
            k = by[shared[0]][3] / float(yc)
            for d, row in rows.items():
                if d < first and d not in by:
                    o, h, l, c, v = (row.get(x) for x in ("Open", "High", "Low", "Close", "Volume"))
                    if c is None or c != c:
                        continue
                    put(bars, s, d, (o or c) * k, (h or c) * k, (l or c) * k, c * k, (v or 0) / k if k else v)
                    filled += 1
                    earliest = d if earliest is None or d < earliest else earliest
        time.sleep(2)
    log.info("[IN] added %d older daily bars from Yahoo for %d stocks", filled, len(need))
    return max(firsts.values()) if filled else None


def nifty500() -> list[str]:
    try:
        r = http_get("https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv", headers={"User-Agent": UA})
        import csv
        return [row["Symbol"] for row in csv.DictReader(io.StringIO(r.text))] + ["NIFTYBEES"]
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"no Indian universe available ({e})") from e


# --------------------------------------------------------------------------- market indices (Markets page)
INDICES = {
    "US": [("^GSPC", "S&P 500"), ("^IXIC", "Nasdaq Composite"), ("^DJI", "Dow Jones Industrial Average")],
    "CA": [("^GSPTSE", "S&P/TSX Composite"), ("^TX60", "S&P/TSX 60")],
    "IN": [("^NSEI", "Nifty 50"), ("^BSESN", "BSE Sensex"), ("^NSEBANK", "Nifty Bank")],
}


def update_indices(markets: list[str], state: Path, out: Path) -> None:
    """indices.json: daily OHLC of each market's main indices (real index points, from Yahoo Finance). Only the markets in
    this run are refreshed; the others are kept from the last published file."""
    old = {}
    if (state / "indices.json").exists():
        try:
            old = json.loads((state / "indices.json").read_text()).get("indices", {})
        except Exception:  # noqa: BLE001
            old = {}
    idx = dict(old)
    start = dt.date.today() - dt.timedelta(days=int(HISTORY * 1.45) + 10)
    for m in markets:
        for sym, name in INDICES.get(m, []):
            try:
                if MOCK:
                    r = random.Random(sym); px = {"US": 5000, "CA": 25000, "IN": 24000}[m]; rows = []
                    for d in weekdays_between(start, dt.date.today()):
                        px *= math.exp(r.gauss(.0004, .01)); hi, lo = px * (1 + abs(r.gauss(0, .005))), px * (1 - abs(r.gauss(0, .005)))
                        rows.append((d.isoformat(), px * (1 + r.gauss(0, .003)), hi, lo, px, 1e9))
                else:
                    import yfinance as yf
                    df = yf.download(sym, start=start.isoformat(), auto_adjust=False, progress=False)
                    if df is None or df.empty:
                        raise RuntimeError("no data returned")
                    if hasattr(df.columns, "levels"):   # newer yfinance returns (field, ticker) columns
                        df.columns = df.columns.get_level_values(0)
                    rows = [(i.date().isoformat(), r_.get("Open"), r_.get("High"), r_.get("Low"), r_.get("Close"), r_.get("Volume") or 0)
                            for i, r_ in df.dropna(subset=["Close"]).iterrows()]
                rows = [x for x in rows if all(v is not None and v == v for v in x[1:5])][-HISTORY:]
                if len(rows) < 30:
                    raise RuntimeError(f"only {len(rows)} sessions")
                idx[sym] = {"s": sym, "n": name, "m": m, "asOf": rows[-1][0], "dates": [x[0] for x in rows],
                            "o": [rnd(x[1]) for x in rows], "h": [rnd(x[2]) for x in rows], "l": [rnd(x[3]) for x in rows],
                            "c": [rnd(x[4]) for x in rows], "v": [int(x[5] or 0) for x in rows]}
                log.info("[%s] index %s: %d sessions to %s", m, sym, len(rows), rows[-1][0])
            except Exception as e:  # noqa: BLE001 — an index failing must never stop the stock data
                log.warning("[%s] index %s skipped: %s", m, sym, e)
            if not MOCK:
                time.sleep(1)
    (out / "indices.json").write_text(json.dumps({"v": 1, "generated": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "indices": idx}, separators=(",", ":")))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--markets", default="US,CA,IN")
    ap.add_argument("--state", default="state", help="folder holding the previously published feed files")
    ap.add_argument("--out", default="out")
    ap.add_argument("--backfill", action="store_true", help="re-download the full history")
    a = ap.parse_args()
    MY.update(load_my_tickers())
    state, out = Path(a.state), Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    meta_path = state / "meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {"markets": {}}
    failed = []
    for m in [x.strip().upper() for x in a.markets.split(",") if x.strip()]:
        try:
            meta["markets"][m] = run_market(m, state, out, a.backfill)
            meta.setdefault("errors", {}).pop(m, None)
        except Exception as e:  # noqa: BLE001
            log.error("%s", e)
            failed.append(m)
            meta.setdefault("errors", {})[m] = {"at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "message": str(e)}
            if (state / f"{m}.json").exists():  # keep publishing yesterday's file
                (out / f"{m}.json").write_bytes((state / f"{m}.json").read_bytes())
    try:
        update_indices([x.strip().upper() for x in a.markets.split(",") if x.strip()], state, out)
    except Exception as e:  # noqa: BLE001
        log.warning("Indices not updated: %s", e)
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
