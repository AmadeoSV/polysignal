"""
kalshi.py — Kalshi API scanner and order flow detection.
"""
from __future__ import annotations
import os, time, threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

KALSHI_API  = "https://external-api.kalshi.com/trade-api/v2"
MAX_MARKETS = 300  # was 200 -- raised alongside reordering WATCHED_SERIES
                    # (sports moved to front) so the cap-starvation issue
                    # doesn't just shift onto whatever's now near the
                    # bottom of the list instead of sports.

WATCHED_SERIES = [
    # Sports first -- these resolve within hours/days, unlike most of the
    # macro series below which can take months. Previously listed last
    # out of 48 series; MAX_MARKETS=200 combined with how many bracket/
    # threshold sub-markets the macro series (esp. Nasdaq, BTC) produce
    # per event meant the fetch loop was very likely hitting the cap and
    # breaking before ever reaching a single sports series -- confirmed
    # zero KXMLBGAME signals ever, despite real, liquid MLB markets
    # existing (checked directly against Kalshi's live API).
    "KXNBAGAME","KXNBAFINALS","KXNHLGAME","KXNHLSTANCUP",
    "KXNFLGAME","KXMLBGAME","KXATPGAME","KXWTPGAME",
    "KXFEDDECISION","KXRATECUT","KXRATECUTS","KXRATEHIKE","KXFEDHIKE",
    "KXTERMINALRATE","KXFEDRATEMIN","KXCPI","KXCPIYOY","KXCPICORE",
    "KXCPICOREYOY","KXPCECORE","KXGDP","KXPAYROLLS","KXNFPDELAY",
    "KXNASDAQ100","KXNASDAQ100M","KXNASDAQ100W","KXBTC","KXBTCMAXY",
    "KXBTCMAXM","KXBTCMINY","KXBTCD","BTC","KXBTCATH",
    "KXWTI","KXWTIW","KXWTIMONTHLY","KXOIL","KXBRENTD",
    "KXEARNINGSMENTIONNVDA","KXEARNINGSMENTIONAAPL","KXEARNINGSMENTIONTSLA",
    "KXEARNINGSMENTIONMSFT","KXEARNINGSMENTIONMETA","KXEARNINGSMENTIONAMZN",
    "KXEARNINGSMENTIONJPM","KXEARNINGSMENTIONGOOGL",
    "KXFXEURO","KXJPY","KXGBP",
]

# Subset of WATCHED_SERIES that are live, in-progress sports games —
# same category of market that caused the Polymarket "days left /
# long-term play" bug (a live event can spike near resolution well
# before it's actually over, and days-left framing pulled from the
# market's admin close date doesn't reflect that). Used so Telegram
# alerts on these show a "LIVE" indicator instead, matching the fix
# already made on the Polymarket side.
LIVE_SPORTS_SERIES = (
    "KXNBAGAME", "KXNBAFINALS", "KXNHLGAME", "KXNHLSTANCUP",
    "KXNFLGAME", "KXMLBGAME", "KXATPGAME", "KXWTPGAME",
)

def is_live_sports_ticker(ticker: str) -> bool:
    return any(ticker.startswith(series) for series in LIVE_SPORTS_SERIES)


# Capture floor — the bar for saving a price move to the DB at all.
# Deliberately much lower than any alert threshold: the goal right now is
# broad data collection to empirically find where real signal lives, the
# same way the Polymarket price-bucket analysis did, rather than hand-
# guessing per-market thresholds up front. Alerting stays gated by the
# stricter, tunable cfg thresholds in worker.py — this floor only filters
# out true noise (a 0.1c tick, a market with no real book at all).
CAPTURE_MIN_MOVE  = 0.005   # 0.5c
CAPTURE_MIN_DEPTH = 100.0   # $100

# Accumulator — tracks moves per market in a rolling window
_accumulator: Dict[str, list] = defaultdict(list)
ACCUM_WINDOW    = 4 * 3600  # 4 hours
ACCUM_MIN_MOVES = 2
ACCUM_MIN_DEPTH = 2000


def get_json(url: str, params=None, retries=3, pause=0.7):
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params={k:v for k,v in (params or {}).items()
                                          if v is not None},
                             headers={"Accept":"application/json"}, timeout=20)
            if r.status_code == 429: time.sleep(pause*(attempt+2)); continue
            r.raise_for_status(); return r.json()
        except Exception as e:
            last = e; time.sleep(pause*(attempt+1))
    raise RuntimeError(f"GET {url} failed: {last}")


def fetch_orderbook(ticker: str) -> Optional[dict]:
    try:
        return get_json(f"{KALSHI_API}/markets/{ticker}/orderbook").get("orderbook_fp")
    except: return None


def fetch_market_status(ticker: str) -> Optional[dict]:
    """
    Returns {"status": ..., "result": "yes"/"no"/""} for a ticker, or None
    on failure. `result` is Kalshi's own authoritative settlement field —
    empty until the market actually resolves, then "yes" or "no". Used so
    resolution never has to infer a market's outcome from a price
    threshold, the same class of bug that caused the 53% mislabeling
    issue on the Polymarket side (a price spike near 95c/5c during a
    still-live event was mistaken for the market having closed).
    """
    try:
        m = get_json(f"{KALSHI_API}/markets/{ticker}").get("market", {})
        return {"status": m.get("status", ""), "result": m.get("result", "")}
    except:
        return None


def best_yes_price(ob: dict) -> Optional[float]:
    bids = ob.get("yes_dollars") or []
    if not bids: return None
    try: return float(bids[-1][0])
    except: return None


def orderbook_depth(ob: dict) -> float:
    total = 0.0
    for side in ["yes_dollars","no_dollars"]:
        for entry in (ob.get(side) or []):
            try: total += float(entry[0]) * float(entry[1])
            except: pass
    return total


def fetch_markets() -> List[dict]:
    all_m, seen = [], set()
    for series in WATCHED_SERIES:
        if len(all_m) >= MAX_MARKETS: break
        try:
            data = get_json(f"{KALSHI_API}/markets",
                            params={"limit":20,"status":"open","series_ticker":series})
            for m in data.get("markets",[]):
                t = m.get("ticker","")
                if t and t not in seen:
                    seen.add(t); all_m.append(m)
        except: pass
        time.sleep(0.1)
    return all_m[:MAX_MARKETS]


def ts_now() -> int: return int(time.time())

def ts_label(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC") if ts else "n/a"

def af(x, d=0.0):
    try: return float(x) if x not in (None,"") else d
    except: return d


def detect_move(ticker: str, market: dict, ob: dict,
                prev_price: Optional[float],
                min_move: float, min_depth: float) -> Optional[dict]:
    cur = best_yes_price(ob)
    if cur is None:
        cur = af(market.get("last_price") or market.get("yes_bid") or 0) / 100.0
    if cur == 0 or prev_price is None: return None
    move  = cur - prev_price
    if abs(move) < min_move: return None
    depth = orderbook_depth(ob)
    if depth < min_depth: return None
    event = market.get("event_ticker","")
    url   = (f"https://kalshi.com/events/{event}/{ticker}" if event
             else f"https://kalshi.com/events/{ticker}")
    now   = ts_now()
    return {
        "ticker":     ticker,
        "title":      market.get("title") or ticker,
        "category":   market.get("category",""),
        "direction":  "UP" if move > 0 else "DOWN",
        "prev_price": round(prev_price, 4),
        "cur_price":  round(cur, 4),
        "move":       round(move, 4),
        "move_abs":   round(abs(move), 4),
        "depth":      round(depth, 0),
        "end_date":   str(market.get("close_time") or "")[:10],
        "timestamp":  now,
        "ts_label":   ts_label(now),
        "sig_key":    f"K:{ticker}:{'UP' if move>0 else 'DOWN'}:{round(prev_price,2)}:{round(cur,2)}",
        "url":        url,
    }


def check_accumulator(ticker: str, market: dict,
                      sig: dict, cur_price: float,
                      depth: float) -> Optional[dict]:
    """
    Track repeated moves on the same market.
    Returns a cluster alert dict if threshold met, else None.
    """
    now = ts_now()
    _accumulator[ticker].append((now, sig["direction"], sig["move_abs"], depth))
    # Prune old entries
    _accumulator[ticker] = [
        e for e in _accumulator[ticker] if now - e[0] <= ACCUM_WINDOW
    ]
    entries = _accumulator[ticker]
    for direction in ("UP","DOWN"):
        same = [e for e in entries if e[1]==direction]
        if len(same) >= ACCUM_MIN_MOVES:
            combined = sum(e[3] for e in same)
            if combined >= ACCUM_MIN_DEPTH:
                span_min = round((max(e[0] for e in same) - min(e[0] for e in same)) / 60)
                event    = market.get("event_ticker","")
                url      = (f"https://kalshi.com/events/{event}/{ticker}" if event
                            else f"https://kalshi.com/events/{ticker}")
                return {
                    "ticker":    ticker,
                    "title":     market.get("title") or ticker,
                    "direction": direction,
                    "count":     len(same),
                    "combined":  round(combined, 0),
                    "span_min":  span_min,
                    "cur_price": round(cur_price, 4),
                    "end_date":  str(market.get("close_time") or "")[:10],
                    "url":       url,
                    "cluster_key": f"CLUSTER:{ticker}:{direction}:{len(same)}",
                }
    return None