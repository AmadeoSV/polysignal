"""
signals.py — Signal checking, outcome tracking, FRED calendar, and new signal alerts.
"""
from __future__ import annotations
import os, time, threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import requests

from database import Session, Signal, Trade, engine, db_update_trade_price
from kalshi import fetch_orderbook, best_yes_price
from telegram_bot import tg_send, format_kalshi_alert, format_cluster_alert, format_poly_alert

POLY_API = "https://data-api.polymarket.com"
FRED_KEY = os.environ.get("FRED_API_KEY","")

FRED_RELEASES = {
    10: ("CPI",              "high"),
    19: ("PPI",              "med"),
    50: ("GDP",              "high"),
    51: ("Jobs Report (NFP)","high"),
    21: ("PCE",              "high"),
    53: ("Retail Sales",     "med"),
}

FED_DATES = [
    {"date":"2026-06-17","time":"2:00pm ET","label":"Fed Decision","importance":"high"},
    {"date":"2026-07-29","time":"2:00pm ET","label":"Fed Decision","importance":"high"},
    {"date":"2026-09-16","time":"2:00pm ET","label":"Fed Decision + SEP","importance":"high"},
    {"date":"2026-10-28","time":"2:00pm ET","label":"Fed Decision","importance":"high"},
    {"date":"2026-12-09","time":"2:00pm ET","label":"Fed Decision + SEP","importance":"high"},
]

_fred_cache: List[dict] = []
_fred_ts: float = 0.0
_seen_signals: Set[str] = set()
_seen_lock = threading.Lock()


def get_seen_signals() -> Set[str]:
    return _seen_signals


def check_new_signals(rows: List[dict], platform: str):
    """Send Telegram alerts for signals we haven't seen before."""
    with _seen_lock:
        for r in rows:
            key = r.get("sig_key","")
            if not key or key in _seen_signals: continue
            _seen_signals.add(key)
            url = r.get("url") or r.get("market_url","")

            if platform == "kalshi":
                msg     = format_kalshi_alert(r)
                buttons = [{"text":"View on Kalshi","url":url}] if url else []
            else:
                msg     = format_poly_alert(r)
                buttons = [{"text":"View on Polymarket","url":url}] if url else []

            tg_send(msg, buttons=buttons or None)


def check_cluster_alert(cluster: dict):
    """Send Telegram alert for a repeated-order cluster."""
    key = cluster.get("cluster_key","")
    with _seen_lock:
        if key in _seen_signals: return
        _seen_signals.add(key)
    msg = format_cluster_alert(cluster)
    url = cluster.get("url","")
    tg_send(msg, buttons=[{"text":"View on Kalshi","url":url}] if url else None)


def fetch_fred_events() -> List[dict]:
    """Fetch upcoming economic release dates from FRED. Cached 6 hours."""
    global _fred_cache, _fred_ts
    if not FRED_KEY: return []
    if time.time() - _fred_ts < 21600 and _fred_cache: return _fred_cache

    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    end   = (datetime.now(tz=timezone.utc)+timedelta(days=120)).strftime("%Y-%m-%d")
    events = []

    for rid, (label, imp) in FRED_RELEASES.items():
        try:
            r = requests.get("https://api.stlouisfed.org/fred/release/dates", params={
                "api_key": FRED_KEY, "release_id": rid, "file_type": "json",
                "realtime_start": today, "realtime_end": end,
                "sort_order": "asc", "limit": 4,
                "include_release_dates_with_no_data": "false",
            }, timeout=10)
            if r.status_code == 200:
                for d in r.json().get("release_dates",[]):
                    if d.get("date","") >= today:
                        events.append({"date":d["date"],"time":"8:30am ET",
                                       "label":label,"importance":imp})
            time.sleep(0.2)
        except: pass

    for m in FED_DATES:
        if m["date"] >= today: events.append(m)

    events.sort(key=lambda e: e["date"])
    _fred_cache = events[:20]
    _fred_ts    = time.time()
    return _fred_cache


def update_open_trade_prices():
    """Update current_price for all open trades."""
    with Session(engine) as s:
        open_trades = s.query(Trade).filter(Trade.status=="OPEN").all()
        trade_data  = [(t.id, t.platform, t.ticker) for t in open_trades]

    for tid, platform, ticker in trade_data:
        try:
            if platform == "kalshi":
                ob = fetch_orderbook(ticker)
                if ob:
                    price = best_yes_price(ob)
                    if price: db_update_trade_price(tid, price)
            else:
                data = requests.get(f"{POLY_API}/positions",
                    params={"user": ticker, "limit":1}, timeout=8).json()
                if data and data[0].get("curPrice"):
                    db_update_trade_price(tid, float(data[0]["curPrice"]))
        except: pass
        time.sleep(0.3)


def check_signal_outcomes():
    """
    For every unresolved signal check if the market has resolved.
    Updates outcome to WON or LOST automatically.
    """
    with Session(engine) as s:
        pending = s.query(Signal).filter(
            Signal.outcome == None,
            Signal.detected_at >= datetime.utcnow() - timedelta(days=60)
        ).all()
        pending_data = [(p.id, p.platform, p.ticker, p.signal_type, p.market_title)
                        for p in pending]

    if not pending_data:
        return

    print(f"Outcome check: {len(pending_data)} pending signals…")
    resolved = 0

    for sig_id, platform, ticker, sig_type, title in pending_data:
        try:
            cur_price = None
            if platform == "kalshi":
                ob = fetch_orderbook(ticker)
                if ob: cur_price = best_yes_price(ob)
            else:
                try:
                    data = requests.get(f"{POLY_API}/markets",
                        params={"clob_token_ids": ticker}, timeout=8).json()
                    if data and isinstance(data,list) and data[0].get("outcomePrices"):
                        cur_price = float(data[0]["outcomePrices"][0]) / 100
                except: pass

            if cur_price is None: time.sleep(0.2); continue

            if   cur_price >= 0.98: resolved_yes = True
            elif cur_price <= 0.02: resolved_yes = False
            else: time.sleep(0.2); continue

            # Only mark resolved if close date has passed
            with Session(engine) as s2:
                row2 = s2.get(Signal, sig_id)
                close_time = row2.market_close_time if row2 else ""
            if close_time:
                try:
                    from datetime import datetime
                    if datetime.strptime(close_time[:10],"%Y-%m-%d") > datetime.utcnow():
                        time.sleep(0.2); continue
                except: pass

            bullish = sig_type in ("UP","BUY","OPEN_POSITION","LIVE_BUY")
            outcome = "WON" if (bullish == resolved_yes) else "LOST"

            with Session(engine) as s:
                row = s.get(Signal, sig_id)
                if row:
                    row.outcome = outcome
                    s.commit()
                    resolved += 1
                    icon = "\u2705" if outcome=="WON" else "\u274c"
                    tg_send(
                        f"{icon} <b>Signal resolved: {outcome}</b>\n"
                        f"<b>{title or ticker}</b>\n"
                        f"Direction: {sig_type} | Final: {round(cur_price*100,1)}\u00a2"
                    )
        except Exception as e:
            print(f"  Outcome error for {ticker}: {e}")
        time.sleep(0.3)

    if resolved:
        print(f"Outcome check done: {resolved} resolved.")