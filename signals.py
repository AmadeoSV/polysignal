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

POLY_API       = "https://data-api.polymarket.com"
POLY_GAMMA_API = "https://gamma-api.polymarket.com"
FRED_KEY       = os.environ.get("FRED_API_KEY", "")

FRED_RELEASES = {
    10: ("CPI",               "high"),
    19: ("PPI",               "med"),
    50: ("GDP",               "high"),
    51: ("Jobs Report (NFP)", "high"),
    21: ("PCE",               "high"),
    53: ("Retail Sales",      "med"),
}

FED_DATES = [
    {"date": "2026-06-17", "time": "2:00pm ET", "label": "Fed Decision",           "importance": "high"},
    {"date": "2026-07-29", "time": "2:00pm ET", "label": "Fed Decision",           "importance": "high"},
    {"date": "2026-09-16", "time": "2:00pm ET", "label": "Fed Decision + SEP",     "importance": "high"},
    {"date": "2026-10-28", "time": "2:00pm ET", "label": "Fed Decision",           "importance": "high"},
    {"date": "2026-12-09", "time": "2:00pm ET", "label": "Fed Decision + SEP",     "importance": "high"},
]

_fred_cache: List[dict] = []
_fred_ts: float = 0.0
_seen_signals: Set[str] = set()
_seen_lock = threading.Lock()


def _parse_outcome_price(prices) -> float:
    """
    Safely parse outcomePrices from Gamma API.
    Gamma returns strings like '0.95' or nested lists.
    Always returns a float in 0-1 range.
    """
    raw = prices[0]
    if isinstance(raw, list):
        raw = raw[0]
    val = str(raw).replace("[", "").replace("]", "").replace('"', "").strip()
    return float(val)


def seed_seen_signals():
    from database import db_get_alerted_keys
    keys = db_get_alerted_keys()
    with _seen_lock:
        _seen_signals.update(keys)
    print(f"Seeded {len(keys)} alerted signal keys from DB.")


def get_seen_signals() -> Set[str]:
    return _seen_signals


def check_new_signals(rows: List[dict], platform: str):
    if not rows:
        return
    try:
        from database import db_mark_alert_sent, db_get_alerted_keys
        already_alerted = db_get_alerted_keys()
    except Exception as e:
        print(f"check_new_signals: failed to load alerted keys: {e}")
        already_alerted = set()

    for r in rows:
        key = r.get("sig_key", "")
        if not key or key in already_alerted:
            continue
        url = r.get("url") or r.get("market_url", "")
        try:
            if platform == "kalshi":
                msg     = format_kalshi_alert(r)
                buttons = [{"text": "View on Kalshi", "url": url}] if url else []
            else:
                msg     = format_poly_alert(r)
                buttons = [{"text": "View on Polymarket", "url": url}] if url else []
            tg_send(msg, buttons=buttons or None)
            db_mark_alert_sent(key)
            with _seen_lock:
                _seen_signals.add(key)
        except Exception as e:
            print(f"  Alert failed for {key[:50]}: {e}")


def check_cluster_alert(cluster: dict):
    key = cluster.get("cluster_key", "")
    with _seen_lock:
        if key in _seen_signals:
            return
        _seen_signals.add(key)
    msg = format_cluster_alert(cluster)
    url = cluster.get("url", "")
    tg_send(msg, buttons=[{"text": "View on Kalshi", "url": url}] if url else None)


def fetch_fred_events() -> List[dict]:
    global _fred_cache, _fred_ts
    if not FRED_KEY:
        return []
    if time.time() - _fred_ts < 21600 and _fred_cache:
        return _fred_cache

    today  = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    end    = (datetime.now(tz=timezone.utc) + timedelta(days=120)).strftime("%Y-%m-%d")
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
                for d in r.json().get("release_dates", []):
                    if d.get("date", "") >= today:
                        events.append({"date": d["date"], "time": "8:30am ET",
                                       "label": label, "importance": imp})
            time.sleep(0.2)
        except Exception:
            pass

    for m in FED_DATES:
        if m["date"] >= today:
            events.append(m)

    events.sort(key=lambda e: e["date"])
    _fred_cache = events[:20]
    _fred_ts    = time.time()
    return _fred_cache


def send_morning_brief(state_ref: dict):
    from database import db_analytics, db_get_signals
    now_utc = datetime.now(timezone.utc)
    if now_utc.hour not in (12, 13):
        return
    if now_utc.minute > 10:
        return

    today     = now_utc.strftime("%Y-%m-%d")
    flag_file = f"/tmp/morning_brief_{today}.sent"
    if os.path.exists(flag_file):
        return
    try:
        open(flag_file, "w").close()
    except Exception as e:
        print(f"Morning brief flag write failed: {e}")

    try:
        a      = db_analytics()
        sigs   = db_get_signals(limit=200)
        active = [s for s in sigs if s.get("outcome") is None]
        k_sigs = [s for s in active if s["platform"] == "kalshi"]
        p_sigs = [s for s in active if s["platform"] == "polymarket"]

        top_poly = state_ref.get("poly_positions", [])[:4]
        top_k    = state_ref.get("kalshi_signals", [])[:3]

        lines = [
            "\u2600\ufe0f <b>PolySignal Morning Brief</b>",
            "\u2501" * 20,
            f"Signals active: <b>{len(active)}</b> ({len(k_sigs)} Kalshi, {len(p_sigs)} Polymarket)",
            f"Open trades: <b>{a['open_trades']}</b> | PnL: <b>${a['total_pnl']:+.2f}</b>",
            "",
        ]
        if top_poly:
            lines.append("<b>\U0001f4ca Top Polymarket positions right now:</b>")
            for r in top_poly:
                dom  = round(r.get("dominance", 0) * 100)
                mom  = round(r.get("momentum", 0) * 100, 1)
                icon = "\U0001f7e2" if dom >= 80 else "\U0001f7e1"
                lines.append(f"{icon} {r.get('title','')[:45]} | {r.get('traders',0)} traders, {dom}% | +{mom}\u00a2")
            lines.append("")
        if top_k:
            lines.append("<b>\u26a1 Recent Kalshi signals:</b>")
            for s in top_k:
                up   = s.get("direction") == "UP"
                icon = "\U0001f7e2" if up else "\U0001f534"
                move = round(s.get("move_abs", 0) * 100, 1)
                lines.append(f"{icon} {s.get('title','')[:45]} | {'+' if up else ''}{move}\u00a2")
            lines.append("")

        events = fetch_fred_events()
        if events:
            nxt = events[0]
            lines.append(f"\U0001f4c5 Next release: <b>{nxt['label']}</b> on {nxt['date']} at {nxt['time']}")

        lines.append("\nGood luck today \U0001f91d")
        tg_send("\n".join(lines))
        print(f"Morning brief sent for {today}.")

    except Exception as e:
        print(f"Morning brief error: {e}")
        try:
            os.remove(flag_file)
        except Exception:
            pass


def update_open_trade_prices():
    with Session(engine) as s:
        open_trades = s.query(Trade).filter(Trade.status == "OPEN").all()
        trade_data  = [(t.id, t.platform, t.ticker) for t in open_trades]

    for tid, platform, ticker in trade_data:
        try:
            if platform == "kalshi":
                ob = fetch_orderbook(ticker)
                if ob:
                    price = best_yes_price(ob)
                    if price:
                        db_update_trade_price(tid, price)
            else:
                resp = requests.get(f"{POLY_API}/positions",
                                    params={"user": ticker, "limit": 1}, timeout=8)
                if resp.status_code == 200:
                    data = resp.json()
                    if data and data[0].get("curPrice"):
                        db_update_trade_price(tid, float(data[0]["curPrice"]))
        except Exception:
            pass
        time.sleep(0.3)


def check_signal_outcomes():
    """
    For every unresolved signal check if the market has resolved.
    Uses Gamma API (gamma-api.polymarket.com) for Polymarket lookups
    since data-api endpoints are blocked from Railway IP range.
    """
    with Session(engine) as s:
        pending = s.query(Signal).filter(
            Signal.outcome == None,
            Signal.detected_at >= datetime.utcnow() - timedelta(days=60)
        ).all()
        pending_data = [
            (p.id, p.platform, p.ticker, p.market_url, p.signal_type, p.market_title)
            for p in pending
        ]

    if not pending_data:
        return

    print(f"Outcome check: {len(pending_data)} pending signals...")
    resolved = 0

    for sig_id, platform, ticker, market_url, sig_type, title in pending_data:
        try:
            cur_price = None

            if platform == "kalshi":
                ob = fetch_orderbook(ticker)
                if ob:
                    cur_price = best_yes_price(ob)

            else:
                if not market_url:
                    time.sleep(0.2)
                    continue

                slug = market_url.rstrip("/").split("/event/")[-1]
                if not slug or slug == market_url:
                    time.sleep(0.2)
                    continue

                try:
                    resp = requests.get(
                        f"{POLY_GAMMA_API}/events",
                        params={"slug": slug},
                        timeout=8
                    )
                    if resp.status_code != 200:
                        time.sleep(0.2)
                        continue
                    data = resp.json()
                    if data and isinstance(data, list) and len(data) > 0:
                        markets = data[0].get("markets", [])
                        if markets:
                            prices = markets[0].get("outcomePrices")
                            if prices:
                                cur_price = _parse_outcome_price(prices)
                except Exception as e:
                    print(f"  Poly outcome error for {title}: {e}")

            if cur_price is None:
                time.sleep(0.2)
                continue

            if cur_price >= 0.95:
                resolved_yes = True
            elif cur_price <= 0.05:
                resolved_yes = False
            else:
                time.sleep(0.2)
                continue

            bullish = sig_type in ("UP", "BUY", "OPEN_POSITION", "LIVE_BUY")
            outcome = "WON" if (bullish == resolved_yes) else "LOST"

            with Session(engine) as s:
                row = s.get(Signal, sig_id)
                if row:
                    row.outcome = outcome
                    s.commit()
                    resolved += 1
                    icon = "\u2705" if outcome == "WON" else "\u274c"
                    tg_send(
                        f"{icon} <b>Signal resolved: {outcome}</b>\n"
                        f"<b>{title or slug}</b>\n"
                        f"Direction: {sig_type} | Final: {round(cur_price * 100, 1)}\u00a2"
                    )
        except Exception as e:
            print(f"  Outcome error for {title}: {e}")
        time.sleep(0.3)

    if resolved:
        print(f"Outcome check done: {resolved} resolved.")


def update_price_history():
    from database import (db_get_pending_price_history, db_get_pending_trader_history,
                          db_update_price_bucket)
    now = datetime.utcnow()

    pending_sigs = db_get_pending_price_history()
    for row in pending_sigs:
        sig_time  = row["signal_time"]
        elapsed   = (now - sig_time).total_seconds()
        base      = row["price_at_signal"]
        direction = 1
        try:
            cur = None
            if row["platform"] == "kalshi":
                ob  = fetch_orderbook(row["ticker"])
                cur = best_yes_price(ob) if ob else None
            else:
                slug = (row.get("market_url") or "").rstrip("/").split("/event/")[-1]
                if slug:
                    resp = requests.get(f"{POLY_GAMMA_API}/events",
                                        params={"slug": slug}, timeout=8)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data and data[0].get("markets"):
                            prices = data[0]["markets"][0].get("outcomePrices")
                            if prices:
                                cur = _parse_outcome_price(prices)
            if cur is None:
                continue
            buckets = [("15m", 15*60), ("1h", 3600), ("4h", 4*3600),
                       ("24h", 24*3600), ("7d", 7*24*3600)]
            for bucket, seconds in buckets:
                if elapsed >= seconds and row.get(f"price_{bucket}") is None:
                    db_update_price_bucket("signal", row["id"], bucket, cur, base, direction)
        except Exception as e:
            print(f"Price history update error (signal {row['id']}): {e}")
        time.sleep(0.2)

    pending_traders = db_get_pending_trader_history()
    for row in pending_traders:
        entry_time = row["entry_time"]
        elapsed    = (now - entry_time).total_seconds()
        base       = row["entry_price"]
        try:
            cur  = None
            resp = requests.get(f"{POLY_GAMMA_API}/markets",
                                params={"clob_token_ids": row["condition_id"]}, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                if data and data[0].get("outcomePrices"):
                    cur = _parse_outcome_price(data[0]["outcomePrices"])
            if cur is None:
                continue
            buckets = [("15m", 15*60), ("1h", 3600), ("4h", 4*3600),
                       ("24h", 24*3600), ("7d", 7*24*3600)]
            for bucket, seconds in buckets:
                if elapsed >= seconds and row.get(f"price_{bucket}") is None:
                    db_update_price_bucket("trader", row["id"], bucket, cur, base, 1)
        except Exception as e:
            print(f"Price history update error (trader {row['id']}): {e}")
        time.sleep(0.2)

    if pending_sigs or pending_traders:
        print(f"Price history: updated {len(pending_sigs)} signals, {len(pending_traders)} trader entries.")
