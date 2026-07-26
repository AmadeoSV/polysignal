"""
signals.py — Signal checking, outcome tracking, FRED calendar, and new signal alerts.
"""
from __future__ import annotations
import json as _json
import os, time, threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import requests

from database import Session, Signal, Trade, engine, db_update_trade_price
from kalshi import fetch_orderbook, best_yes_price
from telegram_bot import tg_send, format_kalshi_alert, format_cluster_alert, format_poly_alert, format_resolution_msg

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
    {"date": "2026-06-17", "time": "2:00pm ET", "label": "Fed Decision",       "importance": "high"},
    {"date": "2026-07-29", "time": "2:00pm ET", "label": "Fed Decision",       "importance": "high"},
    {"date": "2026-09-16", "time": "2:00pm ET", "label": "Fed Decision + SEP", "importance": "high"},
    {"date": "2026-10-28", "time": "2:00pm ET", "label": "Fed Decision",       "importance": "high"},
    {"date": "2026-12-09", "time": "2:00pm ET", "label": "Fed Decision + SEP", "importance": "high"},
]

_fred_cache: List[dict] = []
_fred_ts: float = 0.0
_seen_signals: Set[str] = set()
_seen_lock = threading.Lock()


def _parse_outcome_price(raw_prices) -> Optional[float]:
    """
    Safely parse outcomePrices from Gamma API.
    Gamma returns outcomePrices as a stringified JSON list e.g. '[0.95, 0.05]'
    or sometimes already a list. Returns YES price as float 0-1, or None.
    """
    if not raw_prices:
        return None
    try:
        if isinstance(raw_prices, str):
            prices = _json.loads(raw_prices)
        else:
            prices = raw_prices
        if not prices:
            return None
        val = float(prices[0])
        return val
    except (ValueError, TypeError, _json.JSONDecodeError):
        return None


def _gamma_event_price(slug: str, market_title: str = "") -> Optional[float]:
    """
    Fetch current YES price for a Polymarket event using Gamma API.
    Uses /events/slug/{slug} per official Polymarket docs.

    For multi-market events (e.g. Iran peace deal with multiple date markets),
    matches the specific sub-market by question text similarity to market_title.
    Falls back to markets[0] only for single-market events.

    Returns float 0-1 or None.
    """
    try:
        resp = requests.get(
            f"{POLY_GAMMA_API}/events/slug/{slug}",
            timeout=8
        )
        if resp.status_code != 200:
            return None
        data = resp.json()

        # Handle both list and single object responses
        if isinstance(data, list):
            if not data:
                return None
            event = data[0]
        else:
            event = data

        markets = event.get("markets", [])
        if not markets:
            return None

        # If only one market, use it directly
        if len(markets) == 1:
            return _parse_outcome_price(markets[0].get("outcomePrices"))

        # Multi-market event — try to find the matching sub-market
        # by comparing question text to our signal's market_title
        if market_title:
            title_lower = market_title.lower()
            best_match = None
            best_score = 0

            for m in markets:
                question = (m.get("question") or "").lower()
                group_item = (m.get("groupItemTitle") or "").lower()
                end_date = (m.get("endDate") or "")[:10]  # YYYY-MM-DD

                # Score based on keyword overlap
                score = 0
                for word in title_lower.split():
                    if len(word) > 3 and word in question:
                        score += 1
                    if len(word) > 3 and word in group_item:
                        score += 2  # groupItemTitle is more specific

                # Bonus for end date match if title contains a date fragment
                if end_date and end_date in title_lower.replace(" ", "-"):
                    score += 5

                if score > best_score:
                    best_score = score
                    best_match = m

            if best_match and best_score > 0:
                return _parse_outcome_price(best_match.get("outcomePrices"))

        # Fallback: if signal is unresolved multi-market and we can't match,
        # return None rather than guessing wrong
        return None

    except Exception:
        return None


POLY_CLOB_API = "https://clob.polymarket.com"


def _norm_outcome(s: str) -> str:
    return (s or "").strip().lower()


def _clob_market_status(condition_id: str, outcome_name: str) -> Optional[dict]:
    """
    Primary resolution source. CLOB reliably indexes markets that Gamma's
    /markets search sometimes doesn't (older/settled sports markets in
    particular), and returns an explicit per-outcome "winner" flag —
    which is what actually fixes the mislabeling bug, not just closed
    status. See _gamma_market_closed_status for why matching the SPECIFIC
    named outcome (not just "some price >= 0.5") matters.
    """
    if not condition_id:
        return None
    try:
        resp = requests.get(f"{POLY_CLOB_API}/markets/{condition_id}", timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data:
            return None
        closed = bool(data.get("closed", False))
        tokens = data.get("tokens", []) or []
        winner = None
        for t in tokens:
            if _norm_outcome(t.get("outcome")) == _norm_outcome(outcome_name):
                winner = bool(t.get("winner", False))
                break
        return {"closed": closed, "winner": winner, "matched": winner is not None}
    except Exception:
        return None


def _gamma_market_closed_status(condition_id: str, outcome_name: str = "") -> Optional[dict]:
    """
    Fallback resolution source (used only if CLOB has no data).

    Originally this returned a single generic price and compared it
    against a flat 0.5/0.95 threshold — which is wrong for any market
    with more than a trivial binary "the only outcome that matters is
    index 0" shape. A signal betting on the SECOND outcome of a two-way
    market (e.g. "Under" instead of "Over", a specific named candidate)
    would get checked against the wrong side's price entirely, silently
    flipping WON and LOST. An audit of historical signals confirmed this
    was happening on ~53% of resolved Polymarket signals. Now matches
    by the specific outcome name, same as the CLOB path above.
    """
    if not condition_id:
        return None
    try:
        resp = requests.get(f"{POLY_GAMMA_API}/markets",
                            params={"condition_ids": condition_id}, timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data:
            return None
        m = data[0]
        closed = bool(m.get("closed", False))
        outcomes = m.get("outcomes")
        prices = m.get("outcomePrices")
        if isinstance(outcomes, str):
            import json as _json
            outcomes = _json.loads(outcomes)
        if isinstance(prices, str):
            import json as _json
            prices = _json.loads(prices)
        winner = None
        if outcomes and prices and len(outcomes) == len(prices):
            for name, price in zip(outcomes, prices):
                if _norm_outcome(name) == _norm_outcome(outcome_name):
                    winner = float(price) >= 0.5
                    break
        return {"closed": closed, "winner": winner, "matched": winner is not None}
    except Exception:
        return None


def _polymarket_resolution(condition_id: str, outcome_name: str) -> Optional[dict]:
    """Try CLOB first (more reliable coverage), fall back to Gamma."""
    status = _clob_market_status(condition_id, outcome_name)
    if status is None:
        status = _gamma_market_closed_status(condition_id, outcome_name)
    return status


def _gamma_market_price(slug: str) -> Optional[float]:
    """
    Fetch current YES price for a single Polymarket market using its
    market-level slug (not event slug) via /markets/slug/{slug}.

    Unlike _gamma_event_price, this needs no sub-market matching — the
    endpoint returns exactly one market object with outcomePrices
    directly on it. Market-level slug is reliably present on both
    /positions and /trades responses (eventSlug is often empty on
    trades), so this is the more robust lookup for trader-entry
    tracking specifically.

    Returns float 0-1 or None.
    """
    if not slug:
        return None
    try:
        resp = requests.get(f"{POLY_GAMMA_API}/markets/slug/{slug}", timeout=8)
        if resp.status_code != 200:
            return None
        return _parse_outcome_price(resp.json().get("outcomePrices"))
    except Exception:
        return None


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

        # Only PRIME (fresh, <2c moved) signals get sent to Telegram now.
        # After the outcome-audit fix and tightening to <=35c, re-running
        # the freshness split on corrected data showed STANDARD (2c+
        # moved) no longer has a real edge (-2.1c, essentially a coin
        # flip) — only PRIME does (+16.1c, confirmed on 417 distinct
        # markets). Still mark non-fresh signals as alerted so they're
        # not re-evaluated every scan, just don't notify on them.
        if platform != "kalshi":
            momentum = r.get("momentum")
            if momentum is None:
                momentum = r.get("curPrice", 0) - r.get("avgEntry", 0)
            is_fresh = abs(momentum) < 0.02
            if not is_fresh:
                try:
                    from database import db_mark_alert_sent
                    db_mark_alert_sent(key)
                except Exception:
                    pass
                with _seen_lock:
                    _seen_signals.add(key)
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

            # Every alert that actually goes out from here forward is a
            # PRIME signal (the gate above already filtered). Log it as
            # a hypothetical $5 position, no real money, so there's a
            # live forward track record building while any real capital
            # decision waits on its own timeline.
            if platform != "kalshi" and r.get("db_id"):
                try:
                    from database import db_log_paper_trade
                    db_log_paper_trade(
                        r["db_id"], key, r.get("title", ""),
                        r.get("curPrice", 0)
                    )
                except Exception as e:
                    print(f"  Paper trade log failed for {key[:50]}: {e}")
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
    from database import db_get_signals
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
        sigs   = db_get_signals(limit=200)
        active = [s for s in sigs if s.get("outcome") is None]
        k_sigs = [s for s in active if s["platform"] == "kalshi"]
        p_sigs = [s for s in active if s["platform"] == "polymarket"]

        from database import db_paper_trade_stats
        pt = db_paper_trade_stats()

        top_poly = state_ref.get("poly_positions", [])[:4]
        top_k    = state_ref.get("kalshi_signals", [])[:3]

        if pt.get("resolved"):
            pt_line = (f"PRIME paper trades: <b>{pt['win_rate']}%</b> "
                       f"({pt['won']}W/{pt['lost']}L) | "
                       f"PnL: <b>${pt['total_pnl']:+.2f}</b>"
                       + (f" | {pt['pending']} pending" if pt.get("pending") else ""))
        else:
            pending = pt.get("pending", 0)
            pt_line = (f"PRIME paper trades: accumulating\u2026"
                       + (f" ({pending} pending)" if pending else ""))

        lines = [
            "\u2600\ufe0f <b>PolySignal Morning Brief</b>",
            "\u2501" * 20,
            f"Signals active: <b>{len(active)}</b> ({len(k_sigs)} Kalshi, {len(p_sigs)} Polymarket)",
            pt_line,
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
    For every unresolved signal check if the market has ACTUALLY closed.

    Previously this used price >= 0.95 / <= 0.05 as a proxy for
    "resolved" — which is wrong for live, volatile markets. A close
    match can legitimately spike to 99c on one dramatic moment without
    the match being over, and once a signal's outcome is set it's never
    re-checked (Signal.outcome == None filter excludes it forever) — so
    a single price spike could permanently mislabel a still-live signal.

    Now: for Polymarket, pull condition_id from platform_signal_id and
    check the market's own "closed" flag via Gamma before trusting its
    price as final. Kalshi's path (orderbook-based) is separate and
    unaffected — that mechanism doesn't have this failure mode.
    """
    with Session(engine) as s:
        pending = s.query(Signal).filter(
            Signal.outcome == None,
            Signal.detected_at >= datetime.utcnow() - timedelta(days=60)
        ).all()
        pending_data = [
            (p.id, p.platform, p.ticker, p.market_url, p.signal_type,
             p.market_title, p.platform_signal_id)
            for p in pending
        ]

    if not pending_data:
        return

    print(f"Outcome check: {len(pending_data)} pending signals...")
    resolved = 0

    for sig_id, platform, ticker, market_url, sig_type, title, sig_key in pending_data:
        try:
            cur_price = None

            if platform == "kalshi":
                ob = fetch_orderbook(ticker)
                if ob:
                    cur_price = best_yes_price(ob)
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
            else:
                # sig_key format: "P:{conditionId}:{outcome}" or
                # "P:{conditionId}:{outcome}:LIVE_BUY"
                parts = (sig_key or "").split(":")
                condition_id = parts[1] if len(parts) >= 2 else ""
                outcome_name = parts[2] if len(parts) >= 3 else ""
                status = _polymarket_resolution(condition_id, outcome_name)
                if status is None or not status["closed"]:
                    # Not actually closed yet — genuinely still pending,
                    # regardless of what the live price happens to be
                    # doing right now.
                    time.sleep(0.2)
                    continue
                if not status["matched"]:
                    # Market's closed but we couldn't match the recorded
                    # outcome name against it (rare — naming/encoding
                    # mismatch). Leave pending rather than guess.
                    time.sleep(0.2)
                    continue
                # status["winner"] already tells us directly whether THIS
                # specific outcome won — no generic threshold, no
                # bullish/direction comparison needed for Polymarket.
                outcome = "WON" if status["winner"] else "LOST"
                with Session(engine) as s:
                    row = s.get(Signal, sig_id)
                    if row:
                        row.outcome = outcome
                        s.commit()
                        resolved += 1
                        # Only notify if this signal was actually alerted
                        # in the first place (PRIME, has a linked paper
                        # trade). Otherwise you'd get a "resolved" ping
                        # for a signal you were never told existed —
                        # confusing, and it happened before this fix.
                        from database import PaperTrade
                        with Session(engine) as s2:
                            was_alerted = s2.query(PaperTrade).filter_by(
                                signal_id=sig_id
                            ).first() is not None
                        if was_alerted:
                            tg_send(format_resolution_msg(
                                title=title or ticker or "market",
                                outcome=outcome,
                                sig_type=sig_type,
                                cur_price=(1.0 if status["winner"] else 0.0),
                            ))
                time.sleep(0.3)
                continue

            bullish = sig_type in ("UP", "BUY", "OPEN_POSITION", "LIVE_BUY")
            outcome = "WON" if (bullish == resolved_yes) else "LOST"

            with Session(engine) as s:
                row = s.get(Signal, sig_id)
                if row:
                    row.outcome = outcome
                    s.commit()
                    resolved += 1
                    tg_send(format_resolution_msg(
                        title=title or ticker or "market",
                        outcome=outcome,
                        sig_type=sig_type,
                        cur_price=cur_price,
                    ))
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
                    cur = _gamma_event_price(slug, row.get("market_title") or "")
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
            cur = _gamma_market_price(row.get("market_slug") or "")
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
