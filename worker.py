#!/usr/bin/env python3
"""
worker.py — Standalone background scanner for PolySignal.
Runs independently of the Flask web server on Railway as a worker service.
This process handles all scanning, alerting, and data collection.
"""
from __future__ import annotations
import os, time, threading, sys
from datetime import datetime, timezone

# ── Environment ────────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    os.environ["DATABASE_URL"] = DATABASE_URL.replace("postgres://", "postgresql://", 1)

PORT              = int(os.environ.get("PORT", 5050))
KALSHI_INTERVAL   = 60
POLY_POS_INTERVAL = 300
POLY_LIVE_INTERVAL= 90
SNAPSHOT_INTERVAL = 300   # persist market snapshots every 5 min (scans still run every 60s)

# ── Imports ────────────────────────────────────────────────────────────────────
from database import (engine, db_save_signal, db_get_signals,
                      db_mark_alert_sent, db_get_alerted_keys,
                      db_analytics, db_cleanup, db_size_mb, Session, Trade,
                      MarketSnapshot, db_init_signal_price_history,
                      db_init_trader_entry)
import kalshi as kal
import polymarket as poly
from signals import (check_new_signals, check_cluster_alert, fetch_fred_events,
                     update_open_trade_prices, check_signal_outcomes,
                     send_morning_brief, seed_seen_signals, update_price_history)
from telegram_bot import (tg_send, poll_loop,
                           format_cmd_brief, format_cmd_signals,
                           format_cmd_kalshi, format_cmd_poly,
                           format_cmd_trades, format_cmd_stats,
                           format_cmd_next, format_cmd_help)

# ── Shared state ───────────────────────────────────────────────────────────────
_lock = threading.Lock()
_last_snapshot_write = 0.0  # snapshots persist on first scan, then every SNAPSHOT_INTERVAL

# Kalshi's own price memory, kept ACROSS scans (not reset each call). Was
# previously declared fresh inside run_kalshi_scan() every single scan,
# meaning detect_move() always received prev_price=None and could never
# actually register a move — Kalshi has effectively never been able to
# fire a real detect_move() signal since the worker/dashboard split.
_kalshi_prev_prices = {}
_st = {
    "config": {
        "min_move":          0.03,
        "min_depth":         1000.0,
        "poly_top":          100,
        "poly_min_traders":  3,
        "poly_min_total":    500.0,
        "poly_dominance":    0.65,
        "poly_min_momentum": 0.08,
        "poly_max_price":    0.35,   # was 0.80, then 0.45 — the 0.45 cap and the
                                     # edge analysis behind it were based on outcome
                                     # data later found to be ~53% mislabeled (a
                                     # resolution bug matched the wrong side of
                                     # two-outcome markets). Re-run after fixing that
                                     # bug and correcting historical data showed the
                                     # real edge is much smaller and concentrated
                                     # specifically under 35c (+7c/contract, n=391
                                     # distinct markets); 36-50c dropped to a thin
                                     # +2.4c, and everything above 50c turned out to
                                     # be efficiently priced, not overpriced as
                                     # first thought.
        "poly_min_value":    250.0,
        "poly_window_min":   30,
    },
    "kalshi_signals":    [],
    "poly_positions":    [],
    "poly_live":         [],
    "last_kalshi":       None,
    "last_poly_pos":     None,
    "last_poly_live":    None,
    "scanning_kalshi":   False,
    "scanning_poly_pos": False,
    "scanning_poly_live":False,
    "scan_count":        0,
    "error":             None,
}

def utcnow_s():
    return int(datetime.now(timezone.utc).timestamp())


# ── Telegram command handler ───────────────────────────────────────────────────
def handle_command(text: str, chat_id: str):
    """Dispatch Telegram bot commands."""
    cmd = text.strip().lower().split()[0]
    try:
        if cmd == "/brief":
            msg = format_cmd_brief(_st)
        elif cmd == "/signals":
            sigs = db_get_signals(limit=200)
            msg  = format_cmd_signals(sigs)
        elif cmd == "/kalshi":
            sigs = db_get_signals(limit=200)
            msg  = format_cmd_kalshi(sigs)
        elif cmd == "/poly":
            sigs = db_get_signals(limit=200)
            msg  = format_cmd_poly(sigs, _st)
        elif cmd == "/trades":
            a   = db_analytics()
            msg = format_cmd_trades(a)
        elif cmd == "/stats":
            sigs = db_get_signals(limit=500)
            a    = db_analytics()
            msg  = format_cmd_stats(sigs, a)
        elif cmd == "/next":
            events = fetch_fred_events()
            msg    = format_cmd_next(events)
        elif cmd in ("/help", "/start"):
            msg = format_cmd_help()
        else:
            msg = "Unknown command. Send /help for a list of available commands."
        tg_send(msg, chat_id=chat_id)
    except Exception as e:
        print(f"Command handler error ({cmd}): {e}")
        tg_send(f"Error handling {cmd}: {e}", chat_id=chat_id)


def tg_poll():
    poll_loop(_st, handle_command)


# ── Scanner functions ──────────────────────────────────────────────────────────
def run_kalshi_scan():
    with _lock:
        if _st["scanning_kalshi"]: return
        _st["scanning_kalshi"] = True
        cfg = dict(_st["config"])
    try:
        print("Kalshi scan starting…")
        markets     = kal.fetch_markets()
        print(f"  {len(markets)} markets")
        new_sigs    = []

        # Throttle snapshot persistence: scans run every 60s for signal
        # detection, but snapshots only need to be written every 5 min.
        # (60s snapshots x 200 markets x 7 days = ~2M rows = full volume.)
        global _last_snapshot_write
        now_ts        = time.time()
        write_snaps   = (now_ts - _last_snapshot_write) >= SNAPSHOT_INTERVAL
        snapshot_rows = []

        for i, m in enumerate(markets, 1):
            ticker = m.get("ticker","")
            if not ticker: continue
            ob = kal.fetch_orderbook(ticker)
            if not ob: continue
            cur = kal.best_yes_price(ob)
            sig = kal.detect_move(ticker, m, ob, _kalshi_prev_prices.get(ticker),
                                  kal.CAPTURE_MIN_MOVE, kal.CAPTURE_MIN_DEPTH)
            if sig:
                sig["db_id"] = db_save_signal(sig, "kalshi")
                # Seed price-after tracking so update_price_history() has
                # something to fill in at the 15m/1h/4h/24h/7d buckets.
                # (Previously only wired up in polysignal.py, which never runs
                # scans — so this table stayed essentially empty.)
                if sig["db_id"]:
                    try:
                        db_init_signal_price_history(
                            sig["db_id"], ticker, "kalshi",
                            datetime.utcnow(), sig["cur_price"]
                        )
                    except Exception as e:
                        print(f"  price history init failed ({ticker}): {e}")

                # Everything above clears the low capture floor and gets
                # saved either way, for later empirical threshold research.
                # Alerting and the dashboard's active-signals list stay
                # gated behind the real, stricter thresholds — nothing
                # changes there.
                meets_alert = (sig["move_abs"] >= cfg["min_move"]
                                and sig["depth"] >= cfg["min_depth"])
                if meets_alert:
                    new_sigs.append(sig)
                    cluster = kal.check_accumulator(ticker, m, sig,
                                                    cur or 0,
                                                    kal.orderbook_depth(ob))
                    if cluster:
                        check_cluster_alert(cluster)
            if cur is not None:
                _kalshi_prev_prices[ticker] = cur
            if write_snaps:
                snapshot_rows.append(MarketSnapshot(
                    platform="kalshi", ticker=ticker,
                    yes_price=cur or 0,
                    depth=kal.orderbook_depth(ob),
                ))
            if i % 50 == 0:
                print(f"  kalshi {i}/{len(markets)}")

        # One batched commit instead of one session per market
        if write_snaps and snapshot_rows:
            try:
                with Session(engine) as s:
                    s.add_all(snapshot_rows)
                    s.commit()
                _last_snapshot_write = now_ts
                print(f"  {len(snapshot_rows)} snapshots persisted.")
            except Exception as e:
                print(f"  snapshot write failed: {e}")
        check_new_signals(new_sigs, "kalshi")
        with _lock:
            _st["kalshi_signals"]  = new_sigs
            _st["last_kalshi"]     = utcnow_s()
            _st["scanning_kalshi"] = False
            _st["scan_count"]     += 1
        print(f"Kalshi done — {len(new_sigs)} signals.")
        update_open_trade_prices()
        check_signal_outcomes(cfg)
        try:
            from database import db_resolve_paper_trades
            n = db_resolve_paper_trades()
            if n:
                print(f"  {n} paper trades resolved.")
        except Exception as e:
            print(f"  Paper trade resolution error: {e}")
        db_cleanup()
        send_morning_brief(_st)
    except Exception as e:
        print(f"Kalshi scan error: {e}")
        with _lock: _st["scanning_kalshi"] = False


def run_poly_positions():
    with _lock:
        if _st["scanning_poly_pos"]: return
        _st["scanning_poly_pos"] = True
        cfg = dict(_st["config"])
    try:
        print("Poly positions scan…")
        traders = poly.fetch_leaderboard(cfg["poly_top"])
        rows    = poly.scan_positions(traders, cfg)
        for r in rows:
            r["sig_key"] = f"P:{r['conditionId']}:{r['outcome']}"
            r["db_id"]   = db_save_signal(r, "polymarket")
            if r["db_id"]:
                try:
                    db_init_signal_price_history(
                        r["db_id"], r["conditionId"], "polymarket",
                        datetime.utcnow(), r.get("curPrice", 0)
                    )
                except Exception as e:
                    print(f"  price history init failed ({r['conditionId']}): {e}")
            # Seed per-trader tracking too — was defined in database.py but
            # never actually called from anywhere, so trader_price_history
            # has been sitting empty. Uses market-level slug (r["slug"]),
            # which is reliable on both /positions and /trades, unlike
            # eventSlug which is often blank.
            for tr in r.get("topTraders", []):
                try:
                    db_init_trader_entry(
                        r["conditionId"], r["outcome"], r.get("title", ""),
                        tr["rank"], tr["username"], tr["avgPrice"],
                        r.get("slug", "")
                    )
                except Exception as e:
                    print(f"  trader entry init failed ({tr.get('username')}): {e}")
        check_new_signals(rows, "polymarket")
        with _lock:
            _st["poly_positions"]    = rows
            _st["last_poly_pos"]     = utcnow_s()
            _st["scanning_poly_pos"] = False
    except Exception as e:
        print(f"Poly positions error: {e}")
        with _lock: _st["scanning_poly_pos"] = False


def run_poly_live():
    with _lock:
        if _st["scanning_poly_live"]: return
        _st["scanning_poly_live"] = True
        cfg = dict(_st["config"])
    try:
        # Debug: confirm the actual runtime config, since we're seeing
        # signals above poly_max_price despite the filter fix being deployed.
        print(f"  [config check] poly_max_price={cfg.get('poly_max_price')}")
        traders = poly.fetch_leaderboard(cfg["poly_top"])
        rows    = poly.scan_live(traders, cfg)
        print(f"Poly live scan: {len(traders)} traders, {len(rows)} signals found")
        for r in rows:
            r["sig_key"] = f"P:{r['conditionId']}:{r['outcome']}:LIVE_BUY"
            r["db_id"]   = db_save_signal(r, "polymarket")
            if r["db_id"]:
                try:
                    db_init_signal_price_history(
                        r["db_id"], r["conditionId"], "polymarket",
                        datetime.utcnow(), r.get("curPrice", 0)
                    )
                except Exception as e:
                    print(f"  price history init failed ({r['conditionId']}): {e}")
            for tr in r.get("topTraders", []):
                try:
                    db_init_trader_entry(
                        r["conditionId"], r["outcome"], r.get("title", ""),
                        tr["rank"], tr["username"], tr["avgPrice"],
                        r.get("slug", "")
                    )
                except Exception as e:
                    print(f"  trader entry init failed ({tr.get('username')}): {e}")
        check_new_signals(rows, "polymarket")
        with _lock:
            _st["poly_live"]          = rows
            _st["last_poly_live"]     = utcnow_s()
            _st["scanning_poly_live"] = False
    except Exception as e:
        print(f"Poly live scan error: {e}")
        with _lock: _st["scanning_poly_live"] = False


def scheduler():
    print("Worker scheduler started.")
    now = time.time()
    nk  = now
    npp = now + 30
    npl = now + 60
    nph = now + 3600

    while True:
        now = time.time()
        if now >= nk:
            threading.Thread(target=run_kalshi_scan,    daemon=True).start()
            nk = now + KALSHI_INTERVAL
        if now >= npp:
            threading.Thread(target=run_poly_positions, daemon=True).start()
            npp = now + POLY_POS_INTERVAL
        if now >= npl:
            threading.Thread(target=run_poly_live,      daemon=True).start()
            npl = now + POLY_LIVE_INTERVAL
        if now >= nph:
            threading.Thread(target=update_price_history, daemon=True).start()
            nph = now + 3600
        time.sleep(5)


if __name__ == "__main__":
    print("PolySignal Worker starting…")
    seed_seen_signals()
    print("Startup complete.")

    threading.Thread(target=tg_poll, daemon=True).start()
    scheduler()  # blocks forever — no daemon, this IS the main process
