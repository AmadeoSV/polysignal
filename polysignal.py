#!/usr/bin/env python3
"""
PolySignal — Unified Kalshi + Polymarket scanner.
Entry point: imports all modules, runs schedulers and Flask.

pip install flask requests sqlalchemy psycopg2-binary gunicorn
TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy FRED_API_KEY=zzz DATABASE_URL=zzz python3 polysignal.py
"""
from __future__ import annotations
import os, sys, threading, time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, render_template_string, request as freq
from functools import wraps

# Local modules
from database import (engine, db_save_signal, db_get_signals, db_get_trades,
                      db_add_trade, db_close_trade, db_analytics, db_cleanup,
                      db_size_mb, db_mark_alert_sent, db_get_alerted_keys,
                      Session, Trade)
import kalshi as kal
import polymarket as poly
from signals import (check_new_signals, check_cluster_alert, fetch_fred_events,
                     update_open_trade_prices, check_signal_outcomes, send_morning_brief,
                     seed_seen_signals, update_price_history)

# Seed seen signals from DB on startup
seed_seen_signals()

print("Startup complete — scheduler and self-ping running.")
from telegram_bot import tg_send, tg_get_updates, poll_loop

# ── Config ─────────────────────────────────────────────────────────────────────
PORT              = int(os.environ.get("PORT", 5050))
KALSHI_INTERVAL   = 60
POLY_POS_INTERVAL = 300
POLY_LIVE_INTERVAL= 90

DEFAULT_CONFIG = {
    "kalshi_min_move":   0.03,
    "kalshi_min_depth":  1000.0,
    "poly_top":          100,
    "poly_min_traders":  3,
    "poly_min_value":    50.0,
    "poly_min_total":    500.0,
    "poly_dominance":    0.65,
    "poly_min_momentum": 0.08,
    "poly_max_price":    0.35,   # was 0.80 — this is a SEPARATE config default
                                 # from worker.py's (two independent processes,
                                 # each with their own in-memory _st). worker.py
                                 # was correctly tightened days ago; this one
                                 # was missed, meaning every manual Scan/Apply
                                 # click here ran with the old, loose 0.80 cap
                                 # the whole time, regardless of what worker.py
                                 # was doing in the background. See the note in
                                 # worker.py for the full history of this value.
    "poly_window_min":   30,
}

# ── Shared state ───────────────────────────────────────────────────────────────
_lock = threading.Lock()
_st: Dict[str,Any] = {
    "kalshi_signals":  [],
    "poly_positions":  [],
    "poly_live":       [],
    "kalshi_watched":  0,
    "poly_traders":    0,
    "last_kalshi":     None,
    "last_poly_pos":   None,
    "last_poly_live":  None,
    "scanning_kalshi": False,
    "scanning_poly_pos":  False,
    "scanning_poly_live": False,
    "error":      None,
    "scan_count": 0,
    "price_history": {},
    "config": dict(DEFAULT_CONFIG),
}

app = Flask(__name__)

# ── Basic Auth ────────────────────────────────────────────────────────────────
_AUTH_USER = os.environ.get("AUTH_USERNAME", "amadeo")
_AUTH_PASS = os.environ.get("AUTH_PASSWORD", "")

def check_auth(username, password):
    return username == _AUTH_USER and password == _AUTH_PASS and _AUTH_PASS != ""

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _AUTH_PASS:  # no password set — skip auth (local dev)
            return f(*args, **kwargs)
        auth = freq.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return (
                "Unauthorized",
                401,
                {"WWW-Authenticate": 'Basic realm="PolySignal"'}
            )
        return f(*args, **kwargs)
    return decorated

# ── Scan runners ───────────────────────────────────────────────────────────────

def utcnow_s() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def run_kalshi_scan():
    with _lock:
        if _st["scanning_kalshi"]: return
        _st["scanning_kalshi"] = True
        cfg  = dict(_st["config"])
        hist = dict(_st["price_history"])

    new_sigs, new_prices = [], {}
    try:
        print("Kalshi scan starting…")
        markets = kal.fetch_markets()
        print(f"  {len(markets)} markets")

        for idx, mkt in enumerate(markets, 1):
            ticker = mkt.get("ticker")
            if not ticker: continue

            ob = kal.fetch_orderbook(ticker)
            if not ob: time.sleep(0.1); continue

            cur   = kal.best_yes_price(ob)
            depth = kal.orderbook_depth(ob)
            if cur: new_prices[ticker] = cur

            # Save snapshot
            from database import MarketSnapshot
            with Session(engine) as s:
                s.add(MarketSnapshot(platform="kalshi", ticker=ticker,
                                     yes_price=cur or 0, depth=depth))
                s.commit()

            prev = hist.get(ticker)
            sig  = kal.detect_move(ticker, mkt, ob, prev,
                                   cfg["kalshi_min_move"], cfg["kalshi_min_depth"])
            if sig:
                new_sigs.append(sig)
                sig["db_id"] = db_save_signal(sig, "kalshi")
                # Start price-after tracking for this signal
                if sig["db_id"]:
                    from database import db_init_signal_price_history
                    from datetime import datetime as _dt
                    db_init_signal_price_history(
                        sig["db_id"], ticker, "kalshi",
                        _dt.utcnow(), sig["cur_price"]
                    )

                # Check accumulator for cluster
                cluster = kal.check_accumulator(ticker, mkt, sig, cur or 0, depth)
                if cluster:
                    from signals import get_seen_signals
                    if cluster["cluster_key"] not in get_seen_signals():
                        check_cluster_alert(cluster)

            if idx % 50 == 0:
                print(f"  kalshi {idx}/{len(markets)}")
            time.sleep(0.15)

        print(f"Kalshi done — {len(new_sigs)} signals.")
        threading.Thread(target=update_open_trade_prices, daemon=True).start()
        threading.Thread(target=db_cleanup, daemon=True).start()
        threading.Thread(target=check_signal_outcomes, daemon=True).start()
        # Morning brief — runs daily at 8am ET, no-op any other time
        threading.Thread(target=send_morning_brief, args=(_st,), daemon=True).start()

    except Exception as e:
        print(f"Kalshi error: {e}")
        with _lock:
            _st["scanning_kalshi"] = False
            _st["error"] = str(e)
        return

    with _lock:
        _st["price_history"].update(new_prices)
        all_s = new_sigs + _st["kalshi_signals"]
        seen, dedup = set(), []
        for s in all_s:
            if s["sig_key"] not in seen:
                seen.add(s["sig_key"]); dedup.append(s)
        _st["kalshi_signals"]   = dedup[:50]
        _st["kalshi_watched"]   = len(new_prices)
        _st["last_kalshi"]      = utcnow_s()
        _st["scanning_kalshi"]  = False
        _st["scan_count"]      += 1
        _st["error"]            = None

    check_new_signals(new_sigs, "kalshi")


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
            r["sig_key"] = f"P:{r['conditionId']}:{r['outcome']}:OPEN_POSITION"
            db_save_signal(r, "polymarket")
        check_new_signals(rows, "polymarket")
        with _lock:
            _st["poly_positions"]    = rows
            _st["poly_traders"]      = len(traders)
            _st["last_poly_pos"]     = utcnow_s()
            _st["scanning_poly_pos"] = False
    except Exception as e:
        print(f"Poly positions error: {e}")
        with _lock: _st["scanning_poly_pos"] = False; _st["error"] = str(e)


def run_poly_live():
    with _lock:
        if _st["scanning_poly_live"]: return
        _st["scanning_poly_live"] = True
        cfg = dict(_st["config"])
    try:
        traders = poly.fetch_leaderboard(cfg["poly_top"])
        rows    = poly.scan_live(traders, cfg)
        print(f"Poly live scan: {len(traders)} traders, {len(rows)} signals found")
        for r in rows:
            r["sig_key"] = f"P:{r['conditionId']}:{r['outcome']}:LIVE_BUY"
            db_save_signal(r, "polymarket")
        check_new_signals(rows, "polymarket")
        with _lock:
            _st["poly_live"]          = rows
            _st["last_poly_live"]     = utcnow_s()
            _st["scanning_poly_live"] = False
    except Exception as e:
        print(f"Poly live scan error: {e}")
        with _lock: _st["scanning_poly_live"] = False


def self_ping():
    """Ping our own health endpoint every 4 minutes to prevent Railway from sleeping."""
    # Try env var first, fall back to known URL
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN","")
    if not domain or "railway.internal" in domain:
        domain = "polysignal-production-0227.up.railway.app"
    url = f"https://{domain}" if not domain.startswith("http") else domain
    try:
        requests.get(f"{url}/api/health", timeout=10,
                     auth=(os.environ.get("AUTH_USERNAME","amadeo"),
                           os.environ.get("AUTH_PASSWORD","")))
        print(f"Self-ping OK: {url}")
    except Exception as e:
        print(f"Self-ping failed: {e}")


def scheduler():
    now    = time.time()
    nk     = now
    npp    = now + 30          # positions starts 30s after kalshi
    npl    = now + 60          # live buys starts 60s after kalshi
    nph    = now + 3600
    nping  = now + 60          # first ping after 60s
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
        if now >= nping:
            threading.Thread(target=self_ping, daemon=True).start()
            nping = now + 240
        time.sleep(5)


# ── Telegram command handler ───────────────────────────────────────────────────

def handle_cmd(text: str, chat_id: str):
    cmd = text.strip().lower().split()[0].lstrip("/").split("@")[0]
    a   = db_analytics()

    if cmd == "help":
        tg_send(
            "\U0001f916 <b>PolySignal commands</b>\n"
            "/status \u2014 overview\n/top \u2014 best signals\n"
            "/trades \u2014 open trades\n/scan \u2014 force scan\n/help",
            chat_id=chat_id
        )
    elif cmd == "status":
        tg_send(
            f"\u2705 <b>PolySignal status</b>\n"
            f"Kalshi: {_st['last_kalshi'] or 'pending'}\n"
            f"Poly: {_st['last_poly_pos'] or 'pending'}\n"
            f"Signals in DB: {a['total_signals']} | Open trades: {a['open_trades']}\n"
            f"Win rate: {a['win_rate']}% | PnL: ${a['total_pnl']:+.2f}\n"
            f"Signal accuracy: {a['sig_accuracy'] or 'pending'}"
            f" ({a['sig_won']}W/{a['sig_lost']}L)",
            chat_id=chat_id
        )
    elif cmd == "top":
        k = _st["kalshi_signals"][:3]
        p = _st["poly_positions"][:3]
        msg = "\U0001f4cb <b>Top signals</b>\n\n<b>Kalshi:</b>\n"
        for s in k:
            icon = "\U0001f7e2" if s["direction"]=="UP" else "\U0001f534"
            msg += f"{icon} {s['title'][:45]} {round(s['move_abs']*100,1)}\u00a2\n"
        msg += "\n<b>Polymarket:</b>\n"
        for r in p:
            msg += f"{'⭐'*r['strength']} {r['title'][:45]}\n"
        tg_send(msg, chat_id=chat_id)
    elif cmd == "trades":
        trades = db_get_trades(status="OPEN")
        if not trades: tg_send("No open trades.", chat_id=chat_id); return
        msg = f"\U0001f4ca <b>{len(trades)} open trades</b>\n"
        for t in trades[:5]:
            pnl = t.get("unrealized_pnl") or 0
            msg += (f"\n[{t['platform'].upper()}] {t['ticker']} {t['side']} "
                    f"@ {(t['entry_price'] or 0)*100:.0f}\u00a2 | "
                    f"{'+' if pnl>=0 else ''}{pnl:.2f} unrealized")
        tg_send(msg, chat_id=chat_id)
    elif cmd == "scan":
        tg_send("\U0001f504 Full scan started\u2026", chat_id=chat_id)
        threading.Thread(target=run_kalshi_scan,    daemon=True).start()
        threading.Thread(target=run_poly_positions, daemon=True).start()
    else:
        tg_send("Unknown command. Try /help", chat_id=chat_id)


# ── Flask routes ───────────────────────────────────────────────────────────────

@app.route("/")
@requires_auth
def index():
    return render_template_string(HTML)

@app.route("/api/recent_signals")
@requires_auth
def api_recent_signals():
    """
    Real, DB-backed recent activity — unlike /api/state's kalshi_signals/
    poly_positions (this process's own local in-memory scan results,
    which stay empty unless THIS specific process has run its own scan),
    this reflects what the actual worker process has detected, since
    both processes write to the same shared database.
    """
    from datetime import datetime, timedelta
    all_recent = db_get_signals(limit=500)
    cutoff = datetime.utcnow() - timedelta(hours=24)
    recent = [
        s for s in all_recent
        if s.get("detected_at") and
        datetime.strptime(s["detected_at"], "%Y-%m-%d %H:%M") > cutoff
    ]

    def dedupe_by_ticker(sigs):
        # recent is already ordered most-recent-first, so keeping the
        # first occurrence per ticker keeps the latest one. Otherwise a
        # single moving index can flood the list with near-duplicate
        # bracket-range variants of the same underlying market, burying
        # anything actually varied underneath repeats of one thing.
        seen, out = set(), []
        for s in sigs:
            key = s.get("ticker") or s.get("market_title")
            if key in seen: continue
            seen.add(key); out.append(s)
        return out

    kalshi_all = [s for s in recent if s["platform"] == "kalshi"]
    poly_all   = [s for s in recent if s["platform"] == "polymarket"]
    kalshi = dedupe_by_ticker(kalshi_all)[:50]
    poly   = dedupe_by_ticker(poly_all)[:50]
    return jsonify({"kalshi": kalshi, "polymarket": poly,
                     "kalshi_count": len(kalshi_all),
                     "poly_count": len(poly_all)})

@app.route("/api/state")
@requires_auth
def api_state():
    with _lock:
        base = {k: _st[k] for k in [
            "kalshi_signals","poly_positions","poly_live",
            "kalshi_watched","poly_traders","last_kalshi",
            "last_poly_pos","last_poly_live","scanning_kalshi",
            "scanning_poly_pos","scanning_poly_live","error",
            "scan_count","config"
        ]}
    base["events"]       = fetch_fred_events()
    base["fred_enabled"] = bool(os.environ.get("FRED_API_KEY",""))
    base["db_size_mb"]   = db_size_mb()
    return jsonify(base)

@app.route("/api/signals")
@requires_auth
def api_signals():
    return jsonify(db_get_signals(100, freq.args.get("platform")))

@app.route("/api/trades")
@requires_auth
def api_trades():
    return jsonify(db_get_trades(freq.args.get("status"), freq.args.get("platform")))

@app.route("/api/trades", methods=["POST"])
@requires_auth
def api_add_trade():
    return jsonify(db_add_trade(freq.get_json(force=True) or {}))

@app.route("/api/trades/<int:tid>/close", methods=["POST"])
@requires_auth
def api_close_trade(tid):
    data = freq.get_json(force=True) or {}
    return jsonify(db_close_trade(tid, float(data.get("exit_price",0)), data.get("notes","")))

@app.route("/api/analytics")
@requires_auth
def api_analytics():
    return jsonify(db_analytics())

@app.route("/api/paper_trades")
@requires_auth
def api_paper_trades():
    from database import db_paper_trade_stats, db_control_group_stats, db_fair_comparison_start
    # Was db_paper_trade_stats("PRIME") here -- PRIME was confirmed to
    # underperform even a plain, unfiltered baseline (30.5% vs. the
    # baseline's own 38.5%, both measured the same clean way), so there
    # was nothing left worth showing it for on the live dashboard.
    # Replaced with the actual baseline STANDARD needs to beat.
    #
    # Found 2026-09-05: STANDARD_15C's backfill reached back to
    # 2026-05-29 while STANDARD only starts 2026-08-04, so Baseline/
    # STANDARD/STANDARD_15C were being compared over three different,
    # unmatched date ranges dressed up as one comparison. since is now
    # computed fresh on every request (not hardcoded) as whichever
    # series actually has the latest start date, so this stays a fair,
    # matched comparison automatically even as more history accumulates
    # or another tier gets added later.
    since = db_fair_comparison_start()
    baseline = db_control_group_stats(since=since)
    baseline["standard"] = db_paper_trade_stats("STANDARD", since=since)
    # Third, stricter tier tracked alongside STANDARD -- every 15c+ crash
    # is also a 2c+ STANDARD signal, so this isn't a competing group,
    # just a narrower one being watched to see if it holds up live
    # before it's trusted the way STANDARD now is.
    baseline["standard_15c"] = db_paper_trade_stats("STANDARD_15C", since=since)
    baseline["comparison_since"] = since.strftime("%Y-%m-%d") if since else None
    return jsonify(baseline)

@app.route("/api/scan_now", methods=["POST"])
@requires_auth
def api_scan_now():
    threading.Thread(target=run_kalshi_scan,    daemon=True).start()
    threading.Thread(target=run_poly_positions, daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/api/health")
@requires_auth
def api_health():
    return jsonify({"status":"ok","scan_count":_st["scan_count"]})


@app.route("/api/config", methods=["POST"])
@requires_auth
def api_config():
    data = freq.get_json(force=True) or {}
    with _lock:
        cfg = _st["config"]
        for k, t in [
            ("kalshi_min_move",float),("kalshi_min_depth",float),
            ("poly_top",int),("poly_min_traders",int),("poly_min_value",float),
            ("poly_min_total",float),("poly_dominance",float),
            ("poly_min_momentum",float),("poly_max_price",float),("poly_window_min",int)
        ]:
            if k in data:
                v = data[k]
                if k in ("kalshi_min_move","poly_min_momentum","poly_max_price","poly_dominance"):
                    v = float(v)/100 if float(v) > 1 else float(v)
                cfg[k] = t(v)
    return jsonify({"status":"ok"})

HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PolySignal</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.1"></script>
<style>
:root{--bg:#0f0f11;--surf:#18181c;--card:#1e1e24;--border:#2a2a32;--text:#e8e8f0;--muted:#7a7a8a;
  --green:#22c55e;--red:#ef4444;--amber:#f59e0b;--blue:#3b82f6;--purple:#a855f7;--teal:#14b8a6}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,sans-serif;font-size:14px}
a{color:var(--blue);text-decoration:none}a:hover{text-decoration:underline}
header{display:flex;align-items:center;justify-content:space-between;padding:10px 24px;
  background:var(--surf);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:50;gap:12px}
.logo{font-weight:700;font-size:17px;white-space:nowrap}.logo span{color:var(--green)}
.hstats{display:flex;gap:16px;flex:1;justify-content:center}
.hstat{font-size:12px;color:var(--muted);white-space:nowrap}
.hstat b{color:var(--text)}
.hright{display:flex;align-items:center;gap:10px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--muted);flex-shrink:0}
.dot.on{background:var(--green);box-shadow:0 0 6px var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.hbtn{border:none;padding:6px 14px;border-radius:6px;font-weight:700;font-size:13px;cursor:pointer;transition:opacity .15s}
.hbtn.pri{background:var(--green);color:#000}.hbtn.sec{background:var(--surf);color:var(--text);border:1px solid var(--border)}
.hbtn:hover{opacity:.85}.hbtn:disabled{opacity:.35;cursor:not-allowed}
.tabs{display:flex;gap:0;border-bottom:1px solid var(--border);background:var(--bg);
  position:sticky;top:49px;z-index:40;padding:0 24px}
.tab{padding:11px 16px;cursor:pointer;font-weight:500;font-size:13px;color:var(--muted);
  border-bottom:2px solid transparent;transition:.15s;white-space:nowrap}
.tab.active{color:var(--text);border-bottom-color:var(--green)}
.layout{display:grid;grid-template-columns:1fr 256px;min-height:calc(100vh - 90px)}
.main{padding:18px 24px;border-right:1px solid var(--border);min-width:0}
.sidebar{padding:16px;overflow-y:auto}
.panel{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:12px}
.ptitle{font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:10px}
.field{margin-bottom:8px}
.field label{display:block;font-size:11px;color:var(--muted);margin-bottom:3px}
.field input,.field select{width:100%;background:var(--surf);border:1px solid var(--border);
  border-radius:6px;color:var(--text);padding:5px 8px;font-size:12px}
.field input:focus,.field select:focus{outline:none;border-color:var(--green)}
.sbtn{width:100%;background:var(--surf);color:var(--text);border:1px solid var(--border);
  border-radius:6px;padding:6px;font-size:12px;cursor:pointer;font-weight:600;margin-top:4px}
.sbtn:hover{background:var(--border)}
.hint{font-size:11px;color:var(--muted);line-height:1.7}
.summary{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap}
.scard{flex:1;min-width:70px;background:var(--card);border:1px solid var(--border);
  border-radius:8px;padding:10px;text-align:center}
.sv{font-size:20px;font-weight:700}.sl{font-size:10px;color:var(--muted);margin-top:2px}
.frow{display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap}
.fbtn{background:var(--surf);border:1px solid var(--border);border-radius:6px;
  padding:4px 10px;font-size:12px;cursor:pointer;color:var(--muted)}
.fbtn.on{border-color:var(--green);color:var(--green)}
.grid{display:flex;flex-direction:column;gap:10px}
.card{background:var(--card);border:1px solid var(--border);border-radius:10px;
  padding:13px 16px;border-left:3px solid var(--border)}
.card.up{border-left-color:var(--green)}.card.down{border-left-color:var(--red)}
.card.poly{border-left-color:var(--blue)}
.card-head{display:flex;align-items:flex-start;justify-content:space-between;gap:8px;margin-bottom:8px}
.card-title{font-weight:600;font-size:13px;line-height:1.4;flex:1}
.badges{display:flex;gap:4px;flex-shrink:0}
.badge{font-size:11px;font-weight:700;padding:2px 7px;border-radius:4px}
.b-up{background:#052e16;color:#22c55e}.b-dn{background:#2d0a0a;color:#ef4444}
.b-live{background:#1a1205;color:#f59e0b}.b-held{background:#0c1429;color:#3b82f6}
.b-out{background:#1a0a2e;color:#a855f7}
.three{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:8px}
.stat{background:var(--surf);border-radius:6px;padding:8px;text-align:center}
.sv2{font-size:16px;font-weight:700;margin-bottom:2px}
.sl2{font-size:10px;color:var(--muted);text-transform:uppercase}
.foot{display:flex;justify-content:space-between;align-items:center;margin-top:8px;
  border-top:1px solid var(--border);padding-top:8px}
.meta{font-size:11px;color:var(--muted)}
.platform-tag{font-size:10px;font-weight:700;padding:2px 6px;border-radius:3px}
.pt-k{background:#1a1205;color:var(--amber)}.pt-p{background:#0c1429;color:var(--blue)}
.empty{text-align:center;padding:50px 20px;color:var(--muted)}
.empty h3{font-size:15px;margin-bottom:6px;color:var(--text)}
.scanbar{height:2px;background:linear-gradient(90deg,var(--green),var(--blue),var(--green));
  background-size:200%;animation:sh 1.5s infinite;position:fixed;top:0;left:0;right:0;z-index:100;display:none}
.scanbar.on{display:block}@keyframes sh{0%{background-position:200%}100%{background-position:-200%}}
.err{background:#2d0a0a;border:1px solid var(--red);color:var(--red);border-radius:8px;
  padding:8px 12px;margin-bottom:12px;font-size:12px;display:none}
.err.on{display:block}
.rec-banner{border-radius:6px;padding:6px 10px;margin-bottom:8px;font-size:12px;font-weight:600;display:flex;align-items:center;gap:6px}
.rec-enter{background:#052e16;color:#22c55e}.rec-watch{background:#1a1205;color:#f59e0b}
.dom-row{display:flex;justify-content:space-between;font-size:11px;color:var(--muted);margin-bottom:3px}
.dom-track{height:5px;background:var(--border);border-radius:3px;overflow:hidden;margin-bottom:8px}
.dom-fill{height:100%;border-radius:3px}
table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:left;padding:7px 10px;font-size:10px;font-weight:700;letter-spacing:.06em;
   text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border)}
td{padding:9px 10px;border-bottom:1px solid var(--border);vertical-align:middle}
tr:hover td{background:var(--surf)}
.tag{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600}
.t-open{background:#0c1429;color:var(--blue)}.t-closed{background:#1a1a2e;color:var(--muted)}
.t-win{background:#052e16;color:var(--green)}.t-loss{background:#2d0a0a;color:var(--red)}
.modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:200;align-items:center;justify-content:center}
.modal-bg.on{display:flex}
.modal{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:22px;width:400px;max-width:95vw}
.modal h3{font-size:15px;font-weight:600;margin-bottom:14px}
.modal-foot{display:flex;gap:8px;margin-top:14px;justify-content:flex-end}
.chart-wrap{background:var(--surf);border-radius:8px;padding:16px 14px 10px;margin-bottom:12px;height:240px}
.chart-wrap.chart-tall{height:360px}
.agrid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-bottom:14px}
.pgrid{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-bottom:14px}
.pcard{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px}
.pcard-title{font-size:11px;color:var(--muted);margin-bottom:6px}
.pcard-val{font-size:20px;font-weight:700}
.sec-title{font-size:13px;font-weight:600;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border)}
</style>
</head><body>
<div class="scanbar" id="scanbar"></div>
<header>
  <div class="logo">Poly<span>Signal</span></div>
  <div class="hstats" id="hstats"></div>
  <div class="hright">
    <div class="dot" id="dot"></div>
    <span style="font-size:11px;color:var(--muted)" id="dbsize"></span>
    <button class="hbtn pri" id="scanbtn" onclick="triggerScan()">↺ Scan</button>
  </div>
</header>
<div class="tabs">
  <div class="tab active" id="tab-home"      onclick="showTab('home')">🏠 Dashboard</div>
  <div class="tab"        id="tab-kalshi"    onclick="showTab('kalshi')">⚡ Kalshi</div>
  <div class="tab"        id="tab-polymarket" onclick="showTab('polymarket')">📊 Polymarket</div>
  <div class="tab"        id="tab-trades"    onclick="showTab('trades')">💼 Trades</div>
  <div class="tab"        id="tab-analytics" onclick="showTab('analytics')">📈 Analytics</div>
</div>
<div class="layout">
  <div class="main">
    <div class="err" id="errbanner"></div>
    <div id="tab-content"></div>
  </div>
  <div class="sidebar">
    <div class="panel">
      <div class="ptitle">⚡ Kalshi filters</div>
      <div class="field"><label>Min price move (¢)</label><input id="k-move" type="number" min="1" max="20" value="3"></div>
      <div class="field"><label>Min order depth ($)</label><input id="k-depth" type="number" min="100" value="1000"></div>
    </div>
    <div class="panel">
      <div class="ptitle">📊 Polymarket filters</div>
      <div class="field"><label>Top traders</label><input id="p-top" type="number" min="10" max="200" value="100"></div>
      <div class="field"><label>Min traders same side</label><input id="p-mt" type="number" min="1" value="3"></div>
      <div class="field"><label>Min cluster ($)</label><input id="p-total" type="number" min="0" value="500"></div>
      <div class="field"><label>Min dominance (0-100)</label><input id="p-dom" type="number" min="50" max="100" value="65"></div>
      <div class="field"><label>Min momentum (¢)</label><input id="p-mom" type="number" min="0" value="8"></div>
      <div class="field"><label>Max price (¢)</label><input id="p-maxp" type="number" min="10" max="95" value="35"></div>
    </div>
    <button class="sbtn" onclick="saveConfig()">Apply & Rescan Both</button>
    <div class="panel" style="margin-top:12px;padding:12px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <div class="ptitle" style="margin:0">📅 Calendar</div>
        <div style="display:flex;gap:4px;align-items:center">
          <span onclick="calNav(-1)" style="cursor:pointer;color:var(--muted);font-size:16px;padding:0 3px">‹</span>
          <span id="cal-lbl" style="font-size:11px;font-weight:600;min-width:65px;text-align:center"></span>
          <span onclick="calNav(1)"  style="cursor:pointer;color:var(--muted);font-size:16px;padding:0 3px">›</span>
        </div>
      </div>
      <div id="cal-grid"></div>
      <div style="margin-top:6px;font-size:10px;display:flex;gap:8px;color:var(--muted)">
        <span><span style="color:var(--red)">●</span> High</span>
        <span><span style="color:var(--amber)">●</span> Med</span>
      </div>
      <div id="cal-detail" style="margin-top:8px"></div>
    </div>
  </div>
</div>

<!-- Log Trade Modal -->
<div class="modal-bg" id="trade-modal">
  <div class="modal">
    <h3>Log Trade</h3>
    <input type="hidden" id="tm-sid"><input type="hidden" id="tm-ticker"><input type="hidden" id="tm-platform">
    <div class="field"><label>Market</label><input id="tm-title" readonly></div>
    <div class="field"><label>Platform</label><input id="tm-plat-display" readonly></div>
    <div class="field"><label>Side</label>
      <select id="tm-side"><option value="YES">YES</option><option value="NO">NO</option></select></div>
    <div class="field"><label>Entry price (¢)</label><input id="tm-entry" type="number" min="1" max="99"></div>
    <div class="field"><label>Amount ($)</label><input id="tm-qty" type="number" min="1" placeholder="e.g. 25"></div>
    <div class="field"><label>Strategy</label>
      <select id="tm-strat">
        <option value="fed">Fed/Rates</option><option value="crypto">Crypto</option>
        <option value="sports">Sports</option><option value="earnings">Earnings</option>
        <option value="macro">Macro/CPI/GDP</option><option value="scalp">Scalp</option>
        <option value="momentum">Momentum</option><option value="other">Other</option>
      </select></div>
    <div class="field"><label>Notes</label><input id="tm-notes" placeholder="optional"></div>
    <div class="modal-foot">
      <button class="hbtn sec" onclick="closeModal('trade-modal')">Cancel</button>
      <button class="hbtn pri" onclick="submitTrade()">Log Trade</button>
    </div>
  </div>
</div>

<!-- Close Trade Modal -->
<div class="modal-bg" id="close-modal">
  <div class="modal">
    <h3>Close Trade</h3>
    <input type="hidden" id="cm-id">
    <div class="field"><label>Exit price (¢)</label><input id="cm-exit" type="number" min="1" max="100"></div>
    <div class="field"><label>Notes</label><input id="cm-notes" placeholder="optional"></div>
    <div class="modal-foot">
      <button class="hbtn sec" onclick="closeModal('close-modal')">Cancel</button>
      <button class="hbtn pri" onclick="submitClose()">Close Trade</button>
    </div>
  </div>
</div>

<script>
let tab='home', liveFilter='all', polyTab='positions';
let state={kalshi_signals:[],poly_positions:[],poly_live:[],config:{},events:[],db_size_mb:0};
let sigs_db=[], trades_db=[], analytics={}, paperTrades={}, recentSignals={kalshi:[],polymarket:[],kalshi_count:0,poly_count:0};
let calY=new Date().getFullYear(), calM=new Date().getMonth(), selDay=null;
let charts={};
// Called from the "Reset zoom" buttons above the pnl/edge charts.
// chartjs-plugin-zoom attaches resetZoom() to the chart instance once a
// zoom/pan config is present -- guarded here in case a chart hasn't been
// built yet (e.g. no paper trade data at all) or the plugin failed to
// load, so a stray click can't throw and break the rest of the page.
function resetChartZoom(which){
  const c = charts[which];
  if(c && typeof c.resetZoom === 'function') c.resetZoom();
}

const TABS=['home','kalshi','polymarket','trades','analytics'];

function showTab(t) {
  tab=t;
  TABS.forEach(x=>document.getElementById('tab-'+x).classList.toggle('active',x===t));
  if(t==='home'||t==='kalshi'||t==='polymarket') fetchRecentSignals();
  if(t==='trades')    fetchTrades();
  if(t==='analytics') fetchAnalytics();
  render();
}

function render() {
  const el=document.getElementById('tab-content');
  if(tab==='home')       el.innerHTML=renderHome();
  if(tab==='kalshi')     el.innerHTML=renderKalshi();
  if(tab==='polymarket') el.innerHTML=renderPoly();
  if(tab==='trades')     el.innerHTML=renderTrades();
  if(tab==='analytics')  el.innerHTML=renderAnalytics();
  if(tab==='analytics')  initCharts();
}

// ── Helpers ───────────────────────────────────────────────────────────────────
const usd=n=>'$'+Math.round(n||0).toLocaleString();
const c=n=>((n||0)*100).toFixed(1)+'¢';
const pnlC=n=>(n||0)>=0?'var(--green)':'var(--red)';
const pnlS=n=>((n||0)>=0?'+':'')+parseFloat(n||0).toFixed(2);
const oc=o=>{const u=o.toUpperCase();return u==='YES'?'b-up':u==='NO'?'b-dn':'b-out';}
const stars=s=>{const ic=['','🔵','🟢','🟡','🟠','🔴'];return ic[Math.min(s||1,5)];}

// ── Dashboard tab ─────────────────────────────────────────────────────────────
// Adapters: DB-shaped signal dict -> the same shape renderKalshiCard/
// renderPolyCard/getAction already expect from a live in-process scan.
// Reuses the real, rich card renderers instead of a separate, simpler
// display -- nearly everything they need (price before/after, dominance,
// trader count, depth, opposite-trader count, close time) is already
// in the database. The one real gap: opposite-side traders is stored
// as a count, not a dollar value, so that specific warning line is
// left out here rather than shown with a fabricated percentage.
function adaptKalshiFromDb(s) {
  return {
    direction: s.signal_type, move_abs: s.move_size,
    prev_price: s.price_before, cur_price: s.price_after,
    depth: s.depth, title: s.market_title, ts_label: s.detected_at,
    end_date: s.market_close_time, url: s.market_url,
    ticker: s.ticker, db_id: s.id,
  };
}
function adaptPolyFromDb(r) {
  const parts = (r.platform_signal_id||'').split(':');
  const outcomeName = parts.length>=3 ? parts[2] : '';
  return {
    kind: r.signal_type, dominance: r.dominance,
    upside: 1-(r.price_after||0), momentum: r.move_size,
    avgEntry: r.price_before, curPrice: r.price_after,
    traders: r.trader_count, outcome: outcomeName,
    totalValue: r.depth, oppositeTraders: r.opposite_traders,
    oppositeValue: 0, // not stored as a $ amount -- see note above
    title: r.market_title, market_url: r.market_url, category: r.category,
    endDate: r.market_close_time, end_date: r.market_close_time,
    db_id: r.id, conditionId: parts.length>=2 ? parts[1] : '',
  };
}

function renderHome() {
  const a=analytics; const sc=state.scanning_kalshi||state.scanning_poly_pos||state.scanning_poly_live;

  // Real, DB-backed counts (last 24h) -- NOT state.kalshi_signals /
  // state.poly_positions, which only ever reflect THIS process's own
  // local scans. worker.py runs the real, continuous scanner as a
  // separate process; the dashboard has to ask the database what
  // actually happened, the same way a person checking via SQL would.
  const kCount = recentSignals.kalshi_count||0;
  const pCount = recentSignals.poly_count||0;

  let html=`<div class="summary">
    <div class="scard"><div class="sv" style="color:var(--amber)">${kCount}</div><div class="sl">Kalshi signals (24h)</div></div>
    <div class="scard"><div class="sv" style="color:var(--blue)">${pCount}</div><div class="sl">Poly signals (24h)</div></div>
    <div class="scard"><div class="sv" style="color:var(--green)">${a.open_trades||0}</div><div class="sl">Open trades</div></div>
    <div class="scard"><div class="sv" style="color:${pnlC(a.total_pnl)}">${pnlS(a.total_pnl)}</div><div class="sl">Total PnL</div></div>
  </div>`;

  const ksigs = recentSignals.kalshi||[];
  if(ksigs.length){
    html+=`<div class="sec-title">⚡ Recent Kalshi signals (last 24h)</div><div class="grid">`;
    ksigs.forEach(s=>{html+=renderKalshiCard(adaptKalshiFromDb(s),true);});
    html+=`</div><div style="margin-bottom:14px"></div>`;
  }

  const psigs = recentSignals.polymarket||[];
  if(psigs.length){
    html+=`<div class="sec-title">📊 Recent Polymarket signals (last 24h)</div><div class="grid">`;
    psigs.forEach(r=>{html+=renderPolyCard(adaptPolyFromDb(r),true);});
    html+=`</div>`;
  }

  if(!ksigs.length && !psigs.length)
    html+=`<div class="empty"><h3>${sc?'Scanning\u2026':'No signals in the last 24h'}</h3>
      <p>${sc?'Both scanners running. Check back in a minute.':'The background scanner runs continuously \u2014 check the Kalshi/Polymarket tabs or Telegram for the latest.'}</p></div>`;

  return html;
}

// ── Kalshi tab ────────────────────────────────────────────────────────────────
function renderKalshi() {
  const minMove  = state.config.kalshi_min_move  || 0.03;
  const minDepth = state.config.kalshi_min_depth || 1000;
  // Only alert-quality signals here -- the DB also holds broad-capture
  // noise-floor data (as of today, saved for later research), which
  // would otherwise flood this tab with sub-threshold ticks.
  const adapted = (recentSignals.kalshi||[])
    .filter(s => Math.abs(s.move_size||0) >= minMove && (s.depth||0) >= minDepth)
    .map(adaptKalshiFromDb);
  const rows=liveFilter==='up'?adapted.filter(s=>s.direction==='UP')
            :liveFilter==='down'?adapted.filter(s=>s.direction==='DOWN')
            :adapted;
  const buys=adapted.filter(s=>s.direction==='UP').length;
  const sells=adapted.filter(s=>s.direction==='DOWN').length;
  let html=`<div class="summary">
    <div class="scard"><div class="sv" style="color:var(--green)">${buys}</div><div class="sl">🟢 Buys</div></div>
    <div class="scard"><div class="sv" style="color:var(--red)">${sells}</div><div class="sl">🔴 Sells</div></div>
    <div class="scard"><div class="sv" style="color:var(--muted)">${state.kalshi_watched}</div><div class="sl">Markets</div></div>
  </div>
  <div class="frow">
    <div class="fbtn ${liveFilter==='all'?'on':''}" onclick="liveFilter='all';render()">All</div>
    <div class="fbtn ${liveFilter==='up'?'on':''}"  onclick="liveFilter='up';render()">🟢 Buys</div>
    <div class="fbtn ${liveFilter==='down'?'on':''}" onclick="liveFilter='down';render()">🔴 Sells</div>
  </div>
  <div class="grid">`;
  if(!rows.length) html+=`<div class="empty"><h3>${state.scanning_kalshi?'Scanning…':'No signals in the last 24h'}</h3>
    <p>Real detections from the background scanner \u2014 lower filters won't change this, it just means nothing's cleared the alert threshold recently.</p></div>`;
  else rows.forEach(s=>{html+=renderKalshiCard(s,false);});
  return html+'</div>';
}

// ── Action recommendation ─────────────────────────────────────────────────────
function getAction(r, platform) {
  if (platform === 'kalshi') {
    const up     = r.direction === 'UP';
    const upside = (1 - r.cur_price) * 100;
    const move   = r.move_abs * 100;
    if (!up)                       return {label:'STAY OUT',    color:'#ef4444', bg:'#3b0a0a', icon:'🔴', reason:'Large sell — big money flowing out. Do not buy YES.'};
    if (upside < 15)               return {label:'TOO LATE',    color:'#6b7280', bg:'#111111', icon:'⚫', reason:'Under 15¢ upside left. Not worth the risk.'};
    if (move >= 5 && upside >= 30) return {label:'STRONG BUY ✦',color:'#4ade80', bg:'#022c12', icon:'🟢', reason:`Big ${move.toFixed(1)}¢ move. ${upside.toFixed(0)}¢ upside remaining. Enter now.`};
    return                                {label:'CONSIDER',    color:'#fb923c', bg:'#2a1200', icon:'🟠', reason:'Moderate signal. Lower confidence — small position only.'};
  } else {
    const upside  = (r.upside||0)*100;
    const mom     = (r.momentum||0)*100;
    const dom     = Math.round((r.dominance||0)*100);
    const traders = r.traders||0;
    const isLive  = r.kind==='LIVE_BUY';
    const hor     = timeHorizon(r.endDate||r.end_date||'');
    if (upside < 10) return {label:'TOO LATE',     color:'#6b7280', bg:'#111111', icon:'⚫', reason:'Under 10¢ upside. Smart money already captured the move. Skip.'};
    if (mom < 0)     return {label:'AVOID',         color:'#ef4444', bg:'#3b0a0a', icon:'🔴', reason:'Price dropped since smart money entered. Thesis may be broken.'};
    if (dom>=80 && traders>=4 && mom>=10 && upside>=20)
                     return {label:'STRONG BUY ✦',  color:'#4ade80', bg:'#022c12', icon:'🟢', reason:`${traders} traders, ${dom}% consensus, +${mom.toFixed(0)}¢ momentum. High confidence.`};
    if (dom>=65 && traders>=3 && upside>=15)
                     return {label:'BUY',            color:'#86efac', bg:'#052e16', icon:'🟩', reason:`Solid signal. ${traders} traders agree, ${dom}% consensus. Good entry.`};
    if (hor.type==='short' && upside>=20)
                     return {label:'QUICK ENTRY ⚡', color:'#fbbf24', bg:'#2a1a00', icon:'🟡', reason:'Short-term play. Resolves soon — enter fast or miss it.'};
    return           {label:'WATCH 👀',              color:'#d97706', bg:'#1c1200', icon:'🟠', reason:'Some signal but not all criteria met. Watch before committing.'};
  }
}

// ── Polymarket tab ─────────────────────────────────────────────────────────────
let polyFilter = 'all'; // all / sports / crypto / politics / economics

function renderPoly() {
  const adaptedAll = (recentSignals.polymarket||[]).map(adaptPolyFromDb);
  const positions = adaptedAll.filter(r=>r.kind==='OPEN_POSITION');
  const live       = adaptedAll.filter(r=>r.kind==='LIVE_BUY');
  const rows = polyTab==='live' ? live : positions;
  const cats = [...new Set(rows.map(r=>r.category||'').filter(Boolean))].sort();

  const filtered = polyFilter==='all' ? rows : rows.filter(r=>(r.category||'').toLowerCase()===polyFilter.toLowerCase());

  let html = `<div class="frow">
    <div class="fbtn ${polyTab==='positions'?'on':''}" onclick="polyTab='positions';render()">📊 Open Positions</div>
    <div class="fbtn ${polyTab==='live'?'on':''}"      onclick="polyTab='live';render()">⚡ Live Buys</div>
  </div>
  <div class="summary">
    <div class="scard"><div class="sv" style="color:var(--blue)">${positions.length}</div><div class="sl">Positions</div></div>
    <div class="scard"><div class="sv" style="color:var(--amber)">${live.length}</div><div class="sl">Live buys</div></div>
    <div class="scard"><div class="sv" style="color:var(--muted)">${state.poly_traders}</div><div class="sl">Traders</div></div>
  </div>`;

  // Category filter pills
  if(cats.length > 1) {
    html += `<div class="frow" style="margin-bottom:12px">
      <div class="fbtn ${polyFilter==='all'?'on':''}" onclick="polyFilter='all';render()">All</div>`;
    cats.forEach(c=>{
      html+=`<div class="fbtn ${polyFilter===c?'on':''}" onclick="polyFilter='${c}';render()">${c}</div>`;
    });
    html += `</div>`;
  }

  html += `<div class="grid">`;
  if(!filtered.length) html+=`<div class="empty"><h3>No signals in the last 24h</h3><p>Real detections from the background scanner.</p></div>`;
  else filtered.forEach(r=>{html+=renderPolyCard(r,false);});
  return html+'</div>';
}

function timeHorizon(endDate) {
  if(!endDate) return {type:'long',label:'',color:'var(--muted)'};
  const d=new Date(endDate.slice(0,10)+'T12:00:00');
  const days=Math.ceil((d-new Date())/86400000);
  if(days<=0)  return {type:'short',label:'⏰ Resolves TODAY',    color:'var(--red)'};
  if(days===1) return {type:'short',label:'⏰ Resolves TOMORROW', color:'var(--amber)'};
  if(days<=7)  return {type:'short',label:`📅 ${days} days left`, color:'var(--amber)'};
  return        {type:'long', label:`📅 ${days} days left`,       color:'var(--muted)'};
}

function renderKalshiCard(s,compact=false) {
  const up   = s.direction==='UP';
  const mc   = (s.move_abs*100).toFixed(1);
  const pc   = (s.prev_price*100).toFixed(1);
  const cc   = (s.cur_price*100).toFixed(1);
  const up_c = ((1-s.cur_price)*100).toFixed(1);
  const dbid = s.db_id||'';
  const hor  = timeHorizon(s.end_date||'');
  const act = getAction(s, 'kalshi');
  return `<div class="card ${up?'up':'down'}">
    <div class="card-head">
      <div style="flex:1">
        <div style="font-size:10px;font-weight:700;color:var(--amber);letter-spacing:.06em;margin-bottom:3px">⚡ KALSHI — ORDER FLOW</div>
        <div class="card-title">${s.title}</div>
      </div>
      <span class="badge ${up?'b-up':'b-dn'}">${up?'LARGE BUY':'LARGE SELL'}</span>
    </div>

    <!-- ACTION BANNER -->
    <div style="background:${act.bg};border:1px solid ${act.color};border-radius:8px;padding:10px 14px;margin-bottom:12px">
      <div style="font-size:15px;font-weight:700;color:${act.color};margin-bottom:3px">${act.icon} ${act.label}</div>
      <div style="font-size:12px;color:${act.color};opacity:.85">${act.reason}</div>
    </div>

    <div style="font-size:11px;color:var(--muted);margin-bottom:10px;line-height:1.6">
      ${up?'📈':'📉'} YES moved <b>${pc}¢ → <span style="color:${up?'var(--green)':'var(--red)'}">${cc}¢</span></b> (${up?'+':''}${mc}¢) · ${s.ts_label}<br>
      <span style="font-size:10px;font-style:italic">Anonymous large order — you're following the money, not a specific person</span>
    </div>
    <div class="three">
      <div class="stat"><div class="sv2" style="color:${up?'var(--green)':'var(--red)'}">${up?'+':''}${mc}¢</div><div class="sl2">Move size</div></div>
      <div class="stat"><div class="sv2" style="color:var(--green)">${up_c}¢</div><div class="sl2">Upside left</div></div>
      <div class="stat"><div class="sv2" style="color:var(--amber)">${usd(s.depth)}</div><div class="sl2">Order depth</div></div>
    </div>
    ${hor.label?`<div style="font-size:11px;color:${hor.color};padding:4px 8px;background:var(--surf);border-radius:4px;margin-bottom:6px">${hor.label} · ${hor.type==='short'?'<b>Short-term</b>':'<b>Long-term</b>'}</div>`:''}
    <div class="foot">
      <a href="${s.url}" target="_blank" style="font-size:12px">View on Kalshi ↗</a>
      <button class="hbtn pri" style="font-size:11px;padding:3px 9px" onclick="openLog('${dbid}','${s.ticker}','${(s.title||'').replace(/'/g,"\'")}',${s.cur_price},'kalshi',${up?1:0})">+ Trade</button>
    </div>
  </div>`;
}

function renderPolyCard(r,compact=false) {
  const live  = r.kind==='LIVE_BUY';
  const dom   = Math.round((r.dominance||0)*100);
  const dcolor= dom>=85?'var(--green)':dom>=65?'var(--amber)':'var(--red)';
  const up_c  = ((r.upside||0)*100).toFixed(1);
  const mom_c = ((r.momentum||0)*100).toFixed(1);
  const mom   = (r.momentum||0)*100;
  const slug  = r.eventSlug||r.slug||'';
  const url   = slug?`https://polymarket.com/event/${slug}`:(r.market_url||'');
  const dbid  = r.db_id||'';
  const hor   = timeHorizon(r.endDate||r.end_date||'');
  const avgC  = ((r.avgEntry||0)*100).toFixed(1);
  const curC  = ((r.curPrice||0)*100).toFixed(1);
  const opp_pct = r.totalValue>0 ? Math.round((r.oppositeValue||0)/r.totalValue*100) : 0;

  const typeHeader = live
    ? `<div style="font-size:10px;font-weight:700;color:var(--amber);letter-spacing:.06em;margin-bottom:3px">⚡ POLYMARKET — LIVE BUY CLUSTER</div>
       <div style="font-size:11px;color:var(--muted);margin-bottom:6px">Top traders just bought <b>${r.outcome}</b> in the last 30 min</div>`
    : `<div style="font-size:10px;font-weight:700;color:var(--blue);letter-spacing:.06em;margin-bottom:3px">📊 POLYMARKET — SMART MONEY POSITION</div>
       <div style="font-size:11px;color:var(--muted);margin-bottom:6px">Top traders are currently holding <b>${r.outcome}</b></div>`;

  const momLine = mom>0
    ? `<div style="font-size:11px;color:var(--green);margin-bottom:6px">📈 Up +${mom.toFixed(1)}¢ since smart money entered (avg ${avgC}¢ → now ${curC}¢)</div>`
    : `<div style="font-size:11px;color:var(--amber);margin-bottom:6px">📉 Down ${mom.toFixed(1)}¢ since entry (avg ${avgC}¢ → now ${curC}¢)</div>`;

  const oppWarn = opp_pct>20
    ? `<div style="font-size:11px;color:var(--amber);padding:4px 8px;background:var(--surf);border-radius:4px;margin-bottom:6px">⚠️ ${r.oppositeTraders} traders on opposite side (${opp_pct}% of value)</div>`
    : '';

  const act = getAction(r, 'polymarket');
  return `<div class="card poly">
    <div class="card-head">
      <div style="flex:1">
        ${typeHeader}
        <div class="card-title">${r.title||r.slug||r.conditionId}</div>
      </div>
      <div class="badges">
        <span class="badge ${oc(r.outcome)}">${r.outcome}</span>
        ${live?'<span class="badge b-live">LIVE</span>':'<span class="badge b-held">HELD</span>'}
      </div>
    </div>

    <!-- ACTION BANNER -->
    <div style="background:${act.bg};border:1px solid ${act.color};border-radius:8px;padding:10px 14px;margin-bottom:12px">
      <div style="font-size:15px;font-weight:700;color:${act.color};margin-bottom:3px">${act.icon} ${act.label}</div>
      <div style="font-size:12px;color:${act.color};opacity:.85">${act.reason}</div>
    </div>

    <div class="dom-row">
      <span style="font-size:12px"><b>${r.traders}</b> top traders agree</span>
      <span style="color:${dcolor};font-weight:700">${dom}% consensus</span>
    </div>
    <div class="dom-track"><div class="dom-fill" style="width:${dom}%;background:${dcolor}"></div></div>
    ${momLine}
    ${oppWarn}
    <div class="three">
      <div class="stat"><div class="sv2" style="color:var(--green)">${up_c}¢</div><div class="sl2">Upside left</div></div>
      <div class="stat"><div class="sv2" style="color:var(--blue)">${usd(r.totalValue)}</div><div class="sl2">Smart $ in</div></div>
      <div class="stat"><div class="sv2" style="color:${mom>0?'var(--green)':'var(--amber)'}">+${mom_c}¢</div><div class="sl2">Momentum</div></div>
    </div>
    ${live
      ? `<div style="font-size:11px;color:var(--red);padding:4px 8px;background:var(--surf);border-radius:4px;margin-bottom:6px">🔴 <b>LIVE</b> — match in progress, may resolve soon</div>`
      : (hor.label?`<div style="font-size:11px;color:${hor.color};padding:4px 8px;background:var(--surf);border-radius:4px;margin-bottom:6px">${hor.label} · ${hor.type==='short'?'<b>Short-term</b>':'<b>Long-term</b>'}</div>`:'')}
    <div class="foot">
      ${url?`<a href="${url}" target="_blank" style="font-size:12px">View on Polymarket ↗</a>`:'<span></span>'}
      <button class="hbtn pri" style="font-size:11px;padding:3px 9px" onclick="openLog('${dbid}','${r.conditionId}','${(r.title||'').replace(/'/g,"\'")}',${r.curPrice||r.cur_price||0},'polymarket',1)">+ Trade</button>
    </div>
  </div>`;
}

// ── Trades tab ────────────────────────────────────────────────────────────────
function renderTrades() {
  const open=trades_db.filter(t=>t.status==='OPEN');
  const closed=trades_db.filter(t=>t.status==='CLOSED');
  const total_pnl=closed.reduce((a,t)=>a+(t.pnl||0),0);
  let html=`<div class="summary">
    <div class="scard"><div class="sv" style="color:var(--blue)">${open.length}</div><div class="sl">Open</div></div>
    <div class="scard"><div class="sv" style="color:var(--muted)">${closed.length}</div><div class="sl">Closed</div></div>
    <div class="scard"><div class="sv" style="color:var(--green)">${analytics.win_rate||0}%</div><div class="sl">Win rate</div></div>
    <div class="scard"><div class="sv" style="color:${pnlC(total_pnl)}">${pnlS(total_pnl)}</div><div class="sl">Total PnL</div></div>
  </div>`;
  if(!trades_db.length) return html+`<div class="empty"><h3>No trades yet</h3>
    <p>Click "+ Trade" on any signal card to log a trade.</p></div>`;
  html+=`<table><thead><tr>
    <th>Platform</th><th>Market</th><th>Side</th><th>Entry</th><th>Now/Exit</th><th>Amount</th><th>PnL</th><th>Strategy</th><th>Status</th><th></th>
  </tr></thead><tbody>`;
  trades_db.forEach(t=>{
    const isOpen=t.status==='OPEN';
    const pnl=isOpen?(t.unrealized_pnl||0):(t.pnl||0);
    const pnlDisp=`<span style="color:${pnlC(pnl)}">${pnlS(pnl)}${isOpen?' <small>(unr.)</small>':''}</span>`;
    const curP=isOpen?`<span style="color:var(--blue)">${((t.current_price||t.entry_price||0)*100).toFixed(0)}¢</span>`
                     :`${((t.exit_price||0)*100).toFixed(0)}¢`;
    html+=`<tr>
      <td><span class="platform-tag ${t.platform==='kalshi'?'pt-k':'pt-p'}">${(t.platform||'').toUpperCase()}</span></td>
      <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${t.market_title||t.ticker}</td>
      <td><span class="badge ${t.side==='YES'?'b-up':'b-dn'}">${t.side}</span></td>
      <td>${((t.entry_price||0)*100).toFixed(0)}¢</td>
      <td>${curP}</td>
      <td>${usd(t.quantity)}</td>
      <td>${pnlDisp}</td>
      <td style="font-size:11px;color:var(--muted)">${t.strategy_tag||'—'}</td>
      <td><span class="tag ${isOpen?'t-open':(pnl>=0?'t-win':'t-loss')}">${t.status}</span></td>
      <td>${isOpen?`<button class="hbtn sec" style="font-size:11px;padding:3px 7px" onclick="openClose(${t.id})">Close</button>`:''}</td>
    </tr>`;
  });
  return html+'</tbody></table>';
}

// ── Analytics tab ─────────────────────────────────────────────────────────────
function renderAnalytics() {
  const a=analytics;
  if(!a.total_signals) return `<div class="empty"><h3>No data yet</h3><p>Signals accumulate as the scanner runs.</p></div>`;
  const sigAcc = a.sig_accuracy !== null && a.sig_accuracy !== undefined
    ? `${a.sig_accuracy}% <span style="font-size:11px;color:var(--muted)">(${a.sig_won}W/${a.sig_lost}L)</span>`
    : '<span style="font-size:13px;color:var(--muted)">Pending</span>';
  const sigAccClean = a.sig_accuracy_clean !== null && a.sig_accuracy_clean !== undefined
    ? `${a.sig_accuracy_clean}% <span style="font-size:11px;color:var(--muted)">(${a.sig_won_clean}W/${a.sig_lost_clean}L)</span>`
    : '<span style="font-size:13px;color:var(--muted)">Accumulating\u2026</span>';
  const sigAccCleanKalshi = a.sig_accuracy_clean_kalshi !== null && a.sig_accuracy_clean_kalshi !== undefined
    ? `${a.sig_accuracy_clean_kalshi}% <span style="font-size:11px;color:var(--muted)">(${a.sig_won_clean_kalshi}W/${a.sig_lost_clean_kalshi}L)</span>`
    : '<span style="font-size:13px;color:var(--muted)">Accumulating\u2026</span>';
  const sigAccKalshiDedup = a.sig_accuracy_kalshi_dedup !== null && a.sig_accuracy_kalshi_dedup !== undefined
    ? `${a.sig_accuracy_kalshi_dedup}% <span style="font-size:11px;color:var(--muted)">(${a.sig_won_kalshi_dedup}W/${a.sig_lost_kalshi_dedup}L, ${a.sig_pending_kalshi_dedup||0} pending)</span>`
    : '<span style="font-size:13px;color:var(--muted)">Accumulating\u2026</span>';

  // ZONE 1 -- Live validation: the actual thing being tested right now.
  const std = paperTrades.standard || {};
  const std15 = paperTrades.standard_15c || {};
  let html=`<div class="sec-title">\u{1f3af} Live validation \u2014 STANDARD vs the real baseline</div>
  <div style="font-size:12px;color:var(--muted);margin:-4px 0 10px 2px">All three below are measured from <b>${paperTrades.comparison_since || '?'}</b> onward \u2014 the latest start date among them, computed fresh each load so this stays a fair, matched comparison even as more history accumulates or another tier gets added. (Found 2026-09-05: STANDARD_15C's backfill quietly reached back to May, giving it ~9 extra weeks STANDARD was never measured against \u2014 this fixes that.)</div>
  <div class="agrid">
    <div class="scard" style="grid-column:span 4">
      <div class="sv" style="color:var(--amber);font-size:16px">
        ${paperTrades.resolved ? `${paperTrades.win_rate}% (${paperTrades.won}W/${paperTrades.lost}L) &nbsp; | &nbsp; PnL: <span style="color:${pnlC(paperTrades.total_pnl)}">${pnlS(paperTrades.total_pnl)}</span> on $${paperTrades.total_staked} staked &nbsp; | &nbsp; ${paperTrades.pending||0} pending` : `Accumulating\u2026 (${paperTrades.pending||0} pending)`}
      </div>
      <div class="sl">BASELINE (no filter) \u2014 hypothetical $5/signal on every qualifying signal, no PRIME/STANDARD split \u2014 the honest bar STANDARD actually has to clear</div>
    </div>
    <div class="scard" style="grid-column:span 4">
      <div class="sv" style="color:var(--blue);font-size:16px">
        ${std.resolved ? `${std.win_rate}% (${std.won}W/${std.lost}L) &nbsp; | &nbsp; PnL: <span style="color:${pnlC(std.total_pnl)}">${pnlS(std.total_pnl)}</span> on $${std.total_staked} staked &nbsp; | &nbsp; ${std.pending||0} pending` : `Accumulating\u2026 (${std.pending||0} pending)`}
      </div>
      <div class="sl">STANDARD (moved 2c+) \u2014 $5/signal, no real money \u2014 confirmed beating the baseline above by ~5 points, holding steady across separate weeks. PRIME (fresh, &lt;2c moved) was retired from this view after confirming it underperforms even the unfiltered baseline.</div>
    </div>
    <div class="scard" style="grid-column:span 4">
      <div class="sv" style="color:var(--green);font-size:16px">
        ${std15.resolved ? `${std15.win_rate}% (${std15.won}W/${std15.lost}L) &nbsp; | &nbsp; PnL: <span style="color:${pnlC(std15.total_pnl)}">${pnlS(std15.total_pnl)}</span> on $${std15.total_staked} staked &nbsp; | &nbsp; ${std15.pending||0} pending` : `Accumulating\u2026 (${std15.pending||0} pending)`}
      </div>
      <div class="sl">STANDARD_15C (moved 15c+, a stricter STANDARD subset) \u2014 $5/signal, no real money \u2014 date-matched to STANDARD above, still proving itself live before this gets the same trust STANDARD has now</div>
    </div>
  </div>
  <div style="display:flex;justify-content:flex-end;margin-bottom:4px">
    <button onclick="resetChartZoom('pnl')" style="background:var(--card);border:1px solid var(--border);color:var(--muted);font-size:11px;padding:4px 10px;border-radius:6px;cursor:pointer">Reset zoom</button>
  </div>
  <div class="chart-wrap chart-tall"><canvas id="pnl-chart"></canvas></div>
  <div style="font-size:11px;color:var(--muted);margin:2px 0 8px 2px">Scroll/pinch to zoom, drag to pan, either axis \u2014 click "Reset zoom" to return to the full view.</div>
  <div style="display:flex;justify-content:flex-end;margin-bottom:4px">
    <button onclick="resetChartZoom('edge')" style="background:var(--card);border:1px solid var(--border);color:var(--muted);font-size:11px;padding:4px 10px;border-radius:6px;cursor:pointer">Reset zoom</button>
  </div>
  <div class="chart-wrap"><canvas id="edge-chart"></canvas></div>


  <div class="sec-title" style="margin-top:18px">\u{1f50d} Data integrity checks \u2014 is the outcome data trustworthy</div>
  <div class="agrid">
    <div class="scard" style="grid-column:span 2"><div class="sv" style="color:var(--purple);font-size:16px">${sigAcc}</div><div class="sl">All-time (unfiltered, includes pre-fix history)</div></div>
    <div class="scard" style="grid-column:span 2"><div class="sv" style="color:var(--green);font-size:16px">${sigAccClean}</div><div class="sl">Polymarket \u2014 since July 24 filter fix</div></div>
    <div class="scard" style="grid-column:span 4"><div class="sv" style="color:var(--green);font-size:16px">${sigAccCleanKalshi}</div><div class="sl">Kalshi \u2014 since Aug 2 resolution fix (raw signal rows \u2014 a market re-firing many times counts many times)</div></div>
    <div class="scard" style="grid-column:span 4"><div class="sv" style="color:var(--amber);font-size:16px">${sigAccKalshiDedup}</div><div class="sl">Kalshi \u2014 deduplicated, distinct markets only (one row per ticker+direction, earliest detection) \u2014 use this one, not the raw tile above</div></div>
  </div>

  <div class="sec-title" style="margin-top:18px">\u{1f4ca} Volume & background</div>
  <div class="agrid">
    <div class="scard"><div class="sv" style="color:var(--amber)">${a.total_signals||0}</div><div class="sl">Total signals</div></div>
    <div class="scard" style="grid-column:span 3">
      <div class="sv" style="font-size:13px;color:var(--muted)">${a.total_trades||0} manual trade logged \u2014 ${a.win_rate||0}% win rate, ${pnlS(a.total_pnl)} \u2014 not the live strategy, see paper trading above</div>
      <div class="sl">Legacy manual trade log</div>
    </div>
  </div>`;

  if(a.by_platform&&Object.keys(a.by_platform).length){
    html+=`<div class="pgrid">`;
    Object.entries(a.by_platform).forEach(([plat,d])=>{
      const alertLine = (plat==='kalshi' && d.alert_worthy!==undefined)
        ? `<div style="font-size:11px;color:var(--muted)">of which alert-worthy: <b style="color:var(--amber)">${d.alert_worthy}</b> <span style="opacity:.7">(rest is broad-capture research data)</span></div>`
        : '';
      html+=`<div class="pcard">
        <div class="pcard-title">${plat.toUpperCase()}</div>
        <div style="font-size:12px;color:var(--muted)">Signals: <b style="color:var(--text)">${d.signals}</b></div>
        ${alertLine}
        <div style="font-size:12px;color:var(--muted)">Trades: <b style="color:var(--text)">${d.trades}</b></div>
        <div style="font-size:12px;color:var(--muted)">PnL: <b style="color:${pnlC(d.pnl)}">${pnlS(d.pnl)}</b></div>
      </div>`;
    });
    html+=`</div>`;
  }

  html+=`<div class="chart-wrap"><canvas id="sig-chart"></canvas></div>`;

  if(a.by_strategy&&Object.keys(a.by_strategy).length){
    html+=`<div class="sec-title">Performance by strategy</div>
      <table><thead><tr><th>Strategy</th><th>Trades</th><th>Win Rate</th><th>PnL</th></tr></thead><tbody>`;
    Object.entries(a.by_strategy).forEach(([k,v])=>{
      html+=`<tr><td>${k}</td><td>${v.count}</td>
        <td style="color:${v.win_rate>=50?'var(--green)':'var(--red)'}">${v.win_rate}%</td>
        <td style="color:${pnlC(v.pnl)}">${pnlS(v.pnl)}</td></tr>`;
    });
    html+='</tbody></table>';
  }
  return html;
}

function initCharts() {
  const a=analytics;
  const opts={responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false}},
    scales:{x:{ticks:{color:'#7a7a8a',font:{size:10}}},y:{ticks:{color:'#7a7a8a',font:{size:10}}}}};
  const pnlEl=document.getElementById('pnl-chart');
  const stdSeries = (paperTrades.standard && paperTrades.standard.pnl_series) || [];
  const std15Series = (paperTrades.standard_15c && paperTrades.standard_15c.pnl_series) || [];
  if(pnlEl&&paperTrades.pnl_series&&paperTrades.pnl_series.length){
    if(charts.pnl) charts.pnl.destroy();
    // PRIME and STANDARD resolve on different dates at different rates --
    // can't just reuse PRIME's date list as the x-axis for both lines,
    // that would silently misalign STANDARD's points once it has more
    // than a couple. Build one shared, sorted date axis and forward-fill
    // each series' last known cumulative value onto it instead.
    const allDates=[...new Set([...paperTrades.pnl_series.map(p=>p.date), ...stdSeries.map(p=>p.date), ...std15Series.map(p=>p.date)])].sort();
    const fillOnto=(series,dates)=>{
      let last=0, out=[], i=0;
      for(const d of dates){
        while(i<series.length && series[i].date<=d){ last=series[i].pnl; i++; }
        out.push(last);
      }
      return out;
    };
    // Baseline styled to recede -- a quiet, dashed reference line, not
    // competing for attention. STANDARD gets a real gradient glow under
    // it, since that's the actual, longest-proven result. STANDARD_15C
    // is now backfilled with a real historical sample rather than sitting
    // empty, so it gets a real, visible line too -- solid, its own light
    // fill, just slightly thinner than STANDARD's since it hasn't logged
    // the same weeks of live-only data yet.
    //
    // Redesigned 2026-09-05: the three lines converge closely enough in a
    // fair, date-matched comparison that the overlapping semi-transparent
    // area fills were blending into one indistinct blob rather than three
    // readable lines. Dropped fill on STANDARD/STANDARD_15C entirely (kept
    // only on baseline, which is meant to recede anyway), gave
    // STANDARD_15C its own dash pattern so it stays visually distinct from
    // STANDARD even where the values nearly touch, and gave the canvas
    // more vertical room (chart-tall, 360px vs 240px) so small gaps
    // between the lines are actually visible instead of compressed flat.

    const datasets=[{label:'Baseline (no filter)',data:fillOnto(paperTrades.pnl_series,allDates),
      borderColor:'#6b7280',borderWidth:1.5,borderDash:[4,3],
      backgroundColor:'rgba(107,114,128,.05)',fill:true,tension:.35,
      pointRadius:0,pointHoverRadius:4,pointHitRadius:10,
      pointBackgroundColor:'#6b7280',order:3}];
    if(stdSeries.length){
      datasets.push({label:'STANDARD',data:fillOnto(stdSeries,allDates),
        borderColor:'#3b82f6',borderWidth:2.75,
        fill:false,tension:.35,
        pointRadius:0,pointHoverRadius:6,pointHitRadius:10,
        pointBackgroundColor:'#3b82f6',pointBorderColor:'#0d1117',pointBorderWidth:2,
        order:1});
    }
    if(std15Series.length){
      datasets.push({label:'STANDARD_15C',data:fillOnto(std15Series,allDates),
        borderColor:'#22c55e',borderWidth:2.25,borderDash:[7,3],
        fill:false,tension:.35,
        pointRadius:0,pointHoverRadius:6,pointHitRadius:10,
        pointBackgroundColor:'#22c55e',pointBorderColor:'#0d1117',pointBorderWidth:2,
        order:2});
    }
    const titleParts=['Baseline'];
    if(stdSeries.length) titleParts.push('STANDARD');
    if(std15Series.length) titleParts.push('STANDARD_15C');
    charts.pnl=new Chart(pnlEl,{type:'line',data:{
      labels:allDates,
      datasets:datasets
    },options:{...opts,
      interaction:{mode:'index',intersect:false},
      elements:{line:{capBezierPoints:true}},
      layout:{padding:{top:2,right:6}},
      plugins:{...opts.plugins,
        legend:{display:datasets.length>1,position:'top',align:'end',
          labels:{color:'#9ca3af',font:{size:11},usePointStyle:true,pointStyle:'line',boxWidth:24,padding:14}},
        title:{display:true,text:titleParts.join(' vs ')+' — Cumulative PnL ($)',
          color:'#e5e7eb',font:{size:13,weight:'600'},padding:{bottom:14}},
        tooltip:{backgroundColor:'#161b22',borderColor:'#30363d',borderWidth:1,
          titleColor:'#e5e7eb',bodyColor:'#9ca3af',padding:10,cornerRadius:6,
          callbacks:{label:(c)=>` ${c.dataset.label}: $${c.parsed.y.toLocaleString()}`}},
        // Zoom/pan added 2026-09-05 -- taller canvas and dash patterns
        // helped, but the real ask was interactive zoom like a normal
        // charting tool: scroll or pinch to zoom on either axis, drag to
        // pan once zoomed, "Reset zoom" button (below) to snap back to
        // the full date range. No y-limit imposed -- didn't want to guess
        // at a floor/ceiling and risk clipping real data on a zoom out.
        zoom:{
          pan:{enabled:true,mode:'xy'},
          zoom:{wheel:{enabled:true},pinch:{enabled:true},mode:'xy'}
        }
      },
      scales:{
        x:{grid:{color:'rgba(255,255,255,.04)'},ticks:{color:'#6b7280',font:{size:10},maxRotation:0,autoSkip:true,autoSkipPadding:24}},
        y:{grid:{color:'rgba(255,255,255,.06)'},ticks:{color:'#6b7280',font:{size:10},callback:(v)=>'$'+v.toLocaleString()}}
      }
    }});

    // Second chart: PnL relative to baseline, not raw cumulative PnL.
    // Added 2026-09-05 -- once the three tiers get measured over the same
    // date range, their raw PnL lines sit close enough together (see the
    // chart above) that it's genuinely hard to tell which tier is ahead
    // and by how much. Baseline is flattened to a $0 reference line here
    // and STANDARD/STANDARD_15C are replotted as their PnL minus
    // baseline's PnL on the same date -- so "above zero" directly means
    // "beating doing nothing," and the gap between the two colored lines
    // is exactly the STANDARD_15C-vs-STANDARD edge being tracked.
    const edgeEl = document.getElementById('edge-chart');
    if (edgeEl && (stdSeries.length || std15Series.length)) {
      if (charts.edge) charts.edge.destroy();
      const baseFilled = fillOnto(paperTrades.pnl_series, allDates);
      const stdFilled  = fillOnto(stdSeries, allDates);
      const s15Filled  = fillOnto(std15Series, allDates);
      const edgeDatasets = [{
        label: 'Baseline (0 line)',
        data: allDates.map(() => 0),
        borderColor: '#6b7280', borderWidth: 1, borderDash: [4, 3],
        pointRadius: 0, pointHitRadius: 0, order: 3,
      }];
      if (stdSeries.length) {
        edgeDatasets.push({
          label: 'STANDARD vs baseline',
          data: stdFilled.map((v, i) => Math.round((v - baseFilled[i]) * 100) / 100),
          borderColor: '#3b82f6', borderWidth: 2.5, tension: .35,
          pointRadius: 0, pointHoverRadius: 5, pointHitRadius: 10,
          pointBackgroundColor: '#3b82f6', pointBorderColor: '#0d1117', pointBorderWidth: 2,
          order: 1,
        });
      }
      if (std15Series.length) {
        edgeDatasets.push({
          label: 'STANDARD_15C vs baseline',
          data: s15Filled.map((v, i) => Math.round((v - baseFilled[i]) * 100) / 100),
          borderColor: '#22c55e', borderWidth: 2, tension: .35,
          pointRadius: 0, pointHoverRadius: 5, pointHitRadius: 10,
          pointBackgroundColor: '#22c55e', pointBorderColor: '#0d1117', pointBorderWidth: 2,
          order: 2,
        });
      }
      charts.edge = new Chart(edgeEl, {
        type: 'line',
        data: { labels: allDates, datasets: edgeDatasets },
        options: {
          ...opts,
          interaction: { mode: 'index', intersect: false },
          layout: { padding: { top: 2, right: 6 } },
          plugins: {
            ...opts.plugins,
            legend: { display: true, position: 'top', align: 'end',
              labels: { color: '#9ca3af', font: { size: 11 }, usePointStyle: true, pointStyle: 'line', boxWidth: 24, padding: 14 } },
            title: { display: true, text: 'PnL Ahead of / Behind Baseline ($) — above the dashed line means beating doing nothing',
              color: '#e5e7eb', font: { size: 13, weight: '600' }, padding: { bottom: 14 } },
            tooltip: { backgroundColor: '#161b22', borderColor: '#30363d', borderWidth: 1,
              titleColor: '#e5e7eb', bodyColor: '#9ca3af', padding: 10, cornerRadius: 6,
              callbacks: { label: (c) => ` ${c.dataset.label}: ${c.parsed.y >= 0 ? '+' : ''}$${c.parsed.y.toLocaleString()}` } },
            zoom: {
              pan: { enabled: true, mode: 'xy' },
              zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'xy' },
            },
          },
          scales: {
            x: { grid: { color: 'rgba(255,255,255,.04)' }, ticks: { color: '#6b7280', font: { size: 10 }, maxRotation: 0, autoSkip: true, autoSkipPadding: 24 } },
            y: { grid: { color: 'rgba(255,255,255,.06)' }, ticks: { color: '#6b7280', font: { size: 10 }, callback: (v) => (v >= 0 ? '+' : '') + '$' + v.toLocaleString() } },
          },
        },
      });
    }
  }
  const sigEl=document.getElementById('sig-chart');
  if(sigEl&&a.signals_by_day&&a.signals_by_day.length){
    if(charts.sig) charts.sig.destroy();
    charts.sig=new Chart(sigEl,{type:'bar',data:{
      labels:a.signals_by_day.map(d=>d.date),
      datasets:[
        {label:'Polymarket',data:a.signals_by_day.map(d=>d.polymarket),backgroundColor:'rgba(59,130,246,.7)',borderRadius:3},
        {label:'Kalshi',data:a.signals_by_day.map(d=>d.kalshi),backgroundColor:'rgba(168,85,247,.7)',borderRadius:3}
      ]
    },options:{...opts,
      interaction:{mode:'index',intersect:false},
      plugins:{...opts.plugins,legend:{display:true,labels:{color:'#7a7a8a',font:{size:10}}},title:{display:true,text:'Signals per day',color:'#7a7a8a',font:{size:12}}},
      scales:{x:{...opts.scales.x,stacked:true},y:{...opts.scales.y,stacked:true}}
    }});
  }
}

// ── Trade modals ──────────────────────────────────────────────────────────────
function openLog(sigId,ticker,title,curPrice,platform,isUp) {
  document.getElementById('tm-sid').value=sigId;
  document.getElementById('tm-ticker').value=ticker;
  document.getElementById('tm-title').value=title;
  document.getElementById('tm-platform').value=platform;
  document.getElementById('tm-plat-display').value=platform.toUpperCase();
  document.getElementById('tm-entry').value=Math.round((curPrice||0)*100);
  document.getElementById('tm-side').value=isUp?'YES':'NO';
  document.getElementById('tm-qty').value='';
  document.getElementById('tm-notes').value='';
  document.getElementById('trade-modal').classList.add('on');
}
function closeModal(id){document.getElementById(id).classList.remove('on');}

async function submitTrade(){
  const data={
    signal_id:document.getElementById('tm-sid').value||null,
    ticker:document.getElementById('tm-ticker').value,
    market_title:document.getElementById('tm-title').value,
    platform:document.getElementById('tm-platform').value,
    side:document.getElementById('tm-side').value,
    entry_price:parseFloat(document.getElementById('tm-entry').value)/100,
    quantity:parseFloat(document.getElementById('tm-qty').value),
    strategy_tag:document.getElementById('tm-strat').value,
    notes:document.getElementById('tm-notes').value,
  };
  await fetch('/api/trades',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
  closeModal('trade-modal');
  fetchTrades(); showTab('trades');
}

function openClose(id){
  document.getElementById('cm-id').value=id;
  document.getElementById('cm-exit').value='';
  document.getElementById('cm-notes').value='';
  document.getElementById('close-modal').classList.add('on');
}

async function submitClose(){
  const id=document.getElementById('cm-id').value;
  const exit=parseFloat(document.getElementById('cm-exit').value)/100;
  const notes=document.getElementById('cm-notes').value;
  await fetch(`/api/trades/${id}/close`,{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({exit_price:exit,notes})});
  closeModal('close-modal'); fetchTrades(); fetchAnalytics();
}

// ── Data fetching ─────────────────────────────────────────────────────────────
async function fetchTrades(){try{const r=await fetch('/api/trades');trades_db=await r.json();render();}catch(e){}}
async function fetchAnalytics(){try{const r=await fetch('/api/analytics');analytics=await r.json();await fetchPaperTrades();render();}catch(e){}}
async function fetchPaperTrades(){try{const r=await fetch('/api/paper_trades');paperTrades=await r.json();}catch(e){}}
async function fetchRecentSignals(){try{const r=await fetch('/api/recent_signals');recentSignals=await r.json();render();}catch(e){}}

// ── Calendar ──────────────────────────────────────────────────────────────────
function calNav(d){calM+=d;if(calM>11){calM=0;calY++;}if(calM<0){calM=11;calY--;}renderCal();}
function renderCal(){
  const grid=document.getElementById('cal-grid');
  const lbl=document.getElementById('cal-lbl');
  if(!grid)return;
  const events=state.events||[];
  const today=new Date().toISOString().slice(0,10);
  lbl.textContent=new Date(calY,calM,1).toLocaleString('default',{month:'short',year:'numeric'});
  const byD={};events.forEach(e=>{if(!byD[e.date])byD[e.date]=[];byD[e.date].push(e);});
  const first=new Date(calY,calM,1).getDay();
  const days=new Date(calY,calM+1,0).getDate();
  const dw=['S','M','T','W','T','F','S'];
  let h=`<div style="display:grid;grid-template-columns:repeat(7,1fr);gap:1px;margin-bottom:3px">`;
  dw.forEach(d=>h+=`<div style="text-align:center;font-size:9px;color:var(--muted)">${d}</div>`);
  h+=`</div><div style="display:grid;grid-template-columns:repeat(7,1fr);gap:2px">`;
  for(let i=0;i<first;i++) h+=`<div></div>`;
  for(let d=1;d<=days;d++){
    const ds=`${calY}-${String(calM+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
    const evs=byD[ds]||[];
    const isT=ds===today,isSel=ds===selDay;
    const hasH=evs.some(e=>e.importance==='high'),hasM=evs.some(e=>e.importance==='med');
    const bg=isT?'background:var(--amber);color:#000':isSel?'background:var(--surf);border:1px solid var(--amber)':'background:var(--surf)';
    const dot=hasH?`<div style="width:4px;height:4px;border-radius:50%;background:var(--red);margin:0 auto"></div>`
             :hasM?`<div style="width:4px;height:4px;border-radius:50%;background:var(--amber);margin:0 auto"></div>`
             :`<div style="height:4px"></div>`;
    h+=`<div onclick="selCalDay('${ds}')" style="aspect-ratio:1;${bg};border-radius:3px;cursor:${evs.length?'pointer':'default'};
      display:flex;flex-direction:column;align-items:center;justify-content:center">
      <span style="font-size:10px;font-weight:${isT?700:400}">${d}</span>${dot}</div>`;
  }
  h+='</div>';
  grid.innerHTML=h;
  const up=events.filter(e=>e.date>=today)[0];
  if(selDay&&byD[selDay]) showCalDetail(selDay,byD[selDay]);
  else if(up) showCalDetail(up.date,byD[up.date]||[]);
}
function selCalDay(ds){selDay=selDay===ds?null:ds;renderCal();}
function showCalDetail(ds,evs){
  const d=document.getElementById('cal-detail');
  if(!d||!evs.length)return;
  const diff=Math.ceil((new Date(ds+'T12:00:00')-new Date())/86400000);
  const when=diff<=0?'<span style="color:var(--red);font-weight:700">TODAY</span>'
            :diff===1?'<span style="color:var(--amber)">Tomorrow</span>'
            :`<span style="color:var(--muted)">${diff}d</span>`;
  d.innerHTML=`<div style="background:var(--surf);border-radius:6px;padding:8px;border:1px solid var(--border)">
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px">${ds} · ${when}</div>
    ${evs.map(e=>`<div style="display:flex;gap:5px;padding:2px 0">
      <span style="color:${e.importance==='high'?'var(--red)':'var(--amber)'};font-size:11px">●</span>
      <div><div style="font-size:11px;font-weight:600">${e.label}</div>
      <div style="font-size:10px;color:var(--muted)">${e.time}</div></div>
    </div>`).join('')}
  </div>`;
}

// ── UI update ─────────────────────────────────────────────────────────────────
function updateUI(){
  const sc=state.scanning_kalshi||state.scanning_poly_pos||state.scanning_poly_live;
  document.getElementById('dot').className='dot'+(sc?' on':'');
  document.getElementById('scanbar').className='scanbar'+(sc?' on':'');
  document.getElementById('scanbtn').disabled=sc;
  document.getElementById('scanbtn').textContent=sc?'↺ Scanning…':'↺ Scan';
  document.getElementById('dbsize').textContent=`DB: ${state.db_size_mb}MB`;
  const err=document.getElementById('errbanner');
  if(state.error){err.textContent='⚠️ '+state.error;err.className='err on';}else err.className='err';

  // header stats
  const a=analytics;
  const ptHeader = paperTrades.resolved
    ? `${paperTrades.win_rate}% <span style="font-size:11px;opacity:.7">(${paperTrades.won}W/${paperTrades.lost}L)</span>`
    : `<span style="font-size:12px;opacity:.7">accumulating\u2026</span>`;
  document.getElementById('hstats').innerHTML=
    `<div class="hstat">Kalshi: <b>${state.last_kalshi?state.last_kalshi.split(' ')[1]:'—'}</b></div>
     <div class="hstat">Poly: <b>${state.last_poly_pos?state.last_poly_pos.split(' ')[1]:'—'}</b></div>
     <div class="hstat" title="Baseline: unfiltered signals, no real money">Baseline win rate: <b style="color:var(--green)">${ptHeader}</b></div>
     <div class="hstat" title="Baseline: unfiltered signals, no real money">Paper PnL: <b style="color:${pnlC(paperTrades.total_pnl||0)}">${paperTrades.resolved?pnlS(paperTrades.total_pnl):'$0.00'}</b></div>`;

  if(state.config){
    document.getElementById('k-move').value=Math.round((state.config.kalshi_min_move||0.03)*100);
    document.getElementById('k-depth').value=state.config.kalshi_min_depth||1000;
    document.getElementById('p-top').value=state.config.poly_top||100;
    document.getElementById('p-mt').value=state.config.poly_min_traders||3;
    document.getElementById('p-total').value=state.config.poly_min_total||500;
    document.getElementById('p-dom').value=Math.round((state.config.poly_dominance||0.65)*100);
    document.getElementById('p-mom').value=Math.round((state.config.poly_min_momentum||0.08)*100);
    document.getElementById('p-maxp').value=Math.round((state.config.poly_max_price||0.35)*100);
  }
  render(); renderCal();
}

async function poll(){
  try{const r=await fetch('/api/state');state=await r.json();updateUI();}catch(e){}
  setTimeout(poll,5000);
}

async function triggerScan(){
  document.getElementById('scanbtn').disabled=true;
  try{await fetch('/api/scan_now',{method:'POST'});}catch(e){}
  state.scanning_kalshi=true;updateUI();
}

async function saveConfig(){
  const cfg={
    kalshi_min_move:parseFloat(document.getElementById('k-move').value),
    kalshi_min_depth:parseFloat(document.getElementById('k-depth').value),
    poly_top:parseInt(document.getElementById('p-top').value),
    poly_min_traders:parseInt(document.getElementById('p-mt').value),
    poly_min_total:parseFloat(document.getElementById('p-total').value),
    poly_dominance:parseFloat(document.getElementById('p-dom').value),
    poly_min_momentum:parseFloat(document.getElementById('p-mom').value),
    poly_max_price:parseFloat(document.getElementById('p-maxp').value),
  };
  await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
  await fetch('/api/scan_now',{method:'POST'});
  state.scanning_kalshi=true;updateUI();
}

// init analytics so header shows something
fetchAnalytics();
fetchRecentSignals();
poll();
</script></body></html>"""



if __name__ == "__main__":
    db_url = os.environ.get("DATABASE_URL","")
    if db_url:
        print(f"✅ PostgreSQL configured")
    else:
        print(f"ℹ️  Using local SQLite")
    if os.environ.get("TELEGRAM_BOT_TOKEN"):
        print(f"✅ Telegram configured. Chat: {os.environ.get('TELEGRAM_CHAT_ID','')}")
    if os.environ.get("FRED_API_KEY"):
        print(f"✅ FRED API configured")
    print(f"Starting PolySignal → http://localhost:{PORT}")
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=poll_loop, args=(_st, handle_cmd), daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)
