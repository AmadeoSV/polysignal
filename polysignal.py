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

# Local modules
from database import (engine, db_save_signal, db_get_signals, db_get_trades,
                      db_add_trade, db_close_trade, db_analytics, db_cleanup,
                      db_size_mb, Session, Trade)
import kalshi as kal
import polymarket as poly
from signals import (check_new_signals, check_cluster_alert, fetch_fred_events,
                     update_open_trade_prices, check_signal_outcomes, send_morning_brief,
                     seed_seen_signals, update_price_history)

# Seed seen signals from DB on startup
seed_seen_signals()
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
    "poly_max_price":    0.80,
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
        for r in rows:
            r["sig_key"] = f"P:{r['conditionId']}:{r['outcome']}:LIVE_BUY"
            db_save_signal(r, "polymarket")
        check_new_signals(rows, "polymarket")
        with _lock:
            _st["poly_live"]          = rows
            _st["last_poly_live"]     = utcnow_s()
            _st["scanning_poly_live"] = False
    except Exception as e:
        with _lock: _st["scanning_poly_live"] = False


def scheduler():
    nk = npp = npl = nph = time.time()
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
            nph = now + 3600  # every hour
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
def index():
    return render_template_string(HTML)

@app.route("/api/state")
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
def api_signals():
    return jsonify(db_get_signals(100, freq.args.get("platform")))

@app.route("/api/trades")
def api_trades():
    return jsonify(db_get_trades(freq.args.get("status"), freq.args.get("platform")))

@app.route("/api/trades", methods=["POST"])
def api_add_trade():
    return jsonify(db_add_trade(freq.get_json(force=True) or {}))

@app.route("/api/trades/<int:tid>/close", methods=["POST"])
def api_close_trade(tid):
    data = freq.get_json(force=True) or {}
    return jsonify(db_close_trade(tid, float(data.get("exit_price",0)), data.get("notes","")))

@app.route("/api/analytics")
def api_analytics():
    return jsonify(db_analytics())

@app.route("/api/scan_now", methods=["POST"])
def api_scan_now():
    threading.Thread(target=run_kalshi_scan,    daemon=True).start()
    threading.Thread(target=run_poly_positions, daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/api/config", methods=["POST"])
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
.chart-wrap{background:var(--surf);border-radius:8px;padding:14px;margin-bottom:12px;height:180px}
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
      <div class="field"><label>Max price (¢)</label><input id="p-maxp" type="number" min="10" max="95" value="80"></div>
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
let sigs_db=[], trades_db=[], analytics={};
let calY=new Date().getFullYear(), calM=new Date().getMonth(), selDay=null;
let charts={};

const TABS=['home','kalshi','polymarket','trades','analytics'];

function showTab(t) {
  tab=t;
  TABS.forEach(x=>document.getElementById('tab-'+x).classList.toggle('active',x===t));
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
function renderHome() {
  const a=analytics; const sc=state.scanning_kalshi||state.scanning_poly_pos||state.scanning_poly_live;
  const kbuys=state.kalshi_signals.filter(s=>s.direction==='UP').length;
  const ksells=state.kalshi_signals.filter(s=>s.direction==='DOWN').length;
  const penter=state.poly_positions.filter(r=>r.recommendation==='ENTER'||r.traders>=3).length;

  let html=`<div class="summary">
    <div class="scard"><div class="sv" style="color:var(--amber)">${state.kalshi_signals.length}</div><div class="sl">Kalshi signals</div></div>
    <div class="scard"><div class="sv" style="color:var(--blue)">${state.poly_positions.length}</div><div class="sl">Poly positions</div></div>
    <div class="scard"><div class="sv" style="color:var(--green)">${a.open_trades||0}</div><div class="sl">Open trades</div></div>
    <div class="scard"><div class="sv" style="color:${pnlC(a.total_pnl)}">${pnlS(a.total_pnl)}</div><div class="sl">Total PnL</div></div>
  </div>`;

  // top kalshi signals
  const ksigs=state.kalshi_signals.slice(0,3);
  if(ksigs.length){
    html+=`<div class="sec-title">⚡ Latest Kalshi signals</div><div class="grid">`;
    ksigs.forEach(s=>{html+=renderKalshiCard(s,true);});
    html+=`</div><div style="margin-bottom:14px"></div>`;
  }

  // top poly positions
  const psigs=state.poly_positions.slice(0,3);
  if(psigs.length){
    html+=`<div class="sec-title">📊 Top Polymarket positions</div><div class="grid">`;
    psigs.forEach(r=>{html+=renderPolyCard(r,true);});
    html+=`</div>`;
  }

  if(!ksigs.length && !psigs.length)
    html+=`<div class="empty"><h3>${sc?'Scanning…':'No signals yet'}</h3>
      <p>${sc?'Both scanners running. Check back in a minute.':'Hit Scan to start.'}</p></div>`;
  return html;
}

// ── Kalshi tab ────────────────────────────────────────────────────────────────
function renderKalshi() {
  const rows=liveFilter==='up'?state.kalshi_signals.filter(s=>s.direction==='UP')
            :liveFilter==='down'?state.kalshi_signals.filter(s=>s.direction==='DOWN')
            :state.kalshi_signals;
  const buys=state.kalshi_signals.filter(s=>s.direction==='UP').length;
  const sells=state.kalshi_signals.filter(s=>s.direction==='DOWN').length;
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
  if(!rows.length) html+=`<div class="empty"><h3>${state.scanning_kalshi?'Scanning…':'No signals'}</h3>
    <p>Lower min move or depth in filters.</p></div>`;
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
  const rows = polyTab==='live' ? state.poly_live : state.poly_positions;
  const cats = [...new Set(rows.map(r=>r.category||'').filter(Boolean))].sort();

  const filtered = polyFilter==='all' ? rows : rows.filter(r=>(r.category||'').toLowerCase()===polyFilter.toLowerCase());

  let html = `<div class="frow">
    <div class="fbtn ${polyTab==='positions'?'on':''}" onclick="polyTab='positions';render()">📊 Open Positions</div>
    <div class="fbtn ${polyTab==='live'?'on':''}"      onclick="polyTab='live';render()">⚡ Live Buys</div>
  </div>
  <div class="summary">
    <div class="scard"><div class="sv" style="color:var(--blue)">${state.poly_positions.length}</div><div class="sl">Positions</div></div>
    <div class="scard"><div class="sv" style="color:var(--amber)">${state.poly_live.length}</div><div class="sl">Live buys</div></div>
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
  if(!filtered.length) html+=`<div class="empty"><h3>No signals</h3><p>Lower filters or wait for next scan.</p></div>`;
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
  const url   = slug?`https://polymarket.com/event/${slug}`:'';
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

  const strengthBar = '●'.repeat(r.strength||1)+'○'.repeat(5-(r.strength||1));

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
      <span style="font-size:12px"><b>${r.traders}</b> top traders agree · strength ${strengthBar}</span>
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
    ${hor.label?`<div style="font-size:11px;color:${hor.color};padding:4px 8px;background:var(--surf);border-radius:4px;margin-bottom:6px">${hor.label} · ${hor.type==='short'?'<b>Short-term</b>':'<b>Long-term</b>'}</div>`:''}
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
  let html=`<div class="agrid">
    <div class="scard"><div class="sv" style="color:var(--amber)">${a.total_signals||0}</div><div class="sl">Total signals</div></div>
    <div class="scard"><div class="sv" style="color:var(--blue)">${a.total_trades||0}</div><div class="sl">Total trades</div></div>
    <div class="scard"><div class="sv" style="color:var(--green)">${a.win_rate||0}%</div><div class="sl">Trade win rate</div></div>
    <div class="scard"><div class="sv" style="color:${pnlC(a.total_pnl)}">${pnlS(a.total_pnl)}</div><div class="sl">Total PnL</div></div>
    <div class="scard" style="grid-column:span 2"><div class="sv" style="color:var(--purple);font-size:18px">${sigAcc}</div><div class="sl">🎯 Signal accuracy (auto-tracked)</div></div>
  </div>`;

  if(a.by_platform&&Object.keys(a.by_platform).length){
    html+=`<div class="pgrid">`;
    Object.entries(a.by_platform).forEach(([plat,d])=>{
      html+=`<div class="pcard">
        <div class="pcard-title">${plat.toUpperCase()}</div>
        <div style="font-size:12px;color:var(--muted)">Signals: <b style="color:var(--text)">${d.signals}</b></div>
        <div style="font-size:12px;color:var(--muted)">Trades: <b style="color:var(--text)">${d.trades}</b></div>
        <div style="font-size:12px;color:var(--muted)">PnL: <b style="color:${pnlC(d.pnl)}">${pnlS(d.pnl)}</b></div>
      </div>`;
    });
    html+=`</div>`;
  }

  html+=`<div class="chart-wrap"><canvas id="pnl-chart"></canvas></div>`;
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
  if(pnlEl&&a.pnl_series&&a.pnl_series.length){
    if(charts.pnl) charts.pnl.destroy();
    charts.pnl=new Chart(pnlEl,{type:'line',data:{
      labels:a.pnl_series.map(p=>p.date),
      datasets:[{data:a.pnl_series.map(p=>p.pnl),borderColor:'#22c55e',
        backgroundColor:'rgba(34,197,94,.1)',fill:true,tension:.4,pointRadius:2}]
    },options:{...opts,plugins:{...opts.plugins,title:{display:true,text:'Cumulative PnL ($)',color:'#7a7a8a',font:{size:12}}}}});
  }
  const sigEl=document.getElementById('sig-chart');
  if(sigEl&&a.signals_by_day&&a.signals_by_day.length){
    if(charts.sig) charts.sig.destroy();
    charts.sig=new Chart(sigEl,{type:'bar',data:{
      labels:a.signals_by_day.map(d=>d.date),
      datasets:[{data:a.signals_by_day.map(d=>d.count),backgroundColor:'rgba(59,130,246,.6)',borderRadius:3}]
    },options:{...opts,plugins:{...opts.plugins,title:{display:true,text:'Signals per day',color:'#7a7a8a',font:{size:12}}}}});
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
async function fetchAnalytics(){try{const r=await fetch('/api/analytics');analytics=await r.json();render();}catch(e){}}

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
  document.getElementById('hstats').innerHTML=
    `<div class="hstat">Kalshi: <b>${state.last_kalshi?state.last_kalshi.split(' ')[1]:'—'}</b></div>
     <div class="hstat">Poly: <b>${state.last_poly_pos?state.last_poly_pos.split(' ')[1]:'—'}</b></div>
     <div class="hstat">Win rate: <b style="color:var(--green)">${a.win_rate||0}%</b></div>
     <div class="hstat">PnL: <b style="color:${pnlC(a.total_pnl)}">${pnlS(a.total_pnl)}</b></div>`;

  if(state.config){
    document.getElementById('k-move').value=Math.round((state.config.kalshi_min_move||0.03)*100);
    document.getElementById('k-depth').value=state.config.kalshi_min_depth||1000;
    document.getElementById('p-top').value=state.config.poly_top||100;
    document.getElementById('p-mt').value=state.config.poly_min_traders||3;
    document.getElementById('p-total').value=state.config.poly_min_total||500;
    document.getElementById('p-dom').value=Math.round((state.config.poly_dominance||0.65)*100);
    document.getElementById('p-mom').value=Math.round((state.config.poly_min_momentum||0.08)*100);
    document.getElementById('p-maxp').value=Math.round((state.config.poly_max_price||0.80)*100);
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
poll();
</script></body></html>"""

if __name__=="__main__":
    if TG_TOKEN: print(f"✅ Telegram configured. Chat: {TG_CHAT}",file=sys.stderr)
    else:        print("ℹ️  No Telegram token.",file=sys.stderr)
    if FRED_KEY: print("✅ FRED API configured.",file=sys.stderr)
    else:        print("ℹ️  No FRED key — static calendar dates.",file=sys.stderr)
    print(f"Starting PolySignal Unified → http://localhost:{PORT}",file=sys.stderr)
    threading.Thread(target=scheduler,daemon=True).start()
    threading.Thread(target=tg_poll,daemon=True).start()
    app.run(host="0.0.0.0",port=PORT,debug=False)

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
