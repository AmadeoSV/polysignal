"""
telegram_bot.py — Telegram bot polling, command handling, and alert formatting.
"""
from __future__ import annotations
import os, threading, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN","")
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID","")
_tg_offset = 0


def tg_send(text: str, chat_id: str="", buttons=None):
    if not TG_TOKEN: return
    target  = chat_id or TG_CHAT
    if not target: return
    payload = {
        "chat_id": target, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True
    }
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": [buttons]}
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json=payload, timeout=10
        )
    except: pass


def tg_get_updates() -> List[dict]:
    global _tg_offset
    if not TG_TOKEN: return []
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates",
            params={"offset": _tg_offset, "timeout": 10}, timeout=15
        )
        updates = r.json().get("result",[])
        if updates: _tg_offset = updates[-1]["update_id"] + 1
        return updates
    except: return []


# ── Date helpers ───────────────────────────────────────────────────────────────
def _parse_days(end_date: str) -> Optional[int]:
    if not end_date:
        return None
    try:
        target = datetime.strptime(end_date[:10], "%Y-%m-%d")
        today  = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        return (target - today).days
    except:
        return None


def _time_horizon(end_date: str) -> str:
    days = _parse_days(end_date)
    if days is None:
        return "long"
    return "short" if days <= 3 else "long"


def _horizon_label(end_date: str) -> str:
    days = _parse_days(end_date)
    if days is None:
        return ""
    if days <= 0:
        return "Resolves TODAY"
    if days == 1:
        return "Resolves TOMORROW"
    if days <= 3:
        return f"{days} days left (short-term)"
    return f"{days} days left"


# ── Alert formatters ───────────────────────────────────────────────────────────
def format_kalshi_alert(s: dict) -> str:
    up     = s["direction"] == "UP"
    icon   = "\U0001f7e2" if up else "\U0001f534"
    action = "LARGE BUY" if up else "LARGE SELL"
    move   = round(s["move_abs"]*100, 1)
    prev   = round(s["prev_price"]*100, 1)
    cur    = round(s["cur_price"]*100, 1)
    upside = round((1-s["cur_price"])*100, 1)
    arrow  = "\U0001f4c8" if up else "\U0001f4c9"
    hor    = _horizon_label(s.get("end_date",""))
    hor_t  = _time_horizon(s.get("end_date",""))
    lines  = [
        f"{icon} <b>KALSHI \u2014 {action}</b>",
        "\u2501"*20,
        f"<b>{s['title']}</b>",
        "",
        f"{arrow} YES: <b>{prev}\u00a2 \u2192 {cur}\u00a2</b>  ({'+' if up else ''}{move}\u00a2 move)",
        f"\U0001f4b0 Order depth: ~<b>${s['depth']:,.0f}</b>",
        f"\U0001f3af Upside remaining: <b>{upside}\u00a2</b>",
    ]
    if hor:
        lines.append(f"\u23f0 {hor} \u2014 {'Short-term' if hor_t=='short' else 'Long-term'}")
    lines.append(f"\n<a href=\"{s['url']}\">View on Kalshi \u2197</a>")
    return "\n".join(lines)


def format_cluster_alert(c: dict) -> str:
    up    = c["direction"] == "UP"
    icon  = "\U0001f7e2\U0001f7e2" if up else "\U0001f534\U0001f534"
    arrow = "\U0001f4c8" if up else "\U0001f4c9"
    hor   = _horizon_label(c.get("end_date",""))
    hor_t = _time_horizon(c.get("end_date",""))
    lines = [
        f"{icon} <b>KALSHI \u2014 REPEATED {'BUY' if up else 'SELL'} CLUSTER</b>",
        "\u2501"*20,
        f"<b>{c['title']}</b>",
        "",
        f"\u26a0\ufe0f <b>{c['count']} separate large orders</b> same direction in {c['span_min']} min",
        f"Combined depth: ~<b>${c['combined']:,.0f}</b>",
        f"{arrow} Direction: <b>{'YES buying' if up else 'YES selling'}</b>",
        f"\U0001f3af Current: <b>{round(c['cur_price']*100,1)}\u00a2</b>",
        "",
        "\U0001f9e0 Stronger signal than a single order \u2014 multiple actors agree.",
    ]
    if hor:
        lines.append(f"\u23f0 {hor} \u2014 {'Short-term' if hor_t=='short' else 'Long-term'}")
    lines.append(f"\n<a href=\"{c['url']}\">View on Kalshi \u2197</a>")
    return "\n".join(lines)


def format_poly_alert(r: dict) -> str:
    is_live   = r.get("kind") == "LIVE_BUY"
    dom       = round(r.get("dominance",0)*100)
    traders   = r.get("traders",0)
    avg_entry = round(r.get("avgEntry",0)*100,1)
    cur_price = round(r.get("curPrice",0)*100,1)
    upside    = round(r.get("upside",0)*100,1)
    mom       = (r.get("curPrice",0) - r.get("avgEntry",0))*100
    outcome   = r.get("outcome","")
    strength  = r.get("strength",1)
    end_date  = r.get("endDate","") or r.get("end_date","")
    hor       = _horizon_label(end_date)
    hor_t     = _time_horizon(end_date)
    url       = r.get("market_url","") or r.get("url","")
    opp_pct   = round(r.get("oppositeValue",0)/max(r.get("totalValue",1),1)*100)

    if dom>=85:   cons="very strong"
    elif dom>=70: cons="strong"
    else:         cons="moderate"

    if is_live:
        header = "\u26a1 POLYMARKET \u2014 LIVE BUY CLUSTER"
        sub    = f"Top traders just bought <b>{outcome}</b> in last 30 min"
    else:
        header = "\U0001f4ca POLYMARKET \u2014 SMART MONEY POSITION"
        sub    = f"Top traders holding <b>{outcome}</b>"

    stars    = "\u25cf"*strength + "\u25cb"*(5-strength)
    timing   = ("\u26a1 SHORT-TERM \u2014 resolves soon" if hor_t=="short"
                else "\U0001f4c8 LONG-TERM \u2014 more time to enter")
    mom_line = (f"\U0001f4c8 Up +{mom:.1f}\u00a2 since smart money entered"
                if mom>0 else f"\U0001f4c9 Down {mom:.1f}\u00a2 since entry")

    lines = [
        f"<b>{header}</b>",
        "\u2501"*20,
        f"<b>{r.get('title','')}</b>",
        sub, "",
        f"\U0001f465 <b>{traders} top traders</b> | {cons} consensus ({dom}%)",
        f"Signal strength: {stars}", "",
        f"\U0001f4b5 Avg entry: <b>{avg_entry}\u00a2</b>  \u2192  Now: <b>{cur_price}\u00a2</b>",
        mom_line,
        f"\U0001f3af Upside remaining: <b>{upside}\u00a2</b>",
        "", timing,
    ]
    if hor: lines.append(f"\u23f0 {hor}")
    if opp_pct > 20:
        lines.append(f"\n\u26a0\ufe0f {r.get('oppositeTraders',0)} traders opposite ({opp_pct}% of value)")
    lines.append(f"\n<a href=\"{url}\">View on Polymarket \u2197</a>")
    return "\n".join(lines)


# ── Command formatters ─────────────────────────────────────────────────────────
def format_cmd_help() -> str:
    lines = [
        "\U0001f916 <b>PolySignal Commands</b>",
        "\u2501"*20,
        "/brief \u2014 Morning brief on demand",
        "/signals \u2014 Top active signals (all platforms)",
        "/kalshi \u2014 Recent Kalshi order flow signals",
        "/poly \u2014 Top Polymarket smart money positions",
        "/trades \u2014 Open trades and current PnL",
        "/stats \u2014 Signal accuracy and outcome summary",
        "/next \u2014 Next economic release or Fed event",
        "/help \u2014 This message",
    ]
    return "\n".join(lines)


def format_cmd_brief(state_ref: dict) -> str:
    from database import db_analytics, db_get_signals
    a      = db_analytics()
    sigs   = db_get_signals(limit=200)
    active = [s for s in sigs if s.get("outcome") is None]
    k_sigs = [s for s in active if s["platform"]=="kalshi"]
    p_sigs = [s for s in active if s["platform"]=="polymarket"]

    top_poly = state_ref.get("poly_positions",[])[:4]
    top_k    = state_ref.get("kalshi_signals",[])[:3]

    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines = [
        f"\u2600\ufe0f <b>PolySignal Brief</b> \u2014 {now_str}",
        "\u2501"*20,
        f"Signals active: <b>{len(active)}</b> ({len(k_sigs)} Kalshi, {len(p_sigs)} Polymarket)",
        f"Open trades: <b>{a['open_trades']}</b> | PnL: <b>${a['total_pnl']:+.2f}</b>",
        "",
    ]
    if top_poly:
        lines.append("<b>\U0001f4ca Top Polymarket positions:</b>")
        for r in top_poly:
            dom  = round(r.get("dominance",0)*100)
            mom  = round(r.get("momentum",0)*100,1)
            icon = "\U0001f7e2" if dom>=80 else "\U0001f7e1"
            lines.append(f"{icon} {r.get('title','')[:45]} | {r.get('traders',0)} traders, {dom}% | +{mom}\u00a2")
        lines.append("")
    if top_k:
        lines.append("<b>\u26a1 Recent Kalshi signals:</b>")
        for s in top_k:
            up   = s.get("direction")=="UP"
            icon = "\U0001f7e2" if up else "\U0001f534"
            move = round(s.get("move_abs",0)*100,1)
            lines.append(f"{icon} {s.get('title','')[:45]} | {'+' if up else ''}{move}\u00a2")
    return "\n".join(lines)


def format_cmd_signals(sigs: list) -> str:
    active = [s for s in sigs if s.get("outcome") is None]
    if not active:
        return "No active signals right now."
    active.sort(key=lambda s: s.get("detected_at") or "", reverse=True)
    lines = [
        f"\U0001f4e1 <b>Active Signals</b> ({len(active)} total)",
        "\u2501"*20,
    ]
    for s in active[:8]:
        platform = s.get("platform","")
        title    = (s.get("market_title") or s.get("title") or s.get("ticker",""))[:50]
        sig_type = s.get("signal_type","")
        icon     = "\u26a1" if platform=="kalshi" else "\U0001f4ca"
        lines.append(f"{icon} <b>{title}</b>")
        lines.append(f"   {platform.upper()} | {sig_type}")
        lines.append("")
    if len(active) > 8:
        lines.append(f"<i>...and {len(active)-8} more.</i>")
    return "\n".join(lines)


def format_cmd_kalshi(sigs: list) -> str:
    k_sigs = [s for s in sigs
              if s.get("platform")=="kalshi" and s.get("outcome") is None]
    k_sigs.sort(key=lambda s: s.get("detected_at") or "", reverse=True)
    if not k_sigs:
        return "\u26a1 No active Kalshi signals right now. Market is quiet."
    lines = [
        f"\u26a1 <b>Kalshi Order Flow</b> ({len(k_sigs)} active)",
        "\u2501"*20,
    ]
    for s in k_sigs[:6]:
        title = (s.get("market_title") or s.get("ticker",""))[:50]
        stype = s.get("signal_type","")
        up    = stype == "UP"
        icon  = "\U0001f7e2" if up else "\U0001f534"
        lines.append(f"{icon} <b>{title}</b>")
        lines.append(f"   Direction: {stype}")
        lines.append("")
    if len(k_sigs) > 6:
        lines.append(f"<i>...and {len(k_sigs)-6} more.</i>")
    return "\n".join(lines)


def format_cmd_poly(sigs: list, state_ref: dict) -> str:
    top_poly = state_ref.get("poly_positions",[])
    if not top_poly:
        p_sigs = [s for s in sigs
                  if s.get("platform")=="polymarket" and s.get("outcome") is None]
        if not p_sigs:
            return "\U0001f4ca No active Polymarket positions right now."
        lines = [f"\U0001f4ca <b>Polymarket Smart Money</b> ({len(p_sigs)} active)", "\u2501"*20]
        for s in p_sigs[:6]:
            title = (s.get("market_title") or "")[:50]
            lines.append(f"\u2022 <b>{title}</b>")
            lines.append(f"   {s.get('signal_type','')} signal")
            lines.append("")
        return "\n".join(lines)

    lines = [
        f"\U0001f4ca <b>Polymarket Smart Money</b> ({len(top_poly)} positions)",
        "\u2501"*20,
    ]
    for r in top_poly[:6]:
        dom     = round(r.get("dominance",0)*100)
        traders = r.get("traders",0)
        avg_e   = round(r.get("avgEntry",0)*100,1)
        cur_p   = round(r.get("curPrice",0)*100,1)
        mom     = round((r.get("curPrice",0)-r.get("avgEntry",0))*100,1)
        upside  = round(r.get("upside",0)*100,1)
        outcome = r.get("outcome","")
        title   = r.get("title","")[:48]
        icon    = "\U0001f7e2" if dom>=80 else "\U0001f7e1"
        mom_str = f"+{mom}\u00a2" if mom>=0 else f"{mom}\u00a2"
        url     = r.get("market_url","")
        lines.append(f"{icon} <b>{title}</b>")
        lines.append(f"   Side: <b>{outcome}</b> | {traders} traders | {dom}% consensus")
        lines.append(f"   Entry: {avg_e}\u00a2 \u2192 Now: {cur_p}\u00a2 ({mom_str}) | Upside: {upside}\u00a2")
        if url:
            lines.append(f"   <a href=\"{url}\">View \u2197</a>")
        lines.append("")
    return "\n".join(lines)


def format_cmd_trades(a: dict) -> str:
    lines = [
        "\U0001f4bc <b>Trades Summary</b>",
        "\u2501"*20,
        f"Open trades: <b>{a.get('open_trades',0)}</b>",
        f"Closed trades: <b>{a.get('closed_trades',0)}</b>",
        f"Total PnL: <b>${a.get('total_pnl',0):+.2f}</b>",
    ]
    if a.get('win_rate') is not None:
        lines.append(f"Win rate: <b>{a.get('win_rate',0):.0%}</b>")
    lines += ["", "<i>Log trades on the dashboard to track them here.</i>"]
    return "\n".join(lines)


def format_cmd_stats(sigs: list, a: dict) -> str:
    total    = len(sigs)
    resolved = [s for s in sigs if s.get("outcome") in ("WON","LOST")]
    won      = [s for s in resolved if s.get("outcome")=="WON"]
    lost     = [s for s in resolved if s.get("outcome")=="LOST"]
    pending  = [s for s in sigs if s.get("outcome") is None]
    win_rate = len(won)/len(resolved)*100 if resolved else 0

    k_res = [s for s in resolved if s.get("platform")=="kalshi"]
    k_won = [s for s in k_res if s.get("outcome")=="WON"]
    p_res = [s for s in resolved if s.get("platform")=="polymarket"]
    p_won = [s for s in p_res if s.get("outcome")=="WON"]

    lines = [
        "\U0001f4c8 <b>Signal Stats</b>",
        "\u2501"*20,
        f"Total signals: <b>{total}</b>",
        f"Resolved: <b>{len(resolved)}</b> | Pending: <b>{len(pending)}</b>",
        f"Won: <b>{len(won)}</b> | Lost: <b>{len(lost)}</b>",
        f"Overall win rate: <b>{win_rate:.1f}%</b>",
        "",
    ]
    if k_res:
        k_wr = len(k_won)/len(k_res)*100
        lines.append(f"\u26a1 Kalshi: {len(k_won)}/{len(k_res)} ({k_wr:.1f}%)")
    if p_res:
        p_wr = len(p_won)/len(p_res)*100
        lines.append(f"\U0001f4ca Polymarket: {len(p_won)}/{len(p_res)} ({p_wr:.1f}%)")
    lines += [
        "",
        f"Open trades: <b>{a.get('open_trades',0)}</b> | PnL: <b>${a.get('total_pnl',0):+.2f}</b>",
        "",
        "<i>Win rate builds as more signals resolve over time.</i>",
    ]
    return "\n".join(lines)


def format_cmd_next(events: list) -> str:
    if not events:
        return "\U0001f4c5 No upcoming economic events found. FRED API may be unavailable."
    lines = [
        "\U0001f4c5 <b>Upcoming Economic Events</b>",
        "\u2501"*20,
    ]
    for e in events[:6]:
        imp  = e.get("importance","")
        icon = "\U0001f534" if imp=="high" else "\U0001f7e1"
        lines.append(f"{icon} <b>{e['label']}</b>")
        lines.append(f"   {e['date']} at {e['time']}")
        lines.append("")
    return "\n".join(lines)


# ── Poll loop ──────────────────────────────────────────────────────────────────
def poll_loop(state_ref: dict, handle_cmd_fn):
    if not TG_TOKEN:
        print("No Telegram token — bot polling disabled.")
        return
    print("Telegram bot polling started.")
    while True:
        try:
            for upd in tg_get_updates():
                msg = upd.get("message") or upd.get("edited_message")
                if msg and msg.get("text","").startswith("/"):
                    threading.Thread(
                        target=handle_cmd_fn,
                        args=(msg["text"], str(msg["chat"]["id"])),
                        daemon=True
                    ).start()
        except: pass
        time.sleep(1)
