"""
telegram_bot.py — Telegram bot polling and command handling.
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


def _parse_days(end_date: str) -> Optional[int]:
    """Return days until end_date, or None if unparseable."""
    if not end_date:
        return None
    try:
        target = datetime.strptime(end_date[:10], "%Y-%m-%d")
        today  = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        return (target - today).days
    except:
        return None


def _time_horizon(end_date: str) -> str:
    """
    Classify horizon as short or long.
    Sports games resolve same-day or next-day — treat anything <= 1 day as short.
    Use <= 3 as a wider short-term net to catch evening games with end dates
    set to tomorrow UTC.
    """
    days = _parse_days(end_date)
    if days is None:
        return "long"
    return "short" if days <= 3 else "long"


def _horizon_label(end_date: str) -> str:
    """
    Human-readable label for how much time is left.
    """
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
    up  = c["direction"] == "UP"
    icon = "\U0001f7e2\U0001f7e2" if up else "\U0001f534\U0001f534"
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

    stars   = "\u25cf"*strength + "\u25cb"*(5-strength)
    timing  = ("\u26a1 SHORT-TERM \u2014 resolves soon" if hor_t=="short"
               else "\U0001f4c8 LONG-TERM \u2014 more time to enter")
    mom_line= (f"\U0001f4c8 Up +{mom:.1f}\u00a2 since smart money entered"
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


def poll_loop(state_ref: dict, handle_cmd_fn):
    """Run in a background thread — polls for commands."""
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
