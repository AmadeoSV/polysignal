"""
polymarket.py — Polymarket smart money scanner.
Fetches top trader leaderboard and finds consensus positions.
"""
from __future__ import annotations
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

POLY_API = "https://data-api.polymarket.com"


def get_json(url, params=None, retries=3, pause=0.7):
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url,
                params={k:v for k,v in (params or {}).items() if v is not None},
                headers={"Accept":"application/json"}, timeout=20)
            if r.status_code == 429: time.sleep(pause*(attempt+2)); continue
            r.raise_for_status(); return r.json()
        except Exception as e:
            last = e; time.sleep(pause*(attempt+1))
    raise RuntimeError(f"GET {url} failed: {last}")

def af(x, d=0.0):
    try: return float(x) if x not in (None,"") else d
    except: return d

def ai(x, d=0):
    try: return int(float(x)) if x not in (None,"") else d
    except: return d

def fp(row, keys, default=None):
    for k in keys:
        if k in row and row[k] not in (None,""): return row[k]
    return default

def ts_label(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC") if ts else "n/a"

def rank_weight(rank: int, top: int) -> float:
    return max(0.01, 1.0 - (rank-1)/top)


def fetch_leaderboard(top: int, time_period="MONTH", sleep=0.2) -> List[dict]:
    traders, offset, seen = [], 0, set()
    while len(traders) < top:
        limit = min(50, top - len(traders))
        data  = get_json(f"{POLY_API}/v1/leaderboard", {
            "timePeriod": time_period,
            "orderBy": "PNL",
            "limit": limit,
            "offset": offset,
        })
        if not data: break
        for row in data:
            w = fp(row, ["proxyWallet","wallet","address","user"])
            if not w or str(w).lower() in seen: continue
            seen.add(str(w).lower())
            traders.append({
                "rank":     ai(row.get("rank"), len(traders)+1),
                "wallet":   str(w),
                "username": str(fp(row, ["userName","username","name"], str(w)[:10])),
                "pnl":      af(fp(row, ["pnl","profit"])),
                "volume":   af(fp(row, ["vol","volume"])),
            })
        offset += limit; time.sleep(sleep)
        if len(data) < limit: break
    return traders[:top]


def fetch_positions(wallet: str) -> List[dict]:
    return get_json(f"{POLY_API}/positions", {"user":wallet,"limit":500}) or []


def fetch_trades(wallet: str, limit=100) -> List[dict]:
    return get_json(f"{POLY_API}/trades", {"user":wallet,"limit":limit}) or []


def market_key(row: dict) -> Tuple[str,str]:
    cid = str(fp(row, ["conditionId","condition_id","market","slug"], "UNKNOWN"))
    out = str(fp(row, ["outcome","side","name"], "UNKNOWN"))
    return cid, out


def is_buy(trade: dict) -> bool:
    side = str(fp(trade, ["side","type","action"], "")).upper()
    if side in {"BUY","BOUGHT","LONG"}:  return True
    if side in {"SELL","SOLD","SHORT"}: return False
    return False


def trade_value(tr: dict) -> float:
    val = af(fp(tr, ["value","notional","usdValue"]))
    if val > 0: return val
    return af(fp(tr,["size","amount","shares"])) * af(fp(tr,["price","avgPrice"]),1.0)


def summarize(kind, key, meta, entries, opp, top_count, min_traders) -> dict:
    wallets     = {e["wallet"].lower() for e in entries}
    opp_w       = {e["wallet"].lower() for e in opp}
    total       = sum(e.get("value",0) for e in entries)
    opp_val     = sum(e.get("value",0) for e in opp)
    weighted    = sum(e.get("value",0)*rank_weight(e["rank"],top_count) for e in entries)
    total_w     = weighted + sum(e.get("value",0)*rank_weight(e["rank"],top_count) for e in opp)
    dominance   = weighted/total_w if total_w else 0
    avg_entry   = sum(e.get("avgPrice",0)*e.get("value",0) for e in entries)/total if total else 0
    newest      = max((e.get("timestamp",0) for e in entries), default=0)
    score       = len(wallets)*1000+total-0.65*opp_val-400*len(opp_w)
    strength    = min(5, max(1, len(wallets)-min_traders+2))
    momentum    = (meta.get("curPrice",0) - avg_entry)
    upside      = 1.0 - meta.get("curPrice",0)

    # Deduplicated top traders
    seen_w, top8 = [], []
    for e in sorted(entries, key=lambda e: e.get("value",0)*rank_weight(e["rank"],top_count), reverse=True)[:20]:
        wl = e["wallet"].lower()
        if wl not in seen_w: seen_w.append(wl); top8.append(e)
        if len(top8)==8: break

    slug = meta.get("eventSlug") or meta.get("slug","")
    return {
        "kind": kind, "score": round(score,2), "strength": strength,
        "dominance": round(dominance,3), "traders": len(wallets),
        "oppositeTraders": len(opp_w), "totalValue": round(total,2),
        "oppositeValue": round(opp_val,2), "avgEntry": round(avg_entry,4),
        "curPrice": round(meta.get("curPrice",0),4),
        "momentum": round(momentum,4), "upside": round(upside,4),
        "outcome": key[1], "title": meta.get("title",""),
        "slug": meta.get("slug",""), "eventSlug": slug,
        "endDate": meta.get("endDate",""), "conditionId": key[0],
        "newestTs": newest, "newestLabel": ts_label(newest),
        "market_url": f"https://polymarket.com/event/{slug}" if slug else "",
        "sig_key": f"P:{key[0]}:{key[1]}:{kind}",
        "topTraders": [
            {"rank":e["rank"],"username":e["username"],
             "value":round(e.get("value",0),0),"avgPrice":round(e.get("avgPrice",0),3)}
            for e in top8
        ],
    }


def build_signals(raw, meta, kind, cfg, top_count) -> List[dict]:
    by_market = defaultdict(lambda: defaultdict(list))
    for (cid,out), entries in raw.items():
        by_market[cid][out].extend(entries)

    results = []
    for cid, sides in by_market.items():
        side_stats = {}
        for out, entries in sides.items():
            seen_w, uniq = set(), []
            for e in entries:
                wl = e["wallet"].lower()
                if wl not in seen_w: seen_w.add(wl); uniq.append(e)
            total    = sum(e["value"] for e in uniq)
            weighted = sum(e["value"]*rank_weight(e["rank"],top_count) for e in uniq)
            avg      = sum(e.get("avgPrice",0)*e["value"] for e in uniq)/total if total else 0
            newest   = max((e.get("timestamp",0) for e in uniq), default=0)
            side_stats[out] = {"entries":uniq,"traders":len(seen_w),
                               "rawValue":total,"weightedVal":weighted,
                               "avgEntry":avg,"newest":newest}

        total_w = sum(s["weightedVal"] for s in side_stats.values())
        if not total_w: continue
        dom_out   = max(side_stats, key=lambda o: side_stats[o]["weightedVal"])
        dom       = side_stats[dom_out]
        dominance = dom["weightedVal"] / total_w
        m         = next((meta[k] for k in meta if k[0]==cid), {})
        cur       = m.get("curPrice",0)
        momentum  = cur - dom["avgEntry"]
        upside    = 1.0 - cur

        if dom["traders"]  < cfg["poly_min_traders"]: continue
        if dom["rawValue"] < cfg["poly_min_total"]:   continue
        if dominance       < cfg["poly_dominance"]:   continue
        if kind == "OPEN_POSITION":
            if momentum < cfg["poly_min_momentum"]: continue
            if cur > cfg["poly_max_price"]:         continue
            if cur <= 0:                            continue

        opp = [e for o,s in side_stats.items() if o!=dom_out for e in s["entries"]]
        results.append(summarize(kind,(cid,dom_out),m,dom["entries"],opp,
                                 top_count,cfg["poly_min_traders"]))
    results.sort(key=lambda r:(r["traders"],r["score"]),reverse=True)
    return results


def scan_positions(traders, cfg) -> List[dict]:
    raw = defaultdict(list); meta = {}
    for t in traders:
        try: positions = fetch_positions(t["wallet"])
        except: continue
        for pos in positions:
            val  = af(fp(pos,["currentValue","value","marketValue"]))
            size = af(fp(pos,["size","shares","balance"]))
            if val < cfg["poly_min_value"] or size <= 0: continue
            key  = market_key(pos)
            raw[key].append({
                "wallet":t["wallet"],"rank":t["rank"],"username":t["username"],
                "value":val,"size":size,
                "avgPrice":af(fp(pos,["avgPrice","averagePrice","price"])),
                "timestamp":ai(fp(pos,["timestamp","updatedAt","createdAt"])),
            })
            meta.setdefault(key,{
                "title":    str(fp(pos,["title","question","slug"],"")),
                "slug":     str(fp(pos,["slug","marketSlug"],"")),
                "eventSlug":str(pos.get("eventSlug") or ""),
                "endDate":  str(pos.get("endDate") or ""),
                "curPrice": af(fp(pos,["curPrice","price","currentPrice"])),
            })
        time.sleep(0.2)
    return build_signals(raw, meta, "OPEN_POSITION", cfg, len(traders))


def scan_live(traders, cfg) -> List[dict]:
    import time as _time
    cutoff = int(_time.time()) - cfg["poly_window_min"]*60
    raw = defaultdict(list); meta = {}
    for t in traders:
        try: trades = fetch_trades(t["wallet"])
        except: continue
        for tr in trades:
            ts = ai(fp(tr,["timestamp","createdAt","time"]),0)
            if ts and ts < cutoff: continue
            if not is_buy(tr): continue
            val = trade_value(tr)
            if val < cfg["poly_min_value"]: continue
            key   = market_key(tr)
            price = af(fp(tr,["price","avgPrice"]))
            raw[key].append({
                "wallet":t["wallet"],"rank":t["rank"],"username":t["username"],
                "value":val,"size":af(fp(tr,["size","amount","shares"])),
                "avgPrice":price,"timestamp":ts,
            })
            meta.setdefault(key,{
                "title":    str(fp(tr,["title","question","slug"],"")),
                "slug":     str(fp(tr,["slug","marketSlug"],"")),
                "eventSlug":str(tr.get("eventSlug") or ""),
                "endDate":  str(tr.get("endDate") or ""),
                "curPrice": price,
            })
        time.sleep(0.2)
    return build_signals(raw, meta, "LIVE_BUY", cfg, len(traders))