"""
database.py — SQLAlchemy models and all DB helper functions.
Supports both SQLite (local dev) and PostgreSQL (production).
"""
from __future__ import annotations
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import (create_engine, Column, Integer, Float, String,
                        Boolean, DateTime, Text, ForeignKey, func, or_)
from sqlalchemy.orm import DeclarativeBase, Session, relationship

# ── Engine ─────────────────────────────────────────────────────────────────────
# Railway sets DATABASE_URL automatically for PostgreSQL.
# Falls back to local SQLite for development.
_db_url = os.environ.get("DATABASE_URL", "")
if _db_url.startswith("postgres://"):
    # SQLAlchemy requires postgresql:// not postgres://
    _db_url = _db_url.replace("postgres://", "postgresql://", 1)

DB_PATH = os.path.expanduser("~/polysignal.db")
DATABASE_URL = _db_url or f"sqlite:///{DB_PATH}"

# Signals detected before this were potentially affected by the LIVE_BUY
# curPrice bug (price filter checked a stale/arbitrary trade price instead
# of the most recent one). Fix deployed and confirmed ACTIVE on Railway at
# 1:48 PM ET on July 24, 2026 -> 17:48 UTC (server stores UTC via
# datetime.utcnow()). Using 17:50 to give a couple minutes of buffer.
ACCURACY_FIX_CUTOFF = datetime(2026, 7, 24, 17, 50, 0)

# Keep this in sync with worker.py's poly_max_price config value. Was 0.45
# when this stat was first built; poly_max_price was tightened to 0.35
# later the same day based on the corrected edge analysis, and this
# constant went stale until caught and fixed here. If poly_max_price
# changes again, update this too — the "since fix" stat should always
# reflect what the live filter actually allows through, not a historical
# snapshot of it.
ACCURACY_PRICE_CAP = 0.35

engine = create_engine(
    DATABASE_URL,
    echo=False,
    # PostgreSQL connection pooling settings
    **({} if "sqlite" in DATABASE_URL else {
        "pool_size": 5,
        "max_overflow": 2,
        "pool_timeout": 30,
        "pool_recycle": 1800,
    })
)

# ── Models ─────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase): pass

class Signal(Base):
    __tablename__ = "signals"
    id                 = Column(Integer, primary_key=True)
    platform           = Column(String)
    ticker             = Column(String, index=True)
    market_title       = Column(Text)
    category           = Column(String)
    signal_type        = Column(String)
    move_size          = Column(Float)
    price_before       = Column(Float)
    price_after        = Column(Float)
    depth              = Column(Float)
    outcome            = Column(String, nullable=True)
    resolved_at        = Column(DateTime, nullable=True)  # when outcome was written -- added
                                                            # after finding no way to tell *when*
                                                            # a resolved signal's outcome was set,
                                                            # which mattered directly during the
                                                            # Kalshi finalized-status audit (2026-08-07)
    market_url         = Column(Text)
    market_close_time  = Column(String)
    platform_signal_id = Column(String, unique=True)
    detected_at        = Column(DateTime, default=datetime.utcnow)
    alert_sent_at      = Column(DateTime, nullable=True)
    hours_to_close     = Column(Float, nullable=True)  # hours between signal detection and market close
    trader_count       = Column(Integer, nullable=True) # number of top traders on same side
    dominance          = Column(Float, nullable=True)   # consensus % (0-1)
    signal_strength    = Column(Integer, nullable=True) # 1-5 stars
    opposite_traders   = Column(Integer, nullable=True) # traders on opposite side
    price_lookup_suspicious = Column(Boolean, nullable=True)
    # Flags Polymarket signals matching the fingerprint of a real bug found
    # 2026-09-04: build_signals() looked up a market's current price by
    # conditionId alone, not (conditionId, outcome), so it could silently
    # grab a DIFFERENT outcome's price on any multi-outcome market. That
    # bug is now fixed (see polymarket.py), but roughly 10% of historical
    # signals were logged before the fix. Its symptom is a price_after
    # that looks like the complement of the real price rather than a real
    # move, so price_before + price_after landing close to 1.00 is the
    # tell. Backfilled once for existing rows (see backfill_suspicious_
    # prices() below); new rows are never flagged since the root cause is
    # fixed at the source. Kalshi is never flagged -- its price_before/
    # price_after are the same contract at two points in time, not two
    # different outcomes, so this fingerprint doesn't apply there at all.
    trades             = relationship("Trade", back_populates="signal")

class Trade(Base):
    __tablename__ = "trades"
    id            = Column(Integer, primary_key=True)
    signal_id     = Column(Integer, ForeignKey("signals.id"), nullable=True)
    platform      = Column(String, default="kalshi")
    ticker        = Column(String)
    market_title  = Column(Text)
    side          = Column(String)
    entry_price   = Column(Float)
    exit_price    = Column(Float, nullable=True)
    quantity      = Column(Float)
    entry_time    = Column(DateTime, default=datetime.utcnow)
    exit_time     = Column(DateTime, nullable=True)
    pnl           = Column(Float, nullable=True)
    pnl_percent   = Column(Float, nullable=True)
    status        = Column(String, default="OPEN")
    notes         = Column(Text, nullable=True)
    strategy_tag  = Column(String, nullable=True)
    current_price = Column(Float, nullable=True)
    signal        = relationship("Signal", back_populates="trades")

class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    id            = Column(Integer, primary_key=True)
    platform      = Column(String, default="kalshi")
    ticker        = Column(String, index=True)
    yes_price     = Column(Float)
    depth         = Column(Float)
    snapshot_time = Column(DateTime, default=datetime.utcnow)

class PolyPosition(Base):
    __tablename__ = "polymarket_positions"
    id            = Column(Integer, primary_key=True)
    condition_id  = Column(String)
    outcome       = Column(String)
    title         = Column(Text)
    slug          = Column(String)
    traders       = Column(Integer)
    total_value   = Column(Float)
    avg_entry     = Column(Float)
    cur_price     = Column(Float)
    dominance     = Column(Float)
    signal_kind   = Column(String)
    scanned_at    = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)

# ── Migrations — add new columns to existing tables ───────────────────────────
def _run_migrations():
    """Add columns that may not exist in older DB instances."""
    migrations = [
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS alert_sent_at TIMESTAMP",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS hours_to_close FLOAT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS trader_count INTEGER",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS dominance FLOAT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS signal_strength INTEGER",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS opposite_traders INTEGER",
        "ALTER TABLE signal_price_history ADD COLUMN IF NOT EXISTS price_4h FLOAT",
        "ALTER TABLE trader_price_history ADD COLUMN IF NOT EXISTS price_4h FLOAT",
        "ALTER TABLE trader_price_history ADD COLUMN IF NOT EXISTS market_slug VARCHAR",
        "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS tier VARCHAR",
    ]
    with engine.connect() as conn:
        for sql in migrations:
            try:
                conn.execute(__import__('sqlalchemy').text(sql))
                conn.commit()
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"Migration note: {e}")

_run_migrations()

# ── DB helpers ─────────────────────────────────────────────────────────────────

def _calc_hours_to_close(end_date: str) -> Optional[float]:
    """Calculate hours between now and market close time. Returns None if unparseable."""
    if not end_date:
        return None
    try:
        close = datetime.strptime(end_date[:10], "%Y-%m-%d")
        now   = datetime.utcnow()
        delta = close - now
        hours = delta.total_seconds() / 3600
        return round(hours, 1)
    except Exception:
        return None


def db_save_signal(sig: dict, platform: str) -> Optional[int]:
    with Session(engine) as s:
        ex = s.query(Signal).filter_by(
            platform_signal_id=sig.get("sig_key","")
        ).first()
        if ex: return ex.id
        row = Signal(
            platform=platform,
            ticker=sig.get("ticker",""),
            market_title=sig.get("title",""),
            category=sig.get("category",""),
            signal_type=sig.get("direction") or sig.get("kind",""),
            move_size=sig.get("move_abs") or sig.get("momentum",0),
            price_before=sig.get("prev_price") or sig.get("avgEntry",0),
            price_after=sig.get("cur_price") or sig.get("curPrice",0),
            depth=sig.get("depth") or sig.get("totalValue",0),
            market_url=sig.get("url") or f"https://polymarket.com/event/{sig.get('eventSlug','')}",
            market_close_time=sig.get("end_date") or sig.get("endDate",""),
            platform_signal_id=sig.get("sig_key",""),
            hours_to_close=_calc_hours_to_close(sig.get("end_date") or sig.get("endDate","")),
            trader_count=sig.get("traders") or sig.get("tradersHolding"),
            dominance=sig.get("dominance"),
            signal_strength=sig.get("strength"),
            opposite_traders=sig.get("oppositeTraders"),
        )
        s.add(row); s.commit(); s.refresh(row)
        return row.id

def db_get_signals(limit=100, platform=None) -> List[dict]:
    with Session(engine) as s:
        q = s.query(Signal).order_by(Signal.detected_at.desc())
        if platform: q = q.filter(Signal.platform==platform)
        return [_sig_dict(r) for r in q.limit(limit).all()]

def db_get_trades(status=None, platform=None) -> List[dict]:
    with Session(engine) as s:
        q = s.query(Trade).order_by(Trade.entry_time.desc())
        if status:   q = q.filter(Trade.status==status)
        if platform: q = q.filter(Trade.platform==platform)
        return [_trade_dict(r) for r in q.all()]

def db_add_trade(data: dict) -> dict:
    with Session(engine) as s:
        row = Trade(
            signal_id=data.get("signal_id"),
            platform=data.get("platform","kalshi"),
            ticker=data.get("ticker",""),
            market_title=data.get("market_title",""),
            side=data.get("side","YES"),
            entry_price=float(data.get("entry_price",0)),
            quantity=float(data.get("quantity",0)),
            notes=data.get("notes",""),
            strategy_tag=data.get("strategy_tag",""),
            current_price=float(data.get("entry_price",0)),
        )
        s.add(row); s.commit(); s.refresh(row)
        return _trade_dict(row)

def db_close_trade(tid: int, exit_price: float, notes: str="") -> dict:
    with Session(engine) as s:
        row = s.get(Trade, tid)
        if not row: return {}
        row.exit_price = exit_price
        row.exit_time  = datetime.utcnow()
        row.status     = "CLOSED"
        if notes: row.notes = (row.notes or "") + " | " + notes
        mult = 1 if row.side=="YES" else -1
        row.pnl = mult * (exit_price - row.entry_price) * (row.quantity / (row.entry_price or 1))
        row.pnl_percent = (row.pnl / row.quantity)*100 if row.quantity else 0
        s.commit(); s.refresh(row)
        return _trade_dict(row)

def db_update_trade_price(tid: int, price: float):
    with Session(engine) as s:
        row = s.get(Trade, tid)
        if row: row.current_price = price; s.commit()

def db_analytics() -> dict:
    with Session(engine) as s:
        total_sig  = s.query(func.count(Signal.id)).scalar() or 0
        total_tr   = s.query(func.count(Trade.id)).scalar() or 0
        open_tr    = s.query(func.count(Trade.id)).filter(Trade.status=="OPEN").scalar() or 0
        closed     = s.query(Trade).filter(Trade.status=="CLOSED").all()
        wins       = [t for t in closed if (t.pnl or 0) > 0]
        total_pnl  = sum(t.pnl or 0 for t in closed)
        win_rate   = len(wins)/len(closed)*100 if closed else 0

        # Signal accuracy
        won  = s.query(func.count(Signal.id)).filter(Signal.outcome=="WON").scalar() or 0
        lost = s.query(func.count(Signal.id)).filter(Signal.outcome=="LOST").scalar() or 0
        sig_accuracy = round(won/(won+lost)*100,1) if (won+lost) else None

        # Same, but only on signals detected after the LIVE_BUY price-filter
        # bug was fixed, AND explicitly re-checked at <=0.45 here too — not
        # just trusting the deploy timestamp. This way the stat stays
        # correct even if some future bug lets a bad-priced signal through
        # again; it won't silently count it as "clean." Explicitly scoped
        # to Polymarket — was implicitly Polymarket-only for months while
        # Kalshi detection was dead, but now that Kalshi produces real
        # signals too this needs its own platform filter or it silently
        # blends two very different systems into one number.
        won_clean  = s.query(func.count(Signal.id)).filter(
            Signal.platform=="polymarket",
            Signal.outcome=="WON", Signal.detected_at > ACCURACY_FIX_CUTOFF,
            Signal.price_after <= ACCURACY_PRICE_CAP
        ).scalar() or 0
        lost_clean = s.query(func.count(Signal.id)).filter(
            Signal.platform=="polymarket",
            Signal.outcome=="LOST", Signal.detected_at > ACCURACY_FIX_CUTOFF,
            Signal.price_after <= ACCURACY_PRICE_CAP
        ).scalar() or 0
        sig_accuracy_clean = round(won_clean/(won_clean+lost_clean)*100,1) if (won_clean+lost_clean) else None

        # Kalshi's own "clean" stat. No cutoff or price cap needed — unlike
        # Polymarket, Kalshi's history was fully reset (all outcomes set
        # back to NULL) after fixing the resolution bug on 2026-08-02, so
        # every currently-resolved Kalshi signal is clean by construction.
        won_clean_kalshi  = s.query(func.count(Signal.id)).filter(
            Signal.platform=="kalshi", Signal.outcome=="WON"
        ).scalar() or 0
        lost_clean_kalshi = s.query(func.count(Signal.id)).filter(
            Signal.platform=="kalshi", Signal.outcome=="LOST"
        ).scalar() or 0
        sig_accuracy_clean_kalshi = (
            round(won_clean_kalshi/(won_clean_kalshi+lost_clean_kalshi)*100,1)
            if (won_clean_kalshi+lost_clean_kalshi) else None
        )

        # The stat above counts raw signal rows, not distinct markets — a
        # single Kalshi market can fire dozens of repeat signals as price
        # ticks toward resolution (confirmed by hand: 1,142 raw resolved
        # rows collapse to 64 distinct markets, an ~18x duplication rate).
        # If markets that resolve WON happen to re-fire at a different
        # rate than ones that resolve LOST, the raw win/lost counts above
        # are biased relative to the true per-market rate — in either
        # direction, not necessarily favorably. This is the deduplicated
        # version: one row per distinct (ticker, signal_type), keeping the
        # earliest detection, same method validated by hand against the
        # live DB before trusting it. ROW_NUMBER() over ORDER BY works on
        # both SQLite and Postgres, unlike DISTINCT ON (Postgres-only).
        from sqlalchemy import text as _sql_text
        _dedup_rows = s.execute(_sql_text("""
            SELECT outcome, COUNT(*) AS n
            FROM (
                SELECT outcome,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker, signal_type
                           ORDER BY detected_at ASC
                       ) AS rn
                FROM signals
                WHERE platform = 'kalshi' AND signal_type IN ('UP','DOWN')
            ) t
            WHERE rn = 1
            GROUP BY outcome
        """)).fetchall()
        won_kalshi_dedup = lost_kalshi_dedup = pending_kalshi_dedup = 0
        for _outcome, _n in _dedup_rows:
            if _outcome == "WON": won_kalshi_dedup = _n
            elif _outcome == "LOST": lost_kalshi_dedup = _n
            else: pending_kalshi_dedup = _n
        _resolved_kalshi_dedup = won_kalshi_dedup + lost_kalshi_dedup
        sig_accuracy_kalshi_dedup = (
            round(won_kalshi_dedup/_resolved_kalshi_dedup*100,1)
            if _resolved_kalshi_dedup else None
        )

        by_strat = defaultdict(lambda:{"count":0,"pnl":0,"wins":0})
        for t in closed:
            k = t.strategy_tag or "untagged"
            by_strat[k]["count"]+=1; by_strat[k]["pnl"]+=t.pnl or 0
            if (t.pnl or 0)>0: by_strat[k]["wins"]+=1

        by_platform = defaultdict(lambda:{"signals":0,"trades":0,"pnl":0})
        for sig in s.query(Signal).all(): by_platform[sig.platform]["signals"]+=1
        for t in s.query(Trade).all():
            by_platform[t.platform]["trades"]+=1
            by_platform[t.platform]["pnl"]+=t.pnl or 0

        # Kalshi's raw signal count blends real, alert-worthy signals with
        # sub-threshold broad-capture data (saved for research since the
        # Aug 2 fix) -- a distinction Polymarket's count doesn't have, since
        # every Polymarket signal has always cleared the same real filter.
        # Showing "1108" next to Polymarket's "6670" as directly comparable
        # numbers was misleading. These thresholds match kalshi.py's own
        # alert-gate defaults -- keep in sync if those ever change.
        KALSHI_ALERT_MIN_MOVE, KALSHI_ALERT_MIN_DEPTH = 0.03, 1000.0
        if "kalshi" in by_platform:
            kalshi_alert_worthy = s.query(func.count(Signal.id)).filter(
                Signal.platform == "kalshi",
                func.abs(Signal.move_size) >= KALSHI_ALERT_MIN_MOVE,
                Signal.depth >= KALSHI_ALERT_MIN_DEPTH,
            ).scalar() or 0
            by_platform["kalshi"]["alert_worthy"] = kalshi_alert_worthy

        recent = sorted(closed, key=lambda t: t.exit_time or datetime.min)[-30:]
        cum, pnl_series = 0, []
        for t in recent:
            cum += t.pnl or 0
            pnl_series.append({
                "date": t.exit_time.strftime("%m/%d") if t.exit_time else "",
                "pnl":  round(cum, 2)
            })

        sig_by_day = {}
        sig_by_day_platform = {}
        for i in range(14):
            d = (datetime.utcnow()-timedelta(days=i)).strftime("%m/%d")
            sig_by_day[d] = 0
            sig_by_day_platform[d] = {"kalshi": 0, "polymarket": 0}
        for sig in s.query(Signal).filter(
            Signal.detected_at >= datetime.utcnow()-timedelta(days=14)
        ).all():
            d = sig.detected_at.strftime("%m/%d")
            if d in sig_by_day:
                sig_by_day[d] += 1
                if sig.platform in sig_by_day_platform[d]:
                    sig_by_day_platform[d][sig.platform] += 1

        return {
            "total_signals":  total_sig,
            "total_trades":   total_tr,
            "open_trades":    open_tr,
            "closed_trades":  len(closed),
            "win_rate":       round(win_rate, 1),
            "total_pnl":      round(total_pnl, 2),
            "avg_pnl":        round(total_pnl/len(closed), 2) if closed else 0,
            "sig_accuracy":   sig_accuracy,
            "sig_won":        won,
            "sig_lost":       lost,
            "sig_accuracy_clean": sig_accuracy_clean,
            "sig_won_clean":      won_clean,
            "sig_lost_clean":     lost_clean,
            "sig_accuracy_clean_kalshi": sig_accuracy_clean_kalshi,
            "sig_won_clean_kalshi":      won_clean_kalshi,
            "sig_lost_clean_kalshi":     lost_clean_kalshi,
            "sig_accuracy_kalshi_dedup": sig_accuracy_kalshi_dedup,
            "sig_won_kalshi_dedup":      won_kalshi_dedup,
            "sig_lost_kalshi_dedup":     lost_kalshi_dedup,
            "sig_pending_kalshi_dedup":  pending_kalshi_dedup,
            "by_strategy":    {k: {**v, "win_rate": round(v["wins"]/v["count"]*100,1)
                               if v["count"] else 0} for k,v in by_strat.items()},
            "by_platform":    dict(by_platform),
            "pnl_series":     pnl_series,
            "signals_by_day": [{"date":k,"count":v,
                                "kalshi":sig_by_day_platform[k]["kalshi"],
                                "polymarket":sig_by_day_platform[k]["polymarket"]}
                               for k,v in sorted(sig_by_day.items())],
        }

def db_mark_alert_sent(sig_key: str):
    """Mark a signal as alerted so we know not to re-alert after restart."""
    from datetime import datetime
    with Session(engine) as s:
        row = s.query(Signal).filter_by(platform_signal_id=sig_key).first()
        if row and not row.alert_sent_at:
            row.alert_sent_at = datetime.utcnow()
            s.commit()

def db_get_alerted_keys() -> set:
    """Return sig_keys of signals that have already been alerted."""
    with Session(engine) as s:
        rows = s.query(Signal.platform_signal_id).filter(
            Signal.alert_sent_at != None
        ).all()
        return {r[0] for r in rows if r[0]}

def db_cleanup(days_to_keep=2):
    # 2-day retention: at ~60s snapshots across 200 markets, 7 days filled the
    # entire Railway volume (290MB). 2 days caps steady-state well under 100MB.
    cutoff = datetime.utcnow() - timedelta(days=days_to_keep)
    with Session(engine) as s:
        n = s.query(MarketSnapshot).filter(MarketSnapshot.snapshot_time<cutoff).delete()
        o = s.query(PolyPosition).filter(PolyPosition.scanned_at<cutoff).delete()
        s.commit()
        if n or o:
            print(f"Cleanup: {n} snapshots, {o} poly positions deleted.")

def db_size_mb() -> float:
    if "sqlite" in DATABASE_URL:
        try: return round(os.path.getsize(DB_PATH)/1_048_576, 2)
        except: return 0.0
    # PostgreSQL: ask the server for the actual database size
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            size = conn.execute(text(
                "SELECT pg_database_size(current_database())"
            )).scalar()
            return round((size or 0) / 1_048_576, 2)
    except Exception:
        return 0.0

def _sig_dict(r: Signal) -> dict:
    return {
        "id": r.id, "platform": r.platform, "ticker": r.ticker,
        "market_title": r.market_title, "category": r.category,
        "signal_type": r.signal_type, "move_size": r.move_size,
        "price_before": r.price_before, "price_after": r.price_after,
        "depth": r.depth, "outcome": r.outcome, "market_url": r.market_url,
        "market_close_time": r.market_close_time,
        "platform_signal_id": r.platform_signal_id,
        "detected_at": r.detected_at.strftime("%Y-%m-%d %H:%M") if r.detected_at else "",
        "hours_to_close": r.hours_to_close,
        "trader_count": r.trader_count,
        "dominance": r.dominance,
        "signal_strength": r.signal_strength,
        "opposite_traders": r.opposite_traders,
    }

def _trade_dict(r: Trade) -> dict:
    cur = r.current_price or r.entry_price or 0
    unreal = round(
        ((cur - r.entry_price) * (r.quantity / (r.entry_price or 1))) *
        (1 if r.side=="YES" else -1), 2
    ) if r.status=="OPEN" else None
    return {
        "id": r.id, "signal_id": r.signal_id, "platform": r.platform,
        "ticker": r.ticker, "market_title": r.market_title, "side": r.side,
        "entry_price": r.entry_price, "exit_price": r.exit_price,
        "quantity": r.quantity, "pnl": r.pnl, "pnl_percent": r.pnl_percent,
        "status": r.status, "notes": r.notes, "strategy_tag": r.strategy_tag,
        "current_price": cur, "unrealized_pnl": unreal,
        "entry_time": r.entry_time.strftime("%Y-%m-%d %H:%M") if r.entry_time else "",
        "exit_time":  r.exit_time.strftime("%Y-%m-%d %H:%M") if r.exit_time else "",
    }

# ── Price-after tracking ───────────────────────────────────────────────────────

class SignalPriceHistory(Base):
    __tablename__ = "signal_price_history"
    id            = Column(Integer, primary_key=True)
    signal_id     = Column(Integer, ForeignKey("signals.id"), index=True)
    platform      = Column(String)
    ticker        = Column(String)
    signal_time   = Column(DateTime)
    price_at_signal = Column(Float)
    price_15m     = Column(Float, nullable=True)
    price_1h      = Column(Float, nullable=True)
    price_4h      = Column(Float, nullable=True)
    price_24h     = Column(Float, nullable=True)
    price_7d      = Column(Float, nullable=True)
    move_15m      = Column(Float, nullable=True)
    move_1h       = Column(Float, nullable=True)
    move_24h      = Column(Float, nullable=True)
    continued_15m = Column(Boolean, nullable=True)  # did price move same direction?
    continued_1h  = Column(Boolean, nullable=True)
    continued_24h = Column(Boolean, nullable=True)
    created_at    = Column(DateTime, default=datetime.utcnow)

class TraderPriceHistory(Base):
    __tablename__ = "trader_price_history"
    id              = Column(Integer, primary_key=True)
    platform        = Column(String, default="polymarket")
    condition_id    = Column(String, index=True)
    outcome         = Column(String)
    market_title    = Column(Text)
    market_slug     = Column(String, nullable=True)
    trader_rank     = Column(Integer)
    trader_username = Column(String)
    entry_price     = Column(Float)
    entry_time      = Column(DateTime, default=datetime.utcnow)
    price_15m       = Column(Float, nullable=True)
    price_1h        = Column(Float, nullable=True)
    price_4h        = Column(Float, nullable=True)
    price_24h       = Column(Float, nullable=True)
    price_7d        = Column(Float, nullable=True)
    move_15m        = Column(Float, nullable=True)
    move_1h         = Column(Float, nullable=True)
    move_24h        = Column(Float, nullable=True)
    continued_15m   = Column(Boolean, nullable=True)
    continued_1h    = Column(Boolean, nullable=True)
    continued_24h   = Column(Boolean, nullable=True)


class PaperTrade(Base):
    """
    Hypothetical, no-money position logged automatically for every
    Polymarket signal that clears the <=35c filter — both PRIME (fresh,
    <2c moved) and STANDARD (moved 2c+) tiers, tagged separately via
    `tier`. Fixed $5 stake per trade.

    Originally PRIME-only, based on an earlier finding that PRIME had
    the real edge (+16.1c) and STANDARD didn't (-2.1c). A later,
    larger-sample retrospective check (n=687 distinct markets, stable
    across two separate time windows) found the opposite — STANDARD
    showing +19.0c and PRIME roughly flat. Rather than trust either
    retrospective number outright, both tiers are now paper-traded
    forward in parallel, so the live data — not another backward-
    looking query — settles which tier actually has the edge.
    """
    __tablename__ = "paper_trades"
    id                  = Column(Integer, primary_key=True)
    signal_id           = Column(Integer, index=True)
    platform_signal_id  = Column(String, index=True)
    market_title        = Column(Text)
    tier                = Column(String, default="PRIME")  # "PRIME" or "STANDARD"
    entry_price         = Column(Float)
    stake               = Column(Float, default=5.0)
    shares              = Column(Float)
    detected_at         = Column(DateTime, default=datetime.utcnow)
    outcome             = Column(String, nullable=True)  # WON / LOST / None (pending)
    pnl                 = Column(Float, nullable=True)
    resolved_at         = Column(DateTime, nullable=True)
    # Crash-size tagging, added after finding a clean, monotonic relationship
    # between how far a contract's price fell before entry and win rate/ROI --
    # confirmed on both a retrospective pull and live paper trades separately,
    # and directly confirmed to explain 9 of the 10 biggest wins ever logged
    # (all 60c+ crashes). Stored at log time so future queries/dashboard
    # cards can group by crash tier without re-deriving the threshold logic
    # or joining back to signals every time. See _crash_tier() for the
    # actual bucket definitions.
    crash_size_cents    = Column(Float, nullable=True)
    crash_tier          = Column(String, nullable=True)  # "under_20c" / "20_39c" / "40_59c" / "60c_plus"

class ShadowTrade(Base):
    """
    Shadow-mode position for the eventual real-money bot: logs the exact
    same entry a real order would take (STANDARD-tier Polymarket signals,
    the one tier with a confirmed live edge over a real control group),
    but places no order and risks no money. Two things this proves that
    PaperTrade alone doesn't:

    1. The early-exit rule found in the repricing-window analysis (a
       15c+ pop within 1h pays off better sold immediately than held to
       resolution, confirmed on real sample sizes on both platforms) --
       PaperTrade always holds to resolution, this doesn't.
    2. Real execution logic running against live, moving conditions,
       with nothing at stake if it's wrong -- the actual point of
       shadow mode. A bug here just produces a wrong log line to go fix,
       not a real order at a bad price.

    Still doesn't model fill risk, fees, or slippage -- it uses the same
    observed prices PaperTrade does. Closing that last gap needs a real
    execution layer, which is exactly why this is a step before that,
    not a replacement for it.
    """
    __tablename__ = "shadow_trades"
    id                  = Column(Integer, primary_key=True)
    signal_id           = Column(Integer, index=True)
    platform_signal_id  = Column(String, index=True)
    market_title        = Column(Text)
    entry_price         = Column(Float)
    stake               = Column(Float, default=5.0)
    shares              = Column(Float)
    detected_at         = Column(DateTime, default=datetime.utcnow)
    outcome             = Column(String, nullable=True)   # WON / LOST / None (pending)
    pnl                 = Column(Float, nullable=True)
    exit_price          = Column(Float, nullable=True)
    exit_reason         = Column(String, nullable=True)   # "early_pop_1h" or "held_to_resolution"
    exit_time           = Column(DateTime, nullable=True)
    # Same crash-size tagging as PaperTrade -- see that class's docstring
    # for why. Kept identical field names on both tables on purpose, so
    # a query can treat them the same way regardless of which one it's
    # reading from.
    crash_size_cents    = Column(Float, nullable=True)
    crash_tier          = Column(String, nullable=True)

Base.metadata.create_all(engine)

# ── Migrations — add new columns to existing tables ───────────────────────────
def _run_migrations():
    """Add columns that may not exist in older DB instances."""
    migrations = [
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS alert_sent_at TIMESTAMP",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS hours_to_close FLOAT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS trader_count INTEGER",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS dominance FLOAT",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS signal_strength INTEGER",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS opposite_traders INTEGER",
        "ALTER TABLE signal_price_history ADD COLUMN IF NOT EXISTS price_4h FLOAT",
        "ALTER TABLE trader_price_history ADD COLUMN IF NOT EXISTS price_4h FLOAT",
        "ALTER TABLE trader_price_history ADD COLUMN IF NOT EXISTS market_slug VARCHAR",
        "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS tier VARCHAR",
        "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS crash_size_cents FLOAT",
        "ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS crash_tier VARCHAR",
        "ALTER TABLE shadow_trades ADD COLUMN IF NOT EXISTS crash_size_cents FLOAT",
        "ALTER TABLE shadow_trades ADD COLUMN IF NOT EXISTS crash_tier VARCHAR",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS price_lookup_suspicious BOOLEAN",
    ]
    with engine.connect() as conn:
        for sql in migrations:
            try:
                conn.execute(__import__('sqlalchemy').text(sql))
                conn.commit()
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"Migration note: {e}")

_run_migrations()


def db_init_signal_price_history(signal_id: int, ticker: str, platform: str,
                                  signal_time: datetime, price: float):
    """Create a price history row when a signal is first detected."""
    with Session(engine) as s:
        existing = s.query(SignalPriceHistory).filter_by(signal_id=signal_id).first()
        if existing: return
        s.add(SignalPriceHistory(
            signal_id=signal_id, platform=platform, ticker=ticker,
            signal_time=signal_time, price_at_signal=price,
        ))
        s.commit()


def _crash_tier(move_size: float):
    """
    Classifies how far a contract's price fell (or rose) before entry
    into the bands checked against both a retrospective pull and live
    paper trades: win rate and ROI climbed in a clean, uninterrupted
    line from ~40% under 20c up to ~88% at 60c+, and directly explained
    9 of the 10 biggest wins ever logged (all 60c+ crashes). Returns
    (crash_size_cents, tier_label); (None, None) if move_size is
    unavailable so callers don't have to special-case it.
    """
    if move_size is None:
        return None, None
    crash_cents = abs(move_size) * 100
    if crash_cents < 20:
        tier = "under_20c"
    elif crash_cents < 40:
        tier = "20_39c"
    elif crash_cents < 60:
        tier = "40_59c"
    else:
        tier = "60c_plus"
    return round(crash_cents, 2), tier


def db_log_paper_trade(signal_id: int, platform_signal_id: str, title: str,
                        entry_price: float, stake: float = 5.0, tier: str = "PRIME"):
    """
    Log a hypothetical position for the given tier. No real money involved.

    Dedup is scoped to (signal_id, tier), not signal_id alone -- a single
    signal can legitimately have more than one PaperTrade row now that
    STANDARD_15C exists as a stricter subset of STANDARD (every 15c+
    signal is also a 2c+ STANDARD signal). Found and fixed 2026-09-04:
    the old signal_id-only check meant STANDARD_15C's call would always
    find STANDARD's row (logged moments earlier for the same signal) and
    silently skip itself every time -- it would never have logged a
    single trade, caught before this ever deployed.
    """
    if not entry_price or entry_price <= 0:
        return
    with Session(engine) as s:
        existing = s.query(PaperTrade).filter_by(signal_id=signal_id, tier=tier).first()
        if existing:
            return
        sig = s.get(Signal, signal_id)
        crash_size_cents, crash_tier = _crash_tier(sig.move_size if sig else None)
        s.add(PaperTrade(
            signal_id=signal_id, platform_signal_id=platform_signal_id,
            market_title=title, tier=tier, entry_price=entry_price, stake=stake,
            shares=stake / entry_price,
            crash_size_cents=crash_size_cents, crash_tier=crash_tier,
        ))
        s.commit()


def db_resolve_paper_trades():
    """
    Resolve any pending paper trades whose underlying signal now has a
    real, corrected outcome (via the fixed check_signal_outcomes()).
    Called after each outcome check pass — cheap, just a join.
    """
    with Session(engine) as s:
        pending = (
            s.query(PaperTrade, Signal.outcome)
            .join(Signal, Signal.id == PaperTrade.signal_id)
            .filter(PaperTrade.outcome == None, Signal.outcome != None)
            .all()
        )
        for pt, sig_outcome in pending:
            pt.outcome = sig_outcome
            pt.pnl = (pt.shares - pt.stake) if sig_outcome == "WON" else -pt.stake
            pt.resolved_at = datetime.utcnow()
        if pending:
            s.commit()
        return len(pending)


def db_log_shadow_trade(signal_id: int, platform_signal_id: str, title: str,
                         entry_price: float, stake: float = 5.0):
    """
    Log a shadow-mode position for STANDARD-tier Polymarket signals --
    same entry a real order would take, no money at risk. See
    ShadowTrade's docstring for why this exists alongside PaperTrade.
    """
    if not entry_price or entry_price <= 0:
        return
    with Session(engine) as s:
        existing = s.query(ShadowTrade).filter_by(signal_id=signal_id).first()
        if existing:
            return
        sig = s.get(Signal, signal_id)
        crash_size_cents, crash_tier = _crash_tier(sig.move_size if sig else None)
        s.add(ShadowTrade(
            signal_id=signal_id, platform_signal_id=platform_signal_id,
            market_title=title, entry_price=entry_price, stake=stake,
            shares=stake / entry_price,
            crash_size_cents=crash_size_cents, crash_tier=crash_tier,
        ))
        s.commit()


def db_resolve_shadow_trades(pop_threshold: float = 0.15):
    """
    Two-stage resolution, checked in this order every time it runs:

    1. Early exit: if the linked SignalPriceHistory row now has a
       move_1h >= pop_threshold, close the trade at that 1h price right
       now, regardless of what happens to the market afterward. This is
       the validated rule -- a 15c+ pop within 1h paid off better sold
       immediately than held to resolution, confirmed on real sample
       sizes on both platforms during the repricing-window analysis.
    2. Otherwise, exactly like PaperTrade: wait for the real outcome and
       hold to resolution.

    A trade only ever exits one of these two ways, never both -- the
    early-exit check runs first and skips resolution-based exit for any
    trade it already closed.
    """
    exited = 0
    with Session(engine) as s:
        # Stage 1: early exit on a confirmed pop, checked first so a
        # trade that both pops AND later resolves doesn't get double-
        # counted -- the pop exit takes priority since that's the
        # validated rule.
        pending = s.query(ShadowTrade).filter(ShadowTrade.outcome == None).all()
        for st in pending:
            hist = (
                s.query(SignalPriceHistory)
                .filter_by(signal_id=st.signal_id)
                .first()
            )
            if hist and hist.move_1h is not None and hist.move_1h >= pop_threshold:
                exit_price = hist.price_1h
                st.exit_price = exit_price
                st.exit_reason = "early_pop_1h"
                st.exit_time = datetime.utcnow()
                st.pnl = (st.shares * exit_price) - st.stake
                st.outcome = "WON" if st.pnl > 0 else "LOST"
                exited += 1
        if exited:
            s.commit()

        # Stage 2: no pop yet -- hold to resolution, same as PaperTrade.
        still_pending = (
            s.query(ShadowTrade, Signal.outcome)
            .join(Signal, Signal.id == ShadowTrade.signal_id)
            .filter(ShadowTrade.outcome == None, Signal.outcome != None)
            .all()
        )
        for st, sig_outcome in still_pending:
            st.outcome = sig_outcome
            st.pnl = (st.shares - st.stake) if sig_outcome == "WON" else -st.stake
            st.exit_reason = "held_to_resolution"
            st.exit_time = datetime.utcnow()
            exited += 1
        if still_pending:
            s.commit()

    return exited


def db_shadow_trade_stats() -> dict:
    """Summary stats for a future shadow-mode dashboard card."""
    with Session(engine) as s:
        resolved = s.query(ShadowTrade).filter(ShadowTrade.outcome != None).all()
        pending  = s.query(ShadowTrade).filter(ShadowTrade.outcome == None).count()
        won      = [t for t in resolved if t.outcome == "WON"]
        early    = [t for t in resolved if t.exit_reason == "early_pop_1h"]
        total_pnl = sum(t.pnl or 0 for t in resolved)
        return {
            "resolved": len(resolved),
            "won": len(won),
            "lost": len(resolved) - len(won),
            "win_pct": round(100 * len(won) / len(resolved), 1) if resolved else 0,
            "total_pnl": round(total_pnl, 2),
            "pending": pending,
            "early_exits": len(early),
        }


def backfill_standard_15c() -> dict:
    """
    One-time historical backfill for the STANDARD_15C tier -- creates
    PaperTrade rows for every past signal that already qualifies (2c+
    STANDARD criteria plus the stricter 15c+ crash bar found 2026-09-04),
    so the tracked group starts with its real historical sample instead
    of only growing from new live signals forward. Covers both already-
    resolved signals (backfilled with the known outcome/pnl directly)
    and still-pending ones (backfilled as pending, so the existing,
    tier-agnostic db_resolve_paper_trades() picks them up correctly once
    they resolve naturally -- no separate resolution path needed).

    Safe to run more than once: per-(signal_id, tier) dedup means
    already-backfilled or already-live-logged rows are simply skipped,
    not duplicated.
    """
    with Session(engine) as s:
        candidates = (
            s.query(Signal)
            .filter(Signal.platform == "polymarket",
                    Signal.category == "sports",
                    Signal.price_after < 0.35,
                    or_(Signal.price_lookup_suspicious.is_(None),
                        Signal.price_lookup_suspicious == False))
            .all()
        )
        resolved_inserted, pending_inserted = 0, 0
        for sig in candidates:
            if sig.move_size is None or abs(sig.move_size) * 100 < 15:
                continue
            if not sig.price_after or sig.price_after <= 0:
                continue
            existing = s.query(PaperTrade).filter_by(signal_id=sig.id, tier="STANDARD_15C").first()
            if existing:
                continue
            crash_size_cents, crash_tier = _crash_tier(sig.move_size)
            shares = 5.0 / sig.price_after
            row = PaperTrade(
                signal_id=sig.id, platform_signal_id=sig.platform_signal_id or "",
                market_title=sig.market_title, tier="STANDARD_15C",
                entry_price=sig.price_after, stake=5.0, shares=shares,
                detected_at=sig.detected_at,
                crash_size_cents=crash_size_cents, crash_tier=crash_tier,
            )
            if sig.outcome:
                row.outcome     = sig.outcome
                row.pnl         = (shares - 5.0) if sig.outcome == "WON" else -5.0
                row.resolved_at = sig.resolved_at
                resolved_inserted += 1
            else:
                pending_inserted += 1
            s.add(row)
        s.commit()
        return {"resolved_backfilled": resolved_inserted, "pending_backfilled": pending_inserted}


def backfill_suspicious_prices() -> int:
    """
    One-time backfill for the price_lookup_suspicious flag -- run this
    once after deploying, then it never needs to run again since new
    signals are logged correctly at the source now. Not called
    automatically anywhere; call it manually (e.g. from a one-off script
    or a REPL) when ready. Scoped to Polymarket only -- see the column's
    comment on the Signal model for why Kalshi is never flagged.
    """
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE signals
            SET price_lookup_suspicious = TRUE
            WHERE platform = 'polymarket'
              AND price_before IS NOT NULL AND price_after IS NOT NULL
              AND ABS(price_before + price_after - 1.0) < 0.03
              AND (price_lookup_suspicious IS NULL OR price_lookup_suspicious = FALSE)
        """))
        conn.commit()
        return result.rowcount


def db_paper_trade_stats(tier: str = "PRIME") -> dict:
    """
    Summary stats for the paper trading dashboard card. Defaults to
    PRIME to preserve existing behavior at every current call site —
    pass tier='STANDARD' explicitly to get the other track, or
    tier=None for both combined (rarely what you want, since the whole
    point of tracking them separately is to compare them, not blend
    them back together).

    Excludes any trade whose underlying signal is flagged
    price_lookup_suspicious (see that column's comment on Signal) --
    found 2026-09-04: a bug that could grab the wrong outcome's price
    on multi-outcome markets, confirmed to have inflated STANDARD's
    apparent win rate and PnL. This is the permanent fix at the display
    layer: the dashboard now always reflects the clean numbers going
    forward, without needing to rewrite or guess at any historical
    entry prices, which were never recoverable to begin with.
    """
    with Session(engine) as s:
        q = (
            s.query(PaperTrade)
            .join(Signal, Signal.id == PaperTrade.signal_id)
            .filter(or_(Signal.price_lookup_suspicious.is_(None),
                        Signal.price_lookup_suspicious == False))
        )
        if tier:
            q = q.filter(PaperTrade.tier == tier)
        resolved = q.filter(PaperTrade.outcome != None).all()
        pending  = q.filter(PaperTrade.outcome == None).count()
        won      = [t for t in resolved if t.outcome == "WON"]
        total_pnl = sum(t.pnl or 0 for t in resolved)

        # Cumulative PnL over time, ordered by resolution — mirrors the
        # manual-trade pnl_series pattern, but for what's actually being
        # tracked day to day now.
        ordered = sorted(resolved, key=lambda t: t.resolved_at or datetime.min)
        cum, pnl_series = 0, []
        for t in ordered:
            cum += t.pnl or 0
            pnl_series.append({
                "date": t.resolved_at.strftime("%m/%d") if t.resolved_at else "",
                "pnl":  round(cum, 2),
            })

        return {
            "tier":       tier or "ALL",
            "resolved":   len(resolved),
            "pending":    pending,
            "won":        len(won),
            "lost":       len(resolved) - len(won),
            "win_rate":   round(len(won) / len(resolved) * 100, 1) if resolved else None,
            "total_pnl":  round(total_pnl, 2),
            "total_staked": round(sum(t.stake for t in resolved), 2),
            "pnl_series": pnl_series,
        }


def db_control_group_stats() -> dict:
    """
    Same shape as db_paper_trade_stats(), computed instead from every
    qualifying Polymarket sports signal with no PRIME/STANDARD filter
    applied at all -- a hypothetical $5 on literally everything that
    cleared the base price/category filter. This is the honest baseline
    STANDARD actually needs to beat, not just a comparison against PRIME.
    Replaced PRIME here entirely once PRIME was confirmed to underperform
    even this unfiltered baseline -- there was no real reason left to
    keep tracking it on the live dashboard.

    Excludes signals flagged price_lookup_suspicious, same as
    db_paper_trade_stats -- see that column's comment on Signal.
    """
    with Session(engine) as s:
        q = (
            s.query(Signal)
            .filter(Signal.platform == "polymarket",
                    Signal.category == "sports",
                    Signal.price_after < 0.35,
                    Signal.outcome != None,
                    or_(Signal.price_lookup_suspicious.is_(None),
                        Signal.price_lookup_suspicious == False))
        )
        resolved = q.all()
        pending  = (
            s.query(Signal)
            .filter(Signal.platform == "polymarket",
                    Signal.category == "sports",
                    Signal.price_after < 0.35,
                    Signal.outcome == None)
            .count()
        )
        won = [sig for sig in resolved if sig.outcome == "WON"]

        def _pnl(sig):
            # Hypothetical $5 stake, same formula as every other paper
            # trade -- shares = stake/entry_price, payout $1/share if won.
            if not sig.price_after or sig.price_after <= 0:
                return 0.0
            return (5.0 / sig.price_after - 5.0) if sig.outcome == "WON" else -5.0

        total_pnl = sum(_pnl(sig) for sig in resolved)

        ordered = sorted(resolved, key=lambda sig: sig.resolved_at or datetime.min)
        cum, pnl_series = 0, []
        for sig in ordered:
            cum += _pnl(sig)
            pnl_series.append({
                "date": sig.resolved_at.strftime("%m/%d") if sig.resolved_at else "",
                "pnl":  round(cum, 2),
            })

        return {
            "tier":       "BASELINE",
            "resolved":   len(resolved),
            "pending":    pending,
            "won":        len(won),
            "lost":       len(resolved) - len(won),
            "win_rate":   round(len(won) / len(resolved) * 100, 1) if resolved else None,
            "total_pnl":  round(total_pnl, 2),
            "total_staked": round(len(resolved) * 5.0, 2),
            "pnl_series": pnl_series,
        }


def db_init_trader_entry(condition_id: str, outcome: str, title: str,
                          rank: int, username: str, entry_price: float,
                          slug: str = ""):
    """Record a trader entry for price-after tracking. One row per trader per market."""
    with Session(engine) as s:
        existing = s.query(TraderPriceHistory).filter_by(
            condition_id=condition_id, outcome=outcome,
            trader_username=username
        ).first()
        if existing: return
        s.add(TraderPriceHistory(
            platform="polymarket", condition_id=condition_id,
            outcome=outcome, market_title=title, market_slug=slug,
            trader_rank=rank, trader_username=username,
            entry_price=entry_price,
        ))
        s.commit()


def db_get_pending_price_history() -> List[dict]:
    """Return signal price history rows that still have unfilled time buckets."""
    now = datetime.utcnow()
    with Session(engine) as s:
        rows = (
            s.query(SignalPriceHistory, Signal.market_url, Signal.market_title)
            .join(Signal, Signal.id == SignalPriceHistory.signal_id)
            .filter(
                SignalPriceHistory.signal_time >= now - timedelta(days=8),
                (SignalPriceHistory.price_7d == None) |
                (SignalPriceHistory.price_24h == None) |
                (SignalPriceHistory.price_1h == None) |
                (SignalPriceHistory.price_15m == None)
            ).all()
        )
        return [{"id":r.SignalPriceHistory.id,"signal_id":r.SignalPriceHistory.signal_id,
                 "ticker":r.SignalPriceHistory.ticker,
                 "platform":r.SignalPriceHistory.platform,
                 "signal_time":r.SignalPriceHistory.signal_time,
                 "price_at_signal":r.SignalPriceHistory.price_at_signal,
                 "price_15m":r.SignalPriceHistory.price_15m,
                 "price_1h":r.SignalPriceHistory.price_1h,
                 "price_4h":r.SignalPriceHistory.price_4h,
                 "price_24h":r.SignalPriceHistory.price_24h,
                 "price_7d":r.SignalPriceHistory.price_7d,
                 "market_url":r.market_url,"market_title":r.market_title} for r in rows]


def db_get_pending_trader_history() -> List[dict]:
    """Return trader entries with unfilled price buckets."""
    now = datetime.utcnow()
    with Session(engine) as s:
        rows = s.query(TraderPriceHistory).filter(
            TraderPriceHistory.entry_time >= now - timedelta(days=8),
            (TraderPriceHistory.price_7d == None) |
            (TraderPriceHistory.price_24h == None) |
            (TraderPriceHistory.price_1h == None) |
            (TraderPriceHistory.price_15m == None)
        ).all()
        return [{"id":r.id,"condition_id":r.condition_id,"outcome":r.outcome,
                 "entry_price":r.entry_price,"entry_time":r.entry_time,
                 "market_slug":r.market_slug,"market_title":r.market_title,
                 "price_15m":r.price_15m,"price_1h":r.price_1h,
                 "price_4h":r.price_4h,"price_24h":r.price_24h,
                 "price_7d":r.price_7d} for r in rows]


def db_update_price_bucket(table: str, row_id: int, bucket: str,
                            price: float, price_at_signal: float, direction: int):
    """Fill in a time bucket and calculate continuation flag."""
    move  = price - price_at_signal
    cont  = (move > 0) == (direction > 0) if direction != 0 else None
    model = SignalPriceHistory if table == "signal" else TraderPriceHistory
    with Session(engine) as s:
        row = s.get(model, row_id)
        if not row: return
        setattr(row, f"price_{bucket}", price)
        setattr(row, f"move_{bucket}", round(move, 4)) if hasattr(row, f"move_{bucket}") else None
        if bucket in ("15m","1h","24h"):
            setattr(row, f"continued_{bucket}", cont)
        s.commit()
