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
                        Boolean, DateTime, Text, ForeignKey, func)
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
    market_url         = Column(Text)
    market_close_time  = Column(String)
    platform_signal_id = Column(String, unique=True)
    detected_at        = Column(DateTime, default=datetime.utcnow)
    alert_sent_at      = Column(DateTime, nullable=True)
    hours_to_close     = Column(Float, nullable=True)  # hours between signal detection and market close
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
        "ALTER TABLE signal_price_history ADD COLUMN IF NOT EXISTS price_4h FLOAT",
        "ALTER TABLE trader_price_history ADD COLUMN IF NOT EXISTS price_4h FLOAT",
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

        recent = sorted(closed, key=lambda t: t.exit_time or datetime.min)[-30:]
        cum, pnl_series = 0, []
        for t in recent:
            cum += t.pnl or 0
            pnl_series.append({
                "date": t.exit_time.strftime("%m/%d") if t.exit_time else "",
                "pnl":  round(cum, 2)
            })

        sig_by_day = {}
        for i in range(14):
            d = (datetime.utcnow()-timedelta(days=i)).strftime("%m/%d")
            sig_by_day[d] = 0
        for sig in s.query(Signal).filter(
            Signal.detected_at >= datetime.utcnow()-timedelta(days=14)
        ).all():
            d = sig.detected_at.strftime("%m/%d")
            if d in sig_by_day: sig_by_day[d] += 1

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
            "by_strategy":    {k: {**v, "win_rate": round(v["wins"]/v["count"]*100,1)
                               if v["count"] else 0} for k,v in by_strat.items()},
            "by_platform":    dict(by_platform),
            "pnl_series":     pnl_series,
            "signals_by_day": [{"date":k,"count":v}
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

def db_cleanup(days_to_keep=7):
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
    return 0.0  # PostgreSQL size not tracked locally

def _sig_dict(r: Signal) -> dict:
    return {
        "id": r.id, "platform": r.platform, "ticker": r.ticker,
        "market_title": r.market_title, "category": r.category,
        "signal_type": r.signal_type, "move_size": r.move_size,
        "price_before": r.price_before, "price_after": r.price_after,
        "depth": r.depth, "outcome": r.outcome, "market_url": r.market_url,
        "detected_at": r.detected_at.strftime("%Y-%m-%d %H:%M") if r.detected_at else "",
        "hours_to_close": r.hours_to_close,
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

Base.metadata.create_all(engine)

# ── Migrations — add new columns to existing tables ───────────────────────────
def _run_migrations():
    """Add columns that may not exist in older DB instances."""
    migrations = [
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS alert_sent_at TIMESTAMP",
        "ALTER TABLE signals ADD COLUMN IF NOT EXISTS hours_to_close FLOAT",
        "ALTER TABLE signal_price_history ADD COLUMN IF NOT EXISTS price_4h FLOAT",
        "ALTER TABLE trader_price_history ADD COLUMN IF NOT EXISTS price_4h FLOAT",
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


def db_init_trader_entry(condition_id: str, outcome: str, title: str,
                          rank: int, username: str, entry_price: float):
    """Record a trader entry for price-after tracking. One row per trader per market."""
    with Session(engine) as s:
        existing = s.query(TraderPriceHistory).filter_by(
            condition_id=condition_id, outcome=outcome,
            trader_username=username
        ).first()
        if existing: return
        s.add(TraderPriceHistory(
            platform="polymarket", condition_id=condition_id,
            outcome=outcome, market_title=title,
            trader_rank=rank, trader_username=username,
            entry_price=entry_price,
        ))
        s.commit()


def db_get_pending_price_history() -> List[dict]:
    """Return signal price history rows that still have unfilled time buckets."""
    now = datetime.utcnow()
    with Session(engine) as s:
        rows = s.query(SignalPriceHistory).filter(
            SignalPriceHistory.signal_time >= now - timedelta(days=8),
            (SignalPriceHistory.price_7d == None) |
            (SignalPriceHistory.price_24h == None) |
            (SignalPriceHistory.price_1h == None) |
            (SignalPriceHistory.price_15m == None)
        ).all()
        return [{"id":r.id,"signal_id":r.signal_id,"ticker":r.ticker,
                 "platform":r.platform,"signal_time":r.signal_time,
                 "price_at_signal":r.price_at_signal,
                 "price_15m":r.price_15m,"price_1h":r.price_1h,
                 "price_4h":r.price_4h,"price_24h":r.price_24h,
                 "price_7d":r.price_7d} for r in rows]


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
