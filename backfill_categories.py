#!/usr/bin/env python3
"""
backfill_categories.py — one-time fix for the always-blank Kalshi
`category` column caused by market.get("category","") reading a field
that no longer exists on Kalshi's /markets response (category moved to
the Series object). kalshi.py's fetch pipeline now tags new signals with
the real category going forward; this script backfills every existing
row that still has category IS NULL or ''.

Not run automatically anywhere. Run by hand, once, after the kalshi.py
fix is deployed:

    DATABASE_URL="postgresql://postgres:PASSWORD@zephyr.proxy.rlwy.net:16828/railway" \
        python3 backfill_categories.py

Safe to re-run — it only ever touches rows with a blank/missing category.
"""
from __future__ import annotations
import os
import sys

# Make sure this is run from the same directory as database.py/kalshi.py
# (or that they're importable), since it reuses the real engine + the
# real get_series_category() rather than re-implementing either.
from sqlalchemy import distinct
from sqlalchemy.orm import Session

from database import engine, Signal
from kalshi import get_series_category


def main() -> int:
    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL is not set. Example:")
        print('  DATABASE_URL="postgresql://postgres:PASSWORD@zephyr.proxy.rlwy.net:16828/railway" \\')
        print("      python3 backfill_categories.py")
        return 1

    with Session(engine) as s:
        rows = s.query(Signal.id, Signal.ticker).filter(
            Signal.platform == "kalshi",
            (Signal.category == None) | (Signal.category == ""),
        ).all()

    if not rows:
        print("Nothing to backfill — no blank-category Kalshi rows found.")
        return 0

    print(f"Found {len(rows)} Kalshi rows with a blank category.")

    # Kalshi tickers are SERIES-EVENT-MARKET, e.g. "KXMLBGAME-26AUG05DETBOS-DET"
    # -> series ticker is everything before the first "-". This gives the
    # exact real series for every row, no prefix-guessing needed here.
    series_cache: dict[str, str] = {}
    updates: dict[int, str] = {}
    unresolved_series: set[str] = set()

    for row_id, ticker in rows:
        series = (ticker or "").split("-")[0]
        if not series:
            continue
        if series not in series_cache:
            cat = get_series_category(series)  # real API call, cached inside kalshi.py too
            series_cache[series] = cat
            print(f"  {series:20s} -> {cat}")
            if cat == "other":
                unresolved_series.add(series)
        updates[row_id] = series_cache[series]

    print(f"\nResolved categories for {len(series_cache)} distinct series.")
    if unresolved_series:
        print(f"NOTE: {len(unresolved_series)} series fell back to 'other' "
              f"(API lookup failed or genuinely uncategorized): "
              f"{sorted(unresolved_series)}")

    # Apply in one transaction, batched by category for a bit of readability
    # in the confirmation printout.
    with Session(engine) as s:
        updated = 0
        for row_id, cat in updates.items():
            row = s.get(Signal, row_id)
            if row:
                row.category = cat
                updated += 1
        s.commit()

    print(f"\nDone — {updated} rows updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
