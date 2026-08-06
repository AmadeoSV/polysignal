#!/usr/bin/env python3
"""
backfill_polymarket_categories.py — one-time fix for Polymarket signals
that have never had a category at all (confirmed against the live DB:
100% of 6,715+ Polymarket signals have category=""). polymarket.py's
build_signals() now tags new signals with a real category pulled from
the event's own tags on Polymarket's Gamma API; this script backfills
every existing row that still has category IS NULL or ''.

Every Polymarket signal's market_url is built as
"https://polymarket.com/event/{eventSlug}" (confirmed in database.py),
so the event slug needed for the category lookup is recovered directly
from market_url — no separate eventSlug column exists on the Signal
table to read it from.

Not run automatically anywhere. Run by hand, once, after the
polymarket.py fix is deployed:

    DATABASE_URL="postgresql://postgres:PASSWORD@zephyr.proxy.rlwy.net:16828/railway" \
        python3 backfill_polymarket_categories.py

Safe to re-run — it only ever touches rows with a blank/missing category.
"""
from __future__ import annotations
import os
import sys
import time

from sqlalchemy.orm import Session

from database import engine, Signal
from polymarket import get_event_category


def event_slug_from_url(market_url: str) -> str:
    if not market_url or "/event/" not in market_url:
        return ""
    return market_url.rsplit("/event/", 1)[-1].strip("/")


def main() -> int:
    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL is not set. Example:")
        print('  DATABASE_URL="postgresql://postgres:PASSWORD@zephyr.proxy.rlwy.net:16828/railway" \\')
        print("      python3 backfill_polymarket_categories.py")
        return 1

    with Session(engine) as s:
        rows = s.query(Signal.id, Signal.market_url).filter(
            Signal.platform == "polymarket",
            (Signal.category == None) | (Signal.category == ""),
        ).all()

    if not rows:
        print("Nothing to backfill — no blank-category Polymarket rows found.")
        return 0

    print(f"Found {len(rows)} Polymarket rows with a blank category.")

    slug_cache: dict[str, str] = {}
    updates: dict[int, str] = {}
    unresolved_slugs: set[str] = set()
    no_slug = 0

    for i, (row_id, market_url) in enumerate(rows, 1):
        slug = event_slug_from_url(market_url)
        if not slug:
            no_slug += 1
            updates[row_id] = "other"
            continue
        if slug not in slug_cache:
            cat = get_event_category(slug)  # real API call, cached inside polymarket.py too
            slug_cache[slug] = cat
            if cat == "other":
                unresolved_slugs.add(slug)
            if len(slug_cache) % 25 == 0:
                print(f"  ...resolved {len(slug_cache)} distinct events so far "
                      f"({i}/{len(rows)} rows scanned)")
            time.sleep(0.1)  # be polite to the Gamma API across ~thousands of distinct events
        updates[row_id] = slug_cache[slug]

    print(f"\nResolved categories for {len(slug_cache)} distinct events.")
    if no_slug:
        print(f"NOTE: {no_slug} rows had no parseable event slug in market_url — set to 'other'.")
    if unresolved_slugs:
        print(f"NOTE: {len(unresolved_slugs)} events fell back to 'other' "
              f"(no recognized top-level tag, or API lookup failed).")

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
