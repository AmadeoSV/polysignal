#!/usr/bin/env python3
"""
audit_kalshi_outcomes.py — re-verifies every currently-resolved Kalshi
signal against Kalshi's real, live market status.

Why this exists: check_signal_outcomes() used to write an outcome as
soon as Kalshi's `result` field was non-empty, without checking that
`status` had actually reached "finalized". Kalshi's real lifecycle is
active -> closed -> determined -> finalized (disputed/amended also
real, possible states), and `result` can populate at an earlier,
non-final stage. Caught directly on a live MLB game (KXMLBGAME-
26AUG061805WSHPHI-WSH) that had WON/LOST/blank written and rewritten
across 27 signal rows over two hours, while Kalshi's own site still
showed the game live in the 7th inning. The live code is now fixed
(see check_signal_outcomes() in signals.py); this script finds out how
much of the ALREADY-resolved history that same gap may have corrupted.

Three-way classification per distinct (ticker, direction):
  1. OK             — currently finalized, stored outcome matches
  2. WRONG_OUTCOME   — currently finalized, stored outcome does NOT match
  3. NOT_FINALIZED   — resolved in the DB, but Kalshi doesn't currently
                       consider it finalized. Even if the stored value
                       happens to match today's provisional state, it
                       was written before the real bug's fix and isn't
                       safe to trust as final -- flagged for reset back
                       to pending so the fixed resolution loop re-checks
                       it once truly finalized.

Read-only. Reports findings and writes a review file; does not touch
the database. Review the output before applying any correction.

Usage:
    DATABASE_URL="postgresql://..." python3 audit_kalshi_outcomes.py
"""
from __future__ import annotations
import os
import sys
import time
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session

from database import engine
from kalshi import fetch_market_status


def main() -> int:
    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL is not set. Example:")
        print('  DATABASE_URL="postgresql://postgres:PASSWORD@zephyr.proxy.rlwy.net:16828/railway" \\')
        print("      python3 audit_kalshi_outcomes.py")
        return 1

    with Session(engine) as s:
        rows = s.execute(text("""
            SELECT DISTINCT ticker, signal_type, category, outcome
            FROM signals
            WHERE platform = 'kalshi'
              AND outcome IS NOT NULL
              AND signal_type IN ('UP','DOWN')
        """)).fetchall()

    total = len(rows)
    print(f"Checking {total} distinct (ticker, direction) resolved markets against live Kalshi data...")
    if total == 0:
        print("Nothing resolved yet — nothing to audit.")
        return 0

    ok = []
    wrong_outcome = []
    not_finalized = []
    fetch_failed = []
    by_category_total: dict[str, int] = defaultdict(int)
    by_category_bad: dict[str, int] = defaultdict(int)

    for i, (ticker, sig_type, category, stored_outcome) in enumerate(rows, 1):
        by_category_total[category or "uncategorized"] += 1

        status = fetch_market_status(ticker)
        time.sleep(0.15)

        if status is None:
            fetch_failed.append((ticker, sig_type, category, stored_outcome))
            by_category_bad[category or "uncategorized"] += 1
            continue

        if status.get("status") != "finalized":
            # This is the bug signature itself -- resolved in the DB
            # while Kalshi doesn't currently consider the market done,
            # regardless of whether the stored value happens to match
            # a provisional result right now.
            not_finalized.append((ticker, sig_type, category, stored_outcome,
                                   status.get("status"), status.get("result")))
            by_category_bad[category or "uncategorized"] += 1
            continue

        if not status.get("result"):
            # Finalized with no result is unusual (voided market?) --
            # treat like not-finalized: can't trust the stored outcome.
            not_finalized.append((ticker, sig_type, category, stored_outcome,
                                   status.get("status"), status.get("result")))
            by_category_bad[category or "uncategorized"] += 1
            continue

        resolved_yes = status["result"] == "yes"
        bullish = sig_type == "UP"
        correct_outcome = "WON" if (bullish == resolved_yes) else "LOST"

        if correct_outcome != stored_outcome:
            wrong_outcome.append((ticker, sig_type, category, stored_outcome, correct_outcome))
            by_category_bad[category or "uncategorized"] += 1
        else:
            ok.append((ticker, sig_type, category))

        if i % 50 == 0:
            bad_so_far = len(wrong_outcome) + len(not_finalized) + len(fetch_failed)
            print(f"  ...checked {i}/{total}, {bad_so_far} issues found so far")

    print(f"\nDone. Checked {total} distinct markets.\n")
    print(f"  OK (finalized, correct):        {len(ok)}")
    print(f"  WRONG_OUTCOME (finalized, but stored value is wrong): {len(wrong_outcome)}")
    print(f"  NOT_FINALIZED (resolved too early, can't trust yet):  {len(not_finalized)}")
    print(f"  fetch failed (couldn't check):   {len(fetch_failed)}")

    print("\nBy category (bad / total):")
    for cat, tot in sorted(by_category_total.items(), key=lambda x: -x[1]):
        print(f"  {cat:15s} {by_category_bad[cat]:4d} / {tot}")

    if wrong_outcome:
        print(f"\n--- WRONG_OUTCOME details ({len(wrong_outcome)}) ---")
        for ticker, sig_type, category, stored, correct in wrong_outcome[:30]:
            print(f"  {ticker} [{sig_type}] ({category}): stored={stored}, should be={correct}")
        if len(wrong_outcome) > 30:
            print(f"  ...and {len(wrong_outcome) - 30} more")

    if not_finalized:
        print(f"\n--- NOT_FINALIZED details ({len(not_finalized)}) ---")
        for ticker, sig_type, category, stored, live_status, live_result in not_finalized[:30]:
            print(f"  {ticker} [{sig_type}] ({category}): stored={stored}, "
                  f"live status={live_status!r}, live result={live_result!r}")
        if len(not_finalized) > 30:
            print(f"  ...and {len(not_finalized) - 30} more")

    # Write a plain-text review file with everything that needs action,
    # so a correction pass can work off this without re-running the audit.
    out_path = "kalshi_audit_findings.txt"
    with open(out_path, "w") as f:
        f.write(f"Kalshi outcome audit — {total} distinct markets checked\n")
        f.write(f"OK: {len(ok)}  WRONG_OUTCOME: {len(wrong_outcome)}  "
                f"NOT_FINALIZED: {len(not_finalized)}  fetch_failed: {len(fetch_failed)}\n\n")
        f.write("=== WRONG_OUTCOME (needs correction to the correct value) ===\n")
        for ticker, sig_type, category, stored, correct in wrong_outcome:
            f.write(f"{ticker}\t{sig_type}\t{category}\tstored={stored}\tcorrect={correct}\n")
        f.write("\n=== NOT_FINALIZED (needs reset to NULL/pending for re-check) ===\n")
        for ticker, sig_type, category, stored, live_status, live_result in not_finalized:
            f.write(f"{ticker}\t{sig_type}\t{category}\tstored={stored}\t"
                    f"live_status={live_status}\tlive_result={live_result}\n")
        f.write("\n=== fetch_failed (couldn't verify, needs manual check) ===\n")
        for ticker, sig_type, category, stored in fetch_failed:
            f.write(f"{ticker}\t{sig_type}\t{category}\tstored={stored}\n")

    print(f"\nFull findings written to {out_path} — review before correcting anything.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
