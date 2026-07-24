#!/usr/bin/env python3
"""
audit_outcomes.py — Re-verify every resolved Polymarket signal against
Polymarket's actual current data.

Why this exists: check_signal_outcomes() used to (a) treat any price
>=95c/<=5c as proof a market had resolved, with no check that the market
was actually closed, and (b) look up price via event-level fuzzy title
matching, which could grab a sibling market's price on multi-market
events. Both bugs could permanently mislabel a signal's outcome, since
once outcome is set it's never re-checked.

This script re-checks every historically resolved Polymarket signal
using the CORRECT method (condition_id -> Gamma's own "closed" flag)
and reports:
  - FALSE_RESOLUTION: market isn't actually closed yet — was marked
    resolved based on a price spike or bad market match, not reality.
  - OUTCOME_MISMATCH: market IS closed, but the recorded WON/LOST
    doesn't match what the market actually settled to.
  - OK: recorded outcome matches the real, closed, final result.

Usage:
    python3 audit_outcomes.py "postgresql://...connection-string..."

    # To also generate a SQL fix file (does NOT touch your DB itself):
    python3 audit_outcomes.py "postgresql://..." --write-fixes fixes.sql
"""
import subprocess
import sys
import time
import json
import urllib.request
import urllib.error

PSQL = "/opt/homebrew/opt/libpq/bin/psql"
GAMMA_API = "https://gamma-api.polymarket.com"


def run_query(db_url: str, sql: str) -> list[str]:
    """Run a query via psql, return raw pipe-delimited rows."""
    result = subprocess.run(
        [PSQL, db_url, "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"psql error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return [line for line in result.stdout.strip().split("\n") if line]


def parse_outcome_price(raw):
    """Same logic as the app's _parse_outcome_price — first value in the list."""
    if raw is None:
        return None
    try:
        if isinstance(raw, str):
            raw = json.loads(raw)
        if isinstance(raw, list) and raw:
            return float(raw[0])
    except Exception:
        pass
    return None


def gamma_market_status(condition_id: str):
    """Look up a market's real closed status + price via condition_id."""
    if not condition_id:
        return None
    url = f"{GAMMA_API}/markets?condition_ids={condition_id}"
    try:
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36",
        })
        with urllib.request.urlopen(req, timeout=8) as resp:
            if resp.status != 200:
                return None
            data = json.loads(resp.read().decode())
        if not data:
            return None
        m = data[0]
        return {
            "closed": bool(m.get("closed", False)),
            "price": parse_outcome_price(m.get("outcomePrices")),
        }
    except (urllib.error.URLError, urllib.error.HTTPError, Exception) as e:
        if not _first_error_shown[0]:
            _first_error_shown[0] = True
            print(f"\n[!] First lookup failed — showing error for diagnosis: {e}\n"
                  f"    (further failures will be silent, counted as 'Unverifiable')\n")
        return None


_first_error_shown = [False]


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    db_url = sys.argv[1]
    write_fixes_path = None
    if "--write-fixes" in sys.argv:
        idx = sys.argv.index("--write-fixes")
        write_fixes_path = sys.argv[idx + 1]

    print("Fetching resolved Polymarket signals...")
    sql = """
        SELECT id, platform_signal_id, outcome, signal_type, market_title
        FROM signals
        WHERE outcome IN ('WON','LOST') AND platform='polymarket'
        ORDER BY id
    """
    rows = run_query(db_url, sql)
    print(f"Found {len(rows)} resolved signals. Checking each against live data...\n")

    false_resolutions = []
    mismatches = []
    ok_count = 0
    unverifiable = 0

    for i, line in enumerate(rows, 1):
        parts = line.split("|")
        if len(parts) < 5:
            continue
        sig_id, sig_key, outcome, sig_type, title = parts[0], parts[1], parts[2], parts[3], "|".join(parts[4:])

        key_parts = (sig_key or "").split(":")
        condition_id = key_parts[1] if len(key_parts) >= 2 else ""

        status = gamma_market_status(condition_id)
        if status is None:
            unverifiable += 1
            time.sleep(0.15)
            continue

        if not status["closed"]:
            false_resolutions.append((sig_id, title, outcome, sig_type))
        else:
            price = status["price"]
            if price is None:
                unverifiable += 1
            else:
                bullish = sig_type in ("UP", "BUY", "OPEN_POSITION", "LIVE_BUY")
                real_resolved_yes = price >= 0.5
                real_outcome = "WON" if (bullish == real_resolved_yes) else "LOST"
                if real_outcome != outcome:
                    mismatches.append((sig_id, title, outcome, real_outcome, sig_type))
                else:
                    ok_count += 1

        if i % 25 == 0:
            print(f"  checked {i}/{len(rows)}...")
        time.sleep(0.15)

    print("\n" + "=" * 60)
    print("AUDIT RESULTS")
    print("=" * 60)
    print(f"Total checked:        {len(rows)}")
    print(f"Confirmed OK:         {ok_count}")
    print(f"Unverifiable:         {unverifiable}  (Gamma lookup failed/no data)")
    print(f"FALSE RESOLUTIONS:    {len(false_resolutions)}  (market not actually closed)")
    print(f"OUTCOME MISMATCHES:   {len(mismatches)}  (closed, but wrong label)")

    if false_resolutions:
        print("\n--- FALSE RESOLUTIONS (market still open, should be pending) ---")
        for sig_id, title, outcome, sig_type in false_resolutions[:30]:
            print(f"  id={sig_id}  [{sig_type}]  recorded={outcome}  {title}")
        if len(false_resolutions) > 30:
            print(f"  ... and {len(false_resolutions) - 30} more")

    if mismatches:
        print("\n--- OUTCOME MISMATCHES (closed, but wrong label) ---")
        for sig_id, title, recorded, real, sig_type in mismatches[:30]:
            print(f"  id={sig_id}  [{sig_type}]  recorded={recorded} -> should be {real}  {title}")
        if len(mismatches) > 30:
            print(f"  ... and {len(mismatches) - 30} more")

    if write_fixes_path and (false_resolutions or mismatches):
        with open(write_fixes_path, "w") as f:
            f.write("-- Generated by audit_outcomes.py — review before running.\n")
            f.write("-- False resolutions: reset to NULL so the FIXED check_signal_outcomes()\n")
            f.write("-- can properly re-resolve them once the market actually closes.\n")
            for sig_id, title, outcome, sig_type in false_resolutions:
                f.write(f"UPDATE signals SET outcome = NULL WHERE id = {sig_id}; "
                        f"-- was {outcome}, {title}\n")
            f.write("\n-- Outcome mismatches: correct to the real, verified result.\n")
            for sig_id, title, recorded, real, sig_type in mismatches:
                f.write(f"UPDATE signals SET outcome = '{real}' WHERE id = {sig_id}; "
                        f"-- was {recorded}, {title}\n")
        print(f"\nFix SQL written to {write_fixes_path} — review it before running against your DB.")

    print("\nDone.")


if __name__ == "__main__":
    main()
