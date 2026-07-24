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

This version fixes two more things found while building it:
  - Gamma's /markets?condition_ids= search doesn't reliably index older
    or settled sports markets. The CLOB API (clob.polymarket.com) does,
    and returns an explicit closed flag plus a per-outcome "winner"
    boolean — so CLOB is now the primary source, Gamma a fallback.
  - Comparing "price >= 0.5" generically is wrong for named-outcome
    markets (Over/Under, named candidates, etc.) — you have to check
    whether the SPECIFIC recorded outcome (e.g. "Raphael Collignon",
    "Over") is the one that actually won, not just whether some price
    crossed 50%. This version matches by name against CLOB's tokens
    or Gamma's outcomes list.

Reports:
  - FALSE_RESOLUTION: market isn't actually closed yet.
  - OUTCOME_MISMATCH: market IS closed, but the recorded WON/LOST
    doesn't match what actually happened.
  - OK: recorded outcome matches the real, closed, final result.
  - UNVERIFIABLE: neither CLOB nor Gamma had usable data.

Usage:
    python3 audit_outcomes.py "postgresql://...connection-string..."
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
CLOB_API = "https://clob.polymarket.com"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

_first_error_shown = [False]


def run_query(db_url: str, sql: str) -> list[str]:
    result = subprocess.run(
        [PSQL, db_url, "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"psql error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return [line for line in result.stdout.strip().split("\n") if line]


def _get_json(url: str):
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": UA})
    with urllib.request.urlopen(req, timeout=8) as resp:
        if resp.status != 200:
            return None
        return json.loads(resp.read().decode())


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def clob_status(condition_id: str, outcome_name: str):
    """Primary source. Returns {'closed':bool,'winner':bool or None} or None."""
    try:
        data = _get_json(f"{CLOB_API}/markets/{condition_id}")
        if not data:
            return None
        closed = bool(data.get("closed", False))
        tokens = data.get("tokens", []) or []
        winner = None
        for t in tokens:
            if _norm(t.get("outcome")) == _norm(outcome_name):
                winner = bool(t.get("winner", False))
                break
        return {"closed": closed, "winner": winner, "matched": winner is not None}
    except Exception as e:
        if not _first_error_shown[0]:
            _first_error_shown[0] = True
            print(f"\n[!] First CLOB lookup failed — {e}\n"
                  f"    (further failures silent, will try Gamma fallback)\n")
        return None


def gamma_status(condition_id: str, outcome_name: str):
    """Fallback source."""
    try:
        data = _get_json(f"{GAMMA_API}/markets?condition_ids={condition_id}")
        if not data:
            return None
        m = data[0]
        closed = bool(m.get("closed", False))
        outcomes = m.get("outcomes")
        prices = m.get("outcomePrices")
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices, str):
            prices = json.loads(prices)
        winner = None
        if outcomes and prices and len(outcomes) == len(prices):
            for name, price in zip(outcomes, prices):
                if _norm(name) == _norm(outcome_name):
                    winner = float(price) >= 0.5
                    break
        return {"closed": closed, "winner": winner, "matched": winner is not None}
    except Exception:
        return None


def check_market(condition_id: str, outcome_name: str):
    status = clob_status(condition_id, outcome_name)
    if status is None:
        status = gamma_status(condition_id, outcome_name)
    return status


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
    unmatched_name = []
    ok_count = 0
    unverifiable = 0

    for i, line in enumerate(rows, 1):
        parts = line.split("|")
        if len(parts) < 5:
            continue
        sig_id, sig_key, outcome, sig_type = parts[0], parts[1], parts[2], parts[3]
        title = "|".join(parts[4:])

        key_parts = (sig_key or "").split(":")
        condition_id = key_parts[1] if len(key_parts) >= 2 else ""
        outcome_name = key_parts[2] if len(key_parts) >= 3 else ""

        status = check_market(condition_id, outcome_name)

        if status is None:
            unverifiable += 1
        elif not status["closed"]:
            false_resolutions.append((sig_id, title, outcome, sig_type))
        elif not status["matched"]:
            unmatched_name.append((sig_id, title, outcome_name))
        else:
            real_outcome = "WON" if status["winner"] else "LOST"
            if real_outcome != outcome:
                mismatches.append((sig_id, title, outcome, real_outcome, sig_type))
            else:
                ok_count += 1

        if i % 100 == 0:
            print(f"  checked {i}/{len(rows)}...")
        time.sleep(0.12)

    print("\n" + "=" * 60)
    print("AUDIT RESULTS")
    print("=" * 60)
    print(f"Total checked:        {len(rows)}")
    print(f"Confirmed OK:         {ok_count}")
    print(f"Unverifiable:         {unverifiable}  (no data from CLOB or Gamma)")
    print(f"Name unmatched:       {len(unmatched_name)}  (market found, couldn't match outcome name)")
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

    if unmatched_name:
        print("\n--- NAME UNMATCHED (sample, market data format may differ) ---")
        for sig_id, title, outcome_name in unmatched_name[:10]:
            print(f"  id={sig_id}  outcome_name='{outcome_name}'  {title}")

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
