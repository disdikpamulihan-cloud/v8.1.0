"""
confirm.py — v8.2.0 manual trade confirmation for LIVE mode.

Usage:
    python confirm.py list                          # list unconfirmed signals
    python confirm.py take <signal_id> <fill_price>  # mark taken + actual fill
    python confirm.py skip <signal_id>               # mark skipped

Requires SUPABASE_URL + SUPABASE_KEY in environment.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from supabase_client import SUPABASE


def cmd_list(_args) -> int:
    if not SUPABASE.enabled:
        print("❌ Supabase not configured. Set SUPABASE_URL + SUPABASE_KEY.")
        return 1
    rows = SUPABASE.fetch_unconfirmed()
    if not rows:
        print("✅ No unconfirmed signals.")
        return 0
    print(f"{'signal_id':<34} {'ts':<20} {'action':<5} {'entry':>9} "
          f"{'sl':>9} {'tp1':>9} {'lot':>7} {'score':>6}")
    for r in rows:
        print(f"{r.get('signal_id',''):<34} {str(r.get('ts',''))[:19]:<20} "
              f"{r.get('action',''):<5} "
              f"{float(r.get('entry_price',0)):>9.2f} "
              f"{float(r.get('sl',0)):>9.2f} "
              f"{float(r.get('tp1',0)):>9.2f} "
              f"{float(r.get('lot_size',0)):>7.2f} "
              f"{float(r.get('score',0)):>6.1f}")
    return 0


def cmd_take(args) -> int:
    if not SUPABASE.enabled:
        print("❌ Supabase not configured.")
        return 1
    ok = SUPABASE.confirm_signal(args.signal_id, actual_entry=args.fill_price)
    if ok:
        print(f"✅ Confirmed {args.signal_id} @ {args.fill_price}")
        return 0
    print(f"❌ Failed to confirm {args.signal_id}")
    return 1


def cmd_skip(args) -> int:
    if not SUPABASE.enabled:
        print("❌ Supabase not configured.")
        return 1
    ok = SUPABASE.skip_signal(args.signal_id)
    if ok:
        print(f"✅ Skipped {args.signal_id}")
        return 0
    print(f"❌ Failed to skip {args.signal_id}")
    return 1


def main() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="List unconfirmed live signals")

    take = sub.add_parser("take", help="Mark signal as taken with actual fill")
    take.add_argument("signal_id")
    take.add_argument("fill_price", type=float)

    skip = sub.add_parser("skip", help="Mark signal as skipped")
    skip.add_argument("signal_id")

    args = p.parse_args()
    if args.cmd == "list":
        return cmd_list(args)
    if args.cmd == "take":
        return cmd_take(args)
    if args.cmd == "skip":
        return cmd_skip(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
