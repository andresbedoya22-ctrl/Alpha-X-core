from __future__ import annotations

import argparse
import logging
from pathlib import Path

from alpha_x.execution.runner import build_status_text, run_daily_review


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Distance buffer v1 daily review.")
    parser.add_argument("--state-path", type=Path, default=None)
    parser.add_argument("--journal-path", type=Path, default=None)
    parser.add_argument("--dataset-path", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--status", action="store_true")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    if args.status:
        print(
            build_status_text(
                state_path=args.state_path,
                dataset_path=args.dataset_path,
            )
        )
        return 0
    result = run_daily_review(
        state_path=args.state_path,
        journal_path=args.journal_path,
        dataset_path=args.dataset_path,
        send_telegram=not args.no_telegram,
        dry_run=args.dry_run,
    )
    print(result.message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
