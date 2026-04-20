from __future__ import annotations

import argparse
import logging

from alpha_x.execution.scheduler import run_daily_scheduler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Distance buffer v1 daily scheduler.")
    parser.add_argument("--timezone", default="Europe/Amsterdam")
    parser.add_argument("--sleep-seconds", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_daily_scheduler(
        timezone=args.timezone,
        sleep_seconds=args.sleep_seconds,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
