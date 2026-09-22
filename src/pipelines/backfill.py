import argparse
from datetime import datetime

from pipelines.flows import backfill_hourly_snapshots


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not an ISO 8601 timestamp, e.g. 2026-09-19T00:00:00-03:00"
        ) from error
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} must include a UTC offset")
    return parsed


def run() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Re-export whole hours of snapshots to MinIO. Defaults to ARCHIVE_INITIAL_START "
            "through the last complete hour."
        )
    )
    parser.add_argument("--start", type=_timestamp, help="ISO 8601 start, with UTC offset.")
    parser.add_argument("--end", type=_timestamp, help="ISO 8601 exclusive end, with UTC offset.")
    parser.add_argument(
        "--max-hours",
        type=int,
        help="Cap hours processed in this run. 0 removes the cap. "
        "Defaults to ARCHIVE_MAX_CATCHUP_HOURS.",
    )
    args = parser.parse_args()

    result = backfill_hourly_snapshots(start=args.start, end=args.end, max_hours=args.max_hours)
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    run()
