from __future__ import annotations

import argparse

from spark.jobs._pipelines import run_aggregates


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Silver aggregate tables.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()
    run_aggregates(start_date=args.start_date, end_date=args.end_date)


if __name__ == "__main__":
    main()
