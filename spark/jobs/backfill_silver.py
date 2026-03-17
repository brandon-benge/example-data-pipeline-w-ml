from __future__ import annotations

import argparse

from spark.jobs._pipelines import run_aggregates, run_dimensions, run_facts, run_ml_features


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Silver and ML feature tables for a date range.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    run_dimensions(start_date=args.start_date, end_date=args.end_date)
    run_facts(start_date=args.start_date, end_date=args.end_date)
    run_aggregates(start_date=args.start_date, end_date=args.end_date)
    run_ml_features(start_date=args.start_date, end_date=args.end_date)


if __name__ == "__main__":
    main()
