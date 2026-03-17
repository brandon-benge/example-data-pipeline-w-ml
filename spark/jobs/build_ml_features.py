from __future__ import annotations

import argparse

from spark.jobs._pipelines import run_ml_features


def main() -> None:
    parser = argparse.ArgumentParser(description="Build offline ML feature datasets and online parity tables.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()
    run_ml_features(start_date=args.start_date, end_date=args.end_date)


if __name__ == "__main__":
    main()
