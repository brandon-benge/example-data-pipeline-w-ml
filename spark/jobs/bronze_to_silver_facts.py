from __future__ import annotations

import argparse

from spark.jobs._pipelines import run_facts


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Silver clean fact tables from Bronze.")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()
    run_facts(start_date=args.start_date, end_date=args.end_date)


if __name__ == "__main__":
    main()
