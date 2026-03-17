from __future__ import annotations

import logging
import os
import time
import traceback

from spark.jobs._pipelines import run_aggregates, run_dimensions, run_facts, run_ml_features
from spark.utils.common import build_spark, ensure_namespaces


LOGGER = logging.getLogger("spark_batch_scheduler")


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("SPARK_BATCH_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def main() -> None:
    configure_logging()
    interval_seconds = int(os.getenv("SPARK_BATCH_INTERVAL_SECONDS", "60"))
    spark = build_spark("spark-batch-scheduler")
    ensure_namespaces(spark)
    pipelines = (
        ("bronze_to_silver_dimensions", run_dimensions),
        ("bronze_to_silver_facts", run_facts),
        ("silver_aggregates", run_aggregates),
        ("build_ml_features", run_ml_features),
    )

    while True:
        cycle_started = time.time()
        LOGGER.info("Starting Spark batch cycle")
        failed_pipelines: list[str] = []
        for pipeline_name, pipeline_fn in pipelines:
            try:
                LOGGER.info("Running pipeline %s", pipeline_name)
                pipeline_fn(spark=spark)
            except Exception:
                failed_pipelines.append(pipeline_name)
                LOGGER.error("Pipeline %s failed:\n%s", pipeline_name, traceback.format_exc())

        elapsed = time.time() - cycle_started
        if failed_pipelines:
            LOGGER.warning(
                "Completed Spark batch cycle in %.1fs with failures: %s",
                elapsed,
                ", ".join(failed_pipelines),
            )
        else:
            LOGGER.info("Completed Spark batch cycle in %.1fs", elapsed)

        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
