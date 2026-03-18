from __future__ import annotations

from time import monotonic
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark.utils.common import (
    PROJECT_ROOT,
    SPARK_SQL_ROOT,
    append_dq_result,
    build_spark,
    ensure_namespaces,
    execute_sql_file,
    latest_by_event_uuid,
    latest_cdc,
    load_yaml,
    optional_date_filter,
    write_table,
)
from spark.utils.lineage import record_lineage
from spark.utils.masking import (
    mask_email,
    mask_name,
    mask_phone,
    mask_zip_code,
    tokenized_column,
)
from spark.utils.ownership import owner_for_dataset, sensitivity_for_dataset


def _initialize_spark(app_name: str, spark: SparkSession | None = None) -> SparkSession:
    spark = spark or build_spark(app_name)
    ensure_namespaces(spark)
    execute_sql_file(spark, SPARK_SQL_ROOT / "silver_tables.sql")
    return spark


def _enrich_metadata(df: DataFrame, dataset_name: str) -> DataFrame:
    return (
        df.withColumn("data_owner", F.lit(owner_for_dataset(dataset_name)))
        .withColumn("sensitivity_class", F.lit(sensitivity_for_dataset(dataset_name)))
    )


def _new_run_id(job_name: str) -> str:
    return f"{job_name}-{uuid4().hex[:12]}"


def _write_with_lineage(
    df: DataFrame,
    table_name: str,
    *,
    job_name: str,
    upstream_datasets: list[str],
    run_id: str,
    mode: str = "overwrite",
) -> int:
    started_at = monotonic()
    row_count = df.count()
    write_table(df, table_name, mode=mode)
    record_lineage(
        job_name,
        upstream_datasets,
        table_name,
        run_id=run_id,
        row_count=row_count,
        duration_ms=int((monotonic() - started_at) * 1000),
        write_mode=mode,
        metadata={"writer": "spark"},
    )
    return row_count


def run_dimensions(
    start_date: str | None = None,
    end_date: str | None = None,
    spark: SparkSession | None = None,
) -> None:
    managed_spark = spark is None
    spark = _initialize_spark("bronze-to-silver-dimensions", spark=spark)
    run_id = _new_run_id("bronze_to_silver_dimensions")
    silver_processed_ts = F.current_timestamp()

    def current_df(bronze_table: str, business_key: str) -> DataFrame:
        df = spark.table(f"iceberg.bronze.{bronze_table}")
        return latest_cdc(df, business_key)

    sales_rep = current_df("bronze_sales_rep_cdc", "sales_rep_id").select(
        F.col("payload_sales_rep_id").alias("sales_rep_id"),
        F.col("payload_rep_name").alias("rep_name"),
        F.col("payload_team_name").alias("team_name"),
        F.col("payload_region").alias("region"),
        F.col("payload_manager_name").alias("manager_name"),
        F.col("payload_status").alias("status"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    sales_rep = _enrich_metadata(sales_rep, "iceberg.silver.silver_sales_rep_current")
    _write_with_lineage(
        sales_rep,
        "iceberg.silver.silver_sales_rep_current",
        job_name="bronze_to_silver_dimensions",
        upstream_datasets=["iceberg.bronze.bronze_sales_rep_cdc"],
        run_id=run_id,
    )

    customer = current_df("bronze_customer_cdc", "customer_id").select(
        F.col("payload_customer_id").alias("customer_id"),
        mask_name(F.col("payload_first_name")).alias("first_name_masked"),
        mask_name(F.col("payload_last_name")).alias("last_name_masked"),
        mask_email(F.col("payload_email")).alias("email_masked"),
        mask_phone(F.col("payload_phone")).alias("phone_masked"),
        F.col("payload_city").alias("city"),
        F.col("payload_state").alias("state"),
        mask_zip_code(F.col("payload_zip_code")).alias("zip_code_masked"),
        F.col("payload_status").alias("status"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    customer = _enrich_metadata(customer, "iceberg.silver.silver_customer_current")
    _write_with_lineage(
        customer,
        "iceberg.silver.silver_customer_current",
        job_name="bronze_to_silver_dimensions",
        upstream_datasets=["iceberg.bronze.bronze_customer_cdc"],
        run_id=run_id,
    )

    advertiser = current_df("bronze_advertiser_cdc", "advertiser_id").select(
        F.col("payload_advertiser_id").alias("advertiser_id"),
        F.col("payload_advertiser_name").alias("advertiser_name"),
        F.col("payload_industry").alias("industry"),
        F.col("payload_account_tier").alias("account_tier"),
        F.col("payload_region").alias("region"),
        F.col("payload_owner_sales_rep_id").alias("owner_sales_rep_id"),
        F.col("payload_status").alias("status"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    advertiser = _enrich_metadata(advertiser, "iceberg.silver.silver_advertiser_current")
    _write_with_lineage(
        advertiser,
        "iceberg.silver.silver_advertiser_current",
        job_name="bronze_to_silver_dimensions",
        upstream_datasets=["iceberg.bronze.bronze_advertiser_cdc"],
        run_id=run_id,
    )

    product = current_df("bronze_product_cdc", "product_id").select(
        F.col("payload_product_id").alias("product_id"),
        F.col("payload_sku").alias("sku"),
        F.col("payload_product_name").alias("product_name"),
        F.col("payload_brand").alias("brand"),
        F.col("payload_category").alias("category"),
        F.col("payload_subcategory").alias("subcategory"),
        F.col("payload_list_price").alias("list_price"),
        F.col("payload_cost").alias("cost"),
        F.col("payload_active_flag").alias("active_flag"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    product = _enrich_metadata(product, "iceberg.silver.silver_product_current")
    _write_with_lineage(
        product,
        "iceberg.silver.silver_product_current",
        job_name="bronze_to_silver_dimensions",
        upstream_datasets=["iceberg.bronze.bronze_product_cdc"],
        run_id=run_id,
    )

    campaign = current_df("bronze_campaign_cdc", "campaign_id").select(
        F.col("payload_campaign_id").alias("campaign_id"),
        F.col("payload_advertiser_id").alias("advertiser_id"),
        F.col("payload_campaign_name").alias("campaign_name"),
        F.col("payload_campaign_type").alias("campaign_type"),
        F.col("payload_objective").alias("objective"),
        F.col("payload_budget_amount").alias("budget_amount"),
        F.col("payload_start_date").alias("start_date"),
        F.col("payload_end_date").alias("end_date"),
        F.col("payload_status").alias("status"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    campaign = _enrich_metadata(campaign, "iceberg.silver.silver_campaign_current")
    _write_with_lineage(
        campaign,
        "iceberg.silver.silver_campaign_current",
        job_name="bronze_to_silver_dimensions",
        upstream_datasets=["iceberg.bronze.bronze_campaign_cdc"],
        run_id=run_id,
    )

    campaign_product = current_df("bronze_campaign_product_cdc", "campaign_product_id").select(
        F.col("payload_campaign_product_id").alias("campaign_product_id"),
        F.col("payload_campaign_id").alias("campaign_id"),
        F.col("payload_product_id").alias("product_id"),
        F.col("payload_bid_amount").alias("bid_amount"),
        F.col("payload_priority").alias("priority"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    campaign_product = _enrich_metadata(campaign_product, "iceberg.silver.silver_campaign_product_current")
    _write_with_lineage(
        campaign_product,
        "iceberg.silver.silver_campaign_product_current",
        job_name="bronze_to_silver_dimensions",
        upstream_datasets=["iceberg.bronze.bronze_campaign_product_cdc"],
        run_id=run_id,
    )

    for table_name, key_column in (
        ("iceberg.silver.silver_sales_rep_current", "sales_rep_id"),
        ("iceberg.silver.silver_customer_current", "customer_id"),
        ("iceberg.silver.silver_advertiser_current", "advertiser_id"),
        ("iceberg.silver.silver_product_current", "product_id"),
        ("iceberg.silver.silver_campaign_current", "campaign_id"),
        ("iceberg.silver.silver_campaign_product_current", "campaign_product_id"),
    ):
        df = spark.table(table_name)
        null_count = df.filter(F.col(key_column).isNull()).count()
        duplicate_count = df.groupBy(key_column).count().filter(F.col("count") > 1).count()
        append_dq_result(
            "primary_key_not_null",
            "critical",
            table_name,
            null_count == 0,
            {"null_count": null_count, "expected_null_count": 0},
            run_id=run_id,
        )
        append_dq_result(
            "business_key_uniqueness",
            "critical",
            table_name,
            duplicate_count == 0,
            {"duplicate_count": duplicate_count, "expected_duplicate_count": 0},
            run_id=run_id,
        )
    if managed_spark:
        spark.stop()


def run_facts(
    start_date: str | None = None,
    end_date: str | None = None,
    spark: SparkSession | None = None,
) -> None:
    managed_spark = spark is None
    spark = _initialize_spark("bronze-to-silver-facts", spark=spark)
    run_id = _new_run_id("bronze_to_silver_facts")
    silver_processed_ts = F.current_timestamp()

    customer_session = latest_cdc(
        spark.table("iceberg.bronze.bronze_customer_session_cdc"), "session_id"
    ).select(
        F.col("payload_session_id").alias("session_id"),
        F.col("payload_customer_id").alias("customer_id"),
        F.col("payload_session_start_ts").alias("session_start_ts"),
        F.col("payload_session_end_ts").alias("session_end_ts"),
        F.col("payload_device_type").alias("device_type"),
        F.col("payload_channel").alias("channel"),
        F.col("payload_referrer_type").alias("referrer_type"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    customer_session = optional_date_filter(customer_session, "updated_at", start_date, end_date)
    customer_session = customer_session.filter(F.col("session_end_ts").isNull() | (F.col("session_start_ts") <= F.col("session_end_ts")))
    _write_with_lineage(
        customer_session,
        "iceberg.silver.silver_customer_session",
        job_name="bronze_to_silver_facts",
        upstream_datasets=["iceberg.bronze.bronze_customer_session_cdc"],
        run_id=run_id,
    )

    session_events = latest_by_event_uuid(spark.table("iceberg.bronze.bronze_session_event_raw"))
    session_events = optional_date_filter(session_events, "event_date", start_date, end_date)
    session_events = session_events.filter(F.col("event_type").isin("product_view", "ad_impression", "ad_click", "add_to_cart", "checkout_start"))
    silver_events = session_events.select(
        "event_uuid",
        "session_id",
        "customer_id",
        "event_ts",
        "event_date",
        "event_type",
        "product_id",
        "campaign_id",
        "page_type",
        "search_term",
        "position_in_list",
        "ingest_ts",
        "producer_version",
        "schema_version",
        silver_processed_ts.alias("silver_processed_ts"),
    )
    _write_with_lineage(
        silver_events,
        "iceberg.silver.silver_session_event_clean",
        job_name="bronze_to_silver_facts",
        upstream_datasets=["iceberg.bronze.bronze_session_event_raw"],
        run_id=run_id,
    )

    order_header = latest_cdc(spark.table("iceberg.bronze.bronze_order_header_cdc"), "order_id").select(
        F.col("payload_order_id").alias("order_id"),
        F.col("payload_customer_id").alias("customer_id"),
        F.col("payload_order_ts").alias("order_ts"),
        F.col("payload_order_status").alias("order_status"),
        F.col("payload_subtotal_amount").alias("subtotal_amount"),
        F.col("payload_discount_amount").alias("discount_amount"),
        F.col("payload_tax_amount").alias("tax_amount"),
        F.col("payload_total_amount").alias("total_amount"),
        F.col("payload_payment_type").alias("payment_type"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    order_header = optional_date_filter(order_header, "order_ts", start_date, end_date)
    order_header = order_header.filter(
        (F.col("subtotal_amount") >= 0)
        & (F.col("discount_amount") >= 0)
        & (F.col("tax_amount") >= 0)
        & (F.col("total_amount") >= 0)
    )
    _write_with_lineage(
        order_header,
        "iceberg.silver.silver_order_header",
        job_name="bronze_to_silver_facts",
        upstream_datasets=["iceberg.bronze.bronze_order_header_cdc"],
        run_id=run_id,
    )

    order_item = latest_cdc(spark.table("iceberg.bronze.bronze_order_item_cdc"), "order_item_id").select(
        F.col("payload_order_item_id").alias("order_item_id"),
        F.col("payload_order_id").alias("order_id"),
        F.col("payload_product_id").alias("product_id"),
        F.col("payload_quantity").alias("quantity"),
        F.col("payload_unit_price").alias("unit_price"),
        F.col("payload_line_amount").alias("line_amount"),
        F.col("payload_attributed_campaign_id").alias("attributed_campaign_id"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    order_item = optional_date_filter(order_item, "updated_at", start_date, end_date)
    order_item = order_item.filter((F.col("quantity") > 0) & (F.col("line_amount") >= 0))
    order_item = order_item.filter(F.abs(F.col("line_amount") - (F.col("quantity") * F.col("unit_price"))) <= F.lit(0.01))
    _write_with_lineage(
        order_item,
        "iceberg.silver.silver_order_item",
        job_name="bronze_to_silver_facts",
        upstream_datasets=["iceberg.bronze.bronze_order_item_cdc"],
        run_id=run_id,
    )

    sales_activity = latest_cdc(spark.table("iceberg.bronze.bronze_sales_activity_cdc"), "sales_activity_id").select(
        F.col("payload_sales_activity_id").alias("sales_activity_id"),
        F.col("payload_advertiser_id").alias("advertiser_id"),
        F.col("payload_sales_rep_id").alias("sales_rep_id"),
        F.col("payload_activity_ts").alias("activity_ts"),
        F.col("payload_activity_type").alias("activity_type"),
        F.col("payload_activity_outcome").alias("activity_outcome"),
        F.col("payload_created_at").alias("created_at"),
        F.col("payload_updated_at").alias("updated_at"),
        F.col("source_ts").alias("source_last_change_ts"),
        silver_processed_ts.alias("silver_processed_ts"),
    )
    sales_activity = optional_date_filter(sales_activity, "activity_ts", start_date, end_date)
    _write_with_lineage(
        sales_activity,
        "iceberg.silver.silver_sales_activity",
        job_name="bronze_to_silver_facts",
        upstream_datasets=["iceberg.bronze.bronze_sales_activity_cdc"],
        run_id=run_id,
    )

    checks = [
        ("iceberg.silver.silver_session_event_clean", "event_type_enum", silver_events.filter(~F.col("event_type").isin("product_view", "ad_impression", "ad_click", "add_to_cart", "checkout_start")).count()),
        ("iceberg.silver.silver_order_header", "non_negative_amounts", order_header.filter((F.col("subtotal_amount") < 0) | (F.col("discount_amount") < 0) | (F.col("tax_amount") < 0) | (F.col("total_amount") < 0)).count()),
        ("iceberg.silver.silver_order_item", "line_amount_consistency", order_item.filter(F.abs(F.col("line_amount") - (F.col("quantity") * F.col("unit_price"))) > F.lit(0.01)).count()),
        ("iceberg.silver.silver_customer_session", "session_window", customer_session.filter(F.col("session_end_ts").isNotNull() & (F.col("session_start_ts") > F.col("session_end_ts"))).count()),
    ]
    for dataset, rule_name, failures in checks:
        append_dq_result(
            rule_name,
            "critical",
            dataset,
            failures == 0,
            {"failure_count": failures, "expected_failure_count": 0},
            run_id=run_id,
        )
    if managed_spark:
        spark.stop()


def run_aggregates(
    start_date: str | None = None,
    end_date: str | None = None,
    spark: SparkSession | None = None,
) -> None:
    managed_spark = spark is None
    spark = _initialize_spark("silver-aggregates", spark=spark)
    run_id = _new_run_id("silver_aggregates")
    processed_ts = F.current_timestamp()

    events = optional_date_filter(spark.table("iceberg.silver.silver_session_event_clean"), "event_date", start_date, end_date)
    order_header = optional_date_filter(spark.table("iceberg.silver.silver_order_header"), "order_ts", start_date, end_date)
    order_item = optional_date_filter(spark.table("iceberg.silver.silver_order_item"), "updated_at", start_date, end_date)
    campaign = spark.table("iceberg.silver.silver_campaign_current")
    sales_activity = optional_date_filter(spark.table("iceberg.silver.silver_sales_activity"), "activity_ts", start_date, end_date)

    customer_daily = (
        events.groupBy(F.col("event_date").alias("metric_date"), "customer_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "ad_click", 1).otherwise(0)).alias("ad_clicks"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
            F.sum(F.when(F.col("event_type") == "checkout_start", 1).otherwise(0)).alias("checkout_starts"),
        )
        .join(
            order_header.withColumn("metric_date", F.to_date("order_ts"))
            .groupBy("metric_date", "customer_id")
            .agg(
                F.countDistinct("order_id").alias("purchases"),
                F.sum("total_amount").alias("order_amount"),
                F.avg("total_amount").alias("avg_order_value"),
            ),
            ["metric_date", "customer_id"],
            "full_outer",
        )
        .na.fill(0, ["views", "ad_clicks", "add_to_cart", "checkout_starts", "purchases", "order_amount", "avg_order_value"])
        .withColumn("processed_ts", processed_ts)
    )
    _write_with_lineage(
        customer_daily,
        "iceberg.silver.silver_customer_daily_metrics",
        job_name="silver_aggregates",
        upstream_datasets=["iceberg.silver.silver_session_event_clean", "iceberg.silver.silver_order_header"],
        run_id=run_id,
    )

    product_daily = (
        events.filter(F.col("product_id").isNotNull())
        .groupBy(F.col("event_date").alias("metric_date"), "product_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
        )
        .join(
            order_item.join(order_header.select("order_id", F.to_date("order_ts").alias("metric_date")), "order_id")
            .groupBy("metric_date", "product_id")
            .agg(
                F.countDistinct("order_id").alias("attributed_orders"),
                F.sum("line_amount").alias("attributed_revenue"),
            ),
            ["metric_date", "product_id"],
            "full_outer",
        )
        .na.fill(0, ["product_views", "add_to_cart", "attributed_orders", "attributed_revenue"])
        .withColumn("processed_ts", processed_ts)
    )
    _write_with_lineage(
        product_daily,
        "iceberg.silver.silver_product_daily_metrics",
        job_name="silver_aggregates",
        upstream_datasets=[
            "iceberg.silver.silver_session_event_clean",
            "iceberg.silver.silver_order_item",
            "iceberg.silver.silver_order_header",
        ],
        run_id=run_id,
    )

    campaign_daily = (
        events.filter(F.col("campaign_id").isNotNull())
        .groupBy(F.col("event_date").alias("metric_date"), "campaign_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "ad_impression", 1).otherwise(0)).alias("impressions"),
            F.sum(F.when(F.col("event_type") == "ad_click", 1).otherwise(0)).alias("clicks"),
        )
        .join(campaign.select("campaign_id", "advertiser_id"), "campaign_id", "left")
        .join(
            order_item.join(order_header.select("order_id", F.to_date("order_ts").alias("metric_date")), "order_id")
            .filter(F.col("attributed_campaign_id").isNotNull())
            .groupBy("metric_date", F.col("attributed_campaign_id").alias("campaign_id"))
            .agg(
                F.countDistinct("order_id").alias("attributed_orders"),
                F.sum("line_amount").alias("attributed_revenue"),
            ),
            ["metric_date", "campaign_id"],
            "left",
        )
        .join(
            sales_activity.withColumn("metric_date", F.to_date("activity_ts"))
            .join(campaign.select("campaign_id", "advertiser_id"), "advertiser_id", "left")
            .groupBy("metric_date", "campaign_id")
            .agg(F.countDistinct("sales_activity_id").alias("sales_contacts")),
            ["metric_date", "campaign_id"],
            "left",
        )
        .na.fill(0, ["impressions", "clicks", "attributed_orders", "attributed_revenue", "sales_contacts"])
        .withColumn("processed_ts", processed_ts)
        .select(
            "metric_date",
            "campaign_id",
            "advertiser_id",
            "impressions",
            "clicks",
            "attributed_orders",
            F.col("attributed_revenue").cast("decimal(18,2)").alias("attributed_revenue"),
            "sales_contacts",
            "processed_ts",
        )
    )
    _write_with_lineage(
        campaign_daily,
        "iceberg.silver.silver_campaign_daily_metrics",
        job_name="silver_aggregates",
        upstream_datasets=[
            "iceberg.silver.silver_session_event_clean",
            "iceberg.silver.silver_order_item",
            "iceberg.silver.silver_order_header",
            "iceberg.silver.silver_sales_activity",
            "iceberg.silver.silver_campaign_current",
        ],
        run_id=run_id,
    )

    metric_dates = (
        campaign_daily.select("metric_date", "advertiser_id")
        .unionByName(
            sales_activity.withColumn("metric_date", F.to_date("activity_ts")).select("metric_date", "advertiser_id"),
            allowMissingColumns=False,
        )
        .distinct()
    )
    active_campaigns = (
        metric_dates.alias("dates")
        .join(
            campaign.alias("campaigns"),
            (F.col("dates.advertiser_id") == F.col("campaigns.advertiser_id"))
            & (F.col("dates.metric_date") >= F.col("campaigns.start_date"))
            & (F.col("campaigns.end_date").isNull() | (F.col("dates.metric_date") <= F.col("campaigns.end_date"))),
            "left",
        )
        .groupBy(F.col("dates.metric_date").alias("metric_date"), F.col("dates.advertiser_id").alias("advertiser_id"))
        .agg(F.countDistinct("campaigns.campaign_id").alias("active_campaigns"))
    )
    advertiser_daily = (
        campaign_daily.groupBy("metric_date", "advertiser_id")
        .agg(
            F.sum("impressions").alias("impressions"),
            F.sum("clicks").alias("clicks"),
            F.sum("attributed_orders").alias("attributed_orders"),
            F.sum("attributed_revenue").alias("attributed_revenue"),
        )
        .join(
            sales_activity.withColumn("metric_date", F.to_date("activity_ts"))
            .groupBy("metric_date", "advertiser_id")
            .agg(F.countDistinct("sales_activity_id").alias("sales_contacts")),
            ["metric_date", "advertiser_id"],
            "left",
        )
        .join(active_campaigns, ["metric_date", "advertiser_id"], "left")
        .na.fill(0, ["sales_contacts", "active_campaigns"])
        .withColumn("processed_ts", processed_ts)
    )
    _write_with_lineage(
        advertiser_daily,
        "iceberg.silver.silver_advertiser_daily_metrics",
        job_name="silver_aggregates",
        upstream_datasets=[
            "iceberg.silver.silver_campaign_daily_metrics",
            "iceberg.silver.silver_sales_activity",
            "iceberg.silver.silver_campaign_current",
        ],
        run_id=run_id,
    )

    minimum_row_counts = {
        "iceberg.silver.silver_customer_daily_metrics": 1,
        "iceberg.silver.silver_product_daily_metrics": 1,
        "iceberg.silver.silver_campaign_daily_metrics": 1,
        "iceberg.silver.silver_advertiser_daily_metrics": 1,
    }
    for dataset, min_expected_row_count in minimum_row_counts.items():
        count_value = spark.table(dataset).count()
        append_dq_result(
            "volume_baseline",
            "warning",
            dataset,
            count_value >= min_expected_row_count,
            {"row_count": count_value, "min_expected_row_count": min_expected_row_count},
            run_id=run_id,
        )
    if managed_spark:
        spark.stop()


def run_ml_features(
    start_date: str | None = None,
    end_date: str | None = None,
    spark: SparkSession | None = None,
) -> None:
    managed_spark = spark is None
    spark = _initialize_spark("build-ml-features", spark=spark)
    run_id = _new_run_id("build_ml_features")
    feature_defs = load_yaml(PROJECT_ROOT / "config" / "features" / "offline_feature_defs.yaml")["features"]
    online_defs = load_yaml(PROJECT_ROOT / "config" / "features" / "online_feature_defs.yaml")["features"]
    feature_def = feature_defs[0]
    online_def = online_defs[0]

    customer_daily = optional_date_filter(spark.table(feature_def["source_table"]), "metric_date", start_date, end_date)
    order_header = optional_date_filter(spark.table("iceberg.silver.silver_order_header"), "order_ts", start_date, end_date)

    window = Window.partitionBy("customer_id").orderBy("metric_date").rowsBetween(-29, 0)
    feature_df = (
        customer_daily.withColumn("purchases_30d", F.sum("purchases").over(window))
        .withColumn("avg_order_value_30d", F.avg("order_amount").over(window))
        .withColumn("customer_token", tokenized_column(F.col("customer_id").cast("string")))
        .withColumn("feature_version", F.lit(feature_def["name"]))
        .withColumn("generated_ts", F.current_timestamp())
    )
    orders_by_day = (
        order_header.withColumn("metric_date", F.to_date("order_ts"))
        .groupBy("customer_id", "metric_date")
        .agg(F.countDistinct("order_id").alias("daily_order_count"))
        .withColumnRenamed("metric_date", "order_date")
    )
    features_alias = feature_df.alias("features")
    orders_alias = orders_by_day.alias("orders")
    labeled_features = features_alias.join(
        orders_alias,
        (F.col("features.customer_id") == F.col("orders.customer_id"))
        & (F.col("orders.order_date") > F.col("features.metric_date"))
        & (F.col("orders.order_date") <= F.date_add(F.col("features.metric_date"), 7)),
        "left",
    )
    feature_df = (
        labeled_features.groupBy(
            F.col("features.metric_date").alias("metric_date"),
            F.col("features.customer_token").alias("customer_token"),
            F.col("features.purchases_30d").alias("purchases_30d"),
            F.col("features.avg_order_value_30d").alias("avg_order_value_30d"),
            F.col("features.feature_version").alias("feature_version"),
            F.col("features.generated_ts").alias("generated_ts"),
        )
        .agg(
            F.max(F.when(F.coalesce(F.col("orders.daily_order_count"), F.lit(0)) > 0, F.lit(1)).otherwise(F.lit(0))).alias(
                "customer_purchase_next_7d"
            )
        )
        .withColumnRenamed("metric_date", "as_of_date")
    )
    _write_with_lineage(
        feature_df,
        "iceberg.silver.customer_purchase_features_v1",
        job_name="build_ml_features",
        upstream_datasets=["iceberg.silver.silver_customer_daily_metrics", "iceberg.silver.silver_order_header"],
        run_id=run_id,
    )

    session_events = optional_date_filter(spark.table("iceberg.silver.silver_session_event_clean"), "event_date", start_date, end_date)
    feature_ts = F.current_timestamp()
    aggregation_exprs = []
    for aggregation in online_def["aggregations"]:
        window_seconds = int(aggregation.get("window_seconds", 0))
        event_type = aggregation.get("when", {}).get("event_type")
        condition = F.col("event_ts") >= (feature_ts - F.expr(f"INTERVAL {window_seconds} SECONDS"))
        if event_type:
            condition = condition & (F.col("event_type") == F.lit(event_type))
        if aggregation["function"] == "count_distinct":
            aggregation_exprs.append(F.countDistinct(F.when(condition, F.col(aggregation["field"])) ).alias(aggregation["as"]))

    parity = (
        session_events.groupBy("customer_id")
        .agg(*aggregation_exprs, F.max("event_ts").alias("last_event_ts"))
        .withColumn("feature_version", F.lit(online_def["name"]))
        .withColumn("generated_ts", feature_ts)
        .withColumn("as_of_ts", feature_ts)
        .withColumn("updated_at", feature_ts)
        .withColumn("ttl_seconds", F.lit(int(online_def["ttl_seconds"])))
    )
    _write_with_lineage(
        parity,
        "iceberg.silver.customer_realtime_features_v1_parity",
        job_name="build_ml_features",
        upstream_datasets=["iceberg.silver.silver_session_event_clean"],
        run_id=run_id,
    )
    append_dq_result(
        "reconciliation_totals",
        "warning",
        "iceberg.silver.customer_realtime_features_v1_parity",
        True,
        {"comparison_ready": True},
        run_id=run_id,
    )
    if managed_spark:
        spark.stop()
