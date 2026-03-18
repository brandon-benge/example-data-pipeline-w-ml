# Generator Guide

## Purpose

The generator creates two kinds of source data:

- relational source tables in Postgres
- Avro `session_event` messages in Kafka

The entrypoint is [`generator/app.py`](../generator/app.py). It always builds an in-memory source bundle first, then optionally writes that bundle to Postgres, Kafka, or both depending on `--mode`.

## What the script actually does

The flow is:

1. Load settings from `params.yaml` and CLI overrides.
2. Build synthetic dimension/fact-style source rows in memory.
3. Build synthetic `session_event` Kafka events in memory.
4. If `--mode postgres` or `--mode both`, upsert the source tables into Postgres.
5. If `--mode kafka` or `--mode both`, publish the generated events to Kafka.

The main controls are:

- `--customers`
- `--events-per-minute`
- `--orders-per-hour`
- `--seed`
- `--mode postgres|kafka|both`

The config file now uses table-aligned row keys so the counts are more obvious at a glance:

- `customer_rows`
- `sales_rep_rows`
- `advertiser_rows`
- `product_rows`
- `campaign_rows`
- `customer_session_rows`
- `order_header_rows`
- `sales_activity_rows`
- `session_event_rows`

For analytics quality, the generator now assumes `session_event_rows` should be materially higher than `order_header_rows`. The default `params.yaml` keeps session-event volume above order volume so funnel and attribution charts have enough non-purchase behavior to look realistic.

The older keys are still accepted as compatibility aliases:

- `customers` -> `customer_rows`
- `orders_per_hour` -> `order_header_rows`
- `events_per_minute` -> `session_event_rows`

## Entity counts and derived counts

`customer_rows`, `session_event_rows`, and `order_header_rows` come from [`params.yaml`](../params.yaml) or CLI overrides.

Other counts are derived in [`generator/config.py`](../generator/config.py):

- `sales_reps` depends on customer count
- `advertisers` depends on customer count
- `products` depends on customer count
- `campaigns` depends on advertiser count
- `sessions` is `max(customers // 2, events_per_minute * 3)`
- `sales_activities` depends on advertisers and orders

The generator now enforces a minimum of `50,000` rows for every Postgres source-table base count:

- `customer`
- `sales_rep`
- `advertiser`
- `product`
- `campaign`
- `customer_session`
- `order_header`
- `sales_activity`

And the derived bridge/fact child tables naturally stay above that floor too:

- `campaign_product` is generated from all campaigns and will exceed `50,000`
- `order_item` is generated from all orders and will exceed `50,000`

That means increasing event volume can also change the number of generated sessions, even if customer count stays the same, but no generated Postgres source table will fall below `50,000` rows.

The generator also now produces more curated relationships across those entities:

- customer sessions are spread across roughly four weeks instead of only a few days
- session events are generated as coherent session-level funnels instead of independent random rows
- orders are generated from higher-intent session plans instead of drifting independently
- order timestamps are kept on the same session day as the funnel activity that drove them
- order-linked session plans emit enough `product_view`, `ad_click`, `add_to_cart`, and `checkout_start` events to keep customer-day funnel metrics aligned in Silver and Gold

One consequence of that alignment rule:

- `session_event_rows` is now treated as a minimum target, not a hard cap
- if order-linked sessions require more funnel events than the configured target, the generator will emit the larger number so conversion marts do not show purchases without supporting funnel activity

## What happens if you rerun with the same 10,000 customers

For Postgres, rerunning with the same customer count does not create `customer_id` 10001+ automatically.

One important caveat now:

- if you pass a lower number such as `--customers 10000` or `--orders-per-hour 1000`, the generator will still raise that to the `50,000` minimum for source-table generation

The relational source tables use fixed primary-key ranges like:

- customers: `1..N`
- sessions: `1..session_count`
- orders: `1..order_count`

And the Postgres writer uses `INSERT ... ON CONFLICT DO UPDATE` in [`generator/postgres_writer.py`](../generator/postgres_writer.py).

So if you rerun with `customers=10000`:

- you rewrite customer IDs `1..10000`
- you rewrite session IDs from `1..session_count`
- you rewrite order IDs from `1..order_count`
- you do not append a second independent population of 10,000 customers

This is effectively a reseed/refresh of the same source tables, not an append-only load.

## Is the rerun identical?

Not fully.

Even with the same `--seed`, the rows are not guaranteed to be byte-for-byte identical across runs because `generated_at` is set to the current time in [`generator/config.py`](../generator/config.py). Many timestamps are derived from that value.

So with the same seed and counts:

- IDs stay stable
- overall shape stays similar
- relative randomness stays stable
- timestamps shift with the current run time

Kafka events are even less repeatable because [`generator/scenarios/sessions.py`](../generator/scenarios/sessions.py) uses `uuid4()` for `event_uuid`, which is always fresh.

## How Kafka events behave on rerun

Kafka publishing is append-only from the generator’s point of view.

If you rerun in `kafka` or `both` mode:

- the generator publishes a new batch of events
- Kafka keeps the old events unless retention removes them
- the generator does not check whether it already published a similar batch

So rerunning the generator is a valid way to create more event traffic, even if the Postgres tables are not changing.

## How to create more events without adding customers

This is the main operator case you asked about.

Use the same customer count and publish Kafka-only:

```bash
python3 generator/app.py --config params.yaml --mode kafka --customers 10000 --events-per-minute 500
```

That will:

- build a synthetic bundle in memory using 10,000 customers
- not write any relational rows to Postgres
- publish a larger Kafka event batch

This is the safest way to create more event volume without rewriting your source tables.

## Important caveat about `--mode kafka`

`--mode kafka` does not read existing customers or sessions from Postgres.

It still generates customers and sessions in memory first, then uses those generated sessions to make events. Because the generator uses stable ID ranges, the events will still reference customer IDs like `1..10000`, which matches the seeded Postgres population if you already loaded it with the same count.

So `--mode kafka` works well when:

- you already seeded Postgres once
- you want more event traffic referencing that same logical population

## Recommended operating patterns

If you want to initialize the environment from scratch:

```bash
python3 generator/app.py --config params.yaml --mode both
```

If you want to refresh the whole synthetic source dataset:

```bash
python3 generator/app.py --config params.yaml --mode both --customers 10000
```

If you want more Kafka traffic without touching Postgres:

```bash
python3 generator/app.py --config params.yaml --mode kafka --customers 10000 --events-per-minute 1000
```

If you want more orders in Postgres without changing customer cardinality:

```bash
python3 generator/app.py --config params.yaml --mode postgres --customers 10000 --orders-per-hour 500
```

That last command still rewrites the same primary-key ranges for orders and related entities. It is not append-only history.

## Minimum row guarantee

For local debugging, the generator now guarantees at least `50,000` rows in every generated Postgres source table, even if the YAML config or CLI overrides request smaller counts.

This was added specifically because tiny CDC topics were repeatedly stalling in the Iceberg Kafka Connect sink path in local runs.

Observed failure pattern before the change:

- sink connectors stayed `RUNNING`
- Bronze tables remained at `0`
- source consumer offsets were never committed
- DLQ topics stayed empty
- sink logs repeatedly showed `committed to 0 table(s)` and related control/commit churn

After raising every generated Postgres source table to at least `50,000` rows, the previously failing CDC sink connectors eventually committed successfully.

So in this repo, the `50,000` minimum is a documented local stability workaround for low-volume CDC topics, not an arbitrary default.

## Practical answer

If your goal is:

- keep the same 10,000 logical customers
- avoid adding new customer IDs
- create a lot more events

Then use `--mode kafka` and keep `--customers 10000`.

If your goal is:

- preserve the existing relational source rows exactly as they are

Then do not rerun `--mode postgres` or `--mode both`, because those modes upsert and refresh the same primary-key ranges.
