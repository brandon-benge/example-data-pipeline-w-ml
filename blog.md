
# From Data Pipelines to Real‑Time Decisions
## How Modern Platforms Turn Streaming Data into Operational ML

For many years, most data platforms were designed to answer a single question:

**What happened yesterday?**

Analytics systems were built around batch pipelines, dashboards, and reports that helped teams understand past behavior. That model worked well when the goal was insight.

But modern organizations are increasingly asking a different question:

**What should we do right now?**

Answering that question requires more than analytics. It requires systems capable of producing **real‑time decisions** based on continuously arriving data. That is where modern data platforms begin to evolve from *analytics systems* into *decision systems*.

This article walks through how that transition happens and how streaming systems, governed data pipelines, and ML inference services come together to support operational decisions.

---

# The Real Goal of an ML Platform

A common misconception is that ML platforms exist primarily to train models.

In practice, the real goal is much simpler and much harder:

**Support operational decisions at scale.**

To do that reliably, a platform must support three capabilities:

1. Reliable ingestion of operational data
2. Governed pipelines that produce trusted feature data
3. Inference services that deliver predictions at the moment decisions are made

Training models is necessary, but it is only one step in a much larger system. The real value appears when predictions can be delivered inside live business workflows.

---

# The Operational Decisions

To make this concrete, consider three example decisions a platform might support.

## Customer Purchase Propensity

**Question:** Is this customer likely to buy soon?

Possible actions might include:

- triggering a promotion
- prioritizing recommendations
- initiating cart‑recovery flows

These decisions often need to happen while the customer is still interacting with the product.

---

## Campaign Success Propensity

**Question:** Is this marketing campaign trending toward strong performance?

Possible actions might include:

- allocating additional marketing budget
- prioritizing campaign optimization
- intervening on underperforming campaigns

Here the platform helps marketing teams understand performance signals early enough to act.

---

## Advertiser Budget Expansion Propensity

**Question:** Is this advertiser likely to increase spend soon?

Possible actions might include:

- prioritizing sales outreach
- identifying upsell opportunities
- focusing account management attention

In this case the ML system converts operational signals into a prioritization mechanism for sales teams.

---

# Architecture: Turning Data Into Decisions

Supporting these decisions requires an end‑to‑end system rather than a single model.

A simplified flow looks like this:

Streaming ingestion  
→ Medallion data architecture  
→ Governed feature tables  
→ Model training  
→ Inference services  
→ Operational decisions

The key insight is that **real‑time ML depends on the entire platform**, not just the model.

---

# Streaming Ingestion

Operational systems produce the data that ultimately drives ML decisions.

A typical streaming ingestion layer might include:

- **Postgres** as the OLTP system
- **Debezium** for change data capture (CDC)
- **Kafka** as the event backbone
- **Schema Registry** to manage event schemas

This architecture ensures that operational changes become **streamable and replayable events**.

Many organizations implement the same pattern using managed services such as:

- Confluent Cloud
- Amazon MSK
- AWS Kinesis

The tooling varies, but the underlying pattern is consistent: operational data becomes an event stream.

---

# Where Streaming Changes the System

Up to this point the architecture still resembles many traditional data platforms. Streaming is where the system begins to change.

Batch pipelines are designed to answer questions about the past. If something breaks, the pipeline can usually be rerun and corrected later.

Streaming systems operate under different constraints. Events arrive continuously, ordering is not guaranteed, and stateful computations must remain correct while the system keeps running.

These characteristics become especially important once machine learning enters the platform.

Many useful ML features are based on short windows of recent behavior — for example:

- recent views
- recent clicks
- recent engagement signals

These signals lose much of their value if they are computed hours or days later. Streaming infrastructure allows them to be maintained continuously so they can be used during inference.

Seen this way, streaming systems keep the platform connected to **what is happening now**, while batch pipelines provide stable, reproducible datasets for training.

Together they enable the platform to support real operational decisions.

---

# Governed Data Pipelines

Raw data alone is rarely sufficient for ML systems. Features must be reliable, reproducible, and governed.

A common structure for this is the **medallion architecture**:

Bronze → Silver → Gold

**Bronze** stores raw event and CDC data.  
**Silver** contains cleaned and normalized datasets.  
**Gold** contains curated analytics tables and ML feature tables.

In many modern platforms:

- **Apache Spark** transforms Bronze data into governed Silver datasets
- **dbt** builds curated Gold tables and ML feature tables

This separation ensures that model training relies on stable and well‑understood datasets.

---

# Training and Model Governance

Once feature tables exist, model training becomes relatively straightforward.

Training pipelines often read features through query engines such as **Trino** and then use common Python tooling such as:

- pandas
- numpy
- scikit‑learn

Model artifacts are typically stored in object storage while model metadata is written to a registry table, for example:

iceberg.silver.ml_model_registry

This registry allows models to be versioned, reproduced, and audited.

---

# The Most Important Layer: Inference

The step that ultimately creates value is **model inference**.

An inference service (often implemented with **FastAPI**) exposes endpoints such as:

POST /score/customer_purchase  
POST /score/campaign_success  
POST /score/advertiser_budget_expansion

These endpoints load the latest approved model and produce predictions when applications request them.

For real‑time decisions such as purchase propensity, short‑window behavioral signals are often maintained using **stream processing and Redis**. Redis acts as a low‑latency feature store so the inference service can retrieve recent activity quickly.

---

# Why Platform Thinking Matters

At a leadership level, the challenge is not choosing individual tools.

The challenge is designing a platform where:

- operational systems produce reliable event streams
- governed pipelines produce trusted features
- models are versioned and reproducible
- inference services integrate directly with applications

When these pieces work together, the platform becomes capable of supporting **real operational intelligence**.

---

# Final Thought

The future of data platforms is not only about analytics.

It is increasingly about **decision infrastructure**.

Organizations that succeed will be those that connect:

Streaming data  
→ governed pipelines  
→ feature platforms  
→ inference services  
→ operational decisions

When done well, the platform does more than explain what happened.

It helps the business decide **what to do next.**
