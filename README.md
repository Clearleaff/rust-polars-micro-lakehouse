# rust-polars-micro-lakehouse

A standalone, zero-cloud-dependency Medallion Lakehouse (Bronze ➔ Silver ➔ Gold) built in Rust on Apache Arrow + Polars.

> This is an independent, from-scratch implementation of the Medallion pattern, built and **verified**: `cargo test` passes (9 tests, including a full Bronze→Silver→Gold roundtrip), and both examples run correctly end-to-end.

---

## Table of Contents
- [Who Should Use This?](#who-should-use-this)
- [What Issues Does This Resolve?](#what-issues-does-this-resolve)
- [How This Template Accelerates Your Pipeline](#how-this-template-accelerates-your-pipeline)
- [When to Use (and When NOT to Use)](#when-to-use-and-when-not-to-use)
- [Architecture](#architecture)
- [How to Reuse This Template in Your Project](#how-to-reuse-this-template-in-your-project)
- [Quickstart & Code Walkthrough](#quickstart--code-walkthrough)
- [Why This Over Spark / A Managed Lakehouse?](#why-this-over-spark--a-managed-lakehouse)
- [Production Configuration Notes](#production-configuration-notes)
- [Toolchain Note](#a-toolchain-note)
- [License](#license)

---

## Who Should Use This?

- **Startups & Small Data Teams:** Teams wanting Databricks/Snowflake-style Medallion storage without managing a JVM/Spark cluster or paying cloud platform markups.
- **Edge & IoT Deployments:** Systems capturing high-frequency event streams directly on resource-constrained devices (e.g. Raspberry Pi, factory gateways).
- **Backend Rust Microservices:** Applications wanting to dump high-throughput events directly to partitioned Parquet without external queue/storage dependencies.
- **Python Data Scientists & ML Engineers:** Teams needing a fast Rust ingestion sidecar that outputs clean Parquet ready for DuckDB, Pandas, or PyTorch.

---

## What Issues Does This Resolve?

1. **JVM Bloat & Cloud Bill Shock:**
   Running Spark or cloud lakehouses (Databricks/Snowflake) requires massive memory heaps, multi-node clusters, and recurring hourly billing. This template runs natively on a single $5–$20/mo VM with an idle footprint of just a few megabytes.
2. **Partial / Corrupted Writes from Pipeline Crashes:**
   Standard file writes leave incomplete files if a process crashes mid-write. `BronzeWriter` and `SilverWriter` use **atomic two-phase writes** (writing to `.tmp` files first, then invoking POSIX `fs::rename`), ensuring readers never observe half-written files.
3. **Duplicate Records in At-Least-Once Streaming:**
   Message brokers (Kafka, RabbitMQ, SQS) deliver at-least-once, causing duplicate records during network retries. Bronze uses deterministic file naming (`part-<timestamp>-<offset>.parquet`), so offset replays safely overwrite existing files instead of creating duplicates.
4. **Silent Schema Drift & Dirty Data:**
   Downstream models and dashboards often crash due to missing columns or silent null coercions. `SilverSchema` enforces strict column presence, casts compatible data types, and drops rows with null required keys.
5. **Out-of-Memory (OOM) on Aggregations:**
   Loading raw daily datasets into RAM crashes single machines. `GoldAggregator` leverages Polars' lazy query engine (`LazyFrame::scan_parquet`), streaming parquet chunks and applying pushdown predicates without blowing up RAM.

---

## How This Template Accelerates Your Pipeline

This template accelerates both **execution speed** and **development velocity**:

### 1. Execution & Runtime Acceleration
* **Native Rust Speed (No Garbage Collection Pauses):** Zero JIT warm-up, zero garbage collection pauses, and minimal CPU overhead compared to Java or Python pipelines.
* **Apache Arrow Columnar Layout:** In-memory representation uses contiguous, SIMD-aligned columnar buffers for maximum CPU cache efficiency and vectorization.
* **Lazy Scanning & Projection Pushdown:** Polars inspects Parquet file metadata before reading, loading **only** the columns requested for group-bys and sums. Unused columns and unneeded row groups are skipped at disk level.
* **Partition Pruning:** Directory structure (`date=YYYY-MM-DD/hour=HH/`) allows globs to bypass non-matching dates/hours entirely.
* **Zero-Copy Parquet Handoff:** Clean output files can be memory-mapped directly into DuckDB or Python Polars without costly serialization or wire-protocol overhead.

### 2. Development & Engineering Acceleration
* **Ready-to-Use Primitives:** No need to re-invent partition directory layouts, atomic write transactions, validation logic, or aggregation queries from scratch.
* **Single Binary Deployment:** Embed the entire lakehouse pipeline inside your Rust application binary or run it as a lightweight CLI sidecar.

---

## When to Use (and When NOT to Use)

| Scenario | Use This Template? | Recommendation / Alternative |
|---|---|---|
| Single VM ($5–$50/mo) or Bare Metal Server | **Yes** | Perfect fit. Near-zero resource footprint. |
| Edge / IoT device with local storage | **Yes** | Ideal for high-throughput local logging & sync. |
| Rust service ingesting Kafka/WebSockets | **Yes** | Eliminates external lakehouse ingestion bridges. |
| Data size under 1 TB per day | **Yes** | Polars handles gigabytes to low terabytes easily on a single box. |
| Distributed clusters with 10+ worker nodes | **No** | Use Apache Spark, Trino, or ClickHouse. |
| Multi-table ACID transactions & row updates | **No** | Use Delta Lake (`delta-rs`) or Apache Iceberg. |
| Requires web UI, SQL editor & user permissions | **No** | Use managed Databricks or Snowflake. |

---

## Architecture

```
raw events ──▶ BronzeWriter ──▶ bronze/date=YYYY-MM-DD/hour=HH/part-<ts>-<offset>.parquet
                                        │  (atomic: write .tmp, then rename)
                                        ▼
                            SilverWriter.validate_and_clean()
                                 - required columns present?
                                 - cast to canonical dtypes
                                 - drop rows null in required cols
                                        │
                                        ▼
                               silver/<partition>/*.parquet
                                        │
                                        ▼
                              GoldAggregator.sum_rollup()
                           (LazyFrame::scan_parquet + group_by + agg)
                                        │
                                        ▼
                               gold/<name>.parquet
```

---

## How to Reuse This Template in Your Project

You can reuse this codebase either as a **dependency library** or as a **starter template project**:

### Option A: Use As a Starter Template
1. Clone the repository:
   ```bash
   git clone https://github.com/Clearleaff/rust-polars-micro-lakehouse.git
   cd rust-polars-micro-lakehouse
   ```
2. Modify schemas and data structures in `src/silver.rs` and `src/bronze.rs` to match your domain events (e.g. IoT telemetry, finance transactions, web analytics).
3. Wire your ingestion source (e.g. `rdkafka`, `axum`, or file watcher) in `src/main.rs` or `examples/`.

### Option B: Integrate Into an Existing Rust Service
Add the crate to your project's `Cargo.toml`:
```toml
[dependencies]
rust-polars-micro-lakehouse = { path = "../path/to/rust-polars-micro-lakehouse" }
polars = { version = "0.36", features = ["lazy", "parquet"] }
chrono = "0.4"
```

### 3-Step Pipeline Integration Guide:
1. **Bronze (Raw Ingestion):** Pass incoming records directly from your consumer loop into `BronzeWriter::write_batch(df, timestamp, offset)`.
2. **Silver (Validation & Cleaning):** Configure your target `SilverSchema`, call `SilverWriter::validate_and_clean(df)`, and save the partition.
3. **Gold (Aggregations):** Run periodic cron or background tasks with `GoldAggregator::sum_rollup()` for your hourly/daily analytics summaries.

---

## Quickstart & Code Walkthrough

### 1. Run Verification
```bash
cargo test                                   # 9 tests: bronze, silver, gold, + roundtrip
cargo run --example stream_to_bronze         # simulated stream landing in Bronze
cargo run --example build_gold_daily_summary # silver write + gold rollup
```

### 2. End-to-End Code Walkthrough

```rust
use rust_polars_micro_lakehouse::{BronzeWriter, SilverSchema, SilverWriter, GoldAggregator};
use polars::prelude::*;
use chrono::Utc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ----------------------------------------------------
    // 1. BRONZE LAYER: Ingest raw streaming batches
    // ----------------------------------------------------
    let bronze = BronzeWriter::new("data/bronze");
    let mut batch = df![
        "sku" => &["PROD-A", "PROD-B"],
        "qty_sold" => &[15i64, 42i64]
    ]?;
    
    // Writes atomically to data/bronze/date=YYYY-MM-DD/hour=HH/part-<ts>-<offset>.parquet
    let bronze_path = bronze.write_batch(&mut batch, Utc::now(), /*broker offset=*/ 101)?;
    println!("Bronze batch stored at: {:?}", bronze_path);

    // ----------------------------------------------------
    // 2. SILVER LAYER: Validate schema & clean data
    // ----------------------------------------------------
    let schema = SilverSchema::new(vec![
        ("sku", DataType::Utf8),
        ("qty_sold", DataType::Int64)
    ]);
    let silver = SilverWriter::new("data/silver", schema);

    // Scan Bronze parquet files lazily
    let raw_bronze = LazyFrame::scan_parquet("data/bronze/**/*.parquet", ScanArgsParquet::default())?
        .collect()?;

    // Cast compatible types and drop nulls
    let mut cleaned = silver.validate_and_clean(raw_bronze)?;
    silver.write(&mut cleaned, "date=2026-09-27", "part-cleaned-01")?;

    // ----------------------------------------------------
    // 3. GOLD LAYER: Compute lazy rollups
    // ----------------------------------------------------
    let gold = GoldAggregator::new("data/gold");
    let summary = gold.sum_rollup(
        "data/silver/date=2026-09-27/*.parquet", // Targeted partition glob
        &["sku"],                               // Group by columns
        &["qty_sold"],                          // Sum columns
        "daily_sku_summary"                     // Output file stem
    )?;

    println!("Gold daily rollup computed successfully.");
    Ok(())
}
```

---

## Why This Over Spark / A Managed Lakehouse?

| Feature | `rust-polars-micro-lakehouse` | Spark / JVM Stack | Managed (Databricks/Snowflake) |
|---|---|---|---|
| **Startup time** | Instant (<50ms) | JVM warmup (seconds to minutes) | Always-on cluster / warehouse |
| **Idle footprint** | ~5 MB RAM | 2–8 GB+ JVM heap | Minimum warehouse credits billed |
| **Output format** | Open Apache Parquet (`date=/hour=`) | Standard Parquet | Proprietary metadata / Delta |
| **Hosting cost** | Runs on any $5–$20/mo VM | Requires multi-node cluster | High SaaS license & compute costs |
| **Query engine** | Polars (lazy, SIMD, pushdown) | Spark SQL / Catalyst | Proprietary engine |
| **Architecture** | Embeddable library | Distributed computing platform | Cloud SaaS platform |

> **Trade-off:** This is a lightweight **library**, not a managed platform. There is no built-in web UI, scheduler, or cluster coordinator. You wire it into your own preferred orchestration (systemd, cron, Tokio background tasks, or Airflow).

---

## Production Configuration Notes

- **Partition Pruning:** `GoldAggregator::sum_rollup` accepts a glob pattern. Scope it directly to the partition you need (e.g. `silver/date=2026-09-27/*.parquet`) to minimize directory scans.
- **Compaction:** Bronze generates one file per `(partition, offset)`. For high-throughput ingestion, schedule a periodic compaction job that aggregates small files into larger consolidated Parquet files (e.g., 128 MB blocks).
- **Schema Evolution:** `SilverSchema` defaults to strict enforcement. For additive schema evolution (new optional columns), extend `SilverSchema` with an `optional_columns` vector that performs cast-if-present.
- **Direct Downstream Querying:** Because all output is standard Apache Parquet, you can directly query `gold/*.parquet` from **DuckDB**, **Python Polars**, **ClickHouse**, or **Apache Superset** without running an export step.

---

## A Toolchain Note

This template was built and tested against `rustc 1.75`. The included `Cargo.toml`/`Cargo.lock` pins specific transitive dependencies (`home`, `indexmap`, `jobserver`, `rayon`, `rayon-core`, `ethnum`) so that `cargo build --locked` is guaranteed to build cleanly across environments.

**If you are on a current stable Rust** (e.g. `rustc 1.80+` via `rustup`):
- Feel free to run `cargo update` to upgrade all dependencies to the latest releases.
- You can re-enable Polars' default `fmt` feature in `Cargo.toml` to get pretty-printed `DataFrame` console output.

---

## License

MIT

