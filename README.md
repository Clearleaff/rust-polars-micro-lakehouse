# rust-polars-micro-lakehouse

A standalone, zero-cloud-dependency Medallion Lakehouse (Bronze ➔ Silver ➔
Gold) built in Rust on Apache Arrow + Polars.

> This is an independent, from-scratch implementation of the Medallion
> pattern, built and **verified in this sandbox**: `cargo test` passes (9
> tests, including a full Bronze→Silver→Gold roundtrip), and both examples
> run correctly end-to-end. See "A toolchain note" below — this repo pins
> a few transitive dependencies to slightly older versions so it builds on
> an older `rustc`; on a current stable toolchain you can likely drop the
> pins.

## Who should use this?

- **Startups / small data teams** who want Databricks/Snowflake-style
  Medallion architecture on a cheap VM, without a JVM/Spark cluster.
- **Edge / IoT** capturing high-throughput event streams into durable,
  partitioned Parquet.
- **Python data scientists** who want a fast Rust sidecar producing clean
  Parquet for DuckDB/Pandas/PyTorch to pick up next.

## Why this over Spark / a managed lakehouse?

| | rust-polars-micro-lakehouse | Spark / JVM stack | Managed (Databricks/Snowflake) |
|---|---|---|---|
| Startup time | near-instant | JVM warmup, often seconds+ | N/A (always-on service) |
| Idle footprint | a few MB | multi-GB heap | billed regardless |
| Output format | standard Parquet, `date=/hour=` partitioned | standard Parquet | proprietary-ish / Delta |
| Where it runs | any $5-20/mo VM, or embedded in your own binary | needs a cluster | needs their platform |
| Query engine | Polars (lazy, pushdown, streaming) | Spark SQL | their engine |

The trade-off: this is a **library**, not a platform. No web UI, no
scheduler, no cluster coordination — you own that. It gives you the
storage/compute primitives; wire them into whatever orchestrates your
pipeline (cron, Airflow, your own service).

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

## Quickstart

```bash
cargo test                                   # 9 tests: bronze, silver, gold, + full roundtrip
cargo run --example stream_to_bronze         # simulated stream landing in Bronze, with a replay
cargo run --example build_gold_daily_summary # silver write + gold rollup, prints the summary
```

```rust
use rust_polars_micro_lakehouse::{BronzeWriter, SilverSchema, SilverWriter, GoldAggregator};
use polars::prelude::*;
use chrono::Utc;

let bronze = BronzeWriter::new("data/bronze");
let mut batch = df!["sku" => &["A1"], "qty_sold" => &[10i64]]?;
bronze.write_batch(&mut batch, Utc::now(), /*offset=*/ 0)?;

let schema = SilverSchema::new(vec![("sku", DataType::Utf8), ("qty_sold", DataType::Int64)]);
let silver = SilverWriter::new("data/silver", schema);
// ... read bronze back with LazyFrame::scan_parquet, then:
// let mut cleaned = silver.validate_and_clean(raw)?;
// silver.write(&mut cleaned, "date=2026-09-26", "part-0")?;

let gold = GoldAggregator::new("data/gold");
gold.sum_rollup("data/silver/**/*.parquet", &["sku"], &["qty_sold"], "sku_daily_summary")?;
```

## A toolchain note (please read before filing a "won't build" issue)

This template was built and tested against `rustc 1.75` (Ubuntu's
system package, current as of when this sandbox was set up). By now,
several of Polars' transitive dependencies have shipped newer versions
that require a much newer Rust (`edition2024`, `rustc 1.80+`). The
included `Cargo.toml`/`Cargo.lock` pin those specific transitive crates
(`home`, `indexmap`, `jobserver`, `rayon`, `rayon-core`, `ethnum`) to the
last version compatible with an older toolchain, so `cargo build --locked`
reproduces the verified build here.

**If you're on a current stable Rust** (installed via `rustup`, likely
1.85+), you almost certainly don't need any of this — feel free to run
`cargo update` to pick up latest versions of everything, and to add back
Polars' default `fmt` feature (disabled here for the same toolchain-age
reason — see the comment in `examples/build_gold_daily_summary.rs`) for
pretty-printed `DataFrame` output.

## Production configuration notes

- **Partition pruning**: `GoldAggregator::sum_rollup` takes a glob, so
  scope it to the partitions you actually need
  (`silver/date=2026-09-26/*.parquet`) rather than scanning everything —
  Polars' lazy scan will still push down what it can, but a narrower glob
  means less directory listing.
- **Compaction**: Bronze accumulates one file per `(partition, offset)`.
  For high-frequency ingestion you'll want a periodic compaction job that
  reads a partition's small files and rewrites them as fewer, larger ones
  — this template doesn't include one; it's a natural addition once you
  know your actual file-size distribution in production.
- **Schema evolution**: `SilverSchema` is intentionally simple (name +
  dtype pairs, hard failure on mismatch). If you need additive schema
  evolution (new optional columns over time), extend `SilverSchema` with
  an `optional_columns` list that's cast-if-present but not required.

## License

MIT
