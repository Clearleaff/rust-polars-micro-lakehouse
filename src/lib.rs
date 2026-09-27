//! # rust-polars-micro-lakehouse
//!
//! A standalone, zero-cloud-dependency Medallion Lakehouse (Bronze ➔
//! Silver ➔ Gold) built on Apache Arrow + Polars.
//!
//! - [`bronze`] — durable, crash-safe ingestion: raw batches land as
//!   Parquet, partitioned by `date=/hour=`, written atomically.
//! - [`silver`] — schema validation, dtype casting, and null-handling on
//!   top of Bronze data.
//! - [`gold`] — lazy, pushdown-optimized batch aggregations over Silver
//!   data (e.g. daily rollups).
//!
//! See the `stream_to_bronze` and `build_gold_daily_summary` examples for
//! end-to-end usage, and `tests/lakehouse_roundtrip_test.rs` for a full
//! Bronze → Silver → Gold walkthrough.

pub mod bronze;
pub mod error;
pub mod gold;
pub mod silver;

pub use bronze::BronzeWriter;
pub use error::{LakehouseError, Result};
pub use gold::GoldAggregator;
pub use silver::{SilverSchema, SilverWriter};
