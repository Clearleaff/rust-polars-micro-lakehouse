//! Simulates a mock event stream (as you'd get from Kafka/AMQP) landing
//! directly into Bronze, partitioned by hour, with atomic writes.
//!
//! Run with: `cargo run --release --example stream_to_bronze`

use chrono::{TimeZone, Utc};
use polars::prelude::*;
use rust_polars_micro_lakehouse::BronzeWriter;

fn main() -> rust_polars_micro_lakehouse::Result<()> {
    let out_dir = std::env::temp_dir().join("micro_lakehouse_demo/bronze");
    println!("writing Bronze partitions under {}", out_dir.display());

    let writer = BronzeWriter::new(&out_dir);

    // Pretend these are consecutive broker offsets, each with its own batch.
    for offset in 0..5u64 {
        let ts = Utc.with_ymd_and_hms(2026, 9, 26, 14, 0, 0).unwrap()
            + chrono::Duration::minutes(offset as i64 * 12);

        let mut batch = df![
            "sku" => &[format!("SKU-{offset}"), format!("SKU-{}", offset + 100)],
            "location" => &["WH-1", "WH-2"],
            "qty_sold" => &[(offset as i64 + 1) * 3, (offset as i64 + 1) * 5],
        ]?;

        let path = writer.write_batch(&mut batch, ts, offset)?;
        println!("  offset {offset} -> {}", path.display());
    }

    println!("\nreplaying offset 2 with corrected data (must overwrite, not duplicate):");
    let ts = Utc.with_ymd_and_hms(2026, 9, 26, 14, 0, 0).unwrap() + chrono::Duration::minutes(24);
    let mut corrected = df![
        "sku" => &["SKU-2-CORRECTED"],
        "location" => &["WH-1"],
        "qty_sold" => &[999i64],
    ]?;
    let path = writer.write_batch(&mut corrected, ts, 2)?;
    println!("  offset 2 (replay) -> {}", path.display());

    Ok(())
}
