//! Writes a couple of Silver-shaped Parquet files, then rolls them up into
//! a Gold daily summary (sum of qty_sold, grouped by sku).
//!
//! Run with: `cargo run --release --example build_gold_daily_summary`

use polars::prelude::*;
use rust_polars_micro_lakehouse::{GoldAggregator, SilverSchema, SilverWriter};

fn main() -> rust_polars_micro_lakehouse::Result<()> {
    let base = std::env::temp_dir().join("micro_lakehouse_demo");
    let silver_dir = base.join("silver");
    let gold_dir = base.join("gold");

    let schema = SilverSchema::new(vec![
        ("sku", DataType::Utf8),
        ("qty_sold", DataType::Int64),
    ]);
    let silver = SilverWriter::new(&silver_dir, schema);

    // Two "batches" landing at different times of day for the same date partition.
    let morning = df!["sku" => &["A1", "A2"], "qty_sold" => &[10i64, 4]]?;
    let mut morning_clean = silver.validate_and_clean(morning)?;
    silver.write(&mut morning_clean, "date=2026-09-26", "part-morning")?;

    let afternoon = df!["sku" => &["A1", "A2"], "qty_sold" => &[7i64, 9]]?;
    let mut afternoon_clean = silver.validate_and_clean(afternoon)?;
    silver.write(&mut afternoon_clean, "date=2026-09-26", "part-afternoon")?;

    println!("wrote Silver batches under {}", silver_dir.display());

    let gold = GoldAggregator::new(&gold_dir);
    let glob = format!("{}/date=2026-09-26/*.parquet", silver_dir.display());
    let summary = gold.sum_rollup(&glob, &["sku"], &["qty_sold"], "sku_daily_summary")?;

    println!("\nGold daily summary (qty_sold by sku):");
    // Note: this template disables Polars' default "fmt" feature (it pulls
    // in a dependency that needs a newer Rust toolchain than some
    // environments have available), so we print columns manually here
    // instead of using DataFrame's pretty-printed Display impl. If your
    // toolchain is current, feel free to add the "fmt" feature back in
    // Cargo.toml and just `println!("{summary}")` instead.
    let skus = summary.column("sku")?.utf8()?;
    let qtys = summary.column("qty_sold")?.i64()?;
    for (sku, qty) in skus.into_iter().zip(qtys.into_iter()) {
        println!("  {:<6} qty_sold = {}", sku.unwrap_or("?"), qty.unwrap_or(0));
    }
    println!("\nwritten to {}/sku_daily_summary.parquet", gold_dir.display());

    Ok(())
}
