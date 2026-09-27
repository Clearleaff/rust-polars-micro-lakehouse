//! End-to-end integration test: raw batch -> Bronze -> Silver
//! (validated/cleaned) -> Gold (rolled up), asserting the final numbers
//! are correct and that every stage's atomic-write guarantee held (no
//! `.tmp` files left behind anywhere in the tree).

use polars::prelude::*;
use rust_polars_micro_lakehouse::{BronzeWriter, GoldAggregator, SilverSchema, SilverWriter};
use chrono::Utc;

fn assert_no_leftover_tmp_files(dir: &std::path::Path) {
    for entry in walkdir_lite(dir) {
        assert!(
            !entry.to_string_lossy().ends_with(".tmp"),
            "found a leftover .tmp file, atomic rename did not clean up: {}",
            entry.display()
        );
    }
}

// Tiny recursive walk so the test has no extra dependency beyond std.
fn walkdir_lite(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walkdir_lite(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}

#[test]
fn bronze_to_silver_to_gold_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let bronze_dir = tmp.path().join("bronze");
    let silver_dir = tmp.path().join("silver");
    let gold_dir = tmp.path().join("gold");

    // --- Bronze: two raw batches land, one with a slightly wrong dtype
    // (qty_sold as i32 instead of i64) to prove Silver actually normalizes it. ---
    let bronze = BronzeWriter::new(&bronze_dir);
    let ts = Utc::now();

    let mut batch1 = df![
        "sku" => &["A1", "A2"],
        "qty_sold" => &[10i32, 4i32],
    ]
    .unwrap();
    bronze.write_batch(&mut batch1, ts, 0).unwrap();

    let mut batch2 = df![
        "sku" => &["A1", "A2", "A3"],
        "qty_sold" => &[7i32, 9i32, 100i32],
    ]
    .unwrap();
    bronze.write_batch(&mut batch2, ts, 1).unwrap();

    // --- Silver: read Bronze back, validate + cast to the canonical schema. ---
    let schema = SilverSchema::new(vec![
        ("sku", DataType::Utf8),
        ("qty_sold", DataType::Int64),
    ]);
    let silver = SilverWriter::new(&silver_dir, schema);

    let bronze_glob = format!("{}/**/*.parquet", bronze_dir.to_string_lossy());
    let raw = LazyFrame::scan_parquet(&bronze_glob, ScanArgsParquet::default())
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(raw.height(), 5, "expected all 5 raw rows across both bronze batches");

    let mut cleaned = silver.validate_and_clean(raw).unwrap();
    assert_eq!(
        cleaned.column("qty_sold").unwrap().dtype(),
        &DataType::Int64,
        "silver must have cast qty_sold from i32 to the canonical i64"
    );
    silver.write(&mut cleaned, "date=all", "part-0").unwrap();

    // --- Gold: roll up by sku. ---
    let gold = GoldAggregator::new(&gold_dir);
    let silver_glob = format!("{}/**/*.parquet", silver_dir.to_string_lossy());
    let summary = gold
        .sum_rollup(&silver_glob, &["sku"], &["qty_sold"], "sku_summary")
        .unwrap();

    let mut sorted = summary
        .lazy()
        .sort("sku", SortOptions::default())
        .collect()
        .unwrap();
    let skus: Vec<Option<&str>> = sorted.column("sku").unwrap().utf8().unwrap().into_iter().collect();
    let qtys: Vec<Option<i64>> = sorted.column("qty_sold").unwrap().i64().unwrap().into_iter().collect();

    assert_eq!(skus, vec![Some("A1"), Some("A2"), Some("A3")]);
    assert_eq!(qtys, vec![Some(17), Some(13), Some(100)]); // A1: 10+7, A2: 4+9, A3: 100

    let _ = &mut sorted;

    // --- Crash-safety: no stage should have left a .tmp file behind anywhere. ---
    assert_no_leftover_tmp_files(&bronze_dir);
    assert_no_leftover_tmp_files(&silver_dir);
    assert_no_leftover_tmp_files(&gold_dir);
}
