//! Gold layer: batch aggregations over Silver data using Polars'
//! lazy/streaming query engine, so a rollup can run over far more data
//! than fits in memory at once.

use crate::error::Result;
use polars::prelude::*;
use std::fs;
use std::path::PathBuf;

pub struct GoldAggregator {
    output_dir: PathBuf,
}

impl GoldAggregator {
    pub fn new(output_dir: impl Into<PathBuf>) -> Self {
        Self {
            output_dir: output_dir.into(),
        }
    }

    /// Scan every Parquet file matching `silver_glob` (e.g.
    /// `"silver/date=*/*.parquet"`) lazily, group by `group_by_cols`, sum
    /// each of `sum_cols`, collect the result, and persist it to
    /// `<output_dir>/<output_name>.parquet`. Returns the collected
    /// `DataFrame` for immediate inspection/use as well.
    ///
    /// Using `LazyFrame::scan_parquet` (rather than reading every file
    /// eagerly and concatenating) means Polars' query optimizer can push
    /// down the group-by and avoid materializing the full unaggregated
    /// dataset in memory.
    pub fn sum_rollup(
        &self,
        silver_glob: &str,
        group_by_cols: &[&str],
        sum_cols: &[&str],
        output_name: &str,
    ) -> Result<DataFrame> {
        let lf = LazyFrame::scan_parquet(silver_glob, ScanArgsParquet::default())?;

        let group_exprs: Vec<Expr> = group_by_cols.iter().map(|c| col(*c)).collect();
        let agg_exprs: Vec<Expr> = sum_cols
            .iter()
            .map(|c| col(*c).sum().alias(c))
            .collect();

        let mut result = lf.group_by(group_exprs).agg(agg_exprs).collect()?;

        fs::create_dir_all(&self.output_dir)?;
        let out_path = self.output_dir.join(format!("{output_name}.parquet"));
        let file = fs::File::create(&out_path)?;
        ParquetWriter::new(file).finish(&mut result)?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    /// Writes a couple of Silver-shaped Parquet files, then rolls them up
    /// by `sku` and checks the summed quantity.
    #[test]
    fn rolls_up_multiple_files_by_group() {
        let silver_dir = tempfile::tempdir().unwrap();
        let gold_dir = tempfile::tempdir().unwrap();

        let mut df1 = df!["sku" => &["A1", "A2"], "qty" => &[10i64, 5]].unwrap();
        let file1 = silver_dir.path().join("part-1.parquet");
        ParquetWriter::new(fs::File::create(&file1).unwrap())
            .finish(&mut df1)
            .unwrap();

        let mut df2 = df!["sku" => &["A1", "A2"], "qty" => &[3i64, 7]].unwrap();
        let file2 = silver_dir.path().join("part-2.parquet");
        ParquetWriter::new(fs::File::create(&file2).unwrap())
            .finish(&mut df2)
            .unwrap();

        let aggregator = GoldAggregator::new(gold_dir.path());
        let glob = format!("{}/*.parquet", silver_dir.path().to_string_lossy());
        let result = aggregator
            .sum_rollup(&glob, &["sku"], &["qty"], "sku_daily_summary")
            .unwrap();

        let mut sorted = result
            .lazy()
            .sort("sku", SortOptions::default())
            .collect()
            .unwrap();

        let qty = sorted.column("qty").unwrap().i64().unwrap();
        assert_eq!(qty.get(0), Some(13)); // A1: 10 + 3
        assert_eq!(qty.get(1), Some(12)); // A2: 5 + 7

        let out_path = gold_dir.path().join("sku_daily_summary.parquet");
        assert!(out_path.exists());
        let _ = &mut sorted; // silence unused-mut if optimizer elides the sort in place
    }
}
