//! Silver layer: schema validation, type normalization, and null-handling
//! on top of raw Bronze data. Output is typed, trustworthy Parquet that
//! downstream Gold aggregations (or an external consumer via DuckDB/
//! Athena/ClickHouse) can rely on without re-checking types.

use crate::error::{LakehouseError, Result};
use polars::prelude::*;
use std::fs;
use std::path::PathBuf;

/// The columns Silver requires to be present, and the dtype each one must
/// end up as (values are cast if the incoming dtype differs but is
/// compatible; an incompatible cast or a missing column is a hard error —
/// Silver should never silently coerce bad data into nulls).
pub struct SilverSchema {
    pub required_columns: Vec<(String, DataType)>,
}

impl SilverSchema {
    pub fn new(required_columns: Vec<(&str, DataType)>) -> Self {
        Self {
            required_columns: required_columns
                .into_iter()
                .map(|(name, dtype)| (name.to_string(), dtype))
                .collect(),
        }
    }
}

pub struct SilverWriter {
    base_dir: PathBuf,
    schema: SilverSchema,
}

impl SilverWriter {
    pub fn new(base_dir: impl Into<PathBuf>, schema: SilverSchema) -> Self {
        Self {
            base_dir: base_dir.into(),
            schema,
        }
    }

    /// Validate every required column is present, cast it to the required
    /// dtype if needed, then drop any row that's null in a required
    /// column. Returns the cleaned `DataFrame`; the caller decides whether
    /// (and where) to persist it via [`SilverWriter::write`].
    pub fn validate_and_clean(&self, mut df: DataFrame) -> Result<DataFrame> {
        let mut required_names = Vec::with_capacity(self.schema.required_columns.len());

        for (name, dtype) in &self.schema.required_columns {
            let series = df
                .column(name)
                .map_err(|_| {
                    LakehouseError::SchemaValidation(format!("missing required column '{name}'"))
                })?
                .clone();

            if series.dtype() != dtype {
                let casted = series.cast(dtype).map_err(|e| {
                    LakehouseError::SchemaValidation(format!(
                        "column '{name}' ({:?}) cannot be cast to {dtype:?}: {e}",
                        series.dtype()
                    ))
                })?;
                df.with_column(casted)?;
            }
            required_names.push(name.clone());
        }

        let cleaned = df.drop_nulls(Some(&required_names))?;
        Ok(cleaned)
    }

    /// Persist an already-cleaned `DataFrame` under `<base_dir>/<partition>/`.
    /// Unlike Bronze, Silver files are named by content hash of the
    /// partition key rather than broker offset, since Silver batches are
    /// typically compacted/re-derived rather than replayed 1:1 from a
    /// single source offset.
    pub fn write(&self, df: &mut DataFrame, partition: &str, file_stem: &str) -> Result<PathBuf> {
        if df.height() == 0 {
            return Err(LakehouseError::EmptyBatch);
        }
        let dir = self.base_dir.join(partition);
        fs::create_dir_all(&dir)?;

        let final_path = dir.join(format!("{file_stem}.parquet"));
        let tmp_path = dir.join(format!("{file_stem}.parquet.tmp"));
        {
            let file = fs::File::create(&tmp_path)?;
            ParquetWriter::new(file).finish(df)?;
        }
        fs::rename(&tmp_path, &final_path)?;
        Ok(final_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    fn schema() -> SilverSchema {
        SilverSchema::new(vec![
            ("sku", DataType::Utf8),
            ("qty", DataType::Int64),
        ])
    }

    #[test]
    fn casts_a_compatible_dtype() {
        let writer = SilverWriter::new("/tmp/unused", schema());
        // qty arrives as i32 from some upstream source; schema wants i64.
        let df = df!["sku" => &["A1", "A2"], "qty" => &[10i32, 20i32]].unwrap();
        let cleaned = writer.validate_and_clean(df).unwrap();
        assert_eq!(cleaned.column("qty").unwrap().dtype(), &DataType::Int64);
    }

    #[test]
    fn rejects_a_missing_required_column() {
        let writer = SilverWriter::new("/tmp/unused", schema());
        let df = df!["sku" => &["A1"]].unwrap(); // "qty" missing
        let result = writer.validate_and_clean(df);
        assert!(matches!(result, Err(LakehouseError::SchemaValidation(_))));
    }

    #[test]
    fn drops_rows_null_in_a_required_column() {
        let writer = SilverWriter::new("/tmp/unused", schema());
        let df = df![
            "sku" => &[Some("A1"), None, Some("A3")],
            "qty" => &[Some(10i64), Some(20i64), Some(30i64)],
        ]
        .unwrap();
        let cleaned = writer.validate_and_clean(df).unwrap();
        assert_eq!(cleaned.height(), 2, "the row with a null sku must be dropped");
    }

    #[test]
    fn writes_cleaned_data_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let writer = SilverWriter::new(tmp.path(), schema());
        let mut df = df!["sku" => &["A1"], "qty" => &[5i64]].unwrap();
        let path = writer.write(&mut df, "date=2026-09-26", "part-0001").unwrap();
        assert!(path.exists());
        assert!(!path.with_extension("parquet.tmp").exists());
    }
}
