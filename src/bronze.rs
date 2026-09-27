//! Bronze layer: durable, crash-safe ingestion of raw event batches.
//!
//! The only job of this layer is "never lose or corrupt data on the way
//! to disk". It does no validation and no normalization — that's Silver's
//! job. Every batch lands as Parquet, partitioned by
//! `date=YYYY-MM-DD/hour=HH/`, written via a temp-file-then-rename so a
//! reader never sees a half-written file.

use crate::error::{LakehouseError, Result};
use chrono::{DateTime, Timelike, Utc};
use polars::prelude::*;
use std::fs;
use std::path::PathBuf;

pub struct BronzeWriter {
    base_dir: PathBuf,
}

impl BronzeWriter {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// The partition directory a given event timestamp belongs to.
    pub fn partition_dir(&self, ts: DateTime<Utc>) -> PathBuf {
        self.base_dir
            .join(format!("date={}", ts.format("%Y-%m-%d")))
            .join(format!("hour={:02}", ts.hour()))
    }

    /// Atomically write `df` as one Parquet file under the partition for
    /// `ts`. The file name is deterministic
    /// (`part-<YYYY-MM-DDTHH>-<offset>.parquet`): replaying the same
    /// `(ts, offset)` pair overwrites the same file instead of creating a
    /// duplicate, which is what makes at-least-once delivery from a
    /// message broker safe to write directly to Bronze.
    ///
    /// Crash safety: we write to `<name>.parquet.tmp` first, then
    /// `fs::rename` it into place. A rename within the same filesystem is
    /// atomic on Linux, so a reader scanning the partition directory never
    /// observes a partially-written file.
    pub fn write_batch(&self, df: &mut DataFrame, ts: DateTime<Utc>, offset: u64) -> Result<PathBuf> {
        if df.height() == 0 {
            return Err(LakehouseError::EmptyBatch);
        }

        let dir = self.partition_dir(ts);
        fs::create_dir_all(&dir)?;

        let partition_label = ts.format("%Y-%m-%dT%H").to_string();
        let final_path = dir.join(format!("part-{partition_label}-{offset:012}.parquet"));
        let tmp_path = dir.join(format!("part-{partition_label}-{offset:012}.parquet.tmp"));

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

    #[test]
    fn writes_a_batch_and_names_it_deterministically() {
        let tmp = tempfile::tempdir().unwrap();
        let writer = BronzeWriter::new(tmp.path());
        let ts = DateTime::parse_from_rfc3339("2026-09-26T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let mut df = df![
            "sku" => &["A1", "A2"],
            "qty" => &[10i64, 20],
        ]
        .unwrap();

        let path = writer.write_batch(&mut df, ts, 42).unwrap();
        assert!(path.exists());
        assert!(path.to_string_lossy().contains("date=2026-09-26"));
        assert!(path.to_string_lossy().contains("hour=14"));
        assert!(path.to_string_lossy().ends_with("-000000000042.parquet"));

        // No leftover temp file.
        let tmp_path = path.with_extension("parquet.tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn rejects_an_empty_batch() {
        let tmp = tempfile::tempdir().unwrap();
        let writer = BronzeWriter::new(tmp.path());
        let mut empty = DataFrame::empty();
        let ts = Utc::now();
        assert!(matches!(
            writer.write_batch(&mut empty, ts, 1),
            Err(LakehouseError::EmptyBatch)
        ));
    }

    #[test]
    fn replaying_the_same_offset_overwrites_not_duplicates() {
        let tmp = tempfile::tempdir().unwrap();
        let writer = BronzeWriter::new(tmp.path());
        let ts = Utc::now();

        let mut df1 = df!["sku" => &["A1"], "qty" => &[1i64]].unwrap();
        let path1 = writer.write_batch(&mut df1, ts, 7).unwrap();

        let mut df2 = df!["sku" => &["A1"], "qty" => &[999i64]].unwrap();
        let path2 = writer.write_batch(&mut df2, ts, 7).unwrap();

        assert_eq!(path1, path2, "same (ts, offset) must produce the same file path");

        let read_back = ParquetReader::new(fs::File::open(&path2).unwrap())
            .finish()
            .unwrap();
        let qty: i64 = read_back
            .column("qty")
            .unwrap()
            .i64()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(qty, 999, "replay must have overwritten the original value");
    }
}
