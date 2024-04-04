use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use arrow_array::cast::AsArray;
use async_trait::async_trait;
use datafusion::prelude::*;
use futures_async_stream::try_stream;
use opendatafabric as odf;
use risingwave_common::array::{ArrayImplBuilder, DataChunk, Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, JsonbVal, ScalarRefImpl, Timestamptz};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use with_options::WithOptions;

use super::{
    BoxChunkSourceStream, Column, SourceContextRef, SourceEnumeratorContextRef, SplitEnumerator,
    SplitId, SplitMetaData, SplitReader,
};
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::ParserConfig;
use crate::source::{SourceProperties, UnknownFields};

////////////////////////////////////////////////////////////////////////////////////////

pub const ODF_CONNECTOR: &str = "odf";

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, PartialEq, WithOptions)]
pub struct OdfProperties {
    /// Path to the input manifest that is written by ODF adapter to define the source
    /// data files and the offset range to read
    #[serde(rename = "odf.input_manifest_path")]
    pub input_manifest_path: String,

    /// Path to the marker file that the source will created to signal when it reaches
    /// the end op input. This file helps ODF adapter to know when to trigger a checkpoint.
    #[serde(rename = "odf.idle_marker_path")]
    pub idle_marker_path: String,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl OdfProperties {
    pub fn read_input_manifest(&self) -> ConnectorResult<odf::TransformRequestInput> {
        let f = std::fs::File::open(PathBuf::from(&self.input_manifest_path))?;
        let input: InputManifest = serde_json::from_reader(f)?;
        Ok(input.0)
    }
}

impl UnknownFields for OdfProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SourceProperties for OdfProperties {
    type Split = OdfSplit;
    type SplitEnumerator = OdfSplitEnumerator;
    type SplitReader = OdfSplitReader;

    const SOURCE_NAME: &'static str = ODF_CONNECTOR;
}

#[serde_as]
#[derive(Deserialize, Debug)]
struct InputManifest(
    #[serde_as(as = "odf::serde::yaml::TransformRequestInputDef")] odf::TransformRequestInput,
);

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OdfSplitEnumerator {
    pub split: OdfSplit,
}

#[async_trait]
impl SplitEnumerator for OdfSplitEnumerator {
    type Properties = OdfProperties;
    type Split = OdfSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        let input = properties.read_input_manifest()?;

        let (start_offset, end_offset) = if let Some(iv) = &input.offset_interval {
            // Note: conversion into half-open `[start; end)` interval
            (iv.start, iv.end + 1)
        } else {
            (0, 0)
        };

        let split = OdfSplit::new(input.dataset_id, start_offset, end_offset);

        Ok(Self { split })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
        tracing::debug!(splits = ?[self.split.id.as_ref()], "SplitEnumerator::list_splits()");
        Ok(vec![self.split.clone()])
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct OdfSplit {
    /// Same as DatasetID
    id: SplitId,
    /// ODF record offsets remaining to be read in half-open `[start; end)` format
    start_offset: u64,
    /// ODF record offsets remaining to be read in half-open `[start; end)` format
    end_offset: u64,
}

impl OdfSplit {
    pub fn new(dataset_id: odf::DatasetID, start_offset: u64, end_offset: u64) -> Self {
        Self {
            id: SplitId::from(dataset_id.as_did_str().to_stack_string().as_str()),
            start_offset,
            end_offset,
        }
    }
}

impl SplitMetaData for OdfSplit {
    fn id(&self) -> SplitId {
        self.id.clone()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        let split: Self = serde_json::from_value(value.take())?;
        tracing::info!(
            split_id = %split.id,
            start_offset = split.start_offset,
            end_offset = split.end_offset,
            "OdfSplit::restore_from_json()",
        );
        Ok(split)
    }

    fn encode_to_json(&self) -> JsonbVal {
        tracing::debug!(
            split_id = %self.id,
            start_offset = self.start_offset,
            end_offset = self.end_offset,
            "OdfSplit::encode_to_json()",
        );
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_with_offset(&mut self, start_offset: String) -> ConnectorResult<()> {
        // Reader encodes a *remaining* interval into RW's `offset` field
        let new_interval = start_offset;

        tracing::debug!(
            split_id = %self.id,
            start_offset = self.start_offset,
            end_offset = self.end_offset,
            %new_interval,
            "OdfSplit::update_with_offset()",
        );

        let inner = new_interval
            .strip_prefix('[')
            .unwrap()
            .strip_suffix(')')
            .unwrap();
        let (start, end) = inner.split_once(';').unwrap();

        self.start_offset = start.parse().unwrap();
        self.end_offset = end.parse().unwrap();

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
pub struct OdfSplitReader {
    split: OdfSplit,
    input: odf::TransformRequestInput,
    idle_marker_path: PathBuf,
    source_ctx: SourceContextRef,
    parser_config: ParserConfig,
    columns: Vec<Column>,
    // Used to emit explicit watermarks
    event_time_col_index: usize,
    // Only used to debug print the chunks
    chunk_schema: Schema,
}

impl OdfSplitReader {
    // Reload input from file to see if offset interval has been advanced
    pub fn validate_input_assignment(&self) -> ConnectorResult<(u64, u64)> {
        tracing::info!(
            split_id = %self.split.id,
            split_start_offset = self.split.start_offset,
            input = ?self.input,
            idle_marker_path = %self.idle_marker_path.display(),
            "OdfSplitReader::validate_input_assignment()",
        );

        assert!(!self.idle_marker_path.exists());

        // Possible cases:
        // - input stays the same - we continue processing if anything left to do
        // - input advanced - we ensure that previous slice was fully processed and advance end offset
        // - input is empty - we ensure our offset interval is finished and leave it as is
        if let Some(input_iv) = self.input.offset_interval.as_ref() {
            if (input_iv.end + 1) != self.split.end_offset {
                assert_eq!(
                    self.split.start_offset,
                    self.split.end_offset,
                    "Received new empty input interval while not all records been read on the previous step",
                );

                assert_eq!(
                    input_iv.start, self.split.start_offset,
                    "New input skips some records",
                );

                tracing::info!(
                    split_id = %self.split.id,
                    start_offset = input_iv.start,
                    end_offset = input_iv.end + 1,
                    "Advancing to new input interval",
                );

                // Convert to half-open [start; end) interval
                Ok((input_iv.start, input_iv.end + 1))
            } else {
                tracing::info!(
                    split_id = %self.split.id,
                    start_offset = self.split.start_offset,
                    end_offset = self.split.end_offset,
                    "Maintaining old interval",
                );
                Ok((self.split.start_offset, self.split.end_offset))
            }
        } else {
            assert_eq!(
                self.split.start_offset,
                self.split.end_offset,
                "Received new empty input interval while not all records been read on the previous step",
            );
            Ok((self.split.start_offset, self.split.end_offset))
        }
    }
}

#[async_trait]
impl SplitReader for OdfSplitReader {
    type Properties = OdfProperties;
    type Split = OdfSplit;

    async fn new(
        properties: Self::Properties,
        splits: Vec<Self::Split>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        tracing::info!(?splits, ?columns, ?parser_config, "OdfSplitReader::new()");

        assert_eq!(splits.len(), 1);
        let split = splits.into_iter().next().unwrap();
        let input = properties.read_input_manifest()?;
        let idle_marker_path = PathBuf::from(properties.idle_marker_path);

        // RW uses a very confusing mechanism of invisible columns to propagate some metadata
        // from sources to upper levels. How these columns are handled resides very deep inside
        // the format parsers that we don't use. Below we assert that RW requests same special
        // columns for us every time to detect if implementation changes at some point.
        // See: SourceStreamChunkRowWriter
        let columns = columns.unwrap();
        assert_eq!(
            columns.iter().filter(|c| !c.is_visible).count(),
            3,
            "Unexpected schema: {:#?}",
            columns
        );

        assert_eq!(columns[columns.len() - 3].is_visible, false);
        assert_eq!(columns[columns.len() - 3].name, "_row_id");
        assert_eq!(columns[columns.len() - 3].data_type, DataType::Serial);

        assert_eq!(columns[columns.len() - 2].is_visible, false);
        assert_eq!(columns[columns.len() - 2].name, "_rw_odf_partition");
        assert_eq!(columns[columns.len() - 2].data_type, DataType::Varchar);

        assert_eq!(columns[columns.len() - 1].is_visible, false);
        assert_eq!(columns[columns.len() - 1].name, "_rw_odf_offset");
        assert_eq!(columns[columns.len() - 1].data_type, DataType::Varchar);

        assert!(
            columns
                .iter()
                .position(|c| c.name == input.vocab.operation_type_column)
                .is_none(),
            "Source should not select operation type column",
        );

        let event_time_col_index = columns
            .iter()
            .position(|c| c.name == input.vocab.event_time_column)
            .expect("Event time column is not mentioned in the source schema");

        // This schema is used only for debug output of chunks
        let chunk_schema = Schema::new(
            columns
                .iter()
                .map(|c| Field {
                    data_type: c.data_type.clone(),
                    name: c.name.clone(),
                    // TODO: Nested structs support
                    sub_fields: Vec::new(),
                    type_name: String::new(),
                })
                .collect(),
        );

        Ok(Self {
            split,
            input,
            idle_marker_path,
            source_ctx,
            parser_config,
            columns,
            event_time_col_index,
            chunk_schema,
        })
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        Box::pin(self.into_chunk_stream())
    }
}

impl OdfSplitReader {
    #[try_stream(ok = StreamChunk, error = ConnectorError)]
    async fn into_chunk_stream(self) {
        // Debugging/testing stuff
        // TODO: replace with engine configuration propagated via ODF manifests
        let debug = std::env::var("RW_ODF_SOURCE_DEBUG").ok().as_deref() == Some("1");

        let max_records_per_chunk: usize = std::env::var("RW_ODF_SOURCE_MAX_RECORDS_PER_CHUNK")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(usize::MAX);

        let sleep_between_chunks = Duration::from_millis(
            std::env::var("RW_ODF_SOURCE_SLEEP_CHUNK_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        );

        let (mut offset, end_offset) = self.validate_input_assignment()?;

        if offset == end_offset {
            // Create marker
            std::fs::write(&self.idle_marker_path, "")?;

            tracing::info!(
                split_id = %self.split.id,
                start_offset = offset,
                end_offset,
                "Nothing to read",
            );
            return Ok(());
        };

        tracing::info!(
            split_id = %self.split.id,
            start_offset = offset,
            end_offset,
            "Starting to process the split",
        );

        let ctx = SessionContext::new();

        for path in &self.input.data_paths {
            // Read slice with requested columns and offset interval
            let batches = self.read_slice(&ctx, &path, offset, end_offset).await?;

            // Convert arrow batches into stream chunks
            for batch in batches {
                let mut batch_slice_offset = 0;

                // Slicing is done only for debugging purposes - big batches are good
                while batch_slice_offset != batch.num_rows() {
                    let batch_slice_len =
                        std::cmp::min(batch.num_rows() - batch_slice_offset, max_records_per_chunk);

                    let micro_batch = batch.slice(batch_slice_offset, batch_slice_len);
                    batch_slice_offset += batch_slice_len;

                    // (Ab)using RW's message offset field to pass remaining interval into the split
                    offset += micro_batch.num_rows() as u64;
                    let remaining_interval = format!("[{};{})", offset, end_offset);

                    let chunk =
                        self.record_batch_into_stream_chunk(micro_batch, &remaining_interval)?;

                    if !sleep_between_chunks.is_zero() {
                        tokio::time::sleep(sleep_between_chunks).await;
                    }

                    if debug {
                        tracing::debug!(
                            chunk = %chunk.to_pretty_with_schema(&self.chunk_schema),
                            "Yielding data chunk",
                        );
                    }

                    yield chunk;
                }
            }

            assert_eq!(
                offset, end_offset,
                "Exhausted available data without reaching the target end offset",
            );
        }

        // TODO: Interlace watermarks into the chunk stream
        for wm in &self.input.explicit_watermarks {
            let ts = Timestamptz::from_millis(wm.event_time.timestamp_millis()).unwrap();

            if !sleep_between_chunks.is_zero() {
                tokio::time::sleep(sleep_between_chunks).await;
            }

            tracing::info!(?wm, "Yielding explicit watermark");
            yield StreamChunk::new_watermark(self.event_time_col_index, ts);
        }

        // Create marker
        std::fs::write(&self.idle_marker_path, "")?;

        tracing::info!(
            split_id = %self.split.id,
            idle_marker_path = %self.idle_marker_path.display(),
            "Done processing the split",
        );
    }

    #[tracing::instrument(level = "info", skip_all, fields(path = %path.display(), start_offset, end_offset))]
    async fn read_slice(
        &self,
        ctx: &SessionContext,
        path: &Path,
        start_offset: u64,
        end_offset: u64,
    ) -> ConnectorResult<Vec<arrow_array::RecordBatch>> {
        let df = ctx
            .read_parquet(
                path.as_os_str().to_str().unwrap(),
                ParquetReadOptions {
                    file_extension: "",
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    schema: None,
                    file_sort_order: Vec::new(),
                },
            )
            .await
            .map_err(|e| anyhow!(e))?;

        tracing::info!(arrow_schema = %df.schema(), "Initialized a dataframe from Parquet");

        // Filter range: where offset >= start_offset and offset < end_offset
        let df = df
            .filter(
                col(&self.input.vocab.offset_column)
                    .gt_eq(lit(start_offset))
                    .and(col(&self.input.vocab.offset_column).lt(lit(end_offset))),
            )
            .map_err(|e| anyhow!(e))?;

        // Filter result by requested columns, skipping invisible ones and fetching op type
        let retain_columns: Vec<_> =
            std::iter::once(self.input.vocab.operation_type_column.as_str())
                .chain(
                    self.columns
                        .iter()
                        .filter(|c| c.is_visible)
                        .map(|c| c.name.as_str()),
                )
                .collect();

        let df = df
            .select_columns(&retain_columns[..])
            .map_err(|e| anyhow!(e))?;

        let batches = df.collect().await.map_err(|e| anyhow!(e))?;
        tracing::debug!(num_batches = batches.len(), "Collected the arrow batches");

        Ok(batches)
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn record_batch_into_stream_chunk(
        &self,
        mut batch: arrow_array::RecordBatch,
        remaining_interval: &str,
    ) -> ConnectorResult<StreamChunk> {
        // First column is the operation type
        let ops_arrow = batch.remove_column(0);
        let ops: Vec<Op> = ops_arrow
            .as_primitive::<arrow_array::types::Int32Type>()
            .iter()
            .map(|op| match op {
                Some(0) => Op::Insert,
                Some(1) => Op::Delete,
                Some(2) => Op::UpdateDelete,
                Some(3) => Op::UpdateInsert,
                _ => unreachable!(),
            })
            .collect();

        // Convert the rest
        let data_chunk = DataChunk::try_from(&batch).map_err(|e| anyhow!(e))?;

        // Add special columns
        let data_chunk =
            Self::extend_with_system_columns(data_chunk, &self.split.id, remaining_interval);

        Ok(StreamChunk::from_parts(ops, data_chunk))
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn extend_with_system_columns(
        chunk: DataChunk,
        split_id: &SplitId,
        remaining_interval: &str,
    ) -> DataChunk {
        let num_rows = chunk.cardinality();
        let (mut cols, bitmap) = chunk.into_parts();

        // "_row_id" column
        // Sources emit this as all nulls - the RowIdGen operator later assigns them
        //
        // TODO: Note that this will not work retractions and corrections. RW docs specify that
        // those would require materializing source into a table with primary key.
        // See: https://docs.risingwave.com/docs/current/sql-create-source/
        //
        // The code seems to also expect that delete/update records also have ROW IDs corresponding
        // to the row being deleted/updated. Even if ODF was preserving the link between retractions
        // and the record offsets being retracted we would run into a problem that the code always
        // reassigns IDs for insert records. Supporting retractions in source data therefore looks
        // like a lot of work.
        let mut b = ArrayImplBuilder::with_type(num_rows, DataType::Serial);
        b.append_n_null(num_rows);
        cols.push(Arc::new(b.finish()));

        // "_rw_odf_partition" column
        let mut b = ArrayImplBuilder::with_type(num_rows, DataType::Varchar);
        b.append_n(num_rows, Some(ScalarRefImpl::Utf8(&split_id)));
        cols.push(Arc::new(b.finish()));

        // "_rw_odf_offset" column
        let mut b = ArrayImplBuilder::with_type(num_rows, DataType::Varchar);
        b.append_n(num_rows, Some(ScalarRefImpl::Utf8(remaining_interval)));
        cols.push(Arc::new(b.finish()));

        DataChunk::new(cols, bitmap)
    }
}
