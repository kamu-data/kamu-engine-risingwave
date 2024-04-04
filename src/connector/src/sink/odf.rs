// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use core::fmt::Debug;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;
use opendatafabric as odf;
use risingwave_common::array::{Op, StreamChunk, StreamChunkTestExt};
use risingwave_common::catalog::Schema;
use serde_derive::{Deserialize, Serialize};
use serde_with::serde_as;
use thiserror_ext::AsReport;
use with_options::WithOptions;

use super::{DummySinkCommitCoordinator, SinkWriterParam};
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result as SinkResult, Sink, SinkError, SinkParam};

////////////////////////////////////////////////////////////////////////////////////////

pub const ODF_SINK: &str = "odf";

////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct OdfConfig {
    #[serde(rename = "odf.manifest_path")]
    pub manifest_path: String,
    #[serde(rename = "odf.summary_path")]
    pub summary_path: String,
}

/// Basic data types for use with the mqtt interface
impl OdfConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> SinkResult<Self> {
        let config = serde_json::from_value::<OdfConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }

    pub fn read_manifest(&self) -> SinkResult<odf::TransformRequest> {
        let f = std::fs::File::open(PathBuf::from(&self.manifest_path)).map_err(|e| anyhow!(e))?;
        let manifest: Manifest = serde_json::from_reader(f).map_err(|e| anyhow!(e))?;
        Ok(manifest.0)
    }

    pub fn write_summary(&self, summary: odf::TransformResponseSuccess) -> SinkResult<()> {
        let f =
            std::fs::File::create_new(PathBuf::from(&self.summary_path)).map_err(|e| anyhow!(e))?;
        serde_json::to_writer(f, &Summary(&summary)).map_err(|e| anyhow!(e))?;
        Ok(())
    }
}

#[serde_as]
#[derive(Deserialize, Debug)]
struct Manifest(#[serde_as(as = "odf::serde::yaml::TransformRequestDef")] odf::TransformRequest);

#[serde_as]
#[derive(Serialize, Debug)]
struct Summary<'a>(
    #[serde_as(as = "odf::serde::yaml::TransformResponseSuccessDef")]
    &'a odf::TransformResponseSuccess,
);

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct OdfSink {
    config: OdfConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    db_name: String,
    sink_from_name: String,
}

impl TryFrom<SinkParam> for OdfSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> Result<Self, Self::Error> {
        tracing::info!(?param, "OdfSink::new()");

        let schema = param.schema();
        let config = OdfConfig::from_hashmap(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices: param.downstream_pk,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

// TODO: Research sink decoupling (see parent trait)
impl Sink for OdfSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<OdfSinkWriter>;

    const SINK_NAME: &'static str = ODF_SINK;

    async fn validate(&self) -> SinkResult<()> {
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> SinkResult<Self::LogSinker> {
        Ok(OdfSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.db_name.clone(),
            self.sink_from_name.clone(),
        )
        .await?
        .into_log_sinker(usize::MAX))
    }
}

////////////////////////////////////////////////////////////////////////////////////////

// sink write
pub struct OdfSinkWriter {
    config: OdfConfig,
    schema: Schema,
    manifest: odf::TransformRequest,
    debug: bool,
    buffer: Vec<StreamChunk>,
    initial_checkpoint_seen: bool,
}

impl OdfSinkWriter {
    pub async fn new(
        config: OdfConfig,
        schema: Schema,
        _pk_indices: Vec<usize>,
        _db_name: String,
        _sink_from_name: String,
    ) -> SinkResult<Self> {
        tracing::info!(?schema, "OdfSinkWriter::new()");

        let manifest = config.read_manifest()?;

        // TODO: replace with engine configuration propagated via ODF manifests
        let debug = std::env::var("RW_ODF_SINK_DEBUG").ok().as_deref() == Some("1");

        Ok::<_, SinkError>(Self {
            config: config.clone(),
            schema: schema.clone(),
            manifest,
            debug,
            buffer: Vec::new(),
            initial_checkpoint_seen: false,
        })
    }

    fn validate_schema(schema: &Schema, vocab: &odf::DatasetVocabulary) -> SinkResult<()> {
        let system_columns = [
            &vocab.offset_column,
            &vocab.operation_type_column,
            &vocab.system_time_column,
        ];
        for system_column in system_columns {
            if schema
                .fields
                .iter()
                .position(|f| f.name == *system_column)
                .is_some()
            {
                return Err(anyhow!(
                    "Transformed data contains a column that conflicts with the system column \
                         name, you should either rename the data column or configure the dataset \
                         vocabulary to use a different name: {}",
                    system_column
                )
                .into());
            }
        }

        if schema
            .fields
            .iter()
            .position(|f| f.name == vocab.event_time_column)
            .is_none()
        {
            return Err(anyhow!(
                "Event time column {} was not found amongst: {}",
                vocab.event_time_column,
                schema
                    .fields
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .into());
        }

        Ok(())
    }

    fn num_records_buffered(&self) -> usize {
        self.buffer.iter().map(|c| c.cardinality()).sum()
    }

    /// Computes watermark as `min(max(input1), max(input2), ...)`, returning `None` if one of
    /// the inputs does not have a watermark yet.
    fn compute_output_watermark(&self) -> Option<DateTime<Utc>> {
        if self
            .manifest
            .query_inputs
            .iter()
            .any(|i| i.explicit_watermarks.is_empty())
        {
            return None;
        }

        let wm = self
            .manifest
            .query_inputs
            .iter()
            .map(|i| {
                i.explicit_watermarks
                    .iter()
                    .map(|wm| wm.event_time)
                    .max()
                    .unwrap()
            })
            .min()
            .unwrap();

        Some(wm)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn flush(&mut self) -> SinkResult<()> {
        let num_records = self.num_records_buffered();

        let new_watermark = self.compute_output_watermark();

        tracing::info!(
            num_chunks = self.buffer.len(),
            num_records,
            "Flushing buffered records into Parquet"
        );
        if num_records == 0 {
            self.config.write_summary(odf::TransformResponseSuccess {
                new_offset_interval: None,
                new_watermark,
            })?;
            return Ok(());
        }

        // Take chunks out of the buffer
        let mut chunks = Vec::new();
        std::mem::swap(&mut chunks, &mut self.buffer);

        // Compact into one big chunk to avoid inefficiently small arrow batches
        let chunks = self.concat_chunks(chunks);

        // Convert to arrow
        let batches = self.into_arrow_batches(chunks)?;

        self.write_result(batches).await.map_err(|e| anyhow!(e))?;

        self.config.write_summary(odf::TransformResponseSuccess {
            new_offset_interval: Some(odf::OffsetInterval {
                start: self.manifest.next_offset,
                end: self.manifest.next_offset + num_records as u64 - 1,
            }),
            new_watermark,
        })?;

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn concat_chunks(&self, chunks: Vec<StreamChunk>) -> Vec<StreamChunk> {
        let chunk = StreamChunk::concat(chunks);
        if self.debug {
            tracing::debug!(
                chunk = %chunk.to_pretty_with_schema(&self.schema),
                "Compacted data chunk",
            );
        }
        vec![chunk]
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn into_arrow_batches(&self, mut chunks: Vec<StreamChunk>) -> SinkResult<Vec<RecordBatch>> {
        // Use first chunk to establish the schema
        // TODO: Pass output schema to subsequent operations explicitly
        let first_chunk = chunks.remove(0);
        let batch = self.stream_chunk_to_arrow_batch(
            first_chunk,
            None,
            &self.manifest.vocab.operation_type_column,
        )?;

        let arrow_schema = batch.schema();
        tracing::info!(?arrow_schema, "Inferred schema");

        let mut batches = vec![batch];

        for chunk in chunks {
            let batch = self.stream_chunk_to_arrow_batch(
                chunk,
                Some(arrow_schema.clone()),
                &self.manifest.vocab.operation_type_column,
            )?;
            batches.push(batch);
        }

        if self.debug {
            tracing::debug!(
                batch = %datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap(),
                "Converted arrow batches",
            );
        }

        Ok(batches)
    }

    /// Converts chunk data columns into arrow arrays
    fn stream_chunk_data_to_arrow_arrays(
        chunk: StreamChunk,
    ) -> SinkResult<Vec<Arc<dyn arrow_array::Array>>> {
        let chunk = if chunk.is_compacted() {
            chunk
        } else {
            chunk.compact()
        };

        let mut columns = Vec::new();
        for column in chunk.columns() {
            let arrow_column =
                Arc::<dyn arrow_array::Array>::try_from(column.as_ref()).map_err(|e| anyhow!(e))?;

            columns.push(arrow_column);
        }

        Ok(columns)
    }

    // See: https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#representation-of-retractions-and-corrections
    fn stream_chunk_ops_to_arrow_array(ops: &[Op]) -> SinkResult<Arc<dyn arrow_array::Array>> {
        // TODO: Use u8 type after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
        let mut builder = arrow_array::Int32Array::builder(ops.len());

        for op in ops {
            builder.append_value(match op {
                Op::Insert => 0,
                Op::Delete => 1,
                Op::UpdateDelete => 2,
                Op::UpdateInsert => 3,
            });
        }

        Ok(Arc::new(builder.finish()))
    }

    // Most of this is copied from common/array/arrow_impl.rs
    fn stream_chunk_to_arrow_batch(
        &self,
        chunk: StreamChunk,
        schema: Option<arrow_schema::SchemaRef>,
        operation_type_column: &str,
    ) -> SinkResult<RecordBatch> {
        let ops_array = Self::stream_chunk_ops_to_arrow_array(chunk.ops())?;
        let ops_data_type = ops_array.data_type().clone();

        let mut columns = vec![ops_array];
        columns.append(&mut Self::stream_chunk_data_to_arrow_arrays(chunk)?);

        let schema = if let Some(schema) = schema {
            assert_eq!(columns.len(), schema.fields().len());
            schema
        } else {
            let mut fields = vec![arrow_schema::Field::new(
                operation_type_column,
                ops_data_type,
                false,
            )];

            for (array, field) in columns[1..].iter().zip(&self.schema.fields) {
                // TODO: Nullability is missing from schema and cannot be inferred
                let nullable = true;

                fields.push(arrow_schema::Field::new(
                    field.name.clone(),
                    array.data_type().clone(),
                    nullable,
                ));
            }

            Arc::new(arrow_schema::Schema::new(fields))
        };

        let batch = arrow_array::RecordBatch::try_new(schema, columns).map_err(|e| anyhow!(e))?;
        Ok(batch)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn write_result(&self, batches: Vec<RecordBatch>) -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();
        let table = datafusion::datasource::MemTable::try_new(batches[0].schema(), vec![batches])?;
        let df = ctx.read_table(Arc::new(table))?;

        let df = Self::normalize_raw_result(df)?;

        let df = Self::with_system_columns(
            df,
            &self.manifest.vocab,
            self.manifest.system_time,
            self.manifest.next_offset,
        )?;

        // TODO: Configure columns writer
        df.write_parquet(
            self.manifest.new_data_path.as_os_str().to_str().unwrap(),
            datafusion::dataframe::DataFrameWriteOptions::new().with_single_file_output(true),
            Some(Self::get_writer_properties(&self.manifest.vocab)),
        )
        .await?;

        Ok(())
    }

    // TODO: This function currently ensures that all timestamps in the ouput are
    // represeted as `Timestamp(Millis, "UTC")` for compatibility with other engines
    // (e.g. Flink does not support event time with nanosecond precision).
    fn normalize_raw_result(df: DataFrame) -> datafusion::error::Result<DataFrame> {
        use datafusion::arrow::datatypes::*;

        let utc_tz: Arc<str> = Arc::from("UTC");

        let mut select: Vec<Expr> = Vec::new();
        let mut noop = true;

        for field in df.schema().fields() {
            let expr = match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
                    col(field.unqualified_column())
                }
                DataType::Timestamp(_, _) => {
                    noop = false;
                    cast(
                        col(field.unqualified_column()),
                        DataType::Timestamp(TimeUnit::Millisecond, Some(utc_tz.clone())),
                    )
                    .alias(field.name())
                }
                _ => col(field.unqualified_column()),
            };
            select.push(expr);
        }

        if noop {
            Ok(df)
        } else {
            Ok(df.select(select)?)
        }
    }

    fn with_system_columns(
        df: DataFrame,
        vocab: &odf::DatasetVocabulary,
        system_time: DateTime<Utc>,
        start_offset: u64,
    ) -> datafusion::error::Result<DataFrame> {
        use datafusion::logical_expr::*;

        // Collect non-system column names for later
        let mut data_columns: Vec<_> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| {
                n.as_str() != vocab.event_time_column && n.as_str() != vocab.operation_type_column
            })
            .collect();

        // Offset
        // TODO: For some reason this adds two collumns: the expected "offset", but also
        // "ROW_NUMBER()" for now we simply filter out the latter.
        let df = df.with_column(
            &vocab.offset_column,
            Expr::WindowFunction(expr::WindowFunction {
                fun: expr::WindowFunctionDefinition::BuiltInWindowFunction(
                    BuiltInWindowFunction::RowNumber,
                ),
                args: vec![],
                partition_by: vec![],
                order_by: vec![],
                window_frame: WindowFrame::new(None),
            }),
        )?;

        // TODO: Cast to UInt64 after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
        let df = df.with_column(
            &vocab.offset_column,
            cast(
                col(&vocab.offset_column as &str) + lit(start_offset as i64 - 1),
                arrow_schema::DataType::Int64,
            ),
        )?;

        // System time
        let df = df.with_column(
            &vocab.system_time_column,
            Expr::Literal(datafusion::scalar::ScalarValue::TimestampMillisecond(
                Some(system_time.timestamp_millis()),
                Some("UTC".into()),
            )),
        )?;

        // Reorder columns for nice looks
        let mut full_columns = vec![
            vocab.offset_column.clone(),
            vocab.operation_type_column.clone(),
            vocab.system_time_column.clone(),
            vocab.event_time_column.clone(),
        ];
        full_columns.append(&mut data_columns);
        let full_columns_str: Vec<_> = full_columns.iter().map(String::as_str).collect();

        let df = df.select_columns(&full_columns_str)?;

        tracing::info!(schema = ?df.schema(), "Computed final result schema");
        Ok(df)
    }

    // TODO: Externalize configuration
    fn get_writer_properties(vocab: &odf::DatasetVocabulary) -> WriterProperties {
        // TODO: `offset` column is sorted integers so we could use delta encoding, but
        // Flink does not support it.
        // See: https://github.com/kamu-data/kamu-engine-flink/issues/3
        WriterProperties::builder()
            .set_writer_version(datafusion::parquet::file::properties::WriterVersion::PARQUET_1_0)
            .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
            // op column is low cardinality and best encoded as RLE_DICTIONARY
            .set_column_dictionary_enabled(vocab.operation_type_column.as_str().into(), true)
            // system_time value will be the same for all rows in a batch
            .set_column_dictionary_enabled(vocab.system_time_column.as_str().into(), true)
            .build()
    }
}

impl AsyncTruncateSinkWriter for OdfSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> SinkResult<()> {
        let num_records = chunk.cardinality();

        if num_records == 0 {
            tracing::debug!("Ingoring an empty chunk");
        } else {
            tracing::debug!(
                chunk_records = chunk.cardinality(),
                buffered_records = self.num_records_buffered(),
                "Buffering a chunk",
            );

            self.buffer.push(chunk);
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> SinkResult<()> {
        tracing::info!(is_checkpoint, "Sink received a barrier");

        if is_checkpoint {
            // When restoring from existing state RW will force a checkpoint right after
            // startup - we ignore it and wait for the "real one".
            if self.manifest.prev_checkpoint_path.is_some() && !self.initial_checkpoint_seen {
                tracing::info!("Ignoring the checkpoint taken right after startup");
                assert_eq!(self.buffer.len(), 0);
                self.initial_checkpoint_seen = true;
            } else {
                match self.flush().await {
                    Ok(()) => {}
                    Err(err) => {
                        tracing::error!(error = %err.as_report(), "Sink flush failed");
                        panic!("Sink flush failed - aborting");
                    }
                }
            }
        }

        Ok(())
    }
}

impl Drop for OdfSinkWriter {
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            tracing::error!(
                num_records_buffered = self.num_records_buffered(),
                "OdfSinkWriter is dropped with non-empty buffer",
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{TimeZone, Utc};
    use datafusion::prelude::*;
    use futures::TryStreamExt;
    use opendatafabric::*;
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::types::DataType;
    use risingwave_pb::plan_common::additional_column::ColumnType;
    use risingwave_pb::plan_common::{
        AdditionalColumn, AdditionalColumnOffset, AdditionalColumnPartition,
    };

    use crate::parser::*;
    use crate::sink::log_store::DeliveryFutureManager;
    use crate::sink::odf::*;
    use crate::source::odf::*;
    use crate::source::{
        Column, SourceColumnDesc, SourceColumnType, SourceContextRef, SplitReader,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_odf_e2e() {
        let tempdir = tempfile::tempdir().unwrap();

        let input_dataset_path = PathBuf::from(
            "/home/smikhtoniuk/Work/projects/opensource/risingwave/.priv/.kamu/datasets/counter",
        );

        let input_data_paths: Vec<_> = [
            "f162028afe4797c193e42a33fe042702a6eb1abf00d84daff1c03097db52bd173cb43",
            "f1620076808a1dc209c4b70c85f179a414d55497ea90194f964cd8afabcdc7ab6a919",
            "f1620d56888818e42f5e0c23dac59dd35a6104ef13e57ea0d91caa0dedd0b6beaf013",
        ]
        .iter()
        .map(|h| input_dataset_path.join(format!("data/{h}")))
        .collect();

        let source_manifest_path = tempdir.path().join("source.json");
        let sink_manifest_path = tempdir.path().join("sink.json");
        let idle_marker_path = tempdir.path().join("idle");
        let output_path = tempdir.path().join("output");
        let summary_path = tempdir.path().join("sink.summary.json");

        let request = TransformRequest {
            dataset_id: DatasetID::new_seeded_ed25519(b"output"),
            dataset_alias: DatasetAlias::try_from("alice/output").unwrap(),
            system_time: Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            vocab: DatasetVocabulary::default(),
            transform: Transform::Sql(TransformSql {
                engine: "risingwave".to_string(),
                version: None,
                temporal_tables: None,
                query: None,
                queries: Some(vec![SqlQueryStep {
                    alias: None,
                    query: r#"select * from "alice/input""#.to_string(),
                }]),
            }),
            query_inputs: vec![TransformRequestInput {
                dataset_id: DatasetID::new_seeded_ed25519(b"input"),
                dataset_alias: DatasetAlias::try_from("alice/input").unwrap(),
                query_alias: "alice/input".to_string(),
                vocab: DatasetVocabulary::default(),
                offset_interval: Some(OffsetInterval { start: 0, end: 29 }),
                schema_file: input_data_paths[0].clone(),
                data_paths: input_data_paths,
                explicit_watermarks: vec![],
            }],
            next_offset: 0,
            prev_checkpoint_path: None,
            new_checkpoint_path: tempdir.path().join("new_checkpoint"),
            new_data_path: output_path.clone(),
        };

        let f = std::fs::File::create_new(&sink_manifest_path).unwrap();
        serde_json::to_writer_pretty(f, &SinkManifest(&request)).unwrap();

        let f = std::fs::File::create_new(&source_manifest_path).unwrap();
        serde_json::to_writer_pretty(f, &SourceManifest(&request.query_inputs[0])).unwrap();

        let source = OdfSplitReader::new(
            OdfProperties {
                input_manifest_path: source_manifest_path.display().to_string(),
                idle_marker_path: idle_marker_path.display().to_string(),
                unknown_fields: HashMap::default(),
            },
            vec![OdfSplit::new(
                request.query_inputs[0].dataset_id.clone(),
                0,
                0,
            )],
            ParserConfig {
                common: CommonParserConfig {
                    rw_columns: vec![
                        SourceColumnDesc {
                            name: "event_time".to_string(),
                            data_type: DataType::Timestamptz,
                            column_id: ColumnId::new(1),
                            fields: vec![],
                            column_type: SourceColumnType::Normal,
                            is_pk: false,
                            is_hidden_addition_col: false,
                            additional_column: AdditionalColumn { column_type: None },
                        },
                        SourceColumnDesc {
                            name: "counter".to_string(),
                            data_type: DataType::Int32,
                            column_id: ColumnId::new(2),
                            fields: vec![],
                            column_type: SourceColumnType::Normal,
                            is_pk: false,
                            is_hidden_addition_col: false,
                            additional_column: AdditionalColumn { column_type: None },
                        },
                        SourceColumnDesc {
                            name: "_row_id".to_string(),
                            data_type: DataType::Serial,
                            column_id: ColumnId::new(0),
                            fields: vec![],
                            column_type: SourceColumnType::RowId,
                            is_pk: true,
                            is_hidden_addition_col: false,
                            additional_column: AdditionalColumn { column_type: None },
                        },
                        SourceColumnDesc {
                            name: "_rw_odf_partition".to_string(),
                            data_type: DataType::Varchar,
                            column_id: ColumnId::new(3),
                            fields: vec![],
                            column_type: SourceColumnType::Normal,
                            is_pk: false,
                            is_hidden_addition_col: true,
                            additional_column: AdditionalColumn {
                                column_type: Some(ColumnType::Partition(
                                    AdditionalColumnPartition {},
                                )),
                            },
                        },
                        SourceColumnDesc {
                            name: "_rw_odf_offset".to_string(),
                            data_type: DataType::Varchar,
                            column_id: ColumnId::new(5),
                            fields: vec![],
                            column_type: SourceColumnType::Normal,
                            is_pk: false,
                            is_hidden_addition_col: true,
                            additional_column: AdditionalColumn {
                                column_type: Some(ColumnType::Offset(AdditionalColumnOffset {})),
                            },
                        },
                    ],
                },
                specific: SpecificParserConfig {
                    key_encoding_config: None,
                    encoding_config: EncodingProperties::Csv(CsvProperties {
                        delimiter: 44,
                        has_header: false,
                    }),
                    protocol_config: ProtocolProperties::Plain,
                },
            },
            SourceContextRef::default(),
            Some(vec![
                Column {
                    name: "event_time".to_string(),
                    data_type: DataType::Timestamptz,
                    is_visible: true,
                },
                Column {
                    name: "counter".to_string(),
                    data_type: DataType::Int32,
                    is_visible: true,
                },
                Column {
                    name: "_row_id".to_string(),
                    data_type: DataType::Serial,
                    is_visible: false,
                },
                Column {
                    name: "_rw_odf_partition".to_string(),
                    data_type: DataType::Varchar,
                    is_visible: false,
                },
                Column {
                    name: "_rw_odf_offset".to_string(),
                    data_type: DataType::Varchar,
                    is_visible: false,
                },
            ]),
        )
        .await
        .unwrap();

        let mut sink = OdfSinkWriter::new(
            OdfConfig {
                manifest_path: sink_manifest_path.display().to_string(),
                summary_path: summary_path.display().to_string(),
            },
            risingwave_common::catalog::Schema {
                fields: vec![
                    risingwave_common::catalog::Field {
                        data_type: DataType::Timestamptz,
                        name: "event_time".to_string(),
                        sub_fields: Vec::new(),
                        type_name: "".to_string(),
                    },
                    risingwave_common::catalog::Field {
                        data_type: DataType::Int32,
                        name: "counter".to_string(),
                        sub_fields: Vec::new(),
                        type_name: "".to_string(),
                    },
                ],
            },
            Vec::new(),
            "".to_string(),
            "".to_string(),
        )
        .await
        .unwrap();

        let mut fm = DeliveryFutureManager::new(0);

        let mut source = source.into_stream();
        let mut chunk_id = 0;
        while let Some(chunk) = source.try_next().await.unwrap() {
            assert!(!idle_marker_path.exists());

            // Throw away source system columns
            let chunk = chunk.project(&[0, 1]);

            sink.write_chunk(chunk, fm.start_write_chunk(0, chunk_id))
                .await
                .unwrap();

            chunk_id += 1;
        }

        assert!(idle_marker_path.exists());
        assert!(!output_path.exists());

        sink.barrier(true).await.unwrap();

        assert!(output_path.exists());

        let ctx = SessionContext::new();
        let df = ctx
            .read_parquet(
                vec![output_path.display().to_string()],
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
            .unwrap();

        let arrow_schema = df.schema().clone();
        let batches = df.collect().await.unwrap();

        let actual = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();

        assert_eq!(
            indoc::indoc!(
                r#"
                +--------+----+----------------------+----------------------+---------+
                | offset | op | system_time          | event_time           | counter |
                +--------+----+----------------------+----------------------+---------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2000-01-01T00:00:00Z | 1       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2000-01-02T00:00:00Z | 2       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2000-01-03T00:00:00Z | 3       |
                | 3      | 0  | 2050-01-01T12:00:00Z | 2000-01-04T00:00:00Z | 4       |
                | 4      | 0  | 2050-01-01T12:00:00Z | 2000-01-05T00:00:00Z | 5       |
                | 5      | 0  | 2050-01-01T12:00:00Z | 2000-01-06T00:00:00Z | 6       |
                | 6      | 0  | 2050-01-01T12:00:00Z | 2000-01-07T00:00:00Z | 7       |
                | 7      | 0  | 2050-01-01T12:00:00Z | 2000-01-08T00:00:00Z | 8       |
                | 8      | 0  | 2050-01-01T12:00:00Z | 2000-01-09T00:00:00Z | 9       |
                | 9      | 0  | 2050-01-01T12:00:00Z | 2000-01-10T00:00:00Z | 10      |
                | 10     | 0  | 2050-01-01T12:00:00Z | 2000-02-01T00:00:00Z | 11      |
                | 11     | 0  | 2050-01-01T12:00:00Z | 2000-02-02T00:00:00Z | 12      |
                | 12     | 0  | 2050-01-01T12:00:00Z | 2000-02-03T00:00:00Z | 13      |
                | 13     | 0  | 2050-01-01T12:00:00Z | 2000-02-04T00:00:00Z | 14      |
                | 14     | 0  | 2050-01-01T12:00:00Z | 2000-02-05T00:00:00Z | 15      |
                | 15     | 0  | 2050-01-01T12:00:00Z | 2000-02-06T00:00:00Z | 16      |
                | 16     | 0  | 2050-01-01T12:00:00Z | 2000-02-07T00:00:00Z | 17      |
                | 17     | 0  | 2050-01-01T12:00:00Z | 2000-02-08T00:00:00Z | 18      |
                | 18     | 0  | 2050-01-01T12:00:00Z | 2000-02-09T00:00:00Z | 19      |
                | 19     | 0  | 2050-01-01T12:00:00Z | 2000-02-10T00:00:00Z | 20      |
                | 20     | 0  | 2050-01-01T12:00:00Z | 2000-03-01T00:00:00Z | 21      |
                | 21     | 0  | 2050-01-01T12:00:00Z | 2000-03-02T00:00:00Z | 22      |
                | 22     | 0  | 2050-01-01T12:00:00Z | 2000-03-03T00:00:00Z | 23      |
                | 23     | 0  | 2050-01-01T12:00:00Z | 2000-03-04T00:00:00Z | 24      |
                | 24     | 0  | 2050-01-01T12:00:00Z | 2000-03-05T00:00:00Z | 25      |
                | 25     | 0  | 2050-01-01T12:00:00Z | 2000-03-06T00:00:00Z | 26      |
                | 26     | 0  | 2050-01-01T12:00:00Z | 2000-03-07T00:00:00Z | 27      |
                | 27     | 0  | 2050-01-01T12:00:00Z | 2000-03-08T00:00:00Z | 28      |
                | 28     | 0  | 2050-01-01T12:00:00Z | 2000-03-09T00:00:00Z | 29      |
                | 29     | 0  | 2050-01-01T12:00:00Z | 2000-03-10T00:00:00Z | 30      |
                +--------+----+----------------------+----------------------+---------+
                "#
            )
            .trim(),
            actual.trim(),
            "Actual:\n{}\n",
            actual.trim(),
        );

        let parquet_schema =
            datafusion::parquet::arrow::arrow_to_parquet_schema(&arrow_schema.into()).unwrap();
        let mut actual = Vec::new();
        datafusion::parquet::schema::printer::print_schema(
            &mut actual,
            &parquet_schema.root_schema_ptr(),
        );
        assert_eq!(
            indoc::indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT32 counter;
                }
                "#
            )
            .trim(),
            std::str::from_utf8(&actual).unwrap().trim()
        );

        let summary: SinkSummary =
            serde_json::from_reader(std::fs::File::open(summary_path).unwrap()).unwrap();

        assert_eq!(
            summary.0,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 0, end: 29 }),
                new_watermark: None,
            }
        )
    }

    #[serde_with::serde_as]
    #[derive(::serde::Serialize, Debug)]
    struct SourceManifest<'a>(
        #[serde_as(as = "opendatafabric::serde::yaml::TransformRequestInputDef")]
        &'a TransformRequestInput,
    );

    #[serde_with::serde_as]
    #[derive(::serde::Serialize, Debug)]
    struct SinkManifest<'a>(
        #[serde_as(as = "opendatafabric::serde::yaml::TransformRequestDef")] &'a TransformRequest,
    );

    #[serde_with::serde_as]
    #[derive(::serde::Deserialize, Debug)]
    struct SinkSummary(
        #[serde_as(as = "opendatafabric::serde::yaml::TransformResponseSuccessDef")]
        TransformResponseSuccess,
    );
}
