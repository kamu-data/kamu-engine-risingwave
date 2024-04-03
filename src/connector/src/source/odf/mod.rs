use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use opendatafabric as odf;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, JsonbVal, ScalarImpl, Serial};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use with_options::WithOptions;

use super::{
    BoxChunkSourceStream, Column, CommonSplitReader, SourceContextRef, SourceEnumeratorContextRef,
    SourceMessage, SplitEnumerator, SplitId, SplitMetaData, SplitReader,
};
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::{CommonParserConfig, ParserConfig};
use crate::source::{SourceMeta, SourceProperties, UnknownFields};

////////////////////////////////////////////////////////////////////////////////////////

pub const ODF_CONNECTOR: &str = "odf";

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Deserialize, PartialEq, WithOptions)]
pub struct OdfProperties {
    /// The path to the dataset
    #[serde(rename = "odf.input_manifest_path")]
    pub input_manifest_path: String,

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
        tracing::error!(splits = ?[self.split.id.as_ref()], "kamu: list_splits");
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
        tracing::error!(
            split_id = %split.id,
            start_offset = split.start_offset,
            end_offset = split.end_offset,
            "kamu: restore_from_json",
        );
        Ok(split)
    }

    fn encode_to_json(&self) -> JsonbVal {
        tracing::error!(
            split_id = %self.id,
            start_offset = self.start_offset,
            end_offset = self.end_offset,
            "kamu: encode_to_json",
        );
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_with_offset(&mut self, start_offset: String) -> ConnectorResult<()> {
        // Reader encodes a *remaining* interval into RW's `offset` field
        let new_interval = start_offset;

        tracing::error!(
            split_id = %self.id,
            start_offset = self.start_offset,
            end_offset = self.end_offset,
            %new_interval,
            "kamu: update_with_offset",
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
    source_ctx: SourceContextRef,
    parser_config: ParserConfig,
}

impl OdfSplitReader {
    // Reload input from file to see if offset interval has been advanced
    pub fn validate_input_assignment(&self) -> ConnectorResult<(u64, u64)> {
        tracing::error!(
            split_id = %self.split.id,
            split_start_offset = self.split.start_offset,
            input = ?self.input,
            "kamu: validate_input_assignment",
        );

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

                tracing::error!(
                    split_id = %self.split.id,
                    start_offset = input_iv.start,
                    end_offset = input_iv.end + 1,
                    "kamu: advancing to new input interval",
                );

                // Convert to half-open [start; end) interval
                Ok((input_iv.start, input_iv.end + 1))
            } else {
                tracing::error!(
                    split_id = %self.split.id,
                    start_offset = self.split.start_offset,
                    end_offset = self.split.end_offset,
                    "kamu: maintaining old interval",
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

// impl OdfSplitReader {
// #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
// async fn into_chunk_stream(self) {
// tracing::error!("kamu: into_chunk_stream begin");
//
// let mut chunk_builder = StreamChunkBuilder::new(
// 3,
// vec![
// Column { name: "id", data_type: Int32, is_visible: true },
// DataType::Int32,
// Column { name: "symbol", data_type: Varchar, is_visible: true },
// DataType::Varchar,
// Column { name: "price", data_type: Float64, is_visible: true },
// DataType::Float64,
// Column { name: "_row_id", data_type: Serial, is_visible: false },
// DataType::Serial,
// Column { name: "_rw_odf_partition", data_type: Varchar, is_visible: false },
// DataType::Varchar,
// Column { name: "_rw_odf_offset", data_type: Varchar, is_visible: false }
// DataType::Varchar,
// ],
// );
//
// let mut row_num: i64 = 0;
//
// for split in self.splits {
// let mut row = Vec::with_capacity(3);
// row.push(Some(ScalarImpl::Int32(row_num as i32 + 1)));
// row.push(Some(ScalarImpl::Utf8("SPY".into())));
// row.push(Some(ScalarImpl::Float64(1.1.into())));
// row.push(Some(ScalarImpl::Serial(Serial::from(row_num))));
// row.push(Some(ScalarImpl::Utf8(split.id.as_ref().into())));
// row.push(Some(ScalarImpl::Utf8(row_num.to_string().into_boxed_str())));
// row_num += 1;
// if let Some(chunk) = chunk_builder.append_row(Op::Insert, OwnedRow::new(row)) {
// tracing::error!(?chunk, "Chunk");
// yield chunk;
// }
// }
//
// if let Some(chunk) = chunk_builder.take() {
// tracing::error!(?chunk, "Chunk (last)");
// yield chunk;
// }
//
// tracing::error!("kamu: into_chunk_stream done");
// }
// }

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
        assert_eq!(splits.len(), 1);
        let split = splits.into_iter().next().unwrap();

        let input = properties.read_input_manifest()?;

        tracing::error!(?split, ?columns, "kamu: new_split_reader");

        Ok(Self {
            split,
            input,
            source_ctx,
            parser_config,
        })
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        super::into_chunk_stream(self, parser_config, source_context)
    }
}

impl CommonSplitReader for OdfSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(self) {
        let (mut offset, end_offset) = self.validate_input_assignment()?;

        if offset == end_offset {
            tracing::error!(
                split_id = %self.split.id,
                start_offset = offset,
                end_offset,
                "kamu: into_data_stream: exhausted",
            );
            return Ok(());
        };

        tracing::error!(
            split_id = %self.split.id,
            start_offset = offset,
            end_offset,
            "kamu: into_data_stream: starting to process split",
        );

        'outer: for path in &self.input.data_paths {
            let file = std::fs::File::open(path)?;
            let reader = std::io::BufReader::new(file);

            for line in std::io::BufRead::lines(reader) {
                let line = line?;

                // (Ab)using RW message offset field to pass remaining interval into the split
                let remaining_interval = format!("[{};{})", offset + 1, end_offset);

                let msg = SourceMessage {
                    key: None,
                    payload: Some(line.into_bytes()),
                    offset: remaining_interval,
                    split_id: self.split.id(),
                    meta: SourceMeta::Empty,
                };

                tracing::error!(
                    path = ?path,
                    offset,
                    remaining_interval = msg.offset,
                    "kamu: into_data_stream: yield",
                );

                yield vec![msg];

                offset += 1;
                if offset == end_offset {
                    break 'outer;
                }
            }
        }

        assert_eq!(
            offset, end_offset,
            "Exhausted available data without reaching the target end offset",
        );

        tracing::error!("kamu: into_data_stream: done");
    }
}
