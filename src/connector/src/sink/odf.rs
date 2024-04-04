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

use anyhow::anyhow;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::row::RowExt;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use serde_derive::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::catalog::SinkFormatDesc;
use super::formatter::SinkFormatterImpl;
use super::writer::FormattedSink;
use super::{DummySinkCommitCoordinator, SinkWriterParam};
use crate::dispatch_sink_formatter_impl;
use crate::sink::catalog::desc::SinkDesc;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, Sink, SinkError, SinkParam};

////////////////////////////////////////////////////////////////////////////////////////

pub const ODF_SINK: &str = "odf";

////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct OdfConfig {
    #[serde(rename = "odf.output_path")]
    pub output_path: String,
}

/// Basic data types for use with the mqtt interface
impl OdfConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<OdfConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct OdfSink {
    config: OdfConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    // format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl TryFrom<SinkParam> for OdfSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        tracing::error!(?param, "kamu: sink_from_param");

        let schema = param.schema();
        let config = OdfConfig::from_hashmap(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices: param.downstream_pk,
            // format_desc: param
            //     .format_desc
            //     .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

impl Sink for OdfSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<OdfSinkWriter>;

    const SINK_NAME: &'static str = ODF_SINK;

    fn is_sink_decouple(desc: &SinkDesc, user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default => Ok(desc.sink_type.is_append_only()),
            SinkDecouple::Disable => Ok(false),
            SinkDecouple::Enable => Ok(true),
        }
    }

    async fn validate(&self) -> Result<()> {
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        tracing::error!("kamu: new_log_sinker");

        Ok(OdfSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            // &self.format_desc,
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
    // payload_writer: OdfSinkPayloadWriter,
    schema: Schema,
    // formatter: SinkFormatterImpl,
    records: usize,
}

impl OdfSinkWriter {
    pub async fn new(
        config: OdfConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        // format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        // let formatter = SinkFormatterImpl::new(
        //     format_desc,
        //     schema.clone(),
        //     pk_indices.clone(),
        //     db_name,
        //     sink_from_name,
        //     "default",
        // )
        // .await?;

        // let payload_writer = OdfSinkPayloadWriter {
        //     config: config.clone(),
        // };

        Ok::<_, SinkError>(Self {
            config: config.clone(),
            // payload_writer,
            schema: schema.clone(),
            // formatter,
            records: 0,
        })
    }
}

impl AsyncTruncateSinkWriter for OdfSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        tracing::error!("kamu: write_chunk");

        println!(">>>\n{}", chunk.to_pretty_with_schema(&self.schema));
        Ok(())

        // dispatch_sink_formatter_impl!(&self.formatter, formatter, {
        //     self.payload_writer.write_chunk(chunk, formatter).await
        // })
    }
}

impl Drop for OdfSinkWriter {
    fn drop(&mut self) {
        tracing::error!(records = self.records, "kamu: drop_sink_writer");
    }
}

////////////////////////////////////////////////////////////////////////////////////////

// struct OdfSinkPayloadWriter {
// config: OdfConfig,
// }
//
// impl FormattedSink for OdfSinkPayloadWriter {
// type K = Vec<u8>;
// type V = Vec<u8>;
//
// async fn write_one(&mut self, _k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
// if let Some(v) = &v {
// let value = std::str::from_utf8(&v[..]).unwrap();
// tracing::error!(value, "kamu: write_one");
// } else {
// tracing::error!("kamu: write_one");
// }
// Ok(())
// }
// }
