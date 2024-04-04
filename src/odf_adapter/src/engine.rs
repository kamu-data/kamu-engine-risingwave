use std::path::{Path, PathBuf};
use std::time::Duration;

use ::serde::{Deserialize, Serialize};
use datafusion::prelude::*;
use indoc::indoc;
use internal_error::*;
use opendatafabric::engine::{ExecuteRawQueryError, ExecuteTransformError};
use opendatafabric::*;
use serde_with::serde_as;

use crate::rw::{RisingWave, RisingWaveConfig};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct Engine {
    rw_config: RisingWaveConfig,

    /// Directory that we use as a side channel to pametrize sources.
    /// Note that this directory will appear in source settings that are defined once and
    /// cannot change. This means that this directory must remain the same across multiple
    /// engine runs as the previous value will be restored from the checkpoint.
    io_dir: PathBuf,

    /// Datafusion context only used for reading schemas from parquet files
    ctx: SessionContext,
}

impl Engine {
    pub async fn new(rw_config: RisingWaveConfig, io_dir: PathBuf) -> Result<Self, InternalError> {
        if !io_dir.exists() {
            std::fs::create_dir_all(&io_dir).int_err()?;
        } else {
            assert!(
                !std::fs::read_dir(&io_dir).int_err()?.next().is_some(),
                "IO dir exists and is not empty",
            );
        }

        Ok(Self {
            rw_config,
            io_dir,
            ctx: SessionContext::new(),
        })
    }

    pub async fn execute_raw_query(
        &self,
        _request: RawQueryRequest,
    ) -> Result<RawQueryResponseSuccess, ExecuteRawQueryError> {
        todo!()
    }

    pub async fn execute_transform(
        &self,
        request: TransformRequest,
    ) -> Result<TransformResponseSuccess, ExecuteTransformError> {
        let store_dir = self.io_dir.join("store");

        let rw = if let Some(prev_checkpoint) = &request.prev_checkpoint_path {
            // Unpack checkpoint
            Self::unpack_checkpoint(&prev_checkpoint, &store_dir).int_err()?;

            // Write new manifests
            for input in &request.query_inputs {
                self.write_source_manifest(input).int_err()?;
            }
            self.write_sink_manifest(&request).int_err()?;

            // Spawn engine from checkpoint
            RisingWave::new(self.rw_config.clone(), store_dir.clone()).await?
        } else {
            // Spawn engine
            let rw = RisingWave::new(self.rw_config.clone(), store_dir.clone()).await?;

            // Setup sources
            for input in &request.query_inputs {
                self.register_source(&rw, input).await?;
            }

            // Setup queries
            let Transform::Sql(transform) = &request.transform;
            let queries = transform.queries.as_ref().unwrap();

            for step in queries.iter().filter(|s| s.alias.is_some()) {
                self.register_view_for_step(&rw, step.alias.as_deref().unwrap(), &step.query)
                    .await?;
            }

            // Setup sink
            let sink_query = queries.last().unwrap();
            assert!(sink_query.alias.is_none());

            self.register_sink(
                &rw,
                &sink_query.query,
                &request.dataset_id.as_multibase().to_stack_string(),
                &request,
            )
            .await?;

            rw
        };

        // Wait for all source to go idle
        tracing::info!("Waiting for sources to become idle...");
        self.wait_for_sources_to_idle(&request).await?;

        // Trigger a checkpoint
        tracing::info!("Flushing with checkpoint...");
        rw.pg.execute("FLUSH", &[]).await.int_err()?;
        // rw.meta.flush(true).await.int_err()?;

        let summary = self
            .read_sink_summary(&self.summary_path(&request.dataset_id))
            .int_err()?;

        tracing::info!(?summary, "Sink finished with summary");

        // Shutdown into checkpoint
        tracing::info!("Shutting down...");
        rw.close().await;

        // Pack checkpoint
        Self::pack_checkpoint(&store_dir, &request.new_checkpoint_path).int_err()?;

        // Clean up IO dir
        std::fs::remove_dir_all(&self.io_dir).int_err()?;

        Ok(summary)
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            dataset_id = %input.dataset_id,
            dataset_alias = %input.dataset_alias,
            query_alias = %input.query_alias,
        )
    )]
    async fn register_source(
        &self,
        rw: &RisingWave,
        input: &TransformRequestInput,
    ) -> Result<(), ExecuteTransformError> {
        assert!(
            (input.data_paths.is_empty() && input.offset_interval.is_none())
                || (!input.data_paths.is_empty() && input.offset_interval.is_some())
        );

        let schema = self
            .get_source_schema(&input.schema_file, &input.vocab)
            .await?;

        let input_path = self.write_source_manifest(input).int_err()?;
        let input_path = input_path.into_os_string().into_string().unwrap();

        let idle_marker_path = self.idle_marker_path(input);
        let idle_marker_path = idle_marker_path.into_os_string().into_string().unwrap();

        // TODO: Remove `format plain encode csv` and use `native` or no format
        let pg_query = indoc!(
            r#"
            create source "{alias}" (
                {schema},
                watermark for "{event_time_col}" as timestamp with time zone '0000-01-01 00:00:00+00'
            )
            with (
                connector = 'odf',
                odf.input_manifest_path = '{input_path}',
                odf.idle_marker_path = '{idle_marker_path}',
            ) format plain encode csv (
                without_header = 'true',
                delimiter = ','
            );
            "#
        )
        .replace("{alias}", &input.query_alias)
        .replace("{schema}", &schema)
        .replace("{event_time_col}", &input.vocab.event_time_column)
        .replace("{input_path}", &input_path)
        .replace("{idle_marker_path}", &idle_marker_path);

        tracing::info!(pg_query, "Create source query");
        rw.pg.execute(&pg_query, &[]).await.map_db_err()?;

        Ok(())
    }

    async fn get_source_schema(
        &self,
        schema_file: &Path,
        vocab: &DatasetVocabulary,
    ) -> Result<String, ExecuteTransformError> {
        // TODO: Switch from reading schema from file to passing it in request
        // and remove datafusion as direct dependency
        let schema_file = schema_file.as_os_str().to_str().unwrap();

        let df = self
            .ctx
            .read_parquet(
                vec![schema_file],
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
            .int_err()?;

        let arrow_schema = datafusion::arrow::datatypes::Schema::from(df.schema());
        let rw_schema = risingwave_common::types::StructType::from(arrow_schema.fields());
        let ddl_schema = rw_schema
            .iter()
            .filter(|(n, _)| {
                *n != vocab.offset_column
                    && *n != vocab.operation_type_column
                    && *n != vocab.system_time_column
            })
            .map(|(n, t)| format!("\"{n}\" {t}"))
            .collect::<Vec<_>>()
            .join(",\n");

        tracing::info!(%arrow_schema, %rw_schema, ddl_schema, "Converted source schema");

        Ok(ddl_schema)
    }

    fn write_source_manifest(
        &self,
        input: &TransformRequestInput,
    ) -> Result<PathBuf, std::io::Error> {
        let path_tmp = self.io_dir.join(format!(
            "source-{}.json.tmp",
            input.dataset_id.as_multibase()
        ));
        let path_final = self
            .io_dir
            .join(format!("source-{}.json", input.dataset_id.as_multibase()));

        let f = std::fs::File::create_new(&path_tmp)?;
        serde_json::to_writer_pretty(f, &SourceManifest(input))?;
        std::fs::rename(path_tmp, &path_final)?;
        Ok(path_final)
    }

    #[tracing::instrument(level = "info", skip_all, fields(alias))]
    async fn register_view_for_step(
        &self,
        rw: &RisingWave,
        alias: &str,
        query: &str,
    ) -> Result<(), ExecuteTransformError> {
        tracing::info!(alias, query, "Creating view for a query",);

        // TODO: HACK: ODF currently doesn't support declaring materialized views, but for
        // prototyping resons it may be useful to be able to declare them. Below we detect that
        // if query starts with `create` - we pass it directly to the engine unchanged.
        let pg_query = if !query.trim().to_lowercase().starts_with("create") {
            indoc!(
                r#"
                create view "{alias}" as
                {query}
                "#
            )
            .replace("{alias}", alias)
            .replace("{query}", query)
        } else {
            query.to_string()
        };

        tracing::info!(pg_query, "Create view query");
        rw.pg.execute(&pg_query, &[]).await.map_db_err()?;

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all, fields(alias))]
    async fn register_sink(
        &self,
        rw: &RisingWave,
        sink_query: &str,
        sink_alias: &str,
        request: &TransformRequest,
    ) -> Result<(), ExecuteTransformError> {
        // TODO: Add system columns

        let manifest_path = self.write_sink_manifest(request).int_err()?;
        let summary_path = self.summary_path(&request.dataset_id);

        let pg_query = indoc!(
            r#"
            create sink "{alias}" as
            {sink_query}
            with (
                connector = 'odf',
                odf.manifest_path = '{manifest_path}',
                odf.summary_path = '{summary_path}',
            ) format debezium encode json;
            "#
        )
        .replace("{alias}", sink_alias)
        .replace("{sink_query}", sink_query)
        .replace("{manifest_path}", &manifest_path.display().to_string())
        .replace("{summary_path}", &summary_path.display().to_string());

        tracing::info!(pg_query, "Create sink query");
        rw.pg.execute(&pg_query, &[]).await.map_db_err()?;

        Ok(())
    }

    fn write_sink_manifest(&self, request: &TransformRequest) -> Result<PathBuf, std::io::Error> {
        let path_tmp = self.io_dir.join(format!(
            "sink-{}.json.tmp",
            request.dataset_id.as_multibase()
        ));
        let path_final = self
            .io_dir
            .join(format!("sink-{}.json", request.dataset_id.as_multibase()));

        let f = std::fs::File::create_new(&path_tmp)?;
        serde_json::to_writer_pretty(f, &SinkManifest(request))?;
        std::fs::rename(path_tmp, &path_final)?;
        Ok(path_final)
    }

    fn read_sink_summary(&self, path: &Path) -> Result<TransformResponseSuccess, std::io::Error> {
        let f = std::fs::File::open(&path)?;
        let summary: SinkSummary = serde_json::from_reader(f)?;
        Ok(summary.0)
    }

    fn idle_marker_path(&self, input: &TransformRequestInput) -> PathBuf {
        self.io_dir
            .join(format!("idle-{}", input.dataset_id.as_multibase()))
    }

    fn summary_path(&self, dataset_id: &DatasetID) -> PathBuf {
        self.io_dir
            .join(format!("summary-{}", dataset_id.as_multibase()))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn wait_for_sources_to_idle(
        &self,
        request: &TransformRequest,
    ) -> Result<(), InternalError> {
        for input in &request.query_inputs {
            let idle_marker_path = self.idle_marker_path(input);
            while !idle_marker_path.exists() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "info")]
    fn unpack_checkpoint(checkpoint_path: &Path, store_dir: &Path) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(store_dir)?;
        let mut archive = tar::Archive::new(std::fs::File::open(checkpoint_path)?);
        archive.unpack(store_dir)?;
        Ok(())
    }

    #[tracing::instrument(level = "info")]
    fn pack_checkpoint(store_dir: &Path, checkpoint_path: &Path) -> Result<(), std::io::Error> {
        let mut ar = tar::Builder::new(std::fs::File::create_new(checkpoint_path)?);
        ar.follow_symlinks(false);
        ar.append_dir_all(".", store_dir)?;
        ar.finish()?;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Serialize, Debug)]
struct SourceManifest<'a>(
    #[serde_as(as = "opendatafabric::serde::yaml::TransformRequestInputDef")]
    &'a TransformRequestInput,
);

#[serde_as]
#[derive(Serialize, Debug)]
struct SinkManifest<'a>(
    #[serde_as(as = "opendatafabric::serde::yaml::TransformRequestDef")] &'a TransformRequest,
);

#[serde_as]
#[derive(Deserialize, Debug)]
struct SinkSummary(
    #[serde_as(as = "opendatafabric::serde::yaml::TransformResponseSuccessDef")]
    TransformResponseSuccess,
);

/////////////////////////////////////////////////////////////////////////////////////////

trait DbErr {
    type Output;

    fn map_db_err(self) -> Result<Self::Output, ExecuteTransformError>;
}

impl<T> DbErr for Result<T, tokio_postgres::Error> {
    type Output = T;

    fn map_db_err(self) -> Result<Self::Output, ExecuteTransformError> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => match e.as_db_error() {
                None => Err(e.int_err().into()),
                Some(db_err) => Err(ExecuteTransformError::InvalidQuery(
                    TransformResponseInvalidQuery {
                        message: db_err.to_string(),
                    },
                )),
            },
        }
    }
}
