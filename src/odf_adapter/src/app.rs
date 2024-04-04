use std::path::PathBuf;
use std::sync::Arc;

use internal_error::*;
use opendatafabric::engine::grpc_generated::engine_server::EngineServer;
use thiserror_ext::AsReport;
use tonic::transport::Server;

use crate::engine::Engine;
use crate::grpc::EngineGRPCImpl;
use crate::rw::RisingWaveConfig;

/////////////////////////////////////////////////////////////////////////////////////////

const VERSION: &str = env!("CARGO_PKG_VERSION");

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct OdfEngineConfig {
    pub listen_addr: String,
    pub io_dir: PathBuf,
    pub rw_config: RisingWaveConfig,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn run(cfg: OdfEngineConfig) -> Result<(), InternalError> {
    tracing::info!(
        message = "Starting RisingWave ODF engine",
        version = VERSION,
        ?cfg,
    );

    let engine = Arc::new(Engine::new(cfg.rw_config, cfg.io_dir).await?);
    let engine_grpc = EngineGRPCImpl::new(engine);

    Server::builder()
        .add_service(EngineServer::new(engine_grpc))
        .serve(cfg.listen_addr.parse().int_err()?)
        .await
        .int_err()
        .inspect_err(|e| {
            tracing::error!(
                error = %e.as_report(),
                "gRPC server exited with error",
            )
        })
}

/////////////////////////////////////////////////////////////////////////////////////////
