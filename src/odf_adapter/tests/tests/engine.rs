use std::path::{Path, PathBuf};
use std::time::Duration;

use internal_error::*;
use kamu_engine_risingwave::app::OdfEngineConfig;
use kamu_engine_risingwave::rw::RisingWaveConfig;
use opendatafabric::engine::EngineGrpcClient;

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn start_engine(
    tmp_dir: &Path,
) -> (
    EngineGrpcClient,
    tokio::task::JoinHandle<Result<(), InternalError>>,
) {
    let workspace_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or(".".into()))
        .join("../..")
        .canonicalize()
        .unwrap();

    let host = "127.0.0.1";
    let port = 2884;
    let addr = format!("{host}:{port}");

    let server = kamu_engine_risingwave::app::run(OdfEngineConfig {
        listen_addr: addr.clone(),
        rw_config: RisingWaveConfig {
            bin: workspace_dir.join("target/debug/risingwave"),
            config_path: workspace_dir.join("odf/config.toml"),
        },
        io_dir: tmp_dir.join("io"),
    });

    let server_handle = tokio::spawn(server);

    tracing::info!("Waiting for ODF adapter socket on {addr}...");
    kamu_engine_risingwave::utils::wait_for_socket(&addr.parse().unwrap(), Duration::from_secs(60))
        .await
        .unwrap();

    let client = EngineGrpcClient::connect(host, port).await.unwrap();

    (client, server_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! await_client_server_flow {
    ($api_server_handle:expr, $client_handle:expr $(,)?) => {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => panic!("test timeout!"),
            _ = $api_server_handle => panic!("server-side aborted"),
            _ = $client_handle => {} // Pass, do nothing
        }
    };
}

pub(crate) use await_client_server_flow;
