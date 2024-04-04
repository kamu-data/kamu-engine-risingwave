use std::path::PathBuf;

use internal_error::*;
use kamu_engine_risingwave::app::OdfEngineConfig;
use kamu_engine_risingwave::rw::RisingWaveConfig;

risingwave_common::enable_jemalloc!();

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<(), InternalError> {
    let odf_cfg = OdfEngineConfig {
        listen_addr: "0.0.0.0:2884".to_string(),
        rw_config: RisingWaveConfig {
            bin: PathBuf::from("/opt/engine/bin/risingwave"),
            config_path: PathBuf::from("/opt/engine/config.toml"),
        },
        io_dir: PathBuf::from("/opt/engine/io"),
    };

    init_logging(&odf_cfg);

    kamu_engine_risingwave::app::run(odf_cfg).await
}

/////////////////////////////////////////////////////////////////////////////////////////

#[cfg(not(feature = "embedded"))]
fn init_logging(_cfg: &OdfEngineConfig) {
    use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
    use tracing_log::LogTracer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{EnvFilter, Registry};

    let default_logging_config = "info";

    // Redirect all standard logging to tracing events
    LogTracer::init().expect("Failed to set LogTracer");

    // Use configuration from RUST_LOG env var if provided
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or(EnvFilter::new(default_logging_config.to_owned()));

    // TODO: Use non-blocking writer?
    // Configure Bunyan JSON formatter
    let formatting_layer =
        BunyanFormattingLayer::new(env!("CARGO_PKG_NAME").into(), std::io::stdout);
    let subscriber = Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}

#[cfg(feature = "embedded")]
fn init_logging(cfg: &OdfEngineConfig) {
    let cfg = risingwave_cmd_all::SingleNodeOpts {
        store_directory: None,
        prometheus_listener_addr: None,
        config_path: Some(cfg.rw_config.config_path.to_string_lossy().into()),
        in_memory: false,
        max_idle_secs: None,
        node_opts: risingwave_cmd_all::NodeSpecificOpts {
            parallelism: Some(1),
            total_memory_bytes: None,
            listen_addr: None,
            prometheus_endpoint: None,
            prometheus_selector: None,
            compaction_worker_threads_number: None,
        },
    };

    let cfg = risingwave_cmd_all::map_single_node_opts_to_standalone_opts(cfg);

    let settings = risingwave_rt::LoggerSettings::from_opts(&cfg)
        .with_target("risingwave_storage", tracing::Level::WARN)
        .with_thread_name(true);

    risingwave_rt::init_risingwave_logger(settings);
}
