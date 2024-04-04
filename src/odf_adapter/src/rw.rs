use std::path::PathBuf;
use std::time::Duration;

use internal_error::*;
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;
use tokio_postgres::Client as PgClient;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RisingWaveConfig {
    pub bin: PathBuf,
    pub config_path: PathBuf,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct RisingWave {
    // pub meta: MetaClient,
    pub pg: PgClient,
    pub store_dir: PathBuf,
    pg_task: JoinHandle<()>,

    #[cfg(not(feature = "embedded"))]
    child: tokio::process::Child,

    #[cfg(feature = "embedded")]
    rw_task: JoinHandle<Result<(), anyhow::Error>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl RisingWave {
    #[cfg(not(feature = "embedded"))]
    pub async fn new(cfg: RisingWaveConfig, store_dir: PathBuf) -> Result<Self, InternalError> {
        use std::process::Stdio;

        tracing::info!(?cfg, "Spawning RW in a child process");

        let child = tokio::process::Command::new(cfg.bin)
            .arg("single-node")
            .arg("--store-directory")
            .arg(&store_dir)
            .arg("--config-path")
            .arg(&cfg.config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .int_err()?;

        // let meta = Self::connect_to_meta("http://127.0.0.1:5690").await?;
        let (pg, pg_task) = Self::connect_to_postgres("127.0.0.1:5690", "127.0.0.1:4566").await?;
        Ok(Self {
            // meta,
            pg,
            store_dir,
            pg_task,
            child,
        })
    }

    #[cfg(feature = "embedded")]
    pub async fn new(cfg: RisingWaveConfig, store_dir: PathBuf) -> Result<Self, InternalError> {
        let cfg = risingwave_cmd_all::SingleNodeOpts {
            store_directory: Some(store_dir.to_string_lossy().into()),
            prometheus_listener_addr: None,
            config_path: Some(cfg.config_path.to_string_lossy().into()),
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
        let rw_task = tokio::spawn(risingwave_cmd_all::standalone(cfg));

        // let meta = Self::connect_to_meta("http://127.0.0.1:5690").await?;
        let (pg, pg_task) = Self::connect_to_postgres("127.0.0.1:4566").await?;

        Ok(Self {
            // meta,
            pg,
            store_dir,
            pg_task,
            rw_task,
        })
    }

    // async fn connect_to_meta(meta_url: &str) -> Result<MetaClient, InternalError> {
    //     use risingwave_common::config::MetaConfig;
    //     use risingwave_common::util::addr::HostAddr;
    //     use risingwave_pb::common::WorkerType;

    //     let meta_socket_addr = meta_url.split_once("//").unwrap().1.parse().int_err()?;

    //     tracing::info!("Waiting for Meta socket on {meta_url}...");
    //     crate::utils::wait_for_socket(&meta_socket_addr, Duration::from_secs(40)).await?;

    //     let (client, _) = MetaClient::register_new(
    //         meta_url.parse().int_err()?,
    //         WorkerType::RiseCtl,
    //         &HostAddr {
    //             host: "odf-adapter".to_string(),
    //             port: 0,
    //         },
    //         risingwave_pb::meta::add_worker_node_request::Property::default(),
    //         &MetaConfig::default(),
    //     )
    //     .await
    //     .int_err()?;

    //     Ok(client)
    // }

    async fn connect_to_postgres(
        meta_addr: &str,
        pg_addr: &str,
    ) -> Result<(PgClient, JoinHandle<()>), InternalError> {
        // Meta and Frontend services start together independently, but frontend will send requests
        // to meta. We therefore wait for meta to be up first before connecting to frontend to avoid
        // "service unavailable" errors due to concurrent startup.
        tracing::info!("Waiting for Meta socket on {meta_addr}...");
        crate::utils::wait_for_socket(&meta_addr.parse().int_err()?, Duration::from_secs(40))
            .await?;

        tracing::info!("Waiting for Postgres socket on {pg_addr}...");
        crate::utils::wait_for_socket(&pg_addr.parse().int_err()?, Duration::from_secs(40)).await?;

        let db_url = format!("postgres://root@{pg_addr}/dev?connect_timeout=10");
        tracing::info!(url = db_url, "Connecting Postgres client");
        let (client, connection) = tokio_postgres::connect(&db_url, tokio_postgres::NoTls)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    err = %e.as_report(),
                    "Failed to connect to Postgres interface"
                )
            })
            .int_err()?;

        let hdl = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e.as_report(), "postgres connection error");
            }
        });

        Ok((client, hdl))
    }

    #[cfg(not(feature = "embedded"))]
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn close(mut self) {
        self.pg_task.abort();

        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.child.id().unwrap() as i32),
            nix::sys::signal::Signal::SIGTERM,
        )
        .expect("cannot send ctrl-c");

        tracing::info!("Waiting for RW to exit");
        if tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .is_err()
        {
            tracing::info!("Killing RW process");
            let _ = self.child.kill().await;
        }
    }

    #[cfg(feature = "embedded")]
    pub async fn close(self) {
        drop(self.pg);
        self.pg_task.abort();
        self.rw_task.abort();
        let _ = self.rw_task.await;
        let _ = self.pg_task.await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
