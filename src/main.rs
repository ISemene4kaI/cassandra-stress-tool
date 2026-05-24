mod cassandra;
mod config;
mod http;
mod metrics;
mod state;
mod util;
mod workload;

use anyhow::Result;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::{
    cassandra::reconnect_loop,
    config::Config,
    http::http_server,
    metrics::{init_prometheus, metrics_upkeep_loop},
    state::RuntimeState,
    util::shutdown_signal,
    workload::worker_loop,
};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let config = Config::from_env()?;

    info!(
        contact_points = %config.contact_points.join(","),
        local_dc = %config.local_dc,
        keyspace = %config.keyspace,
        consistency = %config.consistency_name,
        tls_enabled = config.tls_enabled,
        create_schema = config.create_schema,
        rps_per_pod = config.rps_per_pod,
        workers = config.workers,
        reconnect_after_consecutive_errors = config.reconnect_after_consecutive_errors,
        ready_max_age_seconds = config.ready_max_age.as_secs(),
        "starting mini cassandra downtime detector"
    );

    if config.historical_read_enabled {
        warn!(
            historical_buckets = config.historical_buckets,
            "APP_HISTORICAL_READ_ENABLED=true uses random_miss_probe because no historical id list is configured"
        );
    }

    let prometheus = init_prometheus()?;
    let metrics_upkeep = tokio::spawn(metrics_upkeep_loop(prometheus.clone()));

    let runtime_state = RuntimeState::new(&config);
    let app_state = runtime_state.app_state(prometheus, &config);

    let server = tokio::spawn(http_server(app_state, config.metrics_addr));
    let reconnect = tokio::spawn(reconnect_loop(runtime_state.clone(), config.clone()));

    let mut workers = JoinSet::new();
    for worker_id in 0..config.workers {
        workers.spawn(worker_loop(
            worker_id,
            runtime_state.clone(),
            config.clone(),
        ));
    }

    shutdown_signal().await;
    info!("shutdown signal received");

    workers.abort_all();
    while workers.join_next().await.is_some() {}

    reconnect.abort();
    metrics_upkeep.abort();
    server.abort();
    let _ = reconnect.await;
    let _ = metrics_upkeep.await;
    let _ = server.await;

    info!("shutdown complete");
    Ok(())
}

fn init_logging() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .with_span_list(false)
        .init();
}
