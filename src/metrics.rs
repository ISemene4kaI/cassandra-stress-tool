use anyhow::{Context, Result};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::time::{self, Duration};

pub fn init_prometheus() -> Result<PrometheusHandle> {
    PrometheusBuilder::new()
        .install_recorder()
        .context("failed to install prometheus recorder")
}

pub async fn metrics_upkeep_loop(handle: PrometheusHandle) {
    let mut ticker = time::interval(Duration::from_secs(15));
    loop {
        ticker.tick().await;
        // metrics-exporter-prometheus 0.13 does not expose a public upkeep method
        // on PrometheusHandle; rendering periodically drives the same snapshot path
        // used by /metrics and prunes idle data when that feature is configured.
        let _ = handle.render();
    }
}
