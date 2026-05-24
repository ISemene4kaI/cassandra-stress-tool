use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use metrics::gauge;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{cassandra::CassandraClient, config::Config, workload::KeyRing};

pub const REASON_CLIENT_NOT_READY: &str = "cassandra_client_not_ready";
pub const REASON_CONNECT_FAILED: &str = "connect_failed";
pub const REASON_SCHEMA_CHECK_FAILED: &str = "schema_check_failed";
pub const REASON_PREPARE_FAILED: &str = "prepare_failed";
pub const REASON_TOO_MANY_ERRORS: &str = "too_many_consecutive_errors";
pub const REASON_OPERATIONS_STALE: &str = "cassandra_operations_stale";

#[derive(Default)]
pub struct Stats {
    pub reads_found: AtomicU64,
    pub reads_empty: AtomicU64,
    pub reads_failed: AtomicU64,
    pub writes_ok: AtomicU64,
    pub writes_failed: AtomicU64,
    pub successes_since_log: AtomicU64,
    pub last_log_successes: AtomicU64,
    pub last_log_timestamp: AtomicU64,
}

#[derive(Clone)]
pub struct AppState {
    pub prometheus: PrometheusHandle,
    pub cassandra: Arc<RwLock<Option<Arc<CassandraClient>>>>,
    pub last_success_timestamp: Arc<AtomicU64>,
    pub last_unready_reason: Arc<RwLock<String>>,
    pub ready_max_age: std::time::Duration,
}

#[derive(Clone)]
pub struct RuntimeState {
    pub cassandra: Arc<RwLock<Option<Arc<CassandraClient>>>>,
    pub ring: Arc<RwLock<KeyRing>>,
    pub stats: Arc<Stats>,
    pub last_success_timestamp: Arc<AtomicU64>,
    pub consecutive_errors: Arc<AtomicU64>,
    pub last_unready_reason: Arc<RwLock<String>>,
}

impl RuntimeState {
    pub fn new(config: &Config) -> Self {
        let state = Self {
            cassandra: Arc::new(RwLock::new(None)),
            ring: Arc::new(RwLock::new(KeyRing::new(ring_capacity(config)))),
            stats: Arc::new(Stats::default()),
            last_success_timestamp: Arc::new(AtomicU64::new(0)),
            consecutive_errors: Arc::new(AtomicU64::new(0)),
            last_unready_reason: Arc::new(RwLock::new(REASON_CLIENT_NOT_READY.to_string())),
        };
        state
            .stats
            .last_log_timestamp
            .store(crate::util::unix_timestamp(), Ordering::Relaxed);
        gauge!("miniapp_last_success_timestamp").set(0.0);
        gauge!("miniapp_cassandra_ready").set(0.0);
        state
    }

    pub fn app_state(&self, prometheus: PrometheusHandle, config: &Config) -> AppState {
        AppState {
            prometheus,
            cassandra: Arc::clone(&self.cassandra),
            last_success_timestamp: Arc::clone(&self.last_success_timestamp),
            last_unready_reason: Arc::clone(&self.last_unready_reason),
            ready_max_age: config.ready_max_age,
        }
    }
}

pub fn mark_success(last_success_timestamp: &AtomicU64, consecutive_errors: &AtomicU64) {
    let now = crate::util::unix_timestamp();
    consecutive_errors.store(0, Ordering::Relaxed);
    last_success_timestamp.store(now, Ordering::Relaxed);
    gauge!("miniapp_last_success_timestamp").set(now as f64);
}

pub async fn mark_cassandra_ready(state: &RuntimeState, client: CassandraClient) {
    gauge!("miniapp_cassandra_ready").set(1.0);
    state.consecutive_errors.store(0, Ordering::Relaxed);
    *state.cassandra.write().await = Some(Arc::new(client));
}

pub async fn mark_cassandra_unready(state: &RuntimeState, reason: &str) {
    gauge!("miniapp_cassandra_ready").set(0.0);
    set_unready_reason(state, reason).await;
    *state.cassandra.write().await = None;
}

pub async fn set_unready_reason(state: &RuntimeState, reason: &str) {
    set_reason(&state.last_unready_reason, reason).await;
}

pub async fn set_reason(reason_cell: &Arc<RwLock<String>>, reason: &str) {
    let mut current = reason_cell.write().await;
    if current.as_str() == reason {
        return;
    }

    let previous = current.clone();
    *current = reason.to_string();
    if reason == REASON_OPERATIONS_STALE || reason == REASON_CLIENT_NOT_READY {
        info!(previous, reason, "readiness reason changed");
    } else {
        warn!(previous, reason, "readiness reason changed");
    }
}

fn ring_capacity(config: &Config) -> usize {
    (config.rps_per_pod as usize)
        .saturating_mul(60)
        .clamp(4096, 1_000_000)
}
