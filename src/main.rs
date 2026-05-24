use std::{
    collections::VecDeque,
    env,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use rand::{distributions::Alphanumeric, rngs::SmallRng, Rng, SeedableRng};
use scylla::{
    load_balancing::DefaultPolicy, prepared_statement::PreparedStatement, statement::Consistency,
    transport::execution_profile::ExecutionProfile, transport::session::Session,
    transport::session_builder::SessionBuilder,
};
use thiserror::Error;
use tokio::{net::TcpListener, sync::Mutex, task::JoinSet, time};
use tracing::{error, info, warn};
use uuid::Uuid;

const READY_MAX_AGE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct Config {
    contact_points: Vec<String>,
    local_dc: String,
    keyspace: String,
    username: Option<String>,
    password: Option<String>,
    consistency: Consistency,
    consistency_name: String,
    tls_enabled: bool,
    app_rps: u64,
    read_ratio: u32,
    write_ratio: u32,
    payload_bytes: usize,
    workers: usize,
    buckets: usize,
    metrics_addr: SocketAddr,
    log_every_n_success: u64,
    writer_id: String,
}

#[derive(Clone, Debug)]
struct WrittenKey {
    bucket: String,
    id: Uuid,
}

#[derive(Debug)]
struct KeyRing {
    keys: VecDeque<WrittenKey>,
    capacity: usize,
}

impl KeyRing {
    fn new(capacity: usize) -> Self {
        Self {
            keys: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, key: WrittenKey) {
        if self.keys.len() == self.capacity {
            self.keys.pop_front();
        }
        self.keys.push_back(key);
    }

    fn random_recent<R: Rng>(&self, rng: &mut R) -> Option<WrittenKey> {
        if self.keys.is_empty() {
            return None;
        }
        let idx = rng.gen_range(0..self.keys.len());
        self.keys.get(idx).cloned()
    }
}

#[derive(Default)]
struct Stats {
    reads_ok: AtomicU64,
    reads_failed: AtomicU64,
    writes_ok: AtomicU64,
    writes_failed: AtomicU64,
    successes_since_log: AtomicU64,
    last_log_successes: AtomicU64,
    last_log_timestamp: AtomicU64,
}

#[derive(Clone)]
struct AppState {
    prometheus: PrometheusHandle,
    last_success_timestamp: Arc<AtomicU64>,
}

struct CassandraStatements {
    insert: PreparedStatement,
    select: PreparedStatement,
}

#[derive(Error, Debug)]
enum ConfigError {
    #[error("{name} must be between {min} and {max}, got {value}")]
    Range {
        name: &'static str,
        min: u64,
        max: u64,
        value: u64,
    },
}

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
        app_rps = config.app_rps,
        workers = config.workers,
        "starting mini cassandra load generator"
    );

    let prometheus = PrometheusBuilder::new()
        .install_recorder()
        .context("failed to install prometheus recorder")?;

    let session = Arc::new(connect(&config).await?);
    counter!("miniapp_cassandra_reconnects_total").increment(1);

    ensure_schema(&session, &config).await?;
    let statements = Arc::new(prepare_statements(&session, &config).await?);
    let ring = Arc::new(Mutex::new(KeyRing::new(ring_capacity(&config))));
    let stats = Arc::new(Stats::default());
    stats
        .last_log_timestamp
        .store(unix_timestamp(), Ordering::Relaxed);

    let last_success_timestamp = Arc::new(AtomicU64::new(0));
    let app_state = AppState {
        prometheus,
        last_success_timestamp: Arc::clone(&last_success_timestamp),
    };

    let mut workers = JoinSet::new();
    for worker_id in 0..config.workers {
        workers.spawn(worker_loop(
            worker_id,
            Arc::clone(&session),
            Arc::clone(&statements),
            Arc::clone(&ring),
            Arc::clone(&stats),
            Arc::clone(&last_success_timestamp),
            config.clone(),
        ));
    }

    let server = tokio::spawn(http_server(app_state, config.metrics_addr));

    shutdown_signal().await;
    info!("shutdown signal received");

    workers.abort_all();
    while workers.join_next().await.is_some() {}

    server.abort();
    let _ = server.await;

    info!("shutdown complete");
    Ok(())
}

fn init_logging() {
    let filter = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .with_span_list(false)
        .init();
}

impl Config {
    fn from_env() -> Result<Self> {
        let contact_points_raw = env_string("CASSANDRA_CONTACT_POINTS", "127.0.0.1:9042");
        let contact_points = contact_points_raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if contact_points.is_empty() {
            bail!("CASSANDRA_CONTACT_POINTS must contain at least one host:port");
        }

        let consistency_name = env_string("CASSANDRA_CONSISTENCY", "LOCAL_QUORUM").to_uppercase();
        let consistency = match consistency_name.as_str() {
            "ONE" => Consistency::One,
            "LOCAL_ONE" => Consistency::LocalOne,
            "QUORUM" => Consistency::Quorum,
            "LOCAL_QUORUM" => Consistency::LocalQuorum,
            other => bail!("unsupported CASSANDRA_CONSISTENCY={other}; use ONE, LOCAL_ONE, QUORUM, or LOCAL_QUORUM"),
        };

        let tls_enabled = env_bool("CASSANDRA_TLS_ENABLED", false)?;
        if tls_enabled {
            bail!("CASSANDRA_TLS_ENABLED=true needs CA/cert settings; this mini-app only supports plain TCP");
        }

        let read_ratio = env_u32("APP_READ_RATIO", 70)?;
        let write_ratio = env_u32("APP_WRITE_RATIO", 30)?;
        if read_ratio + write_ratio == 0 {
            bail!("APP_READ_RATIO + APP_WRITE_RATIO must be greater than 0");
        }

        let app_rps = env_u64("APP_RPS", 1000)?;
        validate_range("APP_RPS", app_rps, 1, 1_000_000)?;

        let workers = env_usize("APP_WORKERS", 32)?;
        validate_range("APP_WORKERS", workers as u64, 1, 10_000)?;

        let buckets = env_usize("APP_BUCKETS", 256)?;
        validate_range("APP_BUCKETS", buckets as u64, 1, 1_000_000)?;

        let payload_bytes = env_usize("APP_PAYLOAD_BYTES", 4096)?;
        validate_range(
            "APP_PAYLOAD_BYTES",
            payload_bytes as u64,
            1,
            10 * 1024 * 1024,
        )?;

        Ok(Self {
            contact_points,
            local_dc: env_string("CASSANDRA_LOCAL_DC", "dc1"),
            keyspace: env_string("CASSANDRA_KEYSPACE", "zdm_test"),
            username: env_optional("CASSANDRA_USERNAME"),
            password: env_optional("CASSANDRA_PASSWORD"),
            consistency,
            consistency_name,
            tls_enabled,
            app_rps,
            read_ratio,
            write_ratio,
            payload_bytes,
            workers,
            buckets,
            metrics_addr: env_string("APP_METRICS_ADDR", "0.0.0.0:8080")
                .parse()
                .context("APP_METRICS_ADDR must be a socket address")?,
            log_every_n_success: env_u64("APP_LOG_EVERY_N_SUCCESS", 1000)?,
            writer_id: env_string("HOSTNAME", "local"),
        })
    }
}

async fn connect(config: &Config) -> Result<Session> {
    let load_balancing_policy = DefaultPolicy::builder()
        .prefer_datacenter(config.local_dc.clone())
        .token_aware(true)
        .build();
    let execution_profile = ExecutionProfile::builder()
        .load_balancing_policy(load_balancing_policy)
        .build()
        .into_handle();

    let mut builder = SessionBuilder::new().default_execution_profile_handle(execution_profile);
    for contact_point in &config.contact_points {
        builder = builder.known_node(contact_point);
    }
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        builder = builder.user(username, password);
    }
    builder.build().await.with_context(|| {
        format!(
            "failed to connect to Cassandra at {}",
            config.contact_points.join(",")
        )
    })
}

async fn ensure_schema(session: &Session, config: &Config) -> Result<()> {
    let create_keyspace = format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'NetworkTopologyStrategy', '{}': 3}}",
        config.keyspace, config.local_dc
    );
    session
        .query_unpaged(create_keyspace, &[])
        .await
        .context("failed to create keyspace")?;

    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {}.events (
            bucket text,
            id uuid,
            created_at timestamp,
            payload text,
            writer_id text,
            version int,
            PRIMARY KEY ((bucket), id)
        )",
        config.keyspace
    );
    session
        .query_unpaged(create_table, &[])
        .await
        .context("failed to create events table")?;
    Ok(())
}

async fn prepare_statements(session: &Session, config: &Config) -> Result<CassandraStatements> {
    let insert_cql = format!(
        "INSERT INTO {}.events (bucket, id, created_at, payload, writer_id, version) VALUES (?, ?, toTimestamp(now()), ?, ?, ?)",
        config.keyspace
    );
    let select_cql = format!(
        "SELECT bucket, id, created_at, payload, writer_id, version FROM {}.events WHERE bucket = ? AND id = ?",
        config.keyspace
    );

    let mut insert = session
        .prepare(insert_cql)
        .await
        .context("failed to prepare insert")?;
    insert.set_consistency(config.consistency);

    let mut select = session
        .prepare(select_cql)
        .await
        .context("failed to prepare select")?;
    select.set_consistency(config.consistency);

    Ok(CassandraStatements { insert, select })
}

async fn worker_loop(
    worker_id: usize,
    session: Arc<Session>,
    statements: Arc<CassandraStatements>,
    ring: Arc<Mutex<KeyRing>>,
    stats: Arc<Stats>,
    last_success_timestamp: Arc<AtomicU64>,
    config: Config,
) {
    let mut rng = SmallRng::from_entropy();
    let delay = worker_delay(&config);
    let mut ticker = time::interval(delay);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    info!(worker_id, delay_ms = delay.as_millis(), "worker started");

    loop {
        ticker.tick().await;
        let read_threshold = config.read_ratio;
        let ratio_total = config.read_ratio + config.write_ratio;
        let dice = rng.gen_range(0..ratio_total);
        if dice < read_threshold {
            run_read(
                &session,
                &statements.select,
                &ring,
                &stats,
                &last_success_timestamp,
                &config,
                &mut rng,
            )
            .await;
        } else {
            run_write(
                &session,
                &statements.insert,
                &ring,
                &stats,
                &last_success_timestamp,
                &config,
                &mut rng,
            )
            .await;
        }
    }
}

async fn run_write<R: Rng>(
    session: &Session,
    statement: &PreparedStatement,
    ring: &Arc<Mutex<KeyRing>>,
    stats: &Stats,
    last_success_timestamp: &AtomicU64,
    config: &Config,
    rng: &mut R,
) {
    let bucket = format!("bucket_{}", rng.gen_range(0..config.buckets));
    let id = Uuid::new_v4();
    let payload = random_payload(config.payload_bytes, rng);
    let started = Instant::now();

    gauge!("miniapp_inflight_operations").increment(1.0);
    let result = session
        .execute_unpaged(
            statement,
            (
                bucket.as_str(),
                id,
                payload.as_str(),
                config.writer_id.as_str(),
                1_i32,
            ),
        )
        .await;
    gauge!("miniapp_inflight_operations").decrement(1.0);

    let latency = started.elapsed();
    histogram!("miniapp_operation_latency_seconds", "operation" => "write")
        .record(latency.as_secs_f64());

    match result {
        Ok(_) => {
            counter!("miniapp_writes_total").increment(1);
            stats.writes_ok.fetch_add(1, Ordering::Relaxed);
            mark_success(last_success_timestamp);
            ring.lock().await.push(WrittenKey { bucket, id });
            maybe_log_success(stats, config);
        }
        Err(err) => {
            counter!("miniapp_write_errors_total").increment(1);
            stats.writes_failed.fetch_add(1, Ordering::Relaxed);
            log_cassandra_error("write", &err, latency, config);
        }
    }
}

async fn run_read<R: Rng>(
    session: &Session,
    statement: &PreparedStatement,
    ring: &Arc<Mutex<KeyRing>>,
    stats: &Stats,
    last_success_timestamp: &AtomicU64,
    config: &Config,
    rng: &mut R,
) {
    let key = {
        let ring = ring.lock().await;
        if rng.gen_bool(0.70) {
            ring.random_recent(rng)
        } else {
            None
        }
    }
    .unwrap_or_else(|| WrittenKey {
        bucket: format!("bucket_{}", rng.gen_range(0..config.buckets)),
        id: Uuid::new_v4(),
    });

    let started = Instant::now();
    gauge!("miniapp_inflight_operations").increment(1.0);
    let result = session
        .execute_unpaged(statement, (key.bucket.as_str(), key.id))
        .await;
    gauge!("miniapp_inflight_operations").decrement(1.0);

    let latency = started.elapsed();
    histogram!("miniapp_operation_latency_seconds", "operation" => "read")
        .record(latency.as_secs_f64());

    match result {
        Ok(_) => {
            counter!("miniapp_reads_total").increment(1);
            stats.reads_ok.fetch_add(1, Ordering::Relaxed);
            mark_success(last_success_timestamp);
            maybe_log_success(stats, config);
        }
        Err(err) => {
            counter!("miniapp_read_errors_total").increment(1);
            stats.reads_failed.fetch_add(1, Ordering::Relaxed);
            log_cassandra_error("read", &err, latency, config);
        }
    }
}

async fn http_server(state: AppState, addr: SocketAddr) -> Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state);

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind HTTP server to {addr}"))?;
    info!(%addr, "http server started");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("http server failed")
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn readyz(State(state): State<AppState>) -> Response {
    let last_success = state.last_success_timestamp.load(Ordering::Relaxed);
    let now = unix_timestamp();
    if last_success > 0 && now.saturating_sub(last_success) <= READY_MAX_AGE.as_secs() {
        "ready\n".into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "cassandra operations are stale\n",
        )
            .into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> String {
    state.prometheus.render()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "failed to install ctrl-c handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => warn!(error = %err, "failed to install sigterm handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn random_payload<R: Rng>(size: usize, rng: &mut R) -> String {
    (0..size)
        .map(|_| char::from(rng.sample(Alphanumeric)))
        .collect()
}

fn mark_success(last_success_timestamp: &AtomicU64) {
    let now = unix_timestamp();
    last_success_timestamp.store(now, Ordering::Relaxed);
    gauge!("miniapp_last_success_timestamp").set(now as f64);
}

fn maybe_log_success(stats: &Stats, config: &Config) {
    if config.log_every_n_success == 0 {
        return;
    }
    let successes = stats.successes_since_log.fetch_add(1, Ordering::Relaxed) + 1;
    if !successes.is_multiple_of(config.log_every_n_success) {
        return;
    }

    let now = unix_timestamp();
    let previous_successes = stats.last_log_successes.swap(successes, Ordering::Relaxed);
    let previous_ts = stats.last_log_timestamp.swap(now, Ordering::Relaxed);
    let elapsed = now.saturating_sub(previous_ts).max(1);
    let current_rps = (successes.saturating_sub(previous_successes)) as f64 / elapsed as f64;

    info!(
        reads_ok = stats.reads_ok.load(Ordering::Relaxed),
        reads_failed = stats.reads_failed.load(Ordering::Relaxed),
        writes_ok = stats.writes_ok.load(Ordering::Relaxed),
        writes_failed = stats.writes_failed.load(Ordering::Relaxed),
        current_rps,
        "operation stats"
    );
}

fn log_cassandra_error<E: std::error::Error>(
    operation: &str,
    err: &E,
    latency: Duration,
    config: &Config,
) {
    error!(
        operation,
        consistency = %config.consistency_name,
        error_type = std::any::type_name::<E>(),
        error = %err,
        latency_ms = latency.as_secs_f64() * 1000.0,
        contact_points = %config.contact_points.join(","),
        keyspace = %config.keyspace,
        timestamp = unix_timestamp(),
        "cassandra operation failed"
    );
}

fn worker_delay(config: &Config) -> Duration {
    let secs = config.workers as f64 / config.app_rps as f64;
    Duration::from_secs_f64(secs.max(0.001))
}

fn ring_capacity(config: &Config) -> usize {
    (config.app_rps as usize)
        .saturating_mul(60)
        .clamp(4096, 1_000_000)
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_optional(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => match value.to_lowercase().as_str() {
            "true" | "1" | "yes" | "y" => Ok(true),
            "false" | "0" | "no" | "n" => Ok(false),
            _ => Err(anyhow!("{name} must be true or false")),
        },
        Err(_) => Ok(default),
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn env_u32(name: &str, default: u32) -> Result<u32> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse()
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn validate_range(name: &'static str, value: u64, min: u64, max: u64) -> Result<()> {
    if !(min..=max).contains(&value) {
        return Err(ConfigError::Range {
            name,
            min,
            max,
            value,
        }
        .into());
    }
    Ok(())
}
