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
    frame::value::CqlTimestamp, load_balancing::DefaultPolicy,
    prepared_statement::PreparedStatement, statement::Consistency,
    transport::execution_profile::ExecutionProfile, transport::session::Session,
    transport::session_builder::SessionBuilder,
};
use thiserror::Error;
use tokio::{net::TcpListener, sync::RwLock, task::JoinSet, time};
use tracing::{error, info, warn};
use uuid::Uuid;

const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);

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
    create_schema: bool,
    rps_per_pod: u64,
    read_ratio: u32,
    write_ratio: u32,
    payload_bytes: usize,
    workers: usize,
    buckets: usize,
    historical_read_enabled: bool,
    historical_buckets: usize,
    reconnect_after_consecutive_errors: u64,
    ready_max_age: Duration,
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
    reads_found: AtomicU64,
    reads_empty: AtomicU64,
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
    cassandra: Arc<RwLock<Option<Arc<CassandraClient>>>>,
    last_success_timestamp: Arc<AtomicU64>,
    last_unready_reason: Arc<RwLock<String>>,
    ready_max_age: Duration,
}

#[derive(Clone)]
struct RuntimeState {
    cassandra: Arc<RwLock<Option<Arc<CassandraClient>>>>,
    ring: Arc<RwLock<KeyRing>>,
    stats: Arc<Stats>,
    last_success_timestamp: Arc<AtomicU64>,
    consecutive_errors: Arc<AtomicU64>,
    last_unready_reason: Arc<RwLock<String>>,
}

struct CassandraClient {
    session: Session,
    statements: CassandraStatements,
}

struct CassandraStatements {
    insert: PreparedStatement,
    select: PreparedStatement,
}

#[derive(Debug)]
struct ClientInitError {
    reason: &'static str,
    source: anyhow::Error,
}

#[derive(Clone, Copy)]
enum ReadSource {
    Recent,
    RandomMissProbe,
}

impl ReadSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Recent => "recent",
            Self::RandomMissProbe => "random_miss_probe",
        }
    }
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

    let prometheus = PrometheusBuilder::new()
        .install_recorder()
        .context("failed to install prometheus recorder")?;
    let metrics_upkeep = tokio::spawn(metrics_upkeep_loop(prometheus.clone()));

    let cassandra = Arc::new(RwLock::new(None));
    let ring = Arc::new(RwLock::new(KeyRing::new(ring_capacity(&config))));
    let stats = Arc::new(Stats::default());
    let consecutive_errors = Arc::new(AtomicU64::new(0));
    stats
        .last_log_timestamp
        .store(unix_timestamp(), Ordering::Relaxed);

    let last_success_timestamp = Arc::new(AtomicU64::new(0));
    let last_unready_reason = Arc::new(RwLock::new("cassandra_client_not_ready".to_string()));
    gauge!("miniapp_last_success_timestamp").set(0.0);
    gauge!("miniapp_cassandra_ready").set(0.0);

    let runtime_state = RuntimeState {
        cassandra: Arc::clone(&cassandra),
        ring: Arc::clone(&ring),
        stats: Arc::clone(&stats),
        last_success_timestamp: Arc::clone(&last_success_timestamp),
        consecutive_errors: Arc::clone(&consecutive_errors),
        last_unready_reason: Arc::clone(&last_unready_reason),
    };

    let app_state = AppState {
        prometheus,
        cassandra: Arc::clone(&cassandra),
        last_success_timestamp: Arc::clone(&last_success_timestamp),
        last_unready_reason: Arc::clone(&last_unready_reason),
        ready_max_age: config.ready_max_age,
    };

    let server = tokio::spawn(http_server(app_state, config.metrics_addr));
    let reconnect = tokio::spawn(reconnect_loop(
        Arc::clone(&cassandra),
        Arc::clone(&last_unready_reason),
        Arc::clone(&consecutive_errors),
        config.clone(),
    ));

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

        let rps_per_pod = env_u64_with_fallback("APP_RPS_PER_POD", "APP_RPS", 1000)?;
        validate_range("APP_RPS_PER_POD", rps_per_pod, 1, 1_000_000)?;

        let workers = env_usize("APP_WORKERS", 32)?;
        validate_range("APP_WORKERS", workers as u64, 1, 10_000)?;

        let buckets = env_usize("APP_BUCKETS", 256)?;
        validate_range("APP_BUCKETS", buckets as u64, 1, 1_000_000)?;

        let historical_buckets = env_usize("APP_HISTORICAL_BUCKETS", buckets)?;
        validate_range(
            "APP_HISTORICAL_BUCKETS",
            historical_buckets as u64,
            1,
            1_000_000,
        )?;

        let payload_bytes = env_usize("APP_PAYLOAD_BYTES", 4096)?;
        validate_range(
            "APP_PAYLOAD_BYTES",
            payload_bytes as u64,
            1,
            10 * 1024 * 1024,
        )?;

        let reconnect_after_consecutive_errors =
            env_u64("APP_RECONNECT_AFTER_CONSECUTIVE_ERRORS", 10)?;
        validate_range(
            "APP_RECONNECT_AFTER_CONSECUTIVE_ERRORS",
            reconnect_after_consecutive_errors,
            0,
            1_000_000,
        )?;

        let ready_max_age_seconds = env_u64("APP_READY_MAX_AGE_SECONDS", 30)?;
        validate_range("APP_READY_MAX_AGE_SECONDS", ready_max_age_seconds, 1, 3600)?;

        Ok(Self {
            contact_points,
            local_dc: env_string("CASSANDRA_LOCAL_DC", "dc1"),
            keyspace: env_string("CASSANDRA_KEYSPACE", "zdm_test"),
            username: env_optional("CASSANDRA_USERNAME"),
            password: env_optional("CASSANDRA_PASSWORD"),
            consistency,
            consistency_name,
            tls_enabled,
            create_schema: env_bool("APP_CREATE_SCHEMA", false)?,
            rps_per_pod,
            read_ratio,
            write_ratio,
            payload_bytes,
            workers,
            buckets,
            historical_read_enabled: env_bool("APP_HISTORICAL_READ_ENABLED", false)?,
            historical_buckets,
            reconnect_after_consecutive_errors,
            ready_max_age: Duration::from_secs(ready_max_age_seconds),
            metrics_addr: env_string("APP_METRICS_ADDR", "0.0.0.0:8080")
                .parse()
                .context("APP_METRICS_ADDR must be a socket address")?,
            log_every_n_success: env_u64("APP_LOG_EVERY_N_SUCCESS", 1000)?,
            writer_id: env_string("HOSTNAME", "local"),
        })
    }
}

async fn reconnect_loop(
    cassandra: Arc<RwLock<Option<Arc<CassandraClient>>>>,
    last_unready_reason: Arc<RwLock<String>>,
    consecutive_errors: Arc<AtomicU64>,
    config: Config,
) {
    let mut backoff = RECONNECT_INITIAL_BACKOFF;

    loop {
        if cassandra.read().await.is_some() {
            time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        counter!("miniapp_cassandra_connect_attempts_total").increment(1);
        match build_cassandra_client(&config).await {
            Ok(client) => {
                counter!("miniapp_cassandra_connects_total").increment(1);
                gauge!("miniapp_cassandra_ready").set(1.0);
                consecutive_errors.store(0, Ordering::Relaxed);
                *cassandra.write().await = Some(Arc::new(client));
                backoff = RECONNECT_INITIAL_BACKOFF;
                info!("cassandra client ready");
            }
            Err(err) => {
                gauge!("miniapp_cassandra_ready").set(0.0);
                *last_unready_reason.write().await = err.reason.to_string();
                error!(
                    reason = err.reason,
                    error = %err.source,
                    contact_points = %config.contact_points.join(","),
                    keyspace = %config.keyspace,
                    backoff_ms = backoff.as_millis(),
                    "cassandra connect/schema/prepare failed; will retry"
                );
                time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
            }
        }
    }
}

async fn build_cassandra_client(config: &Config) -> Result<CassandraClient, ClientInitError> {
    let session = connect(config).await.map_err(|source| ClientInitError {
        reason: "connect_failed",
        source,
    })?;
    if config.create_schema {
        create_schema(&session, config)
            .await
            .map_err(|source| ClientInitError {
                reason: "schema_check_failed",
                source,
            })?;
    } else {
        check_schema(&session, config)
            .await
            .map_err(|source| ClientInitError {
                reason: "schema_check_failed",
                source,
            })?;
    }
    let statements = prepare_statements(&session, config)
        .await
        .map_err(|source| ClientInitError {
            reason: "prepare_failed",
            source,
        })?;
    Ok(CassandraClient {
        session,
        statements,
    })
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

async fn create_schema(session: &Session, config: &Config) -> Result<()> {
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

async fn check_schema(session: &Session, config: &Config) -> Result<()> {
    let check_cql = format!("SELECT bucket, id FROM {}.events LIMIT 1", config.keyspace);
    session.query_unpaged(check_cql, &[]).await.context(
        "schema check failed; apply keyspace/table separately or set APP_CREATE_SCHEMA=true",
    )?;
    Ok(())
}

async fn prepare_statements(session: &Session, config: &Config) -> Result<CassandraStatements> {
    let insert_cql = format!(
        "INSERT INTO {}.events (bucket, id, created_at, payload, writer_id, version) VALUES (?, ?, ?, ?, ?, ?)",
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

async fn worker_loop(worker_id: usize, state: RuntimeState, config: Config) {
    let mut rng = SmallRng::from_entropy();
    let delay = worker_delay(&config);
    let mut ticker = time::interval(delay);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    info!(worker_id, delay_ms = delay.as_millis(), "worker started");

    loop {
        ticker.tick().await;
        let Some(client) = state.cassandra.read().await.clone() else {
            continue;
        };

        let read_threshold = config.read_ratio;
        let ratio_total = config.read_ratio + config.write_ratio;
        let dice = rng.gen_range(0..ratio_total);
        if dice < read_threshold {
            run_read(&client, &state, &config, &mut rng).await;
        } else {
            run_write(&client, &state, &config, &mut rng).await;
        }
    }
}

async fn run_write<R: Rng>(
    client: &CassandraClient,
    state: &RuntimeState,
    config: &Config,
    rng: &mut R,
) {
    let bucket = format!("bucket_{}", rng.gen_range(0..config.buckets));
    let id = Uuid::new_v4();
    let created_at = CqlTimestamp(unix_timestamp_millis());
    let payload = random_payload(config.payload_bytes, rng);
    let started = Instant::now();

    gauge!("miniapp_inflight_operations").increment(1.0);
    let result = client
        .session
        .execute_unpaged(
            &client.statements.insert,
            (
                bucket.as_str(),
                id,
                created_at,
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
            state.stats.writes_ok.fetch_add(1, Ordering::Relaxed);
            mark_success(&state.last_success_timestamp, &state.consecutive_errors);
            state.ring.write().await.push(WrittenKey { bucket, id });
            maybe_log_success(&state.stats, config);
        }
        Err(err) => {
            counter!("miniapp_write_errors_total").increment(1);
            state.stats.writes_failed.fetch_add(1, Ordering::Relaxed);
            log_cassandra_error("write", &err, latency, config);
            handle_operation_error("write", state, config).await;
        }
    }
}

async fn run_read<R: Rng>(
    client: &CassandraClient,
    state: &RuntimeState,
    config: &Config,
    rng: &mut R,
) {
    let (key, source) = {
        let ring = state.ring.read().await;
        if rng.gen_bool(0.70) {
            match ring.random_recent(rng) {
                Some(key) => (key, ReadSource::Recent),
                None => (random_probe_key(config, rng), ReadSource::RandomMissProbe),
            }
        } else {
            (random_probe_key(config, rng), ReadSource::RandomMissProbe)
        }
    };

    let started = Instant::now();
    gauge!("miniapp_inflight_operations").increment(1.0);
    let result = client
        .session
        .execute_unpaged(&client.statements.select, (key.bucket.as_str(), key.id))
        .await;
    gauge!("miniapp_inflight_operations").decrement(1.0);

    let latency = started.elapsed();
    histogram!("miniapp_operation_latency_seconds", "operation" => "read")
        .record(latency.as_secs_f64());

    match result {
        Ok(result) => {
            let rows = result.rows_num().unwrap_or(0);
            if rows > 0 {
                counter!("miniapp_reads_total", "read_source" => source.as_str()).increment(1);
                counter!("miniapp_reads_found_total", "read_source" => source.as_str())
                    .increment(1);
                state.stats.reads_found.fetch_add(1, Ordering::Relaxed);
                mark_success(&state.last_success_timestamp, &state.consecutive_errors);
                maybe_log_success(&state.stats, config);
            } else {
                counter!("miniapp_reads_empty_total", "read_source" => source.as_str())
                    .increment(1);
                state.stats.reads_empty.fetch_add(1, Ordering::Relaxed);

                if matches!(source, ReadSource::Recent) {
                    counter!("miniapp_read_errors_total", "read_source" => source.as_str())
                        .increment(1);
                    state.stats.reads_failed.fetch_add(1, Ordering::Relaxed);
                    error!(
                        operation = "read",
                        read_source = source.as_str(),
                        consistency = %config.consistency_name,
                        bucket = %key.bucket,
                        id = %key.id,
                        latency_ms = latency.as_secs_f64() * 1000.0,
                        contact_points = %config.contact_points.join(","),
                        keyspace = %config.keyspace,
                        timestamp = unix_timestamp(),
                        "recently written key returned empty result"
                    );
                } else {
                    mark_success(&state.last_success_timestamp, &state.consecutive_errors);
                    maybe_log_success(&state.stats, config);
                }
            }
        }
        Err(err) => {
            counter!("miniapp_read_errors_total", "read_source" => source.as_str()).increment(1);
            state.stats.reads_failed.fetch_add(1, Ordering::Relaxed);
            log_cassandra_error("read", &err, latency, config);
            handle_operation_error("read", state, config).await;
        }
    }
}

async fn handle_operation_error(operation: &str, state: &RuntimeState, config: &Config) {
    if config.reconnect_after_consecutive_errors == 0 {
        return;
    }

    let errors = state.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
    if errors >= config.reconnect_after_consecutive_errors {
        error!(
            consecutive_errors = errors,
            threshold = config.reconnect_after_consecutive_errors,
            operation,
            contact_points = %config.contact_points.join(","),
            "consecutive Cassandra operation error threshold reached; resetting session"
        );
        mark_cassandra_unready(
            &state.cassandra,
            &state.last_unready_reason,
            "too_many_consecutive_errors",
        )
        .await;
    }
}

async fn mark_cassandra_unready(
    cassandra: &Arc<RwLock<Option<Arc<CassandraClient>>>>,
    last_unready_reason: &Arc<RwLock<String>>,
    reason: &str,
) {
    gauge!("miniapp_cassandra_ready").set(0.0);
    *last_unready_reason.write().await = reason.to_string();
    *cassandra.write().await = None;
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
    if state.cassandra.read().await.is_none() {
        let reason = state.last_unready_reason.read().await.clone();
        return (StatusCode::SERVICE_UNAVAILABLE, format!("{reason}\n")).into_response();
    }

    let last_success = state.last_success_timestamp.load(Ordering::Relaxed);
    let now = unix_timestamp();
    if last_success > 0 && now.saturating_sub(last_success) <= state.ready_max_age.as_secs() {
        "ready\n".into_response()
    } else {
        *state.last_unready_reason.write().await = "cassandra_operations_stale".to_string();
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "cassandra_operations_stale\n",
        )
            .into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> String {
    state.prometheus.render()
}

async fn metrics_upkeep_loop(handle: PrometheusHandle) {
    let mut ticker = time::interval(Duration::from_secs(15));
    loop {
        ticker.tick().await;
        // metrics-exporter-prometheus 0.13 does not expose a public upkeep method
        // on PrometheusHandle; rendering periodically drives the same snapshot path
        // used by /metrics and prunes idle data when that feature is configured.
        let _ = handle.render();
    }
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

fn random_probe_key<R: Rng>(config: &Config, rng: &mut R) -> WrittenKey {
    let bucket_count = if config.historical_read_enabled {
        config.historical_buckets
    } else {
        config.buckets
    };
    WrittenKey {
        bucket: format!("bucket_{}", rng.gen_range(0..bucket_count)),
        id: Uuid::new_v4(),
    }
}

fn random_payload<R: Rng>(size: usize, rng: &mut R) -> String {
    (0..size)
        .map(|_| char::from(rng.sample(Alphanumeric)))
        .collect()
}

fn mark_success(last_success_timestamp: &AtomicU64, consecutive_errors: &AtomicU64) {
    let now = unix_timestamp();
    consecutive_errors.store(0, Ordering::Relaxed);
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
        reads_found = stats.reads_found.load(Ordering::Relaxed),
        reads_empty = stats.reads_empty.load(Ordering::Relaxed),
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
    let secs = config.workers as f64 / config.rps_per_pod as f64;
    Duration::from_secs_f64(secs.max(0.001))
}

fn ring_capacity(config: &Config) -> usize {
    (config.rps_per_pod as usize)
        .saturating_mul(60)
        .clamp(4096, 1_000_000)
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn unix_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
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

fn env_u64_with_fallback(name: &str, fallback_name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an unsigned integer")),
        Err(_) => env_u64(fallback_name, default),
    }
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
