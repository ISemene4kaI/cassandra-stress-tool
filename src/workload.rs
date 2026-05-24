use std::{
    collections::VecDeque,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use metrics::{counter, gauge, histogram};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use scylla::frame::value::CqlTimestamp;
use tokio::time;
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    cassandra::CassandraClient,
    config::Config,
    state::{mark_cassandra_unready, mark_success, RuntimeState, Stats, REASON_TOO_MANY_ERRORS},
    util::{random_payload, unix_timestamp, unix_timestamp_millis},
};

#[derive(Clone, Debug)]
pub struct WrittenKey {
    bucket: String,
    id: Uuid,
}

#[derive(Debug)]
pub struct KeyRing {
    keys: VecDeque<WrittenKey>,
    capacity: usize,
}

impl KeyRing {
    pub fn new(capacity: usize) -> Self {
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

pub async fn worker_loop(worker_id: usize, state: RuntimeState, config: Config) {
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
    let errors = state.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
    if config.reconnect_after_consecutive_errors == 0 {
        return;
    }

    if errors >= config.reconnect_after_consecutive_errors {
        error!(
            consecutive_errors = errors,
            threshold = config.reconnect_after_consecutive_errors,
            operation,
            contact_points = %config.contact_points.join(","),
            "consecutive Cassandra operation error threshold reached; resetting session"
        );
        mark_cassandra_unready(state, REASON_TOO_MANY_ERRORS).await;
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
