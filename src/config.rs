use std::{env, net::SocketAddr, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use scylla::statement::Consistency;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct Config {
    pub contact_points: Vec<String>,
    pub local_dc: String,
    pub keyspace: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub consistency: Consistency,
    pub consistency_name: String,
    pub tls_enabled: bool,
    pub create_schema: bool,
    pub rps_per_pod: u64,
    pub read_ratio: u32,
    pub write_ratio: u32,
    pub payload_bytes: usize,
    pub workers: usize,
    pub buckets: usize,
    pub historical_read_enabled: bool,
    pub historical_buckets: usize,
    pub reconnect_after_consecutive_errors: u64,
    pub ready_max_age: Duration,
    pub metrics_addr: SocketAddr,
    pub log_every_n_success: u64,
    pub writer_id: String,
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

impl Config {
    pub fn from_env() -> Result<Self> {
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
