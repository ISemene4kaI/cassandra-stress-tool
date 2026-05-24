use std::time::Duration;

use anyhow::{Context, Result};
use metrics::{counter, gauge};
use scylla::{
    load_balancing::DefaultPolicy, prepared_statement::PreparedStatement,
    transport::execution_profile::ExecutionProfile, transport::session::Session,
    transport::session_builder::SessionBuilder,
};
use tokio::time;
use tracing::{error, info};

use crate::{
    config::Config,
    state::{
        mark_cassandra_ready, set_unready_reason, RuntimeState, REASON_CONNECT_FAILED,
        REASON_PREPARE_FAILED, REASON_SCHEMA_CHECK_FAILED,
    },
};

const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);

pub struct CassandraClient {
    pub session: Session,
    pub statements: CassandraStatements,
}

pub struct CassandraStatements {
    pub insert: PreparedStatement,
    pub select: PreparedStatement,
}

#[derive(Debug)]
pub struct ClientInitError {
    pub reason: &'static str,
    pub source: anyhow::Error,
}

pub async fn reconnect_loop(state: RuntimeState, config: Config) {
    let mut backoff = RECONNECT_INITIAL_BACKOFF;

    loop {
        if state.cassandra.read().await.is_some() {
            time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        counter!("miniapp_cassandra_connect_attempts_total").increment(1);
        match build_cassandra_client(&config).await {
            Ok(client) => {
                counter!("miniapp_cassandra_connects_total").increment(1);
                mark_cassandra_ready(&state, client).await;
                backoff = RECONNECT_INITIAL_BACKOFF;
                info!("cassandra client ready");
            }
            Err(err) => {
                gauge!("miniapp_cassandra_ready").set(0.0);
                set_unready_reason(&state, err.reason).await;
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
        reason: REASON_CONNECT_FAILED,
        source,
    })?;
    if config.create_schema {
        create_schema(&session, config)
            .await
            .map_err(|source| ClientInitError {
                reason: REASON_SCHEMA_CHECK_FAILED,
                source,
            })?;
    } else {
        check_schema(&session, config)
            .await
            .map_err(|source| ClientInitError {
                reason: REASON_SCHEMA_CHECK_FAILED,
                source,
            })?;
    }
    let statements = prepare_statements(&session, config)
        .await
        .map_err(|source| ClientInitError {
            reason: REASON_PREPARE_FAILED,
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
