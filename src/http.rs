use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    state::{set_reason, AppState, REASON_OPERATIONS_STALE},
    util::{shutdown_signal, unix_timestamp},
};

pub async fn http_server(state: AppState, addr: SocketAddr) -> Result<()> {
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

    let last_success = state
        .last_success_timestamp
        .load(std::sync::atomic::Ordering::Relaxed);
    let now = unix_timestamp();
    if last_success > 0 && now.saturating_sub(last_success) <= state.ready_max_age.as_secs() {
        "ready\n".into_response()
    } else {
        set_reason(&state.last_unready_reason, REASON_OPERATIONS_STALE).await;
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
