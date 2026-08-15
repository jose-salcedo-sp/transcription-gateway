mod api;
mod config;
mod db;
mod error;
mod models;
mod worker;

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::{net::TcpListener, signal, sync::Notify};

use crate::{
    api::{AppState, router},
    config::Config,
    models::JobProgress,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let config = Arc::new(Config::from_env()?);
    db::prepare_data_dirs(&config.data_dir).await?;
    let pool = db::connect(&config.database_url).await?;
    let jobs_changed = Arc::new(Notify::new());
    let progress = JobProgress::default();

    tokio::spawn(worker::run(
        Arc::clone(&config),
        pool.clone(),
        Arc::clone(&jobs_changed),
        progress.clone(),
    ));

    let state = AppState {
        config: Arc::clone(&config),
        pool,
        jobs_changed,
        progress,
    };
    let listener = TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("cannot bind {}", config.bind))?;
    tracing::info!(address = %config.bind, "transcription gateway ready");

    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("gateway server failed")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("cannot install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("cannot install termination handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
