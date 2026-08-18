use std::{sync::Arc, time::Duration};

use reqwest::Client;
use serde::Deserialize;
use sqlx::SqlitePool;
use tokio::{sync::Notify, time::sleep};

use crate::{
    config::Config,
    db,
    models::{JobProgress, LiveProgress},
};

pub async fn run(
    config: Arc<Config>,
    pool: SqlitePool,
    jobs_changed: Arc<Notify>,
    progress: JobProgress,
) {
    match db::reset_processing(&pool).await {
        Ok(count) if count > 0 => tracing::warn!(count, "returned interrupted jobs to the queue"),
        Ok(_) => {}
        Err(error) => tracing::error!(?error, "could not reset interrupted jobs"),
    }

    let client = match Client::builder().timeout(Duration::from_secs(2)).build() {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(?error, "could not create WhisperX progress client");
            return;
        }
    };
    let mut active_job_id: Option<String> = None;

    loop {
        match db::expire_uploads(&pool, unix_timestamp()).await {
            Ok(count) if count > 0 => {
                tracing::warn!(count, "expired abandoned audio uploads");
                jobs_changed.notify_waiters();
            }
            Ok(_) => {}
            Err(error) => tracing::error!(?error, "could not expire audio uploads"),
        }

        match db::find_processing(&pool).await {
            Ok(Some(job)) => {
                if active_job_id.as_deref() != Some(&job.job_id)
                    && let Some(previous) = active_job_id.replace(job.job_id.clone())
                {
                    progress.clear(&previous);
                }
                if let Some(snapshot) = fetch_whisperx_progress(&config, &client, &job.job_id).await
                {
                    progress.set(&job.job_id, snapshot);
                    jobs_changed.notify_waiters();
                }
            }
            Ok(None) => {
                if let Some(previous) = active_job_id.take() {
                    progress.clear(&previous);
                    jobs_changed.notify_waiters();
                }
            }
            Err(error) => tracing::error!(?error, "could not find a processing job"),
        }
        sleep(Duration::from_millis(500)).await;
    }
}

#[derive(Debug, Deserialize)]
struct WhisperxProgress {
    busy: bool,
    job_id: Option<String>,
    stage: String,
    progress_percent: u8,
    message: String,
    language: Option<String>,
    audio_seconds: Option<f64>,
    elapsed_ms: Option<u64>,
}

async fn fetch_whisperx_progress(
    config: &Config,
    client: &Client,
    expected_job_id: &str,
) -> Option<LiveProgress> {
    let response = client
        .get(format!("{}/v1/progress", config.whisperx_base_url))
        .bearer_auth(&config.whisperx_api_key)
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let payload: WhisperxProgress = response.json().await.ok()?;
    if !payload.busy || payload.job_id.as_deref() != Some(expected_job_id) {
        return None;
    }
    Some(LiveProgress {
        stage: Some(payload.stage),
        progress_percent: Some(payload.progress_percent),
        message: Some(payload.message),
        language: payload.language.filter(|value| !value.is_empty()),
        audio_seconds: payload.audio_seconds,
        elapsed_ms: payload.elapsed_ms,
    })
}

fn unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
