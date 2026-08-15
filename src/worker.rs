use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode, multipart};
use serde::Deserialize;
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::{fs::File, sync::Notify, time::sleep};
use tokio_util::io::ReaderStream;

use crate::{
    config::Config,
    db,
    models::{JobProgress, LiveProgress, Transcription},
};

const MAX_WHISPERX_ATTEMPTS: u32 = 5;

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

    let client = match Client::builder()
        .timeout(Duration::from_secs(60 * 60 * 2))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(?error, "could not create WhisperX client");
            return;
        }
    };

    loop {
        match db::claim_next(&pool).await {
            Ok(Some(job)) => {
                let job_id = job.job_id.clone();
                tracing::info!(%job_id, sha256 = %job.content_sha256, "processing transcription");
                jobs_changed.notify_waiters();
                let progress_task = tokio::spawn(watch_whisperx_progress(
                    Arc::clone(&config),
                    client.clone(),
                    job_id.clone(),
                    progress.clone(),
                    Arc::clone(&jobs_changed),
                ));
                match process_job(&config, &client, &job).await {
                    Ok((relative_transcript, duration, detected_language)) => {
                        if let Err(error) = db::mark_ready(
                            &pool,
                            &job_id,
                            &relative_transcript,
                            duration,
                            detected_language.as_deref(),
                        )
                        .await
                        {
                            tracing::error!(?error, %job_id, "could not mark job ready");
                        } else {
                            tracing::info!(%job_id, "transcription ready");
                        }
                    }
                    Err(error) => {
                        let message = truncate_error(&format!("{error:#}"));
                        tracing::error!(%job_id, error = %message, "transcription failed");
                        if let Err(db_error) = db::mark_failed(&pool, &job_id, &message).await {
                            tracing::error!(?db_error, %job_id, "could not mark job failed");
                        }
                    }
                }
                progress_task.abort();
                let _ = progress_task.await;
                progress.clear(&job_id);
                jobs_changed.notify_waiters();
            }
            Ok(None) => {
                tokio::select! {
                    _ = jobs_changed.notified() => {}
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
            Err(error) => {
                tracing::error!(?error, "could not claim a job");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn process_job(
    config: &Config,
    client: &Client,
    job: &Transcription,
) -> Result<(String, Option<f64>, Option<String>)> {
    let relative_audio = job.audio_path.as_deref().context("job has no audio path")?;
    let audio_path = config.data_dir.join(relative_audio);
    let transcript = call_whisperx(config, client, job, &audio_path).await?;
    let duration = transcript.get("duration").and_then(Value::as_f64);
    let detected_language = transcript
        .get("language")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let path_language = if job.requested_language.is_empty() {
        "auto"
    } else {
        &job.requested_language
    };
    let relative_transcript = format!(
        "transcripts/{}/{}/{}.json",
        job.content_sha256, job.model, path_language
    );
    let transcript_path = config.data_dir.join(&relative_transcript);
    let transcript_dir = transcript_path
        .parent()
        .context("transcript path has no parent")?;
    tokio::fs::create_dir_all(transcript_dir)
        .await
        .context("cannot create transcript directory")?;
    let temp_path = config
        .data_dir
        .join("tmp")
        .join(format!("{}.json.tmp", job.job_id));
    let bytes = serde_json::to_vec_pretty(&transcript).context("cannot serialize transcript")?;
    tokio::fs::write(&temp_path, bytes)
        .await
        .context("cannot write transcript temporary file")?;
    tokio::fs::rename(&temp_path, &transcript_path)
        .await
        .context("cannot publish transcript file")?;

    Ok((
        relative_transcript,
        duration,
        detected_language.map(str::to_owned),
    ))
}

async fn call_whisperx(
    config: &Config,
    client: &Client,
    job: &Transcription,
    audio_path: &Path,
) -> Result<Value> {
    let url = format!("{}/v1/audio/transcriptions", config.whisperx_base_url);

    for attempt in 1..=MAX_WHISPERX_ATTEMPTS {
        let file = File::open(audio_path)
            .await
            .with_context(|| format!("cannot open {}", audio_path.display()))?;
        let length = file
            .metadata()
            .await
            .context("cannot inspect audio file")?
            .len();
        let filename = audio_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("audio")
            .to_owned();
        let body = reqwest::Body::wrap_stream(ReaderStream::new(file));
        let file_part = multipart::Part::stream_with_length(body, length).file_name(filename);
        let mut form = multipart::Form::new()
            .part("file", file_part)
            .text("model", job.model.clone())
            .text("response_format", "verbose_json");
        if !job.requested_language.is_empty() {
            form = form.text("language", job.requested_language.clone());
        }

        let response = client
            .post(&url)
            .bearer_auth(&config.whisperx_api_key)
            .multipart(form)
            .send()
            .await;

        match response {
            Ok(response) if response.status().is_success() => {
                return response
                    .json()
                    .await
                    .context("WhisperX returned invalid JSON");
            }
            Ok(response) if response.status() == StatusCode::TOO_MANY_REQUESTS => {
                tracing::warn!(attempt, "WhisperX is busy; retrying");
            }
            Ok(response) => {
                let status = response.status();
                let message = response.text().await.unwrap_or_default();
                bail!("WhisperX returned {status}: {message}");
            }
            Err(error) => {
                tracing::warn!(attempt, ?error, "WhisperX request failed; retrying");
            }
        }

        if attempt < MAX_WHISPERX_ATTEMPTS {
            sleep(Duration::from_secs(2_u64.pow(attempt - 1))).await;
        }
    }

    bail!("WhisperX did not accept the job after {MAX_WHISPERX_ATTEMPTS} attempts")
}

fn truncate_error(message: &str) -> String {
    message.chars().take(2_000).collect()
}

#[derive(Debug, Deserialize)]
struct WhisperxProgress {
    busy: bool,
    stage: String,
    progress_percent: u8,
    message: String,
    language: Option<String>,
    audio_seconds: Option<f64>,
    elapsed_ms: Option<u64>,
}

async fn watch_whisperx_progress(
    config: Arc<Config>,
    client: Client,
    job_id: String,
    progress: JobProgress,
    jobs_changed: Arc<Notify>,
) {
    loop {
        if let Some(snapshot) = fetch_whisperx_progress(&config, &client).await {
            progress.set(&job_id, snapshot);
            jobs_changed.notify_waiters();
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn fetch_whisperx_progress(config: &Config, client: &Client) -> Option<LiveProgress> {
    let url = format!("{}/v1/progress", config.whisperx_base_url);
    let response = client
        .get(url)
        .bearer_auth(&config.whisperx_api_key)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let payload: WhisperxProgress = response.json().await.ok()?;
    if !payload.busy {
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
