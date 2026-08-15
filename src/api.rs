use std::{convert::Infallible, path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{
        HeaderValue, Request, StatusCode,
        header::{AUTHORIZATION, HeaderName},
    },
    middleware,
    middleware::Next,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::Notify,
    time::{Duration, sleep},
};
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::{
    config::Config,
    db,
    error::AppError,
    models::{
        JobProgress, JobResponse, LookupRequest, PUBLIC_MODEL_ID, Transcription, validate_key,
    },
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub pool: SqlitePool,
    pub jobs_changed: Arc<Notify>,
    pub progress: JobProgress,
}

struct Upload {
    temp_path: PathBuf,
    sha256: String,
    supplied_sha256: Option<String>,
    extension: String,
    model: String,
    language: String,
    response_format: String,
    wait: bool,
}

pub fn router(state: AppState) -> Router {
    let body_limit = state.config.max_upload_bytes.saturating_add(1024 * 1024);
    let protected = Router::new()
        .route("/v1/audio/lookup", post(lookup))
        .route("/v1/audio/transcriptions", post(upload))
        .route("/v1/jobs/{job_id}", get(job))
        .route("/v1/jobs/{job_id}/events", get(job_events))
        .layer(DefaultBodyLimit::max(body_limit))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));
    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "database": "sqlite",
        "worker": "running",
    }))
}

async fn lookup(
    State(state): State<AppState>,
    Json(request): Json<LookupRequest>,
) -> Result<Response, AppError> {
    validate_key(&request.sha256, &request.model, &request.language)
        .map_err(AppError::BadRequest)?;

    let Some(record) = db::find_by_key(
        &state.pool,
        &request.sha256,
        &request.model,
        &request.language,
    )
    .await?
    else {
        return Err(AppError::NotFound("transcription not found".into()));
    };
    record_response(&state, record, "verbose_json").await
}

async fn job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Response, AppError> {
    let record = db::find_by_job_id(&state.pool, &job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".into()))?;
    record_response(&state, record, "verbose_json").await
}

async fn job_events(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Response, AppError> {
    db::find_by_job_id(&state.pool, &job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".into()))?;
    Ok(job_status_sse(state, job_id))
}

async fn upload(State(state): State<AppState>, multipart: Multipart) -> Result<Response, AppError> {
    let upload = read_upload(&state, multipart).await?;

    if let Some(supplied) = &upload.supplied_sha256
        && supplied != &upload.sha256
    {
        let _ = tokio::fs::remove_file(&upload.temp_path).await;
        return Err(AppError::BadRequest(
            "supplied sha256 does not match the uploaded file".into(),
        ));
    }
    if let Err(message) = validate_key(&upload.sha256, &upload.model, &upload.language) {
        let _ = tokio::fs::remove_file(&upload.temp_path).await;
        return Err(AppError::BadRequest(message));
    }
    if !matches!(
        upload.response_format.as_str(),
        "json" | "verbose_json" | "text"
    ) {
        let _ = tokio::fs::remove_file(&upload.temp_path).await;
        return Err(AppError::BadRequest(
            "response_format must be json, verbose_json, or text".into(),
        ));
    }

    if let Some(record) =
        db::find_by_key(&state.pool, &upload.sha256, &upload.model, &upload.language).await?
    {
        let _ = tokio::fs::remove_file(&upload.temp_path).await;
        let record = requeue_if_failed(&state, record).await?;
        return upload_result(&state, record, upload.wait, &upload.response_format).await;
    }

    let relative_audio = format!("audio/{}{}", upload.sha256, upload.extension);
    let final_audio = state.config.data_dir.join(&relative_audio);
    tokio::fs::rename(&upload.temp_path, &final_audio).await?;

    let new_job_id = Uuid::new_v4().to_string();
    let inserted = db::insert_pending(
        &state.pool,
        &new_job_id,
        &upload.sha256,
        &upload.model,
        &upload.language,
        &relative_audio,
    )
    .await?;

    let record = if inserted {
        state.jobs_changed.notify_waiters();
        db::find_by_job_id(&state.pool, &new_job_id)
            .await?
            .ok_or_else(|| AppError::Internal(anyhow::anyhow!("inserted job disappeared")))?
    } else {
        let record = db::find_by_key(&state.pool, &upload.sha256, &upload.model, &upload.language)
            .await?
            .ok_or_else(|| AppError::Internal(anyhow::anyhow!("conflicting job disappeared")))?;
        if record.audio_path.as_deref() != Some(relative_audio.as_str()) {
            let _ = tokio::fs::remove_file(&final_audio).await;
        }
        requeue_if_failed(&state, record).await?
    };

    upload_result(&state, record, upload.wait, &upload.response_format).await
}

async fn upload_result(
    state: &AppState,
    record: Transcription,
    wait: bool,
    response_format: &str,
) -> Result<Response, AppError> {
    if wait && !matches!(record.status.as_str(), "ready" | "failed") {
        return Ok(job_status_sse(state.clone(), record.job_id));
    }
    record_response(state, record, response_format).await
}

async fn requeue_if_failed(
    state: &AppState,
    record: Transcription,
) -> Result<Transcription, AppError> {
    if !db::requeue_failed(&state.pool, &record.job_id).await? {
        return Ok(record);
    }
    state.jobs_changed.notify_waiters();
    db::find_by_job_id(&state.pool, &record.job_id)
        .await?
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("requeued job disappeared")))
}

async fn read_upload(state: &AppState, mut multipart: Multipart) -> Result<Upload, AppError> {
    let temp_path = state
        .config
        .data_dir
        .join("tmp")
        .join(format!("{}.upload", Uuid::new_v4()));
    let mut temp_file: Option<File> = None;
    let mut hasher = Sha256::new();
    let mut total = 0usize;
    let mut extension = ".audio".to_owned();
    let mut supplied_sha256 = None;
    let mut model = PUBLIC_MODEL_ID.to_owned();
    let mut language = String::new();
    let mut response_format = "json".to_owned();
    let mut wait = false;

    loop {
        let next_field = match multipart.next_field().await {
            Ok(field) => field,
            Err(error) => {
                discard_temp(&mut temp_file, &temp_path).await;
                return Err(AppError::BadRequest(format!(
                    "invalid multipart body: {error}"
                )));
            }
        };
        let Some(mut field) = next_field else {
            break;
        };
        let name = field.name().unwrap_or_default().to_owned();
        if name == "file" {
            if temp_file.is_some() {
                discard_temp(&mut temp_file, &temp_path).await;
                return Err(AppError::BadRequest("only one file is allowed".into()));
            }
            extension = safe_extension(field.file_name());
            let mut file = File::create(&temp_path).await?;
            loop {
                let next_chunk = match field.chunk().await {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        drop(file);
                        let _ = tokio::fs::remove_file(&temp_path).await;
                        return Err(AppError::BadRequest(format!("cannot read upload: {error}")));
                    }
                };
                let Some(chunk) = next_chunk else {
                    break;
                };
                total = total.saturating_add(chunk.len());
                if total > state.config.max_upload_bytes {
                    drop(file);
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    return Err(AppError::PayloadTooLarge(
                        "audio file exceeds the upload limit".into(),
                    ));
                }
                hasher.update(&chunk);
                if let Err(error) = file.write_all(&chunk).await {
                    drop(file);
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    return Err(error.into());
                }
            }
            if let Err(error) = file.flush().await {
                drop(file);
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(error.into());
            }
            temp_file = Some(file);
            continue;
        }

        let value = match field.text().await {
            Ok(value) => value,
            Err(error) => {
                discard_temp(&mut temp_file, &temp_path).await;
                return Err(AppError::BadRequest(format!("invalid form field: {error}")));
            }
        };
        match name.as_str() {
            "sha256" => supplied_sha256 = Some(value),
            "model" => model = value,
            "language" => match normalize_language(&value) {
                Ok(value) => language = value,
                Err(error) => {
                    discard_temp(&mut temp_file, &temp_path).await;
                    return Err(error);
                }
            },
            "response_format" => response_format = value,
            "wait" => wait = matches!(value.as_str(), "true" | "1"),
            _ => {}
        }
    }

    if temp_file.is_none() {
        return Err(AppError::BadRequest("file is required".into()));
    }
    drop(temp_file);
    Ok(Upload {
        temp_path,
        sha256: hex::encode(hasher.finalize()),
        supplied_sha256,
        extension,
        model,
        language,
        response_format,
        wait,
    })
}

async fn discard_temp(file: &mut Option<File>, path: &std::path::Path) {
    drop(file.take());
    let _ = tokio::fs::remove_file(path).await;
}

fn job_status_sse(state: AppState, job_id: String) -> Response {
    let header_job_id = job_id.clone();
    let stream = async_stream::stream! {
        let mut last = None;
        loop {
            match db::find_by_job_id(&state.pool, &job_id).await {
                Ok(Some(record)) => {
                    let payload = job_status(&state, &record).await;
                    if last.as_ref() != Some(&payload) {
                        match Event::default().event("status").json_data(&payload) {
                            Ok(event) => yield Ok::<Event, Infallible>(event),
                            Err(error) => {
                                tracing::error!(?error, "could not encode job status event");
                                break;
                            }
                        }
                        last = Some(payload);
                    }
                    if matches!(record.status.as_str(), "ready" | "failed") {
                        break;
                    }
                }
                Ok(None) => {
                    yield Ok(Event::default().event("error").data(
                        json!({ "error": { "message": "job not found" } }).to_string(),
                    ));
                    break;
                }
                Err(error) => {
                    tracing::error!(?error, "could not load job for status stream");
                    yield Ok(Event::default().event("error").data(
                        json!({ "error": { "message": "internal server error" } }).to_string(),
                    ));
                    break;
                }
            }
            tokio::select! {
                _ = state.jobs_changed.notified() => {}
                _ = sleep(Duration::from_millis(500)) => {}
            }
        }
    };
    with_job_id_header(
        Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
            .into_response(),
        &header_job_id,
    )
}

fn with_job_id_header(mut response: Response, job_id: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(job_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-job-id"), value);
    }
    response
}

async fn record_response(
    state: &AppState,
    record: Transcription,
    response_format: &str,
) -> Result<Response, AppError> {
    match record.status.as_str() {
        "ready" => {
            let relative = record.transcript_path.as_ref().ok_or_else(|| {
                AppError::Internal(anyhow::anyhow!("ready job has no transcript path"))
            })?;
            let bytes = tokio::fs::read(state.config.data_dir.join(relative)).await?;
            let mut transcript: Value =
                serde_json::from_slice(&bytes).map_err(|error| AppError::Internal(error.into()))?;
            if response_format == "text" {
                let text = transcript["text"].as_str().unwrap_or_default().to_owned();
                return Ok((StatusCode::OK, text).into_response());
            }
            if response_format == "json" {
                return Ok(Json(json!({
                    "text": transcript["text"].as_str().unwrap_or_default()
                }))
                .into_response());
            }
            if let Some(object) = transcript.as_object_mut() {
                object.insert(
                    "storage".into(),
                    json!({
                        "audio_path": record.audio_path,
                        "transcript_path": record.transcript_path,
                    }),
                );
            }
            Ok(Json(transcript).into_response())
        }
        "failed" => Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(JobResponse::from_record(
                &record,
                state.progress.get(&record.job_id),
                None,
            )),
        )
            .into_response()),
        _ => Ok(pending_response(state, &record).await),
    }
}

async fn job_status(state: &AppState, record: &Transcription) -> JobResponse {
    let queue_position = if record.status == "pending" {
        db::pending_position(&state.pool, &record.job_id)
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    JobResponse::from_record(record, state.progress.get(&record.job_id), queue_position)
}

async fn pending_response(state: &AppState, record: &Transcription) -> Response {
    let payload = job_status(state, record).await;
    let job_id = payload.job_id.clone();
    with_job_id_header(
        (StatusCode::ACCEPTED, Json(payload)).into_response(),
        &job_id,
    )
}

async fn require_api_key(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, AppError> {
    let headers = request.headers();
    let expected_key = &state.config.gateway_api_key;
    let expected = format!("Bearer {expected_key}");
    if headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        != Some(expected.as_str())
    {
        return Err(AppError::Unauthorized);
    }
    Ok(next.run(request).await)
}

fn normalize_language(value: &str) -> Result<String, AppError> {
    let value = value.trim().to_ascii_lowercase();
    let code = value.split('-').next().unwrap_or_default().to_owned();
    if code.is_empty() {
        return Ok(code);
    }
    if (2..=3).contains(&code.len()) && code.bytes().all(|byte| byte.is_ascii_lowercase()) {
        return Ok(code);
    }
    Err(AppError::BadRequest("invalid language code".into()))
}

fn safe_extension(filename: Option<&str>) -> String {
    let Some(extension) = filename
        .and_then(|name| std::path::Path::new(name).extension())
        .and_then(|extension| extension.to_str())
    else {
        return ".audio".into();
    };
    if extension.len() <= 10 && extension.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        format!(".{}", extension.to_ascii_lowercase())
    } else {
        ".audio".into()
    }
}
