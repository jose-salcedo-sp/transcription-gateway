use std::{
    convert::Infallible,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
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
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tokio::{
    sync::Notify,
    time::{Duration, sleep},
};
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::{
    config::Config,
    db,
    error::AppError,
    models::{JobProgress, JobResponse, LookupRequest, Transcription, validate_key},
    upload_token,
};

const UPLOAD_TTL_SECONDS: i64 = 2 * 60 * 60;
const INTERNAL_BODY_LIMIT: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub pool: SqlitePool,
    pub jobs_changed: Arc<Notify>,
    pub progress: JobProgress,
}

#[derive(Debug, Serialize)]
struct ClaimedJob {
    job_id: String,
    content_sha256: String,
    model: String,
    requested_language: String,
}

#[derive(Debug, Deserialize)]
struct AudioReadyRequest {
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct FailRequest {
    error: String,
    reason: String,
}

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/v1/audio/lookup", post(lookup))
        .route("/v1/jobs/{job_id}", get(job))
        .route("/v1/jobs/{job_id}/events", get(job_events))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));
    let worker = Router::new()
        .route("/v1/internal/worker/claim", post(claim))
        .route("/v1/internal/jobs/{job_id}/audio-ready", post(audio_ready))
        .route("/v1/internal/jobs/{job_id}/result", post(result))
        .route("/v1/internal/jobs/{job_id}/fail", post(fail))
        .layer(DefaultBodyLimit::max(INTERNAL_BODY_LIMIT))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_worker_key,
        ));
    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .merge(worker)
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

    db::expire_uploads(&state.pool, unix_timestamp()).await?;
    let expires_at = unix_timestamp() + UPLOAD_TTL_SECONDS;
    let record = match db::find_by_key(
        &state.pool,
        &request.sha256,
        &request.model,
        &request.language,
    )
    .await?
    {
        Some(record) if record.status == "failed" => {
            db::requeue_failed(&state.pool, &record.job_id, expires_at).await?;
            state.jobs_changed.notify_waiters();
            db::find_by_job_id(&state.pool, &record.job_id)
                .await?
                .ok_or_else(|| AppError::Internal(anyhow::anyhow!("requeued job disappeared")))?
        }
        Some(record) => record,
        None => {
            let job_id = Uuid::new_v4().to_string();
            let inserted = db::insert_awaiting_upload(
                &state.pool,
                &job_id,
                &request.sha256,
                &request.model,
                &request.language,
                expires_at,
            )
            .await?;
            let record = if inserted {
                db::find_by_job_id(&state.pool, &job_id).await?
            } else {
                db::find_by_key(
                    &state.pool,
                    &request.sha256,
                    &request.model,
                    &request.language,
                )
                .await?
            };
            state.jobs_changed.notify_waiters();
            record.ok_or_else(|| {
                AppError::Internal(anyhow::anyhow!("created or conflicting job disappeared"))
            })?
        }
    };
    record_response(&state, record, "verbose_json").await
}

async fn job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Response, AppError> {
    let mut record = db::find_by_job_id(&state.pool, &job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".into()))?;
    if upload_expired(&record) {
        db::expire_uploads(&state.pool, unix_timestamp()).await?;
        record = db::find_by_job_id(&state.pool, &job_id)
            .await?
            .ok_or_else(|| AppError::NotFound("job not found".into()))?;
    }
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

async fn claim(State(state): State<AppState>) -> Result<Response, AppError> {
    let Some(record) = db::claim_next(&state.pool).await? else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    state.jobs_changed.notify_waiters();
    Ok(Json(ClaimedJob {
        job_id: record.job_id,
        content_sha256: record.content_sha256,
        model: record.model,
        requested_language: record.requested_language,
    })
    .into_response())
}

async fn audio_ready(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Json(request): Json<AudioReadyRequest>,
) -> Result<StatusCode, AppError> {
    let record = db::find_by_job_id(&state.pool, &job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".into()))?;
    if request.sha256 != record.content_sha256 {
        return Err(AppError::BadRequest("sha256 does not match the job".into()));
    }
    if record.status == "awaiting_upload" {
        if !db::mark_audio_ready(&state.pool, &job_id).await? {
            return Err(AppError::Conflict("job status changed".into()));
        }
        state.jobs_changed.notify_waiters();
        return Ok(StatusCode::NO_CONTENT);
    }
    if matches!(record.status.as_str(), "pending" | "processing" | "ready") {
        return Ok(StatusCode::NO_CONTENT);
    }
    Err(AppError::Conflict(format!(
        "job is {}, not awaiting_upload",
        record.status
    )))
}

async fn result(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Json(transcript): Json<Value>,
) -> Result<StatusCode, AppError> {
    let record = db::find_by_job_id(&state.pool, &job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".into()))?;
    if record.status == "ready" {
        return Ok(StatusCode::NO_CONTENT);
    }
    if record.status != "processing" {
        return Err(AppError::Conflict(format!(
            "job is {}, not processing",
            record.status
        )));
    }
    validate_transcript(&transcript)?;

    let path_language = if record.requested_language.is_empty() {
        "auto"
    } else {
        &record.requested_language
    };
    let relative_transcript = format!(
        "transcripts/{}/{}/{}.json",
        record.content_sha256, record.model, path_language
    );
    let transcript_path = state.config.data_dir.join(&relative_transcript);
    let transcript_dir = transcript_path
        .parent()
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("transcript path has no parent")))?;
    tokio::fs::create_dir_all(transcript_dir).await?;
    let temp_path = state
        .config
        .data_dir
        .join("tmp")
        .join(format!("{job_id}.json.tmp"));
    let bytes =
        serde_json::to_vec_pretty(&transcript).map_err(|error| AppError::Internal(error.into()))?;
    tokio::fs::write(&temp_path, bytes).await?;
    tokio::fs::rename(&temp_path, &transcript_path).await?;

    let duration = transcript.get("duration").and_then(Value::as_f64);
    let language = transcript
        .get("language")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    db::mark_ready(
        &state.pool,
        &job_id,
        &relative_transcript,
        duration,
        language,
    )
    .await?;
    state.progress.clear(&job_id);
    state.jobs_changed.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

async fn fail(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Json(request): Json<FailRequest>,
) -> Result<StatusCode, AppError> {
    let record = db::find_by_job_id(&state.pool, &job_id)
        .await?
        .ok_or_else(|| AppError::NotFound("job not found".into()))?;
    if request.reason == "audio_missing" {
        if record.status == "awaiting_upload" {
            return Ok(StatusCode::NO_CONTENT);
        }
        if record.status != "processing" {
            return Err(AppError::Conflict(format!(
                "job is {}, not processing",
                record.status
            )));
        }
        let expires_at = unix_timestamp() + UPLOAD_TTL_SECONDS;
        db::return_to_awaiting_upload(&state.pool, &job_id, expires_at).await?;
    } else if request.reason == "transcription_failed" {
        if record.status == "failed" {
            return Ok(StatusCode::NO_CONTENT);
        }
        if record.status != "processing" {
            return Err(AppError::Conflict(format!(
                "job is {}, not processing",
                record.status
            )));
        }
        db::mark_failed(
            &state.pool,
            &job_id,
            &request.error.chars().take(2_000).collect::<String>(),
        )
        .await?;
    } else {
        return Err(AppError::BadRequest("unknown failure reason".into()));
    }
    state.progress.clear(&job_id);
    state.jobs_changed.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
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
    let mut response =
        JobResponse::from_record(record, state.progress.get(&record.job_id), queue_position);
    if record.status == "awaiting_upload"
        && let Some(expires_at) = record.upload_expires_at
    {
        let token = upload_token::sign(
            &state.config.whisperx_upload_secret,
            &record.job_id,
            &record.content_sha256,
            expires_at,
        );
        response.upload_url = Some(format!(
            "{}/v1/jobs/{}/audio?sha256={}",
            state.config.whisperx_public_base_url, record.job_id, record.content_sha256
        ));
        response.upload_token = Some(token);
        response.upload_expires_at = Some(expires_at);
    }
    response
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

async fn require_worker_key(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, AppError> {
    let expected = format!("Bearer {}", state.config.gateway_worker_key);
    if request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        != Some(expected.as_str())
    {
        return Err(AppError::Unauthorized);
    }
    Ok(next.run(request).await)
}

fn validate_transcript(transcript: &Value) -> Result<(), AppError> {
    let object = transcript
        .as_object()
        .ok_or_else(|| AppError::BadRequest("transcript must be a JSON object".into()))?;
    if object.get("task").and_then(Value::as_str) != Some("transcribe")
        || object.get("language").and_then(Value::as_str).is_none()
        || object.get("text").and_then(Value::as_str).is_none()
        || object.get("segments").and_then(Value::as_array).is_none()
    {
        return Err(AppError::BadRequest(
            "transcript must contain task, language, text, and segments".into(),
        ));
    }
    Ok(())
}

fn upload_expired(record: &Transcription) -> bool {
    record.status == "awaiting_upload"
        && record
            .upload_expires_at
            .is_some_and(|expires_at| expires_at <= unix_timestamp())
}

fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header::CONTENT_TYPE},
    };
    use serde_json::{Value, json};
    use tempfile::tempdir;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    use crate::{config::Config, db, models::JobProgress};

    use super::{AppState, router};

    const HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[tokio::test]
    async fn creates_upload_then_claims_and_completes_job() {
        let directory = tempdir().unwrap();
        let data_dir = directory.path().to_path_buf();
        db::prepare_data_dirs(&data_dir).await.unwrap();
        let pool = db::connect(&format!("sqlite://{}", data_dir.join("test.db").display()))
            .await
            .unwrap();
        let state = AppState {
            config: Arc::new(Config {
                bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
                database_url: String::new(),
                data_dir,
                gateway_api_key: "client-key".into(),
                gateway_worker_key: "worker-key".into(),
                whisperx_base_url: "http://whisperx-internal:8000".into(),
                whisperx_public_base_url: "https://uploads.example.test".into(),
                whisperx_api_key: "whisperx-key".into(),
                whisperx_upload_secret: "upload-secret".into(),
            }),
            pool,
            jobs_changed: Arc::new(Notify::new()),
            progress: JobProgress::default(),
        };
        let app = router(state);

        let first = app
            .clone()
            .oneshot(json_request(
                "/v1/audio/lookup",
                "client-key",
                json!({"sha256": HASH}),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::ACCEPTED);
        let first_payload = response_json(first).await;
        let job_id = first_payload["job_id"].as_str().unwrap().to_owned();
        assert_eq!(first_payload["status"], "awaiting_upload");
        assert!(
            first_payload["upload_url"]
                .as_str()
                .unwrap()
                .contains(&format!("/v1/jobs/{job_id}/audio?sha256={HASH}"))
        );
        assert!(
            first_payload["upload_token"]
                .as_str()
                .unwrap()
                .contains('.')
        );

        let second = app
            .clone()
            .oneshot(json_request(
                "/v1/audio/lookup",
                "client-key",
                json!({"sha256": HASH}),
            ))
            .await
            .unwrap();
        let second_payload = response_json(second).await;
        assert_eq!(second_payload["job_id"], job_id);

        let empty_claim = app
            .clone()
            .oneshot(json_request(
                "/v1/internal/worker/claim",
                "worker-key",
                json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(empty_claim.status(), StatusCode::NO_CONTENT);

        let ready = app
            .clone()
            .oneshot(json_request(
                &format!("/v1/internal/jobs/{job_id}/audio-ready"),
                "worker-key",
                json!({"sha256": HASH}),
            ))
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::NO_CONTENT);

        let claim = app
            .clone()
            .oneshot(json_request(
                "/v1/internal/worker/claim",
                "worker-key",
                json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(claim.status(), StatusCode::OK);
        assert_eq!(response_json(claim).await["job_id"], job_id);

        let result = app
            .clone()
            .oneshot(json_request(
                &format!("/v1/internal/jobs/{job_id}/result"),
                "worker-key",
                json!({
                    "task": "transcribe",
                    "language": "en",
                    "duration": 1.5,
                    "text": "Hello.",
                    "segments": []
                }),
            ))
            .await
            .unwrap();
        assert_eq!(result.status(), StatusCode::NO_CONTENT);

        let completed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/jobs/{job_id}"))
                    .header("authorization", "Bearer client-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(completed.status(), StatusCode::OK);
        assert_eq!(response_json(completed).await["text"], "Hello.");

        let missing_audio = app
            .clone()
            .oneshot(json_request(
                "/v1/audio/lookup",
                "client-key",
                json!({"sha256": HASH_B}),
            ))
            .await
            .unwrap();
        let missing_audio_job = response_json(missing_audio).await["job_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let mismatch = app
            .clone()
            .oneshot(json_request(
                &format!("/v1/internal/jobs/{missing_audio_job}/audio-ready"),
                "worker-key",
                json!({"sha256": HASH}),
            ))
            .await
            .unwrap();
        assert_eq!(mismatch.status(), StatusCode::BAD_REQUEST);

        let ready = app
            .clone()
            .oneshot(json_request(
                &format!("/v1/internal/jobs/{missing_audio_job}/audio-ready"),
                "worker-key",
                json!({"sha256": HASH_B}),
            ))
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::NO_CONTENT);
        let claimed = app
            .clone()
            .oneshot(json_request(
                "/v1/internal/worker/claim",
                "worker-key",
                json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(response_json(claimed).await["job_id"], missing_audio_job);

        let failed = app
            .clone()
            .oneshot(json_request(
                &format!("/v1/internal/jobs/{missing_audio_job}/fail"),
                "worker-key",
                json!({"error": "audio not found", "reason": "audio_missing"}),
            ))
            .await
            .unwrap();
        assert_eq!(failed.status(), StatusCode::NO_CONTENT);
        let retry = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/jobs/{missing_audio_job}"))
                    .header("authorization", "Bearer client-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_json(retry).await["status"], "awaiting_upload");
    }

    fn json_request(uri: &str, key: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("authorization", format!("Bearer {key}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
