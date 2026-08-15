use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

pub const PUBLIC_MODEL_ID: &str = "whisper-1";

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LiveProgress {
    pub stage: Option<String>,
    pub progress_percent: Option<u8>,
    pub message: Option<String>,
    pub language: Option<String>,
    pub audio_seconds: Option<f64>,
    pub elapsed_ms: Option<u64>,
}

#[derive(Clone, Default)]
pub struct JobProgress {
    inner: Arc<RwLock<HashMap<String, LiveProgress>>>,
}

impl JobProgress {
    pub fn set(&self, job_id: &str, mut snapshot: LiveProgress) {
        if let Ok(mut map) = self.inner.write() {
            if let Some(percent) = snapshot.progress_percent {
                snapshot.progress_percent = Some(percent.min(99));
            }
            map.insert(job_id.to_owned(), snapshot);
        }
    }

    pub fn get(&self, job_id: &str) -> Option<LiveProgress> {
        self.inner.read().ok()?.get(job_id).cloned()
    }

    pub fn clear(&self, job_id: &str) {
        if let Ok(mut map) = self.inner.write() {
            map.remove(job_id);
        }
    }
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct Transcription {
    pub job_id: String,
    pub content_sha256: String,
    pub model: String,
    pub requested_language: String,
    pub language: Option<String>,
    pub status: String,
    pub audio_path: Option<String>,
    pub transcript_path: Option<String>,
    pub duration_seconds: Option<f64>,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct LookupRequest {
    pub sha256: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default)]
    pub language: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct JobResponse {
    pub job_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_percent: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl JobResponse {
    pub fn from_record(
        record: &Transcription,
        live: Option<LiveProgress>,
        queue_position: Option<i64>,
    ) -> Self {
        let mut live = live.unwrap_or_default();
        match record.status.as_str() {
            "ready" => {
                live = LiveProgress {
                    progress_percent: Some(100),
                    ..LiveProgress::default()
                };
            }
            "pending" => {
                live = LiveProgress {
                    stage: Some("queued".into()),
                    progress_percent: Some(0),
                    message: Some("Queued".into()),
                    ..LiveProgress::default()
                };
            }
            _ => {}
        }
        Self {
            job_id: record.job_id.clone(),
            status: record.status.clone(),
            stage: live.stage,
            progress_percent: live.progress_percent,
            message: live.message,
            queue_position: if record.status == "pending" {
                queue_position
            } else {
                None
            },
            language: match record.status.as_str() {
                "ready" => record.language.clone(),
                _ => live.language,
            },
            audio_seconds: live.audio_seconds,
            elapsed_ms: live.elapsed_ms,
            error: record.error.clone(),
        }
    }
}

pub fn default_model() -> String {
    PUBLIC_MODEL_ID.into()
}

pub fn validate_key(sha256: &str, model: &str, language: &str) -> Result<(), String> {
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("sha256 must be 64 lowercase hexadecimal characters".into());
    }
    if model != PUBLIC_MODEL_ID {
        return Err(format!("model must be {PUBLIC_MODEL_ID}"));
    }
    if !language.is_empty()
        && (language.len() < 2
            || language.len() > 3
            || !language.bytes().all(|byte| byte.is_ascii_lowercase()))
    {
        return Err("language must be an empty, two-letter, or three-letter lowercase code".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{JobResponse, LiveProgress, PUBLIC_MODEL_ID, Transcription, validate_key};

    #[test]
    fn accepts_valid_cache_key() {
        let hash = "a".repeat(64);
        assert!(validate_key(&hash, PUBLIC_MODEL_ID, "").is_ok());
        assert!(validate_key(&hash, PUBLIC_MODEL_ID, "en").is_ok());
    }

    #[test]
    fn rejects_noncanonical_hash() {
        assert!(validate_key(&"A".repeat(64), PUBLIC_MODEL_ID, "").is_err());
        assert!(validate_key("abc", PUBLIC_MODEL_ID, "").is_err());
    }

    #[test]
    fn rejects_unknown_model_and_language() {
        let hash = "0".repeat(64);
        assert!(validate_key(&hash, "large", "").is_err());
        assert!(validate_key(&hash, PUBLIC_MODEL_ID, "english").is_err());
    }

    #[test]
    fn job_response_maps_status_to_progress() {
        let mut record = Transcription {
            job_id: "job-1".into(),
            content_sha256: "a".repeat(64),
            model: PUBLIC_MODEL_ID.into(),
            requested_language: String::new(),
            language: None,
            status: "pending".into(),
            audio_path: None,
            transcript_path: None,
            duration_seconds: None,
            error: None,
            created_at: String::new(),
            updated_at: String::new(),
        };
        let transcribing = LiveProgress {
            stage: Some("transcribing".into()),
            progress_percent: Some(40),
            message: Some("Transcribing".into()),
            language: Some("en".into()),
            audio_seconds: Some(12.5),
            elapsed_ms: Some(800),
        };
        let pending = JobResponse::from_record(&record, Some(transcribing.clone()), Some(2));
        assert_eq!(pending.progress_percent, Some(0));
        assert_eq!(pending.stage.as_deref(), Some("queued"));
        assert_eq!(pending.queue_position, Some(2));

        record.status = "processing".into();
        let unknown = JobResponse::from_record(&record, None, Some(1));
        assert_eq!(unknown.progress_percent, None);
        assert_eq!(unknown.stage, None);
        let processing = JobResponse::from_record(&record, Some(transcribing), Some(1));
        assert_eq!(processing.progress_percent, Some(40));
        assert_eq!(processing.stage.as_deref(), Some("transcribing"));
        assert_eq!(processing.message.as_deref(), Some("Transcribing"));
        assert_eq!(processing.queue_position, None);

        record.status = "ready".into();
        let ready = JobResponse::from_record(&record, None, None);
        assert_eq!(ready.progress_percent, Some(100));
        assert_eq!(ready.stage, None);
    }
}
