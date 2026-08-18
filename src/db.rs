use std::{path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

use crate::models::Transcription;

pub async fn connect(database_url: &str) -> Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str(database_url)
        .context("invalid DATABASE_URL")?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5))
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .context("cannot open SQLite database")?;
    sqlx::migrate!()
        .run(&pool)
        .await
        .context("cannot run SQLite migrations")?;
    Ok(pool)
}

pub async fn prepare_data_dirs(data_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(data_dir.join("transcripts"))
        .await
        .context("cannot create transcript directory")?;
    tokio::fs::create_dir_all(data_dir.join("tmp"))
        .await
        .context("cannot create temporary directory")?;
    Ok(())
}

pub async fn find_by_key(
    pool: &SqlitePool,
    sha256: &str,
    model: &str,
    language: &str,
) -> Result<Option<Transcription>, sqlx::Error> {
    sqlx::query_as::<_, Transcription>(
        "SELECT * FROM transcriptions
         WHERE content_sha256 = ? AND model = ? AND requested_language = ?",
    )
    .bind(sha256)
    .bind(model)
    .bind(language)
    .fetch_optional(pool)
    .await
}

pub async fn find_by_job_id(
    pool: &SqlitePool,
    job_id: &str,
) -> Result<Option<Transcription>, sqlx::Error> {
    sqlx::query_as::<_, Transcription>("SELECT * FROM transcriptions WHERE job_id = ?")
        .bind(job_id)
        .fetch_optional(pool)
        .await
}

pub async fn insert_awaiting_upload(
    pool: &SqlitePool,
    job_id: &str,
    sha256: &str,
    model: &str,
    language: &str,
    upload_expires_at: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO transcriptions
            (job_id, content_sha256, model, requested_language, status, upload_expires_at)
         VALUES (?, ?, ?, ?, 'awaiting_upload', ?)
         ON CONFLICT(content_sha256, model, requested_language) DO NOTHING",
    )
    .bind(job_id)
    .bind(sha256)
    .bind(model)
    .bind(language)
    .bind(upload_expires_at)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn requeue_failed(
    pool: &SqlitePool,
    job_id: &str,
    upload_expires_at: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE transcriptions
         SET status = 'awaiting_upload', error = NULL, upload_expires_at = ?,
             updated_at = datetime('now')
         WHERE job_id = ? AND status = 'failed'",
    )
    .bind(upload_expires_at)
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn mark_audio_ready(pool: &SqlitePool, job_id: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE transcriptions
         SET status = 'pending', upload_expires_at = NULL, error = NULL,
             updated_at = datetime('now')
         WHERE job_id = ? AND status = 'awaiting_upload'",
    )
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn return_to_awaiting_upload(
    pool: &SqlitePool,
    job_id: &str,
    upload_expires_at: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE transcriptions
         SET status = 'awaiting_upload', upload_expires_at = ?, error = NULL,
             updated_at = datetime('now')
         WHERE job_id = ? AND status = 'processing'",
    )
    .bind(upload_expires_at)
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn expire_uploads(pool: &SqlitePool, now: i64) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE transcriptions
         SET status = 'failed', error = 'upload expired', updated_at = datetime('now')
         WHERE status = 'awaiting_upload' AND upload_expires_at <= ?",
    )
    .bind(now)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn reset_processing(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE transcriptions
         SET status = 'pending', updated_at = datetime('now')
         WHERE status = 'processing'",
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn claim_next(pool: &SqlitePool) -> Result<Option<Transcription>, sqlx::Error> {
    sqlx::query_as::<_, Transcription>(
        "UPDATE transcriptions
         SET status = 'processing', updated_at = datetime('now')
         WHERE rowid = (
             SELECT rowid FROM transcriptions
             WHERE status = 'pending'
             ORDER BY created_at
             LIMIT 1
         )
         RETURNING *",
    )
    .fetch_optional(pool)
    .await
}

pub async fn find_processing(pool: &SqlitePool) -> Result<Option<Transcription>, sqlx::Error> {
    sqlx::query_as::<_, Transcription>(
        "SELECT * FROM transcriptions
         WHERE status = 'processing'
         ORDER BY updated_at
         LIMIT 1",
    )
    .fetch_optional(pool)
    .await
}

pub async fn mark_ready(
    pool: &SqlitePool,
    job_id: &str,
    transcript_path: &str,
    duration_seconds: Option<f64>,
    language: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE transcriptions
         SET status = 'ready', transcript_path = ?, duration_seconds = ?,
             language = ?, error = NULL, updated_at = datetime('now')
         WHERE job_id = ?",
    )
    .bind(transcript_path)
    .bind(duration_seconds)
    .bind(language)
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn pending_position(pool: &SqlitePool, job_id: &str) -> Result<Option<i64>, sqlx::Error> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)
         FROM transcriptions AS queued
         JOIN transcriptions AS current ON current.job_id = ?
         WHERE queued.status = 'pending'
           AND current.status = 'pending'
           AND (queued.created_at < current.created_at
                OR (queued.created_at = current.created_at
                    AND queued.job_id <= current.job_id))",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await?;
    Ok((count > 0).then_some(count))
}

pub async fn mark_failed(pool: &SqlitePool, job_id: &str, error: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE transcriptions
         SET status = 'failed', error = ?, updated_at = datetime('now')
         WHERE job_id = ?",
    )
    .bind(error)
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        claim_next, connect, find_by_job_id, find_by_key, insert_awaiting_upload, mark_audio_ready,
        mark_ready, pending_position, reset_processing,
    };

    #[tokio::test]
    async fn waits_for_audio_before_claim_and_completion() {
        let directory = tempdir().unwrap();
        let database_url = format!("sqlite://{}", directory.path().join("test.db").display());
        let pool = connect(&database_url).await.unwrap();
        let hash = "a".repeat(64);

        assert!(
            insert_awaiting_upload(&pool, "job-1", &hash, "whisper-1", "", 1_800_000_000)
                .await
                .unwrap()
        );
        assert!(
            !insert_awaiting_upload(&pool, "job-2", &hash, "whisper-1", "", 1_800_000_000)
                .await
                .unwrap()
        );
        assert!(claim_next(&pool).await.unwrap().is_none());
        assert!(mark_audio_ready(&pool, "job-1").await.unwrap());

        let second_hash = "b".repeat(64);
        assert!(
            insert_awaiting_upload(&pool, "job-2", &second_hash, "whisper-1", "", 1_800_000_000)
                .await
                .unwrap()
        );
        assert!(mark_audio_ready(&pool, "job-2").await.unwrap());
        assert_eq!(pending_position(&pool, "job-1").await.unwrap(), Some(1));
        assert_eq!(pending_position(&pool, "job-2").await.unwrap(), Some(2));

        let claimed = claim_next(&pool).await.unwrap().unwrap();
        assert_eq!(claimed.job_id, "job-1");
        assert_eq!(claimed.status, "processing");
        assert_eq!(pending_position(&pool, "job-1").await.unwrap(), None);
        assert_eq!(pending_position(&pool, "job-2").await.unwrap(), Some(1));

        assert_eq!(reset_processing(&pool).await.unwrap(), 1);
        assert_eq!(
            find_by_key(&pool, &hash, "whisper-1", "")
                .await
                .unwrap()
                .unwrap()
                .status,
            "pending"
        );

        let claimed = claim_next(&pool).await.unwrap().unwrap();
        mark_ready(
            &pool,
            &claimed.job_id,
            "transcripts/file.json",
            Some(2.5),
            Some("en"),
        )
        .await
        .unwrap();
        let ready = find_by_job_id(&pool, "job-1").await.unwrap().unwrap();
        assert_eq!(ready.status, "ready");
        assert_eq!(ready.duration_seconds, Some(2.5));
        assert_eq!(ready.language.as_deref(), Some("en"));
    }
}
