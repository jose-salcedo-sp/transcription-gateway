CREATE TABLE transcriptions_new (
    job_id TEXT NOT NULL UNIQUE,
    content_sha256 TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT 'whisper-1',
    requested_language TEXT NOT NULL DEFAULT '',
    language TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'ready', 'failed')),
    audio_path TEXT,
    transcript_path TEXT,
    duration_seconds REAL,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (content_sha256, model, requested_language)
);

INSERT INTO transcriptions_new (
    job_id,
    content_sha256,
    model,
    requested_language,
    language,
    status,
    audio_path,
    transcript_path,
    duration_seconds,
    error,
    created_at,
    updated_at
)
SELECT
    job_id,
    content_sha256,
    model,
    language,
    NULL,
    status,
    audio_path,
    transcript_path,
    duration_seconds,
    error,
    created_at,
    updated_at
FROM transcriptions;

DROP TABLE transcriptions;

ALTER TABLE transcriptions_new RENAME TO transcriptions;

CREATE INDEX transcriptions_status_created_idx
    ON transcriptions (status, created_at);
