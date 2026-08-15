CREATE TABLE transcriptions (
    job_id TEXT NOT NULL UNIQUE,
    content_sha256 TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT 'whisper-1',
    language TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'ready', 'failed')),
    audio_path TEXT,
    transcript_path TEXT,
    duration_seconds REAL,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (content_sha256, model, language)
);

CREATE INDEX transcriptions_status_created_idx
    ON transcriptions (status, created_at);
