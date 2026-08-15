# Transcription gateway

Rust gateway for content-addressed audio transcription. The service stores
audio and transcript files on local disk. SQLite provides lookup and a durable
job queue. One in-process worker sends jobs to a separate WhisperX VM.

## Requirements

- Rust 1.97 or later
- A persistent disk for `DATA_DIR`
- Network access from this VM to the WhisperX API (`WHISPERX_BASE_URL`)

## Configure

```bash
cp .env.example .env
```

Set both API keys. `GATEWAY_API_KEY` authenticates clients.
`WHISPERX_API_KEY` authenticates the gateway to WhisperX. On OrbStack, set
`WHISPERX_BASE_URL` to `http://whisperx.orb.local:8000`.

The defaults create these paths:

```text
data/
├── audio/
├── lookup.db
├── tmp/
└── transcripts/
```

The service enables SQLite WAL mode and applies migrations at startup.
Audio files use `audio/{sha256}.{extension}`. Transcript files use
`transcripts/{sha256}/{model}/{language-or-auto}.json` to keep cache variants
separate.

## Run

```bash
cargo run --release
```

The gateway listens on `0.0.0.0:8080` by default. Port `8000` is WhisperX.
Send client traffic to the gateway, not to WhisperX.

## Hash-first client flow

Calculate lowercase SHA-256 from the original file bytes:

```bash
HASH=$(shasum -a 256 note.m4a | awk '{print $1}')
```

Check the lookup before uploading:

```bash
curl -sS http://gateway:8080/v1/audio/lookup \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"sha256\":\"$HASH\",\"model\":\"whisper-1\"}"
```

The lookup returns:

- `200` with the stored transcription when the job is ready.
- `202` with a job ID and status when the job is pending or processing.
- `404` when the gateway does not have the hash. Upload the file to create a job.

A `202` body includes `queue_position` while the job is queued. While WhisperX
runs, the body also includes `stage`, `progress_percent`, `message`,
`language`, `audio_seconds`, and `elapsed_ms` from WhisperX
`GET /v1/progress`.

Upload only after a `404`:

```bash
curl -sS http://gateway:8080/v1/audio/transcriptions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -F "file=@note.m4a" \
  -F "sha256=$HASH" \
  -F "model=whisper-1" \
  -F "response_format=verbose_json"
```

The gateway always recalculates the hash. A mismatched client hash returns
`400`. A new or in-progress job returns `202` immediately with a job ID in
the JSON body and in the `x-job-id` header. A ready cache hit returns `200`
with the transcript.

`wait=true` keeps the upload connection open as an SSE status stream. The
first event includes the job ID. After the stream closes on `ready`, call
`GET /v1/jobs/{job_id}` for the transcript.

Poll an asynchronous job:

```bash
curl -sS http://gateway:8080/v1/jobs/JOB_ID \
  -H "Authorization: Bearer $GATEWAY_API_KEY"
```

Subscribe to live status events (preferred while a job runs):

```bash
curl -N -sS http://gateway:8080/v1/jobs/JOB_ID/events \
  -H "Authorization: Bearer $GATEWAY_API_KEY"
```

## API

### `GET /health`

Returns gateway readiness. Authentication is not required.

### `POST /v1/audio/lookup`

Accepts JSON:

```json
{
  "sha256": "64 lowercase hexadecimal characters",
  "model": "whisper-1",
  "language": "en"
}
```

`model` and `language` are optional. An omitted language means automatic
detection.

### `POST /v1/audio/transcriptions`

Accepts multipart fields:

- `file`: Required audio file.
- `sha256`: Optional client hash. The gateway verifies the value.
- `model`: Optional. The only supported value is `whisper-1`.
- `language`: Optional ISO language code.
- `response_format`: `json`, `verbose_json`, or `text`.
- `wait`: `true` or `1` to stream SSE status events until the job finishes.

The worker always requests `verbose_json` from WhisperX. The gateway derives
the smaller formats from the stored object. Because the worker uses
`verbose_json`, WhisperX alignment occupies progress 72-96.

### `GET /v1/jobs/{job_id}`

Returns `202` while the job runs, the transcription after completion, or a
failure object if transcription failed.

A `202` body includes:

- `job_id`
- `status`: `pending` or `processing`
- `queue_position`: 1-based place in the gateway queue. Present only when
  `status` is `pending`.
- `stage`, `progress_percent`, `message`, `language`, `audio_seconds`,
  `elapsed_ms`: copied from WhisperX `GET /v1/progress` while the worker is
  busy. These fields are omitted until WhisperX reports them.

Queued jobs use `stage` `queued`, `progress_percent` `0`, and message
`Queued`. Completed jobs use `progress_percent` `100`.

WhisperX `stage` values are `receiving`, `loading_audio`, `transcribing`,
`aligning`, and `finalizing`. `progress_percent` is an integer from 0 to 100
and does not go backward during a job. `message` is short status-bar text.

### `GET /v1/jobs/{job_id}/events`

Opens an SSE stream of `status` events until the job is `ready` or `failed`.
Each event uses the same JSON object as a `202` job response. After the stream
closes on `ready`, call `GET /v1/jobs/{job_id}` for the transcript.

## Failure behavior

- The worker sends one job at a time.
- A WhisperX `429` or connection error gets five exponential-backoff retries.
- Failed rows keep their audio file.
- Uploading the same hash again requeues a failed row.
- At startup, the service returns interrupted `processing` rows to `pending`.

Back up `DATA_DIR` as one unit. The SQLite database contains relative paths to
the files in that directory.
