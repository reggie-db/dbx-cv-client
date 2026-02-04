# dbx-cv-client

CLI client for streaming camera frames to Databricks ingestion tables via the Zerobus SDK. Supports RTSP streams (via FFmpeg), Meraki camera snapshots, and local image directories for testing/replay.

## Prerequisites

- Python 3.12+
- FFmpeg installed and in PATH (for RTSP sources)

## Installation

```bash
uv pip install -e .
```

## Usage

### Generate Proto

Compile the bundled `record.proto` to Python bindings:

```bash
dbx-cv-client generate-proto
```

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--compile-if-exists` | `PROTO_COMPILE_IF_EXISTS` | `true` | Recompile even if bindings exist |

### Run Client

Stream camera frames to a Databricks table:

```bash
dbx-cv-client client \
  --source "rtsp://<host>:<port>/<path>" \
  --source "<meraki-device-serial>" \
  --host "https://adb-<workspace-id>.<region>.azuredatabricks.net" \
  --region <region> \
  --client-id <client-id> \
  --client-secret <client-secret> \
  --table-name <catalog>.<schema>.<table>
```

#### Databricks Options

| Flag | Env Var | Description |
|------|---------|-------------|
| `--host` | `DATABRICKS_HOST` | Workspace URL or host |
| `--region` | `DATABRICKS_REGION` | Cloud region for Zerobus |
| `--client-id` | `DATABRICKS_CLIENT_ID` | OAuth client ID |
| `--client-secret` | `DATABRICKS_CLIENT_SECRET` | OAuth client secret |
| `--table-name` | `DATABRICKS_TABLE_NAME` | Fully qualified table name |
| `--zerobus-ip` | `DATABRICKS_ZEROBUS_IP` | Override DNS Zerobus host IP |

Optional environment variable:

- `DATABRICKS_ZEROBUS_IP_RESOLVE`: Set to `true` to skip local DNS resolution and resolve the Zerobus endpoint using public DNS (`1.1.1.1`).

#### Client Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--log-stats-interval` | `LOG_STATS_INTERVAL` | `5.0` | Seconds between client summary logs (`0` or empty disables) |
| `--flush-interval` | `FLUSH_INTERVAL` | | Reserved for future use (not used by the current client loop) |
| `--max-inflight-records` | `MAX_INFLIGHT_RECORDS` | | Reserved for future use (not used by the current stream configuration) |
| `--metadata-ip-info-url` | `METADATA_IP_INFO_URL` | `https://ipwho.is/` | If set, fetch IP info JSON and include it as `ip_info` in record metadata |
| `--metadata-ip-info-attempts` | `METADATA_IP_INFO_ATTEMPTS` | `3` | Max attempts to fetch IP info |
| `--metadata-ip-info-retry-interval` | `METADATA_IP_INFO_RETRY_INTERVAL` | `3` | Seconds between IP info fetch retries |

Client summary logs include:

- `run_time`: seconds since client start
- `readers`: number of configured sources
- `frames_produced`: total frames produced by readers
- `frames_consumed`: total frames pulled by the client loop
- `frames_ingested`: total `ingest_record()` calls (can exceed frames_produced when `--frame-multiplier` is enabled)

#### Source Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--source` | `SOURCES` | | Camera source (RTSP URL, Meraki serial, or directory path) |
| `--fps` | `FPS` | `1` | Frames per second (directory defaults to 5) |
| `--scale` | `SCALE` | `1080` | Image height in pixels |
| `--frame-multiplier` | `FRAME_MULTIPLIER` | `0` | Send each frame N extra times |

Source type is auto-detected:
- **Directory**: Local path to a folder containing images (`.jpg`, `.png`, `.bmp`, `.gif`, `.webp`, `.tiff`)
- **RTSP**: URL starting with `rtsp://` or `rtsps://`
- **Meraki**: Any other string (treated as device serial)

#### Meraki Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--meraki-api-base-url` | `MERAKI_API_BASE_URL` | `https://api.meraki.com/api/v1` | API base URL |
| `--meraki-api-token` | `MERAKI_API_TOKEN` | | API token (if not using Key Vault) |
| `--meraki-vault-url` | `MERAKI_VAULT_URL` | | Azure Key Vault URL |
| `--meraki-secret-name` | `MERAKI_SECRET_NAME` | | Secret name in Key Vault |

Meraki cameras include device info (name, model, location, etc.) in frame metadata, fetched once on the first read.

#### RTSP Options

| Flag | Env Var | Description |
|------|---------|-------------|
| `--rtsp-ffmpeg-arg` | `RTPSP_FFMPEG_ARGS` | Additional FFmpeg arguments |

### Examples

#### RTSP Stream

```bash
export DATABRICKS_HOST="https://adb-123456789.11.azuredatabricks.net"
export DATABRICKS_REGION="us-west-2"
export DATABRICKS_CLIENT_ID="<client-id>"
export DATABRICKS_CLIENT_SECRET="<client-secret>"
export DATABRICKS_TABLE_NAME="catalog.schema.frames"

dbx-cv-client client --source "rtsp://camera.local:554/stream"
```

#### Meraki Camera

```bash
dbx-cv-client client \
  --source "XXXX-YYYY-ZZZZ" \
  --meraki-api-token "<api-token>" \
  --host "$DATABRICKS_HOST" \
  --region "$DATABRICKS_REGION" \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET" \
  --table-name "$DATABRICKS_TABLE_NAME"
```

#### Directory (Testing/Replay)

```bash
dbx-cv-client client \
  --source "./sample_images" \
  --fps 5 \
  --host "$DATABRICKS_HOST" \
  --region "$DATABRICKS_REGION" \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET" \
  --table-name "$DATABRICKS_TABLE_NAME"
```

Images are shuffled and emitted at the configured FPS (default 5), looping when exhausted.

#### Multiple Sources

```bash
dbx-cv-client client \
  --source "rtsp://cam1.local:554/stream" \
  --source "MERAKI-SERIAL-001" \
  --source "./test_images"
```

## Development

```bash
uv pip install -e ".[dev]"
uv run pytest
```

## Testing

```bash
uv pip install -e ".[dev]"
uv run pytest
```
