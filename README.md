# dbx-cv-client

CLI client for streaming RTSP video frames to Databricks ingestion tables using the Zerobus SDK.

## Prerequisites

- ffmpeg installed and available in PATH

## Installation

```bash
uv pip install -e .
```

## Usage

### Generate Proto

Compile the bundled `record.proto` file to Python bindings:

```bash
dbx-cv-client generate-proto
```

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--compile-if-exists` | `DATABRICKS_PROTO_COMPILE_IF_EXISTS` | `true` | Recompile even if bindings exist |

### Run Client

Stream RTSP video frames to a Databricks table. This command automatically compiles the proto file if needed before starting the client.

```bash
dbx-cv-client client \
  --rtsp-url "rtsp://<host>:<port>/<path>" \
  --host "https://adb-<workspace-id>.<region-id>.azuredatabricks.net" \
  --region <region> \
  --client-id <client-id> \
  --client-secret <client-secret> \
  --table-name <catalog>.<schema>.<table>
```

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--rtsp-url` | `RTSP_URL` | RTSP stream URL to capture frames from |
| `--host` | `DATABRICKS_HOST` | Databricks workspace URL or host |
| `--region` | `DATABRICKS_REGION` | Cloud region for Zerobus endpoint |
| `--client-id` | `DATABRICKS_CLIENT_ID` | OAuth client ID |
| `--client-secret` | `DATABRICKS_CLIENT_SECRET` | OAuth client secret |
| `--table-name` | `DATABRICKS_TABLE_NAME` | Fully qualified table name |

#### Using Environment Variables

```bash
export RTSP_URL="rtsp://<host>:<port>/<path>"
export DATABRICKS_HOST="https://adb-<workspace-id>.<region-id>.azuredatabricks.net"
export DATABRICKS_REGION="<region>"
export DATABRICKS_CLIENT_ID="<client-id>"
export DATABRICKS_CLIENT_SECRET="<client-secret>"
export DATABRICKS_TABLE_NAME="<catalog>.<schema>.<table>"

dbx-cv-client client
```

## Development

```bash
uv pip install -e ".[dev]"
uv run pytest
```
