# dbx-cv-client

CLI client for streaming data to Databricks Change Vault using the Zerobus SDK.

## Installation

```bash
uv pip install -e .
```

## Configuration

All options can be set via CLI flags or environment variables:

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--host` | `DATABRICKS_HOST` | Databricks workspace URL or host |
| `--region` | `DATABRICKS_REGION` | Cloud region for Zerobus endpoint |
| `--client-id` | `DATABRICKS_CLIENT_ID` | OAuth client ID |
| `--client-secret` | `DATABRICKS_CLIENT_SECRET` | OAuth client secret |
| `--table-name` | `DATABRICKS_TABLE_NAME` | Fully qualified table name |

## Usage

### Generate Proto

Generate a protobuf definition from a Unity Catalog table schema:

```bash
dbx-cv-client \
  --host "https://adb-123456789.11.azuredatabricks.net" \
  --region eastus2 \
  --client-id <client-id> \
  --client-secret <client-secret> \
  --table-name catalog.schema.table \
  generate-proto
```

Optional flags for `generate-proto`:
- `--proto-msg`: Custom proto message name (default: derived from table name)
- `--output`: Output path for the proto file (default: `src/dbx_cv_client/generated/record.proto`)

### Run Client

Stream records to the configured table:

```bash
dbx-cv-client \
  --host "https://adb-123456789.11.azuredatabricks.net" \
  --region eastus2 \
  --client-id <client-id> \
  --client-secret <client-secret> \
  --table-name catalog.schema.table \
  client
```

## Development

```bash
uv pip install -e ".[dev]"
uv run pytest
```

