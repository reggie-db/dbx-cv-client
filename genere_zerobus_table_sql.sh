#!/usr/bin/env sh
set -e

if [ "$#" -ne 1 ]; then
  echo "usage: generate_zerobus_table_sql.sh <catalog.schema.table>" >&2
  exit 1
fi

FQ_NAME="$1"

cat <<EOF
CREATE TABLE $FQ_NAME (
  id STRING,
  timestamp TIMESTAMP,
  metadata STRING,
  content BINARY
)
USING DELTA;
EOF