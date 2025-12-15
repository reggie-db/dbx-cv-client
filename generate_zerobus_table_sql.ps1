<#
.SYNOPSIS
    Generate SQL for creating a Zerobus ingestion table.
.PARAMETER TableName
    Fully qualified table name (catalog.schema.table).
.EXAMPLE
    .\generate_zerobus_table_sql.ps1 my_catalog.my_schema.my_table
#>

param(
    [Parameter(Mandatory=$true, Position=0)]
    [string]$TableName
)

@"
CREATE TABLE $TableName (
  id STRING,
  timestamp TIMESTAMP,
  metadata STRING,
  content BINARY
)
USING DELTA;
"@

