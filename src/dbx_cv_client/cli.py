"""CLI entry point for streaming data to Databricks ingestion tables."""

import logging

import typer

from dbx_cv_client.commands.client import run as client_run
from dbx_cv_client.commands.common import WorkspaceOptions
from dbx_cv_client.commands.generate_proto import run as generate_proto_run

logging.basicConfig(level=logging.INFO)

app = typer.Typer()


@app.command()
def generate_proto(
    compile_if_exists: bool = typer.Option(
        default=True,
        envvar="DATABRICKS_PROTO_COMPILE_IF_EXISTS",
        help="Recompile proto even if Python bindings already exist",
    ),
):
    """Compile the proto file to Python bindings."""
    generate_proto_run(compile_if_exists=compile_if_exists)


@app.command()
def client(
    host: str = typer.Option(
        envvar="DATABRICKS_HOST",
        help="Databricks workspace URL or host",
    ),
    region: str = typer.Option(
        envvar="DATABRICKS_REGION",
        help="Cloud region for the Zerobus endpoint",
    ),
    client_id: str = typer.Option(
        envvar="DATABRICKS_CLIENT_ID",
        help="OAuth client ID",
    ),
    client_secret: str = typer.Option(
        envvar="DATABRICKS_CLIENT_SECRET",
        help="OAuth client secret",
    ),
    table_name: str = typer.Option(
        envvar="DATABRICKS_TABLE_NAME",
        help="Fully qualified table name (catalog.schema.table)",
    ),
):
    """Run the streaming client to ingest records."""
    workspace_options = WorkspaceOptions(
        host=host,
        region=region,
        client_id=client_id,
        client_secret=client_secret,
        table_name=table_name,
    )
    generate_proto_run(compile_if_exists=False)
    client_run(workspace_options)


if __name__ == "__main__":
    app()
