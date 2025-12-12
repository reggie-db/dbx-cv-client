"""CLI entry point for interacting with Databricks Change Vault."""

import asyncio
import logging
import pathlib

import typer

from dbx_cv_client.commands.client import run_client
from dbx_cv_client.commands.common import CommandOptions
from dbx_cv_client.commands.generate_proto import generate_proto

logging.basicConfig(level=logging.INFO)

app = typer.Typer()


@app.callback()
def _common_options(
    ctx: typer.Context,
    host: str = typer.Option(
        envvar="DATABRICKS_HOST",
        help="Databricks workspace URL or host",
    ),
    region: str | None = typer.Option(
        envvar="DATABRICKS_REGION", help="Cloud region for the Zerobus endpoint"
    ),
    client_id: str = typer.Option(
        envvar="DATABRICKS_CLIENT_ID", help="OAuth client ID"
    ),
    client_secret: str = typer.Option(
        envvar="DATABRICKS_CLIENT_SECRET", help="OAuth client secret"
    ),
    table_name: str = typer.Option(
        envvar="DATABRICKS_TABLE_NAME",
        help="Fully qualified table name (catalog.schema.table)",
    ),
):
    """Databricks Change Vault CLI client."""
    ctx.obj = CommandOptions(
        host=host,
        region=region,
        client_id=client_id,
        client_secret=client_secret,
        table_name=table_name,
    )


@app.command(name="generate-proto")
def generate_proto_command(
    ctx: typer.Context,
    proto_msg: str | None = typer.Option(
        envvar="DATABRICKS_PROTO_MSG",
        help="Proto message name (auto-generated if not provided)",
        default=None,
    ),
    output: pathlib.Path | None = typer.Option(
        envvar="DATABRICKS_PROTO_OUTPUT",
        help="Output path for the generated proto file",
        default=None,
    ),
):
    """Generate a protobuf file from a Unity Catalog table schema."""
    common_options: CommandOptions = ctx.obj
    generate_proto(common_options, proto_msg, output)


@app.command(name="client")
def client_command(ctx: typer.Context):
    """Run the streaming client to ingest records."""
    common_options: CommandOptions = ctx.obj
    asyncio.run(run_client(common_options))


if __name__ == "__main__":
    app()
