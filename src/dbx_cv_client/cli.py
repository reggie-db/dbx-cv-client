"""CLI entry point for streaming data to Databricks ingestion tables."""

import logging

import typer

from dbx_cv_client import client
from dbx_cv_client.generate_proto import run as generate_proto_run

logging.basicConfig(level=logging.INFO)

app = typer.Typer(rich_markup_mode=None)


@app.command()
def generate_proto(
    compile_if_exists: bool = typer.Option(
        default=True,
        envvar="PROTO_COMPILE_IF_EXISTS",
        help="Recompile proto even if Python bindings already exist",
    ),
):
    """Compile the proto file to Python bindings."""
    generate_proto_run(compile_if_exists=compile_if_exists)


app.command("client")(client.run)


if __name__ == "__main__":
    app()
