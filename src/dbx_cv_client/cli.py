"""CLI entry point for streaming data to Databricks ingestion tables."""

import logging
from typing import Annotated

import cyclopts

from dbx_cv_client import client
from dbx_cv_client.generate_proto import run as generate_proto_run

logging.basicConfig(level=logging.INFO)

app = cyclopts.App()


@app.command
def generate_proto(
    compile_if_exists: Annotated[
        bool,
        cyclopts.Parameter(
            env_var="PROTO_COMPILE_IF_EXISTS",
            help="Recompile proto even if Python bindings already exist",
        ),
    ] = True,
):
    """Compile the proto file to Python bindings."""
    generate_proto_run(compile_if_exists=compile_if_exists)


app.command(name="client")(client.run)

if __name__ == "__main__":
    app()
