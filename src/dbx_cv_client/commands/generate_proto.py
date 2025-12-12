"""Generate protobuf definitions from Unity Catalog table schemas."""

import os
import pathlib
import re

import grpc_tools.protoc as protoc
import typer
from zerobus.tools.generate_proto import (
    extract_columns,
    fetch_table_info,
    generate_proto_file,
    get_oauth_token,
)

from dbx_cv_client import generated_dir, logger
from dbx_cv_client.commands.common import CommandOptions

LOG = logger(__name__)


def generate_proto(
    command_args: CommandOptions, proto_msg: str | None, output: pathlib.Path | None
) -> None:
    """Fetches table schema from Unity Catalog and generates a proto2 file."""
    proto_msg = proto_msg or _generate_proto_message(command_args.table_name)
    output = _resolve_output_path(output)

    LOG.info("Fetching OAuth token...")
    token = get_oauth_token(
        command_args.workspace_url, command_args.client_id, command_args.client_secret
    )

    LOG.info(f"Fetching table info for: {command_args.table_name}")
    table_info = fetch_table_info(
        command_args.workspace_url, token, command_args.table_name
    )

    columns = extract_columns(table_info)
    LOG.info(f"Extracted {len(columns)} columns from table schema")

    generate_proto_file(proto_msg, columns, output)
    LOG.info(f"Generated proto file: {output}")

    _compile_proto_to_python(output)


def _generate_proto_message(table_name: str) -> str:
    """
    Derives a TitleCase proto message name from a table name.

    Extracts the table identifier, splits by non-alphanumeric chars and camelCase
    boundaries, then combines in TitleCase. Strips trailing version suffixes (v1, V2).

    Example: "catalog.schema.user_events_v2" -> "UserEvents"
    """
    last_section = table_name.split(".")[-1]
    parts = re.split(r"[^a-zA-Z0-9]+", last_section)

    words = []
    for part in parts:
        if part:
            camel_split = re.split(r"(?<=[a-z])(?=[A-Z])", part)
            words.extend(camel_split)

    while words and re.fullmatch(r"[vV]\d+", words[-1]):
        words.pop()

    proto_msg = "".join(word.title() for word in words if word)
    if not proto_msg:
        raise typer.BadParameter(
            f"Unable to generate proto_msg from table_name: {table_name}"
        )
    return proto_msg


def _resolve_output_path(output: pathlib.Path | None) -> pathlib.Path:
    """Resolves the output path, creating the directory and __init__.py if needed."""
    if not output:
        output = generated_dir() / "record.proto"

    output.parent.mkdir(parents=True, exist_ok=True)
    init_file = output.parent / "__init__.py"
    if not init_file.exists():
        init_file.touch()

    return output


def _compile_proto_to_python(output: pathlib.Path) -> None:
    """Compiles a proto file to Python using grpc_tools."""
    python_out = output.parent
    proto_path = output.parent
    proto_file = os.path.basename(output)

    LOG.info(f"Compiling {proto_file} to Python...")
    exit_code = protoc.main(
        [
            "protoc",
            f"--python_out={python_out}",
            f"--proto_path={proto_path}",
            proto_file,
        ]
    )
    if exit_code != 0:
        raise RuntimeError(f"Failed to compile proto file, exit code: {exit_code}")
    LOG.info(f"Compiled proto file: {output}")
