"""Compile protobuf definitions to Python bindings."""

import pathlib

import grpc_tools.protoc as protoc

from dbx_cv_client import logger, models

LOG = logger(__name__)


def run(compile_if_exists: bool) -> None:
    """
    Compiles record.proto to Python bindings using grpc_tools.

    Args:
        compile_if_exists: If False, skips compilation when record_pb2.py already exists.
    """
    models_dir = pathlib.Path(models.__file__).parent
    proto_file_name = "record.proto"
    proto_file = models_dir / proto_file_name
    proto_file_python = models_dir / f"{proto_file.stem}_pb2.py"

    if not compile_if_exists and proto_file_python.exists():
        LOG.info(f"Proto file already exists: {proto_file_python}")
        return

    LOG.info(f"Compiling {proto_file} to {proto_file_python}...")
    exit_code = protoc.main(
        [
            "protoc",
            f"--python_out={models_dir}",
            f"--proto_path={models_dir}",
            proto_file_name,
        ]
    )
    if exit_code != 0:
        raise RuntimeError(f"Failed to compile proto file, exit code: {exit_code}")
    LOG.info(f"Compiled python files to: {models_dir}")
