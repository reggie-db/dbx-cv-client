"""Generate protobuf definitions from Unity Catalog table schemas."""

import pathlib

import grpc_tools.protoc as protoc

from dbx_cv_client import logger, models

LOG = logger(__name__)


def run(compile_if_exists: bool) -> None:
    """Fetches table schema from Unity Catalog and generates a proto2 file."""
    models_dir = pathlib.Path(models.__file__).parent
    """Compiles a proto file to Python using grpc_tools."""
    python_out = models_dir
    proto_path = models_dir
    proto_file_name = "record.proto"
    proto_file = proto_path / proto_file_name
    proto_file_python = proto_path / (f"{proto_file.stem}_pb2.py")
    if not compile_if_exists and proto_file_python.exists():
        LOG.info(f"Proto file already exists: {proto_file_python}")
        return

    LOG.info(f"Compiling {proto_file} to {proto_file_python}...")
    exit_code = protoc.main(
        [
            "protoc",
            f"--python_out={python_out}",
            f"--proto_path={proto_path}",
            proto_file_name,
        ]
    )
    if exit_code != 0:
        raise RuntimeError(f"Failed to compile proto file, exit code: {exit_code}")
    LOG.info(f"Compiled python files to: {python_out}")
