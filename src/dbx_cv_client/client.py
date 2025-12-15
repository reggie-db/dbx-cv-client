"""Streaming client for ingesting RTSP frames to Databricks."""

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field

import typer
from zerobus.sdk import TableProperties
from zerobus.sdk.aio import ZerobusSdk

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader, create_cam_reader
from dbx_cv_client.options import MerakiOptions, WorkspaceOptions

LOG = logger(__name__)


@dataclass
class StreamContext:
    ready: asyncio.Event
    cam_reader: CamReader
    _frame: bytes | None = field(init=False, default=None)

    async def run(self) -> None:
        async for frame in self.cam_reader.read():
            self._frame = frame
            self.ready.set()

    async def frame(self) -> bytes | None:
        if frame := self._frame:
            self._frame = None
            return frame
        return None


async def _run(
    stop: asyncio.Event,
    ready: asyncio.Event,
    flush_interval: float,
    workspace_options: WorkspaceOptions,
    cam_readers: list[CamReader],
) -> None:
    """Streams RTSP frames to a Databricks ingestion table."""
    import dbx_cv_client.models.record_pb2 as record_pb2

    table_properties = TableProperties(
        workspace_options.table_name, record_pb2.Raw.DESCRIPTOR
    )

    sdk_handle = ZerobusSdk(
        workspace_options.server_endpoint,
        workspace_options.workspace_url,
    )

    stream = await sdk_handle.create_stream(
        workspace_options.client_id, workspace_options.client_secret, table_properties
    )

    try:
        count = 0
        last_flush = time.monotonic()
        cam_reader_tasks: list[asyncio.Task] = [
            asyncio.create_task(cam_reader.run()) for cam_reader in cam_readers
        ]
        try:
            while not stop.is_set():
                await ready.wait()
                ready.clear()
                ingested = False
                for cam_reader in cam_readers:
                    if frame := cam_reader.get_frame():
                        metadata = {
                            "source": cam_reader.source,
                            "fps": cam_reader.fps,
                            "scale": cam_reader.scale,
                        }
                        record = record_pb2.Raw(
                            id=str(uuid.uuid4()),
                            timestamp=(time.time_ns() // 1_000),
                            metadata=json.dumps(metadata),
                            content=frame,
                        )
                        await stream.ingest_record(record)
                        LOG.info(f"Ingested frame {cam_reader.source}")
                        count += 1
                        ingested = True
                if ingested:
                    now = time.monotonic()
                    if now - last_flush >= flush_interval:
                        await stream.flush()
                        LOG.info(f"Flushed {count} frames")
                        last_flush = now
        finally:
            stop.set()
            for cam_reader_task in cam_reader_tasks:
                cam_reader_task.cancel()
            await asyncio.gather(*cam_reader_tasks, return_exceptions=True)
    finally:
        await stream.close()


def run(
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
    flush_interval: float = typer.Option(
        default=5.0,
        envvar="FLUSH_INTERVAL",
        help="Seconds between flushes",
    ),
    fps: int = typer.Option(
        default=1,
        envvar="FPS",
        help="Frames per second",
    ),
    scale: int = typer.Option(
        default=480,
        envvar="SCALE",
        help="Scale",
    ),
    sources: list[str] = typer.Option(
        None,
        "--source",
        envvar="SOURCES",
        help="Source URLs for camera frames (RTSP, HTTP, etc.)",
    ),
    meraki_api_base_url: str | None = typer.Option(
        "https://api.meraki.com/api/v1",
        "--meraki-api-base-url",
        envvar="MERAKI_API_BASE_URL",
        help="Meraki API token",
    ),
    meraki_api_token: str | None = typer.Option(
        None,
        "--meraki-api-token",
        envvar="MERAKI_API_TOKEN",
        help="Meraki API token",
    ),
    meraki_vault_url: str | None = typer.Option(
        None,
        "--meraki-vault-url",
        envvar="MERAKI_VAULT_URL",
        help="Meraki Azure Key Vault URL",
    ),
    meraki_secret_name: str | None = typer.Option(
        None,
        "--meraki-secret-name",
        envvar="MERAKI_SECRET_NAME",
        help="Meraki Azure Key Vault secret name",
    ),
    rtsp_ffmpeg_args: list[str] = typer.Option(
        [],
        "--ffmpeg-arg",
        envvar="RTPSP_FFMPEG_ARGS",
        help="Additional FFmpeg arguments for RTSP sources",
    ),
) -> None:
    """Runs the async streaming client until interrupted."""
    workspace_options = WorkspaceOptions(
        host=host,
        region=region,
        client_id=client_id,
        client_secret=client_secret,
        table_name=table_name,
    )

    meraki_options = MerakiOptions(
        api_base_url=meraki_api_base_url,
        api_token=meraki_api_token,
        vault_url=meraki_vault_url,
        secret_name=meraki_secret_name,
    )
    if not sources:
        raise typer.BadParameter("At least one source is required")

    stop = asyncio.Event()
    ready = asyncio.Event()
    try:
        cam_readers = [
            create_cam_reader(
                stop=stop,
                ready=ready,
                fps=fps,
                scale=scale,
                meraki_options=meraki_options,
                rtsp_ffmpeg_args=rtsp_ffmpeg_args,
                source=source,
            )
            for source in sources
        ]
        LOG.info(
            f"Running streaming client for {sources} with process id: {os.getpid()}"
        )
        asyncio.run(
            _run(
                stop=stop,
                ready=ready,
                flush_interval=flush_interval,
                workspace_options=workspace_options,
                cam_readers=cam_readers,
            )
        )
    except KeyboardInterrupt:
        LOG.info("Interrupted, shutting down...")
    finally:
        stop.set()
