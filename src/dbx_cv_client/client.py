"""Streaming client for ingesting RTSP frames to Databricks."""

import asyncio
import json
import os
import time
import uuid

import typer
from zerobus.sdk import StreamConfigurationOptions, TableProperties
from zerobus.sdk.aio import ZerobusStream
from zerobus.sdk.aio.zerobus_sdk import grpc
from zerobus.sdk.shared import OAuthHeadersProvider, zerobus_service_pb2_grpc

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader, create_cam_reader
from dbx_cv_client.options import MerakiOptions, WorkspaceOptions

LOG = logger(__name__)


async def _run_cam_reader(
    stop: asyncio.Event,
    cam_reader: CamReader,
    retry_delay: float = 5.0,
) -> None:
    attempt = 0
    while not stop.is_set():
        try:
            await cam_reader.run()
        except Exception:
            LOG.error(
                f"Error running cam reader {cam_reader} on attempt {attempt}",
                exc_info=True,
            )
            attempt += 1
            await asyncio.sleep(retry_delay)


async def _run(
    stop: asyncio.Event,
    ready: asyncio.Event,
    flush_interval: float,
    frame_multiplier: int,
    workspace_options: WorkspaceOptions,
    cam_readers: list[CamReader],
) -> None:
    """Streams RTSP frames to a Databricks ingestion table."""
    import dbx_cv_client.models.record_pb2 as record_pb2

    stream = await _create_stream(workspace_options)

    try:
        count = 0
        last_flush = time.monotonic()
        cam_reader_tasks: list[asyncio.Task] = [
            asyncio.create_task(_run_cam_reader(stop, cam_reader))
            for cam_reader in cam_readers
        ]
        try:
            while not stop.is_set():
                await ready.wait()
                ready.clear()
                ingested = False
                for cam_reader in cam_readers:
                    if frame := cam_reader.get_frame():
                        for idx in range((0 + (frame_multiplier or 0))):
                            stream_id = cam_reader.stream_id
                            if idx > 0:
                                stream_id = f"{stream_id}_{idx}"
                            metadata = {
                                "stream_id": stream_id,
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
                            LOG.info(
                                f"Ingested frame - stream_id: {stream_id} cam_reader: {cam_reader}"
                            )
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


async def _create_stream(
    workspace_options: WorkspaceOptions,
) -> ZerobusStream:
    LOG.info(f"zerobus server_endpoint: {workspace_options.server_endpoint}")
    LOG.info(f"zerobus workspace_url: {workspace_options.workspace_url}")

    import dbx_cv_client.models.record_pb2 as record_pb2

    table_properties = TableProperties(
        workspace_options.table_name, record_pb2.Raw.DESCRIPTOR
    )

    headers_provider = OAuthHeadersProvider(
        workspace_options.workspace_id,
        workspace_options.workspace_url,
        workspace_options.table_name,
        workspace_options.client_id,
        workspace_options.client_secret,
    )
    channel_options = [
        ("grpc.max_send_message_length", -1),
        ("grpc.max_receive_message_length", -1),
    ]
    if workspace_options.zerobus_ip:
        target = f"{workspace_options.zerobus_ip}:443"
        channel_options.extend(
            [
                ("grpc.ssl_target_name_override", workspace_options.server_endpoint),
                ("grpc.default_authority", workspace_options.server_endpoint),
            ]
        )
    else:
        target = workspace_options.server_endpoint

    channel = grpc.aio.secure_channel(
        target,
        grpc.ssl_channel_credentials(),
        options=channel_options,
    )

    stub = zerobus_service_pb2_grpc.ZerobusStub(channel)

    stream = ZerobusStream(
        stub,
        headers_provider,
        table_properties,
        StreamConfigurationOptions(),
    )

    await stream._initialize()
    return stream


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
        "--rtsp-ffmpeg-arg",
        envvar="RTPSP_FFMPEG_ARGS",
        help="Additional FFmpeg arguments for RTSP sources",
    ),
    zerobus_ip: str = typer.Option(
        None,
        "--zerobus-ip",
        envvar="ZEROBUS_IP",
        help="Override DNS Zerobus host IP",
    ),
    frame_multiplier: int = typer.Option(
        0,
        "--frame-multiplier",
        envvar="FRAME_MULTIPLIER",
        help="Sends a frame multiple times",
    ),
) -> None:
    """Runs the async streaming client until interrupted."""

    workspace_options = WorkspaceOptions(
        host=host,
        region=region,
        client_id=client_id,
        client_secret=client_secret,
        table_name=table_name,
        zerobus_ip=zerobus_ip,
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
                frame_multiplier=frame_multiplier,
                workspace_options=workspace_options,
                cam_readers=cam_readers,
            )
        )
    except KeyboardInterrupt:
        LOG.info("Interrupted, shutting down...")
    finally:
        stop.set()
