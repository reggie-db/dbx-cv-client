"""Streaming client for ingesting camera frames to Databricks via Zerobus."""

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from functools import cache, lru_cache
from typing import Annotated, Any

import aiohttp
import cyclopts
from zerobus.sdk import StreamConfigurationOptions, TableProperties
from zerobus.sdk.aio import ZerobusStream
from zerobus.sdk.aio.zerobus_sdk import grpc
from zerobus.sdk.shared import OAuthHeadersProvider, zerobus_service_pb2_grpc

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader, create_cam_reader
from dbx_cv_client.options import CamReaderOptions, MerakiOptions, WorkspaceOptions

LOG = logger(__name__)


async def _log_client_summary_periodically(
    client_options: CamReaderOptions,
    start_time: float,
    stop: asyncio.Event,
    cam_readers: list[CamReader],
    ingested_count_ref: list[int],
) -> None:
    """
    Periodically log a concise client summary.

    Notes:
    - Logging cadence is controlled by `ClientOptions.log_stats_interval`. If it is falsy,
      no periodic logs are emitted.
    - "frames_produced" is sourced from `CamReader.produce_count` (frames produced by readers).
    - "frames_consumed" is sourced from `CamReader.consume_count` (frames pulled by the client loop).
    - "frames_ingested" counts calls to `stream.ingest_record()`, which can exceed frames_produced
      when `ClientOptions.frame_multiplier` is enabled.
    """
    while client_options.log_stats_interval and not stop.is_set():
        try:
            await asyncio.sleep(client_options.log_stats_interval)
            run_time = time.monotonic() - start_time
            frames_produced = sum(r.produce_count for r in cam_readers)
            frames_consumed = sum(r.consume_count for r in cam_readers)
            reader_count = len(cam_readers)
            LOG.info(
                "Client summary - "
                "run_time=%.1fs "
                "readers:%s "
                "frames_produced=%d "
                "frames_consumed:%s "
                "frames_ingested=%d",
                run_time,
                reader_count,
                frames_produced,
                frames_consumed,
                ingested_count_ref[0],
            )
        except asyncio.CancelledError:
            break
        except Exception:
            LOG.error("Error logging client summary", exc_info=True)


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
    workspace_options: WorkspaceOptions,
    client_options: CamReaderOptions,
    stop: asyncio.Event,
    ready: asyncio.Event,
    cam_readers: list[CamReader],
) -> None:
    """Streams camera frames to a Databricks ingestion table."""
    import dbx_cv_client.models.record_pb2 as record_pb2

    stream = await _create_stream(workspace_options)

    try:
        ip_info_data = (
            await ip_info(client_options.metadata_ip_info_url)
            if client_options.metadata_ip_info_url
            else None
        )
        start_monotonic = time.monotonic()
        # Mutable container to share ingested count with the periodic logger task.
        ingested_count_ref: list[int] = [0]

        cam_reader_tasks: list[asyncio.Task] = [
            asyncio.create_task(_run_cam_reader(stop, cam_reader))
            for cam_reader in cam_readers
        ]
        summary_task = asyncio.create_task(
            _log_client_summary_periodically(
                client_options,
                start_time=start_monotonic,
                stop=stop,
                cam_readers=cam_readers,
                ingested_count_ref=ingested_count_ref,
            )
        )
        try:
            while not stop.is_set():
                await ready.wait()
                ready.clear()
                for cam_reader in cam_readers:
                    if frame := cam_reader.get_frame():
                        for idx in range(1 + (client_options.frame_multiplier or 0)):
                            stream_id = cam_reader.stream_id
                            if idx > 0:
                                stream_id = f"{stream_id}_{idx}"
                            metadata: dict[str, Any] = {
                                "stream_id": stream_id,
                                "source": cam_reader.source,
                                "fps": cam_reader.client_options.fps,
                                "scale": cam_reader.client_options.scale,
                            }
                            if ip_info_data:
                                metadata["ip_info"] = ip_info_data
                            if cam_reader.device_info:
                                metadata["device_info"] = cam_reader.device_info
                            metadata_json = json.dumps(metadata)
                            LOG.debug(
                                "Ingesting frame from %s - metadata:%s",
                                cam_reader,
                                metadata_json,
                            )
                            record = record_pb2.Raw(
                                id=str(uuid.uuid4()),
                                timestamp=(time.time_ns() // 1_000),
                                metadata=metadata_json,
                                content=frame,
                            )
                            # Once this returns, the record is queued for ingestion.
                            # There is no need to track or await server acknowledgements.
                            await stream.ingest_record(record)
                            ingested_count_ref[0] += 1
        finally:
            stop.set()
            summary_task.cancel()
            for cam_reader_task in cam_reader_tasks:
                cam_reader_task.cancel()
            await stream.flush()
            await asyncio.gather(
                *cam_reader_tasks, summary_task, return_exceptions=True
            )
    finally:
        await stream.close()


async def _create_stream(
    workspace_options: WorkspaceOptions,
) -> ZerobusStream:
    """Create and initialize a ZerobusStream."""
    server_endpoint = workspace_options.server_endpoint
    LOG.info(f"zerobus server_endpoint: {server_endpoint}")
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
    channel_options: list[tuple[str, Any]] = [
        ("grpc.max_send_message_length", -1),
        ("grpc.max_receive_message_length", -1),
    ]
    server_endpoint_ip, resolved = workspace_options.server_endpoint_ip()
    if not resolved:
        target = f"{server_endpoint_ip}:443"
        LOG.info(f"zerobus target: {target}")
        channel_options.extend(
            [
                ("grpc.ssl_target_name_override", server_endpoint),
                ("grpc.default_authority", server_endpoint),
            ]
        )
    else:
        target = server_endpoint

    channel = grpc.aio.secure_channel(
        target,
        grpc.ssl_channel_credentials(),
        options=channel_options,
    )

    stub = zerobus_service_pb2_grpc.ZerobusStub(channel)
    stream_configuration_options = StreamConfigurationOptions()

    stream = ZerobusStream(
        stub=stub,
        headers_provider=headers_provider,
        table_properties=table_properties,
        options=stream_configuration_options,
    )

    await stream._initialize()
    return stream


@lru_cache(maxsize=None)
async def ip_info(
    url: str, max_attempts: int = 3, retry_interval: float = 1
) -> dict | None:
    async with aiohttp.ClientSession() as session:
        attempt = 0
        while True:
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            except Exception:
                if attempt < max_attempts:
                    attempt += 1
                    LOG.warning(
                        f"Failed to fetch IP info, retrying in {retry_interval} second{'s' if retry_interval > 1 else ''}: {url}"
                    )
                    await asyncio.sleep(retry_interval)
                else:
                    LOG.warning("Failed to fetch IP info", exc_info=True)
                    return None


def run(
    workspace_options: WorkspaceOptions,
    client_options: CamReaderOptions = CamReaderOptions(),
    meraki_options: MerakiOptions | None = None,
    sources: Annotated[
        list[str] | None,
        cyclopts.Parameter(
            name="source",
            env_var="SOURCES",
            help="Sources for camera frames",
            negative_iterable="",
        ),
    ] = None,
) -> None:
    """Runs the async streaming client until interrupted."""

    if not sources:
        raise ValueError("At least one source is required")

    stop = asyncio.Event()
    ready = asyncio.Event()
    try:
        cam_readers = [
            create_cam_reader(
                client_options=client_options,
                meraki_options=meraki_options,
                stop=stop,
                ready=ready,
                source=source,
            )
            for source in sources
        ]
        LOG.info(
            f"Running streaming client for {sources} with process id: {os.getpid()}"
        )
        asyncio.run(
            _run(
                workspace_options=workspace_options,
                client_options=client_options,
                stop=stop,
                ready=ready,
                cam_readers=cam_readers,
            )
        )
    except KeyboardInterrupt:
        LOG.info("Interrupted, shutting down...")
    finally:
        stop.set()
