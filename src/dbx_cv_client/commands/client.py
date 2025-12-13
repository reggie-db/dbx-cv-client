"""Streaming client for ingesting RTSP frames to Databricks."""

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field

from zerobus.sdk import TableProperties
from zerobus.sdk.aio import ZerobusSdk

from dbx_cv_client import logger
from dbx_cv_client.commands.common import WorkspaceOptions

_JPEG_START = b"\xff\xd8"
_JPEG_END = b"\xff\xd9"

LOG = logger(__name__)


@dataclass
class StreamContext:
    ready: asyncio.Event
    source: str
    fps: int = field(default=1)
    scale: int = field(default=480)
    stop: asyncio.Event = field(init=False, default_factory=asyncio.Event)
    _frame: bytes = field(init=False, default=None)

    async def read_frame(self) -> bytes | None:
        if frame := self._frame:
            self._frame = None
            return frame
        return None

    async def write_frame(self, frame: bytes):
        self._frame = frame
        self.ready.set()


async def _stream_frames(ffmpeg_args: list[str], stream_context: StreamContext) -> None:
    cmds = [
        "ffmpeg",
    ]
    if ffmpeg_args:
        cmds.extend(ffmpeg_args)
    cmds.extend(
        [
            ("-loglevel", "error"),
            "-nostats",
            ("-fflags", "nobuffer"),
            ("-flags", "low_delay"),
            ("-rtsp_transport", "tcp"),
            "-an",
            "-sn",
            ("-i", stream_context.source),
            ("-r", str(stream_context.fps)),
            ("-vf", f"scale=-1:{stream_context.scale}"),
            ("-q:v", "5"),
            ("-f", "mjpeg"),
            "-",
        ]
    )
    proc_cmds = [
        item for part in cmds for item in (part if isinstance(part, tuple) else (part,))
    ]
    buffer = b""
    proc: asyncio.subprocess.Process | None = None
    while not stream_context.stop.is_set():
        if proc is not None:
            LOG.info("Restarting ffmpeg process")
            await asyncio.sleep(1)
        proc = await asyncio.create_subprocess_exec(
            *proc_cmds, stdout=asyncio.subprocess.PIPE, stderr=None
        )
        while not stream_context.stop.is_set():
            chunk = await proc.stdout.read(4096)
            if not chunk:
                break
            buffer += chunk
            while True:
                start = buffer.find(_JPEG_START)
                end = buffer.find(_JPEG_END)
                if start == -1 or end == -1:
                    break
                frame = buffer[start : end + 2]
                buffer = buffer[end + 2 :]
                await stream_context.write_frame(frame)
                LOG.info(f"Wrote frame {stream_context.source}")


async def _run(
    workspace_options: WorkspaceOptions,
    flush_interval: float,
    ffmpeg_args: list[str],
    sources: list[str],
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
        ready = asyncio.Event()
        stream_contexts = [
            StreamContext(ready=ready, source=source) for source in sources
        ]
        try:
            stream_tasks = [
                asyncio.create_task(_stream_frames(ffmpeg_args, stream_context))
                for stream_context in stream_contexts
            ]
            while True:
                await ready.wait()
                ready.clear()
                for stream_context in stream_contexts:
                    if frame := await stream_context.read_frame():
                        metadata = {
                            "source": stream_context.source,
                            "fps": stream_context.fps,
                            "scale": stream_context.scale,
                        }
                        record = record_pb2.Raw(
                            id=str(uuid.uuid4()),
                            timestamp=(time.time_ns() // 1_000),
                            metadata=json.dumps(metadata),
                            content=frame,
                        )
                        await stream.ingest_record(record)
                        LOG.info(f"Ingested frame {stream_context.source}")
                        count += 1

                now = time.monotonic()
                if now - last_flush >= flush_interval:
                    await stream.flush()
                    LOG.info(f"Flushed {count} frames")
                    last_flush = now
        finally:
            try:
                for stream_context in stream_contexts:
                    stream_context.stop.set()
            finally:
                for stream_task in stream_tasks:
                    stream_task.cancel()
                await asyncio.gather(*stream_tasks, return_exceptions=True)
    finally:
        await stream.close()


def run(
    workspace_options: WorkspaceOptions,
    flush_interval: float,
    ffmpeg_args: list[str],
    sources: list[str],
) -> None:
    """Runs the async streaming client until interrupted."""
    try:
        LOG.info(
            f"Running streaming client for {sources} with process id: {os.getpid()}"
        )
        asyncio.run(_run(workspace_options, flush_interval, ffmpeg_args, sources))
    except KeyboardInterrupt:
        LOG.info("Interrupted, shutting down...")
