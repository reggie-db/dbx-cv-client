"""Camera reader base class and factory for creating readers."""

import asyncio
import os
import re
from abc import ABC, abstractmethod
from typing import AsyncIterable
from urllib.parse import urlparse

from dbx_cv_client import logger
from dbx_cv_client.options import ClientOptions, MerakiOptions

LOG = logger(__name__)


def create_cam_reader(
    client_options: ClientOptions,
    stop: asyncio.Event,
    ready: asyncio.Event,
    source: str,
    stream_id: str | None = None,
    meraki_options: MerakiOptions | None = None,
) -> "CamReader":
    # Directory source - replay local images
    if os.path.isdir(source):
        from dbx_cv_client.cam_reader.directory_reader import DirectoryReader

        LOG.info(f"Creating directory reader for source: {source}")
        return DirectoryReader(
            client_options=client_options,
            stop=stop,
            ready=ready,
            source=source,
            stream_id=stream_id,
        )

    # RTSP/RTSPS source - stream via FFmpeg
    if re.match(r"^rtsps?://", source):
        from dbx_cv_client.cam_reader.rtsp_reader import RTSPReader

        LOG.info(f"Creating RTSP reader for source: {source}")
        return RTSPReader(
            client_options=client_options,
            stop=stop,
            ready=ready,
            source=source,
            stream_id=stream_id,
        )

    # Default to Meraki API source
    from dbx_cv_client.cam_reader.meraki_reader import MerakiReader

    LOG.info(f"Creating Meraki reader for source: {source}")
    return MerakiReader(
        meraki_options=meraki_options,
        stop=stop,
        ready=ready,
        client_options=client_options,
        source=source,
        stream_id=stream_id,
    )


class CamReader(ABC):
    """Base class for camera readers."""

    def __init__(
        self,
        client_options: ClientOptions,
        stop: asyncio.Event,
        ready: asyncio.Event,
        source: str,
        stream_id: str | None = None,
    ):
        self.client_options = client_options
        self.stop = stop
        self.ready = ready
        self.source = source
        if not source:
            raise ValueError("source required")
        self._stream_id = stream_id
        self._frame: bytes | None = None
        self.produce_count = 0
        self.consume_count = 0

    @property
    def stream_id(self) -> str:
        if self._stream_id:
            return self._stream_id
        try:
            parsed = urlparse(self.source)
            path = parsed.path or ""
            parts = [p for p in path.split("/") if p]
            if parts:
                joined = "_".join(parts)
                cleaned = re.sub(r"[^0-9A-Za-z]+", "_", joined)
                return cleaned or self.source
        except Exception:
            pass

        return self.source

    @property
    def device_info(self) -> dict | None:
        """Device info from source API (if available)."""
        return None

    async def run(self) -> None:
        """Run the reader, setting ready event when frames are available."""
        async for frame in self._read():
            LOG.debug(
                f"Received frame - stream_id:%s size:%d", self.stream_id, len(frame)
            )
            self._frame = frame
            self.ready.set()
            self.produce_count += 1

    def get_frame(self) -> bytes | None:
        """Get and clear the current frame, or None if no frame available."""
        if frame := self._frame:
            self._frame = None
            self.consume_count += 1
            return frame
        return None

    @abstractmethod
    async def _read(self) -> AsyncIterable[bytes]:
        """Yield frames from the camera source."""
        ...

    def __str__(self):
        redacted = re.sub(r"://.*?@", "://[REDACTED]@", self.source)
        return f"{type(self).__name__}[{redacted}]"
