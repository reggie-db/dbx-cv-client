"""Camera reader base class and factory for creating readers."""

import asyncio
import re
from abc import ABC, abstractmethod
from typing import AsyncIterable
from urllib.parse import urlparse

from dbx_cv_client import logger
from dbx_cv_client.options import MerakiOptions

LOG = logger(__name__)


def create_cam_reader(
    stop: asyncio.Event,
    ready: asyncio.Event,
    fps: int,
    scale: int,
    source: str,
    stream_id: str | None = None,
    meraki_options: MerakiOptions | None = None,
    rtsp_ffmpeg_args: list[str] | None = None,
) -> "CamReader":
    if re.match(r"^rtsps?://", source):
        from dbx_cv_client.cam_reader.rtsp_reader import RTSPReader

        LOG.info(f"Creating RTSP reader for source: {source}")
        return RTSPReader(
            stop=stop,
            ready=ready,
            fps=fps,
            scale=scale,
            source=source,
            stream_id=stream_id,
            rtsp_ffmpeg_args=rtsp_ffmpeg_args,
        )
    else:
        from dbx_cv_client.cam_reader.meraki_reader import MerakiReader

        LOG.info(f"Creating Meraki reader for source: {source}")
        return MerakiReader(
            meraki_options=meraki_options,
            stop=stop,
            ready=ready,
            fps=fps,
            scale=scale,
            source=source,
            stream_id=stream_id,
        )


class CamReader(ABC):
    """Base class for camera readers."""

    def __init__(
        self,
        stop: asyncio.Event,
        ready: asyncio.Event,
        fps: int,
        scale: int,
        source: str,
        stream_id: str | None = None,
    ):
        self.stop = stop
        self.ready = ready
        self.fps = fps
        self.scale = scale
        self.source = source
        if not source:
            raise ValueError("source required")
        self._stream_id = stream_id
        self._frame: bytes | None = None

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

    async def run(self) -> None:
        """Run the reader, setting ready event when frames are available."""
        async for frame in self._read():
            self._frame = frame
            self.ready.set()
            LOG.info(f"Produced frame {self}")

    def get_frame(self) -> bytes | None:
        """Get and clear the current frame, or None if no frame available."""
        if frame := self._frame:
            self._frame = None
            return frame
        return None

    @abstractmethod
    async def _read(self) -> AsyncIterable[bytes]:
        """Yield frames from the camera source."""
        ...

    def __str__(self):
        redacted = re.sub(r"://.*?@", "://[REDACTED]@", self.source)
        return f"{type(self).__name__}[{redacted}]"
