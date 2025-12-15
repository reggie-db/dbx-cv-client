"""Camera reader base class and factory for creating readers."""

import asyncio
import re
from abc import ABC, abstractmethod
from typing import AsyncIterable

from dbx_cv_client import logger
from dbx_cv_client.options import MerakiOptions

LOG = logger(__name__)


def create_cam_reader(
    stop: asyncio.Event,
    ready: asyncio.Event,
    fps: int,
    scale: int,
    meraki_options: MerakiOptions,
    rtsp_ffmpeg_args: list[str],
    source: str,
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
            rtsp_ffmpeg_args=rtsp_ffmpeg_args,
        )
    else:
        from dbx_cv_client.cam_reader.meraki_reader import MerakiReader

        LOG.info(f"Creating Meraki reader for source: {source}")
        return MerakiReader(
            stop=stop,
            ready=ready,
            fps=fps,
            scale=scale,
            source=source,
            meraki_options=meraki_options,
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
    ):
        self.stop = stop
        self.ready = ready
        self.fps = fps
        self.scale = scale
        self.source = source
        if not source:
            raise ValueError("source required")
        self._frame: bytes | None = None

    async def run(self) -> None:
        """Run the reader, setting ready event when frames are available."""
        async for frame in self._read():
            self._frame = frame
            self.ready.set()
            LOG.info(f"Produced frame {self.source}")

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
