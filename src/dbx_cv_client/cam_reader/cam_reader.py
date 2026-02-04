"""Camera reader base class and factory for creating readers."""

import asyncio
import os
import re
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable
from dataclasses import dataclass, field
from typing import AsyncIterator
from urllib.parse import urlparse

import cv2
import numpy as np

from dbx_cv_client import logger
from dbx_cv_client.options import CamReaderOptions, MerakiOptions

LOG = logger(__name__)


def create_cam_reader(
    client_options: CamReaderOptions,
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


@dataclass(kw_only=True)
class CamReader(ABC):
    """Base class for camera readers."""

    client_options: CamReaderOptions
    stop: asyncio.Event
    ready: asyncio.Event
    source: str
    stream_id: str | None = None

    # Internal state and counters.
    produce_count: int = field(init=False, default=0)
    consume_count: int = field(init=False, default=0)
    _frame: bytes | None = field(init=False, default=None)
    _device_info: dict | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        """
        Initialize internal state after dataclass construction.

        This preserves the original `__init__` validation and internal attribute layout.
        """
        if not self.stream_id:
            try:
                parsed = urlparse(self.source)
                path = parsed.path or ""
                parts = [p for p in path.split("/") if p]
                if parts:
                    joined = "_".join(parts)
                    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", joined)
                    if cleaned:
                        self.stream_id = cleaned
                    else:
                        self.stream_id = self.source
            except Exception:
                pass

    @property
    def device_info(self) -> dict:
        """Device info from source API (if available)."""
        return self._device_info or {}

    async def run(self) -> None:
        """Run the reader, setting ready event when frames are available."""
        LOG.debug(
            "Run started %s",
            self,
        )
        async for frame in self._read():
            LOG.debug(
                "Received frame %s - size:%d ",
                self,
                len(frame),
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
    def _read(self) -> AsyncIterator[bytes]:
        """Yield frames from the camera source."""
        ...

    def _resize_image(self, img: np.ndarray) -> bytes | None:
        scale = self.client_options.scale
        if not scale:
            return None

        h, w = img.shape[:2]
        if h == scale:
            return None

        new_h = scale
        new_w = int(w * (new_h / h))

        resized = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)
        ok, buf = cv2.imencode(".jpg", resized)
        if not ok:
            raise ValueError("Failed to encode image")

        return buf.tobytes()

    def __str__(self):
        redacted_source = re.sub(r"://.*?@", "://[REDACTED]@", self.source)
        return f"{type(self).__name__}[{redacted_source}]"
