"""Directory-based camera reader for replaying local image files."""

import asyncio
import random
from dataclasses import asdict
from pathlib import Path
from typing import AsyncIterable

import cv2

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader
from dbx_cv_client.options import ClientOptions

LOG = logger(__name__)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp", ".tiff", ".tif"}


class DirectoryReader(CamReader):
    """Async reader for replaying image files from a directory at fixed FPS."""

    def __init__(
        self,
        client_options: ClientOptions,
        stop: asyncio.Event,
        ready: asyncio.Event,
        source: str,
        stream_id: str | None = None,
    ):
        # Default to 5fps for directory reader
        if not client_options.fps or client_options.fps <= 0:
            client_options = ClientOptions(**asdict(client_options), fps=5)
        super().__init__(client_options, stop, ready, source, stream_id)

        self._image_files: list[Path] = []
        self._device_info: dict | None = None

    @property
    def stream_id(self) -> str:
        if self._stream_id:
            return self._stream_id
        # Use directory name as stream ID
        return Path(self.source).name or "directory"

    @property
    def device_info(self) -> dict | None:
        """Device info (directory metadata)."""
        return self._device_info

    async def _read(self) -> AsyncIterable[bytes]:
        """Yield frames from images in the directory at configured FPS."""
        frame_interval = 1.0 / self.client_options.fps

        # Discover and shuffle images
        self._discover_images()
        if not self._image_files:
            LOG.error(f"No image files found in {self.source}")
            return

        self._device_info = {
            "type": "directory",
            "path": self.source,
            "image_count": len(self._image_files),
        }

        LOG.info(
            f"DirectoryReader starting: {len(self._image_files)} images at {self.client_options.fps}fps"
        )

        idx = 0
        while not self.stop.is_set():
            start_time = asyncio.get_event_loop().time()

            # Loop over images
            image_path = self._image_files[idx % len(self._image_files)]
            idx += 1

            # Reshuffle when we've cycled through all images
            if idx > 0 and idx % len(self._image_files) == 0:
                random.shuffle(self._image_files)
                LOG.debug("Reshuffled image list")

            try:
                frame = await self._read_and_resize(image_path)
                if frame:
                    yield frame
            except Exception as e:
                LOG.warning(f"Failed to read {image_path}: {e}")

            elapsed = asyncio.get_event_loop().time() - start_time
            sleep_time = max(0, int(frame_interval - elapsed))
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    def _discover_images(self) -> None:
        """Find all image files in the directory and shuffle them."""
        source_path = Path(self.source)
        if not source_path.is_dir():
            raise ValueError(f"Source is not a directory: {self.source}")

        self._image_files = [
            f
            for f in source_path.iterdir()
            if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
        ]
        random.shuffle(self._image_files)
        LOG.info(f"Discovered {len(self._image_files)} images in {self.source}")

    async def _read_and_resize(self, path: Path) -> bytes | None:
        """Read an image file and optionally resize it."""
        # Run blocking IO in executor
        loop = asyncio.get_event_loop()
        img = await loop.run_in_executor(
            None, lambda: cv2.imread(str(path), cv2.IMREAD_COLOR)
        )

        if img is None:
            return None

        # Resize if scale is set
        if scale := self.client_options.scale:
            h, w = img.shape[:2]
            if h != scale:
                new_h = scale
                new_w = int(w * (new_h / h))
                img = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)

        # Encode to JPEG
        ok, buf = cv2.imencode(".jpg", img)
        if not ok:
            return None

        return buf.tobytes()
