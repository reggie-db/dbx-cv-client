"""Directory-based camera reader for replaying local image files."""

import asyncio
import random
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import AsyncIterator

import cv2

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader

LOG = logger(__name__)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp", ".tiff", ".tif"}


@dataclass(kw_only=True)
class DirectoryReader(CamReader):
    """Async reader for replaying image files from a directory at fixed FPS."""

    def __post_init__(self) -> None:
        """
        Initialize DirectoryReader after dataclass construction.

        Notes:
        - Preserves the existing behavior of defaulting directory replay to 5 FPS when
          `client_options.fps` is not set or is non-positive.
        """
        if not self.stream_id:
            self.stream_id = Path(self.source).name
        super().__post_init__()

    async def _read(self) -> AsyncIterator[bytes]:
        """Yield frames from images in the directory at configured FPS."""
        frame_interval = 1.0 / self.client_options.fps

        # Discover and shuffle images
        image_files = self._image_files(self.source)

        LOG.info(
            f"DirectoryReader starting: {len(image_files)} images at {self.client_options.fps}fps"
        )

        idx = 0
        while not self.stop.is_set():
            start_time = asyncio.get_event_loop().time()
            # Loop over images
            image_path = image_files[idx % len(image_files)]
            idx += 1

            # Reshuffle when we've cycled through all images
            if idx > 0 and idx % len(image_files) == 0:
                random.shuffle(image_files)
                LOG.debug("Reshuffled image list")

            try:
                frame = await self._read_and_resize(image_path)
                if frame:
                    yield frame
            except Exception:
                LOG.warning(f"Failed to read {image_path}", exc_info=True)

            elapsed = asyncio.get_event_loop().time() - start_time
            sleep_time = frame_interval - elapsed
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    @staticmethod
    @lru_cache(maxsize=None)
    def _image_files(source: str) -> list[Path]:
        """Find all image files in the directory and shuffle them."""
        source_path = Path(source)
        if not source_path.is_dir():
            raise ValueError(f"Source is not a directory: {source}")

        image_files = [
            f
            for f in source_path.iterdir()
            if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
        ]
        random.shuffle(image_files)
        LOG.info(f"Discovered {len(image_files)} images in {source}")
        if not image_files:
            raise ValueError(f"No image files found in {source}")
        return image_files

    async def _read_and_resize(self, path: Path) -> bytes | None:
        """Read an image file and optionally resize it."""
        # Run blocking IO in executor
        loop = asyncio.get_event_loop()
        img = await loop.run_in_executor(None, cv2.imread, str(path), cv2.IMREAD_COLOR)
        return self._resize_image(img) if img is not None else None
