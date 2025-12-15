"""Meraki camera reader for capturing snapshots via the Dashboard API."""

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterable

import aiohttp
import cv2
import numpy as np

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader
from dbx_cv_client.options import MerakiOptions

LOG = logger(__name__)


class MerakiReader(CamReader):
    """Async reader for capturing snapshots from Meraki cameras."""

    def __init__(
        self,
        stop: asyncio.Event,
        ready: asyncio.Event,
        fps: int,
        scale: int,
        source: str,
        meraki_options: MerakiOptions,
    ):
        """
        Args:
            stop: Event to signal stop.
            ready: Event to signal frame ready.
            fps: Target frames per second.
            scale: Target image height (0 to disable resize).
            source: Meraki device serial number.
            meraki_options: Meraki API configuration.
        """
        super().__init__(stop, ready, fps, scale, source)

        self.meraki_options = meraki_options
        self._session: aiohttp.ClientSession | None = None

    async def _read(self) -> AsyncIterable[bytes]:
        """Yield camera frames at the configured FPS rate."""
        frame_interval = 1.0 / self.fps if self.fps > 0 else 1.0

        try:
            while not self.stop.is_set():
                start_time = asyncio.get_event_loop().time()

                frame = await self._request_snapshot()
                if self.scale > 0:
                    frame = self._resize_image(frame)
                yield frame

                elapsed = asyncio.get_event_loop().time() - start_time
                sleep_time = max(0, frame_interval - elapsed)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    async def _request_snapshot(
        self,
        timestamp: str | None = None,
        fullframe: bool = True,
        max_retries: int = 20,
        retry_delay: float = 1.0,
    ) -> bytes:
        """Request a snapshot and poll until ready."""
        session = await self._get_session()

        payload = {"fullframe": fullframe}
        if timestamp is not None:
            payload["timestamp"] = timestamp

        url = f"{self.meraki_options.api_base_url}/devices/{self.source}/camera/generateSnapshot"

        async with session.post(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                raise aiohttp.ClientError(
                    f"Snapshot request failed: {resp.status} - {error_text}"
                )
            snapshot_info = await resp.json()

        image_url = snapshot_info.get("url")
        if not image_url:
            raise ValueError(f"API response missing 'url': {snapshot_info}")

        # Parse expiry if present
        exp_dt = None
        if expiry := snapshot_info.get("expiry"):
            try:
                exp_dt = datetime.fromisoformat(expiry)
                if exp_dt.tzinfo is None:
                    exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        # Poll until image is ready
        for attempt in range(max_retries):
            if exp_dt and datetime.now(tz=exp_dt.tzinfo) >= exp_dt:
                raise TimeoutError(f"Snapshot URL expired at {expiry}")

            if self.stop.is_set():
                raise asyncio.CancelledError("Stop requested")

            async with session.get(
                image_url, timeout=aiohttp.ClientTimeout(total=30)
            ) as img_resp:
                if img_resp.status == 200:
                    return await img_resp.read()
                elif img_resp.status != 404:
                    raise aiohttp.ClientError(f"Unexpected status {img_resp.status}")

            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)

        raise TimeoutError(f"Snapshot not ready after {max_retries} attempts")

    def _resize_image(self, image_bytes: bytes) -> bytes:
        """Resize image to target height (scale) maintaining aspect ratio."""
        img_array = np.frombuffer(image_bytes, dtype=np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

        h, w = img.shape[:2]
        new_h = self.scale
        new_w = int(w * (new_h / h))

        resized = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)
        _, encoded = cv2.imencode(".jpg", resized)
        return encoded.tobytes()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "X-Cisco-Meraki-API-Key": await self.meraki_options.cisco_meraki_api_key(),
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
            )
        return self._session
