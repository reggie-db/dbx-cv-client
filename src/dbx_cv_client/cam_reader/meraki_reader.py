"""Meraki camera reader for capturing snapshots via the Dashboard API."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import aiohttp
import cv2
import numpy as np

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader
from dbx_cv_client.options import MerakiOptions

LOG = logger(__name__)


@dataclass(kw_only=True)
class MerakiReader(CamReader):
    """Async reader for capturing snapshots from Meraki cameras."""

    meraki_options: MerakiOptions

    _device_info_lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)

    async def _read(self) -> AsyncIterator[bytes]:
        """Yield camera frames at the configured FPS rate."""
        fps = self.cam_reader_options.fps
        frame_interval = 1.0 / fps
        session = aiohttp.ClientSession(
            headers={
                "X-Cisco-Meraki-API-Key": await self.meraki_options.cisco_meraki_api_key(),
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        try:
            # Fetch device info on the first read
            if self._device_info is None:
                async with self._device_info_lock:
                    if self._device_info is None:
                        self._device_info = await self._fetch_device_info(session)

            while not self.stop.is_set():
                start_time = asyncio.get_event_loop().time()

                data, img = await self._request_snapshot(session)
                yield self._resize_image(img) or data

                elapsed = asyncio.get_event_loop().time() - start_time
                sleep_time = max(0, int(frame_interval - elapsed))
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
        finally:
            if not session.closed:
                await session.close()

    async def _request_snapshot(
        self,
        session: aiohttp.ClientSession,
        timestamp: str | None = None,
        full_frame: bool = True,
        max_retries: int = 20,
        retry_delay: float = 1.0,
    ) -> tuple[bytes, np.ndarray]:
        """Request a snapshot and poll until ready."""

        payload: dict[str, Any] = {"fullframe": full_frame}
        if timestamp is not None:
            payload["timestamp"] = timestamp

        url = f"{self.meraki_options.api_base_url}/devices/{self.source}/camera/generateSnapshot"

        async with session.post(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status // 100 != 2:
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
            if attempt > 0:
                await asyncio.sleep(retry_delay)
            LOG.info(f"Polling for snapshot: {image_url} attempt {attempt}")
            if exp_dt and datetime.now(tz=exp_dt.tzinfo) >= exp_dt:
                raise TimeoutError(f"Snapshot URL expired at {expiry}")

            if self.stop.is_set():
                raise asyncio.CancelledError("Stop requested")

            async with session.get(
                image_url,
                headers={"Accept": "*/*"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as img_resp:
                if img_resp.status // 100 == 2:
                    data = await img_resp.read()
                    if data:
                        try:
                            img_array = np.frombuffer(data, dtype=np.uint8)
                            img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
                            if img is not None:
                                LOG.info(
                                    f"Snapshot received: {len(data)} bytes, "
                                    f"shape={img.shape}"
                                )
                                return data, img
                            LOG.warning(
                                f"Failed to decode image: {len(data)} bytes, "
                                f"first bytes: {data[:20].hex()}"
                            )
                        except Exception as e:
                            LOG.warning(f"Failed to decode image: {e}")
                    continue
                LOG.debug(f"Unexpected status {img_resp.status}")
        raise TimeoutError(f"Snapshot not ready after {max_retries} attempts")

    async def _fetch_device_info(
        self, session: aiohttp.ClientSession, max_retries: int = 3
    ) -> dict:
        """Fetch device info from Meraki API (one-time, with retries)."""
        url = f"{self.meraki_options.api_base_url}/devices/{self.source}"
        for attempt in range(max_retries):
            try:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status // 100 == 2:
                        device_info = await resp.json()
                        LOG.info(
                            f"Device info fetched: name={device_info.get('name')}, "
                            f"model={device_info.get('model')}, "
                            f"serial={device_info.get('serial')}"
                        )
                        return device_info
                    error_text = await resp.text()
                    LOG.warning(
                        f"Device info request failed (attempt {attempt + 1}/{max_retries}): "
                        f"{resp.status} - {error_text}"
                    )
            except Exception as e:
                LOG.warning(
                    f"Device info request error (attempt {attempt + 1}/{max_retries}): {e}"
                )

            if attempt < max_retries - 1:
                await asyncio.sleep(1.0)

        LOG.warning(
            f"Failed to fetch device info after {max_retries} attempts, continuing without it"
        )
        return {}
