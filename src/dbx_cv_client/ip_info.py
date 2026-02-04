import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import aiohttp

from dbx_cv_client import logger
from dbx_cv_client.options import CamReaderOptions

LOG = logger(__name__)


@dataclass
class _Record:
    fetched_at: datetime
    ip_info: dict


_CACHE_LOCK = asyncio.Lock()
_CACHE: dict[str, _Record] = {}


async def get(
    client_options: CamReaderOptions,
    refresh_interval: timedelta = timedelta(minutes=15),
) -> dict | None:
    url = client_options.metadata_ip_info_url
    if not url:
        return None

    def _get():
        cache_record = _CACHE.get(url, None)
        return (
            cache_record
            if cache_record is not None
            and datetime.now() - cache_record.fetched_at < refresh_interval
            else None
        )

    record = _get()
    if record is None:
        async with _CACHE_LOCK:
            record = _CACHE.get(url, None)
            if record is None:
                data = await _fetch_ip_info(
                    url,
                    client_options.metadata_ip_info_attempts,
                    client_options.metadata_ip_info_retry_interval,
                )
                record = _Record(fetched_at=datetime.now(), ip_info=data)
                _CACHE[url] = record
    return record.ip_info


async def _fetch_ip_info(
    url: str, max_attempts: int | None, retry_interval: float
) -> dict | None:
    async with aiohttp.ClientSession() as session:
        attempt_idx = 0

        def _log(level: int, msg: str, exc_info: bool | None = None, **kwargs) -> None:
            log_data = {
                "url": url,
                "attempt": attempt_idx + 1,
                "max_attempts": max_attempts,
                "retry_interval": retry_interval,
                **kwargs,
            }
            LOG.log(
                level,
                msg + " - %s",
                " ".join(f"{k}:{v}" for k, v in log_data.items()),
                exc_info=exc_info,
            )

        while True:
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    ip_info_data = await resp.json()
                    _log(level=logging.DEBUG, msg="IP info fetched", data=ip_info_data)
                    return ip_info_data
            except Exception as e:
                if max_attempts is None or attempt_idx < (max_attempts - 1):
                    _log(
                        level=logging.WARNING,
                        msg="IP info request failed",
                        error=e,
                    )
                    await asyncio.sleep(retry_interval)
                    attempt_idx += 1
                else:
                    _log(
                        level=logging.ERROR,
                        msg="IP info lookup failed",
                        exc_info=True,
                    )
                    return None
