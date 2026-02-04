"""RTSP camera reader using FFmpeg for frame capture."""

import asyncio
import time
from dataclasses import dataclass
from typing import AsyncIterator

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader

_MAX_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB safety cap
_JPEG_START = b"\xff\xd8"
_JPEG_END = b"\xff\xd9"
_RESTART_DELAY = 5.0
_FRAME_TIMEOUT = 15.0
_INITIAL_FRAME_TIMEOUT = 30.0

LOG = logger(__name__)


@dataclass(kw_only=True)
class RTSPReader(CamReader):
    """Reads JPEG frames from an RTSP stream using FFmpeg."""

    async def _read(self) -> AsyncIterator[bytes]:
        while not self.stop.is_set():
            rtsp_ffmpeg_process, stderr_task = await self._start_ffmpeg_process()
            buffer = b""

            last_frame_time = time.monotonic()
            current_timeout = _INITIAL_FRAME_TIMEOUT

            try:
                while not self.stop.is_set() and rtsp_ffmpeg_process.returncode is None:
                    # Check if we've exceeded the frame timeout
                    elapsed = time.monotonic() - last_frame_time
                    if elapsed >= current_timeout:
                        LOG.warning(
                            f"No frames for {elapsed:.1f}s, restarting ffmpeg: {self}"
                        )
                        break

                    try:
                        chunk = await asyncio.wait_for(
                            rtsp_ffmpeg_process.stdout.read(4096),
                            timeout=current_timeout - elapsed,
                        )
                    except asyncio.TimeoutError:
                        LOG.warning(
                            f"Read timeout after {current_timeout}s, restarting ffmpeg: {self}"
                        )
                        break

                    if not chunk:
                        continue

                    buffer += chunk

                    # Hard safety cap to prevent unbounded buffer growth
                    if len(buffer) > _MAX_BUFFER_SIZE:
                        LOG.warning(f"Buffer overflow, resetting buffer: {self}")
                        buffer = b""
                        break

                    while not self.stop.is_set():
                        start = buffer.find(_JPEG_START)
                        if start == -1:
                            # Keep only the last byte in case it's the first half of \xff\xd8
                            buffer = buffer[-1:] if buffer else b""
                            break

                        if start > 0:
                            # Discard junk before JPEG start
                            buffer = buffer[start:]

                        end = buffer.find(_JPEG_END, start + 2)
                        if end == -1:
                            break

                        frame = buffer[start : end + 2]
                        buffer = buffer[end + 2 :]

                        last_frame_time = time.monotonic()
                        current_timeout = _FRAME_TIMEOUT

                        yield frame
            finally:
                if stderr_task is not None:
                    stderr_task.cancel()
                if (
                    rtsp_ffmpeg_process is not None
                    and rtsp_ffmpeg_process.returncode is None
                ):
                    rtsp_ffmpeg_process.terminate()
                    await rtsp_ffmpeg_process.wait()

            # Sleep before restarting ffmpeg
            if not self.stop.is_set():
                LOG.info(f"Waiting {_RESTART_DELAY}s before restarting ffmpeg: {self}")
                await asyncio.sleep(_RESTART_DELAY)

    async def _start_ffmpeg_process(
        self,
    ) -> tuple[asyncio.subprocess.Process, asyncio.Task]:
        """Start FFmpeg subprocess for RTSP stream capture."""
        cmds = [
            "ffmpeg",
        ]
        if rtsp_ffmpeg_args := self.cam_reader_options.rtsp_ffmpeg_args:
            cmds.extend(rtsp_ffmpeg_args)
        cmds.extend(
            [
                ("-loglevel", "error"),
                "-nostats",
                ("-rtsp_transport", "tcp"),
                "-an",
                "-sn",
                ("-fflags", "nobuffer"),
                ("-flags", "low_delay"),
                ("-max_delay", "500000"),
                ("-rtsp_flags", "prefer_tcp"),
                ("-reorder_queue_size", "0"),
                ("-i", self.source),
                (
                    "-vf",
                    f"fps={self.cam_reader_options.fps},scale=-1:{self.cam_reader_options.scale},format=yuvj420p",
                ),
                ("-pix_fmt", "yuvj420p"),
                ("-q:v", "7"),
                ("-threads", "1"),
                ("-f", "mjpeg"),
                "-",
            ]
        )
        proc_cmds = [
            str(item)
            for part in cmds
            for item in (part if isinstance(part, tuple) else (part,))
        ]

        LOG.info(f"Starting RTSP reader: {self}")

        process = await asyncio.create_subprocess_exec(
            *proc_cmds,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stderr_task = asyncio.create_task(self._log_ffmpeg_stderr(process))
        return process, stderr_task

    async def _log_ffmpeg_stderr(
        self, proc: asyncio.subprocess.Process, color: str = "\033[31m"
    ):
        prefix = f"[{self.stream_id}]"
        # Filter noisy decoder errors
        h264_filters = [
            "error while decoding",
            "cabac decode of qscale diff failed",
            "bad cseq c6de",
            "block unavailable",
        ]
        while True:
            line = await proc.stderr.readline()
            if not line:
                break

            try:
                msg = line.decode("utf-8", errors="ignore").strip()
            except Exception:
                continue

            if "[h264 @ " in msg and any(f in msg for f in h264_filters):
                continue

            LOG.error(f"{color}{prefix} {msg}\033[0m")
