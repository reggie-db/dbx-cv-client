"""RTSP camera reader using FFmpeg for frame capture."""

import asyncio
import time
from typing import AsyncIterable

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader

_MAX_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB safety cap
_JPEG_START = b"\xff\xd8"
_JPEG_END = b"\xff\xd9"
_RESTART_DELAY = 5.0
_FRAME_TIMEOUT = 15.0
_INITIAL_FRAME_TIMEOUT = 30.0


LOG = logger(__name__)


class RTSPReader(CamReader):
    """Reads JPEG frames from an RTSP stream using FFmpeg."""

    def __init__(
        self,
        stop: asyncio.Event,
        ready: asyncio.Event,
        fps: int,
        scale: int,
        source: str,
        stream_id: str | None = None,
        rtsp_ffmpeg_args: list[str] | None = None,
    ):
        super().__init__(stop, ready, fps, scale, source, stream_id)
        self.buffer = b""
        self.rtsp_ffmpeg_args = rtsp_ffmpeg_args
        self._rtsp_ffmpeg_process: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task | None = None

    async def _read(self) -> AsyncIterable[bytes]:
        while not self.stop.is_set():
            process, stderr_task = await self._start_ffmpeg_process()
            self._rtsp_ffmpeg_process = process
            self._stderr_task = stderr_task
            self.buffer = b""
            last_frame_time = time.monotonic()
            current_timeout = _INITIAL_FRAME_TIMEOUT

            try:
                while (
                    not self.stop.is_set()
                    and self._rtsp_ffmpeg_process.returncode is None
                ):
                    # Check if we've exceeded the frame timeout
                    elapsed = time.monotonic() - last_frame_time
                    if elapsed >= current_timeout:
                        LOG.warning(
                            f"No frames for {elapsed:.1f}s, restarting ffmpeg: {self}"
                        )
                        break

                    try:
                        chunk = await asyncio.wait_for(
                            self._rtsp_ffmpeg_process.stdout.read(4096),
                            timeout=current_timeout - elapsed,
                        )
                    except asyncio.TimeoutError:
                        LOG.warning(
                            f"Read timeout after {current_timeout}s, restarting ffmpeg: {self}"
                        )
                        break

                    if not chunk:
                        continue

                    self.buffer += chunk

                    # Hard safety cap to prevent unbounded buffer growth
                    if len(self.buffer) > _MAX_BUFFER_SIZE:
                        LOG.warning(f"Buffer overflow, resetting buffer: {self}")
                        self.buffer = b""
                        break

                    while not self.stop.is_set():
                        start = self.buffer.find(_JPEG_START)
                        if start == -1:
                            # Keep only the last byte in case it's the first half of \xff\xd8
                            self.buffer = self.buffer[-1:] if self.buffer else b""
                            break

                        if start > 0:
                            # Discard junk before JPEG start
                            self.buffer = self.buffer[start:]

                        end = self.buffer.find(_JPEG_END, start + 2)
                        if end == -1:
                            break

                        frame = self.buffer[start : end + 2]
                        self.buffer = self.buffer[end + 2 :]

                        last_frame_time = time.monotonic()
                        current_timeout = _FRAME_TIMEOUT

                        yield frame
            finally:
                self._stderr_task.cancel()
                if self._rtsp_ffmpeg_process.returncode is None:
                    self._rtsp_ffmpeg_process.terminate()
                    await self._rtsp_ffmpeg_process.wait()

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
        if self.rtsp_ffmpeg_args:
            cmds.extend(self.rtsp_ffmpeg_args)
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
                ("-vf", f"fps={self.fps},scale=-1:{self.scale},format=yuvj420p"),
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

                if "[h264 @ " in msg and any(filter in msg for f in h264_filters):
                    continue

                LOG.error(f"{color}{prefix} {msg}\033[0m")

        LOG.info(f"Starting RTSP reader: {self}")

        process = await asyncio.create_subprocess_exec(
            *proc_cmds,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stderr_task = asyncio.create_task(_log_ffmpeg_stderr(self, process))
        return process, stderr_task
