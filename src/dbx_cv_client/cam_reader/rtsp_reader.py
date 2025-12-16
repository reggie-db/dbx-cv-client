"""RTSP camera reader using FFmpeg for frame capture."""

import asyncio
from typing import AsyncIterable

from dbx_cv_client import logger
from dbx_cv_client.cam_reader.cam_reader import CamReader

_JPEG_START = b"\xff\xd8"
_JPEG_END = b"\xff\xd9"

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
        rtsp_ffmpeg_args: list[str],
    ):
        super().__init__(stop, ready, fps, scale, source)
        self.buffer = b""
        self.rtsp_ffmpeg_args = rtsp_ffmpeg_args
        self._rtsp_ffmpeg_process: asyncio.subprocess.Process | None = None

    async def _read(self) -> AsyncIterable[bytes]:
        # Kill ffmpeg if no new frames for fps * 5 seconds
        frame_timeout = max(5.0, self.fps * 5.0)

        while not self.stop.is_set():
            self._rtsp_ffmpeg_process = await self._start_ffmpeg_process()
            self.buffer = b""
            last_frame_time = asyncio.get_event_loop().time()

            try:
                while (
                    not self.stop.is_set()
                    and self._rtsp_ffmpeg_process.returncode is None
                ):
                    # Check if we've exceeded the frame timeout
                    elapsed = asyncio.get_event_loop().time() - last_frame_time
                    if elapsed >= frame_timeout:
                        LOG.warning(
                            f"No frames for {elapsed:.1f}s, restarting ffmpeg: {self}"
                        )
                        break

                    try:
                        chunk = await asyncio.wait_for(
                            self._rtsp_ffmpeg_process.stdout.read(4096),
                            timeout=frame_timeout - elapsed,
                        )
                    except asyncio.TimeoutError:
                        LOG.warning(
                            f"Read timeout after {frame_timeout}s, restarting ffmpeg: {self}"
                        )
                        break

                    if not chunk:
                        continue

                    self.buffer += chunk
                    while not self.stop.is_set():
                        start = self.buffer.find(_JPEG_START)
                        end = self.buffer.find(_JPEG_END)
                        if start == -1 or end == -1:
                            break
                        frame = self.buffer[start : end + 2]
                        self.buffer = self.buffer[end + 2 :]
                        last_frame_time = asyncio.get_event_loop().time()
                        yield frame
            finally:
                if self._rtsp_ffmpeg_process.returncode is None:
                    self._rtsp_ffmpeg_process.terminate()
                    await self._rtsp_ffmpeg_process.wait()

            # Sleep before restarting ffmpeg
            if not self.stop.is_set():
                LOG.info(f"Waiting 5s before restarting ffmpeg: {self}")
                await asyncio.sleep(5.0)

    async def _start_ffmpeg_process(self) -> asyncio.subprocess.Process:
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
                ("-i", self.source),
                ("-vf", f"fps={self.fps},scale=-1:{self.scale}"),
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
        return await asyncio.create_subprocess_exec(
            *proc_cmds,
            stdout=asyncio.subprocess.PIPE,
            stderr=None,
        )
