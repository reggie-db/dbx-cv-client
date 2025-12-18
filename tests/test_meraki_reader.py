"""Test script for MerakiReader against a running mock server."""

import argparse
import asyncio


async def test_meraki_reader(
    base_url: str = "http://127.0.0.1:8089",
    num_frames: int = 3,
    fps: int = 1,
    scale: int = 0,
    device_serial: str = "TEST-SERIAL-001",
):
    """Run MerakiReader test against mock server."""
    from dbx_cv_client.cam_reader.meraki_reader import MerakiReader
    from dbx_cv_client.options import MerakiOptions

    print("MerakiReader Test")
    print("=" * 50)
    print(f"Server: {base_url}")
    print(f"Device serial: {device_serial}")
    print(f"FPS: {fps}")
    print(f"Scale: {scale}")
    print(f"Frames to capture: {num_frames}")
    print("=" * 50)
    print()

    stop_event = asyncio.Event()
    ready_event = asyncio.Event()

    meraki_options = MerakiOptions(
        api_base_url=base_url,
        api_token="test-api-token",
    )

    reader = MerakiReader(
        meraki_options=meraki_options,
        stop=stop_event,
        ready=ready_event,
        fps=fps,
        scale=scale,
        source=device_serial,
    )

    print("Capturing frames...")
    print("-" * 50)

    frame_count = 0
    async for frame in reader._read():
        frame_count += 1
        print(f"[Frame {frame_count}] Received {len(frame)} bytes")

        if frame_count >= num_frames:
            print("-" * 50)
            print(f"Captured {num_frames} frames, stopping")
            stop_event.set()
            break

    print()
    print("Test completed successfully!")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Test MerakiReader against a running mock server"
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8089",
        help="Base URL of mock server (default: http://127.0.0.1:8089)",
    )
    parser.add_argument(
        "--frames",
        type=int,
        default=3,
        help="Number of frames to capture (default: 3)",
    )
    parser.add_argument(
        "--fps",
        type=int,
        default=1,
        help="Frames per second (default: 1)",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=0,
        help="Image scale/height, 0 to disable (default: 0)",
    )
    parser.add_argument(
        "--serial",
        default="TEST-SERIAL-001",
        help="Device serial (default: TEST-SERIAL-001)",
    )

    args = parser.parse_args()

    asyncio.run(
        test_meraki_reader(
            base_url=args.url,
            num_frames=args.frames,
            fps=args.fps,
            scale=args.scale,
            device_serial=args.serial,
        )
    )


if __name__ == "__main__":
    main()
