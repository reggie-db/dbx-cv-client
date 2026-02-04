"""
Mock Meraki API server for testing.

Endpoints:
- POST `/devices/{serial}/camera/generateSnapshot`: returns a snapshot URL to poll
- GET `/snapshot/{serial}/{snapshot_id}.jpg`: returns a JPEG (may return 404 to simulate "not ready")
- GET `/devices/{serial}`: returns basic device info JSON
"""

import argparse
import random
from pathlib import Path

from aiohttp import web

from dbx_cv_client import logger

LOG = logger(__name__)

# Module state
_image_data: bytes = b""
_fail_probability: float = 0.5
_min_failures: int = 2
_request_counts: dict[str, int] = {}


async def handle_generate_snapshot(request: web.Request) -> web.Response:
    """
    Handle POST /devices/{serial}/camera/generateSnapshot.

    Returns a JSON response with a URL to poll for the image.
    """
    serial = request.match_info.get("serial", "unknown")

    # Generate a unique image URL for this snapshot request
    snapshot_id = random.randint(10000, 99999)
    base_url = f"http://{request.host}"
    image_url = f"{base_url}/snapshot/{serial}/{snapshot_id}.jpg"

    # Initialize request count for this URL
    _request_counts[image_url] = 0

    LOG.info(f"[Snapshot Request] Serial: {serial}")
    LOG.info(f"  -> Image URL: {image_url}")

    return web.json_response(
        {
            "url": image_url,
            "expiry": None,
        }
    )


async def handle_device_info(request: web.Request) -> web.Response:
    """Handle GET /devices/{serial} used by MerakiReader device info lookup."""
    serial = request.match_info.get("serial", "unknown")
    LOG.info(f"[Device Info Request] Serial: {serial}")
    return web.json_response(
        {
            "serial": serial,
            "name": f"Mock Camera {serial}",
            "model": "MOCK-MODEL-1",
        }
    )


async def handle_snapshot_image(request: web.Request) -> web.Response:
    """
    Handle GET /snapshot/{serial}/{snapshot_id}.jpg.

    Randomly fails with 404 to simulate image not ready yet.
    """
    full_url = str(request.url)

    # Find matching URL in request counts
    matching_key = None
    for key in _request_counts:
        if request.path in key or key in full_url:
            matching_key = key
            break

    if matching_key is None:
        matching_key = full_url
        _request_counts[matching_key] = 0

    _request_counts[matching_key] += 1
    attempt = _request_counts[matching_key]

    # Always fail for the first few attempts
    if attempt <= _min_failures:
        LOG.info(f"[Image Request] Attempt {attempt}: 404 (forced)")
        return web.Response(status=404, text="Image not ready")

    # After minimum failures, randomly decide
    if random.random() < _fail_probability:
        LOG.info(f"[Image Request] Attempt {attempt}: 404 (random)")
        return web.Response(status=404, text="Image not ready")

    LOG.info(f"[Image Request] Attempt {attempt}: 200 ({len(_image_data)} bytes)")
    return web.Response(
        body=_image_data,
        content_type="image/jpeg",
    )


def create_app() -> web.Application:
    """Create the aiohttp web application."""
    app = web.Application()
    app.router.add_post(
        "/devices/{serial}/camera/generateSnapshot",
        handle_generate_snapshot,
    )
    app.router.add_get(
        "/devices/{serial}",
        handle_device_info,
    )
    app.router.add_get(
        "/snapshot/{serial}/{snapshot_id}.jpg",
        handle_snapshot_image,
    )
    return app


def main():
    """Run the mock server."""
    parser = argparse.ArgumentParser(description="Mock Meraki API server for testing")
    parser.add_argument(
        "image_path",
        help="Path to image file to serve as snapshot",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8089,
        help="Port to bind (default: 8089)",
    )
    parser.add_argument(
        "--fail-prob",
        type=float,
        default=0.5,
        help="Probability of random 404 after min failures (default: 0.5)",
    )
    parser.add_argument(
        "--min-fails",
        type=int,
        default=2,
        help="Minimum forced 404 failures per snapshot (default: 2)",
    )

    args = parser.parse_args()

    global _image_data, _fail_probability, _min_failures

    # Load image
    image_file = Path(args.image_path)
    if not image_file.exists():
        raise FileNotFoundError(f"Image not found: {args.image_path}")

    _image_data = image_file.read_bytes()
    _fail_probability = args.fail_prob
    _min_failures = args.min_fails

    LOG.info("Mock Meraki API Server")
    LOG.info("=" * 50)
    LOG.info(f"Image: {args.image_path} ({len(_image_data)} bytes)")
    LOG.info(f"Fail probability: {_fail_probability}")
    LOG.info(f"Min failures: {_min_failures}")
    LOG.info("=" * 50)
    LOG.info()
    LOG.info("Endpoints:")
    LOG.info(
        f"  POST http://{args.host}:{args.port}/devices/{{serial}}/camera/generateSnapshot"
    )
    LOG.info(f"  GET  http://{args.host}:{args.port}/snapshot/{{serial}}/{{id}}.jpg")
    LOG.info()
    LOG.info(f"Starting server on http://{args.host}:{args.port}")
    LOG.info("Press Ctrl+C to stop")
    LOG.info()

    app = create_app()
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
