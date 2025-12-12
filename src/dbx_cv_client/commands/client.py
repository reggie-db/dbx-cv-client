"""Streaming client for ingesting records to Databricks"""

import asyncio
import time
import uuid

from zerobus.sdk import TableProperties
from zerobus.sdk.aio import ZerobusSdk

from dbx_cv_client.commands.common import WorkspaceOptions


async def _run(workspace_options: WorkspaceOptions) -> None:
    """Opens a stream and ingests sample records to the configured table."""
    import dbx_cv_client.models.record_pb2 as record_pb2

    table_properties = TableProperties(
        workspace_options.table_name, record_pb2.Raw.DESCRIPTOR
    )

    sdk_handle = ZerobusSdk(
        workspace_options.server_endpoint,
        workspace_options.workspace_url,
    )

    stream = await sdk_handle.create_stream(
        workspace_options.client_id, workspace_options.client_secret, table_properties
    )

    NUM_RECORDS = 250_000
    for i in range(NUM_RECORDS):
        if ((i + 1) % 1000) == 0:
            print(f">> Sent {i + 1} records to ingest.")

        await stream.ingest_record(
            record_pb2.Raw(
                id=str(uuid.uuid4()),
                timestamp=(time.time_ns() // 1_000),
                metadata="{}",
                content=b"test",
            )
        )

    await stream.flush()
    await stream.close()


def run(workspace_options: WorkspaceOptions) -> None:
    asyncio.run(_run(workspace_options))
