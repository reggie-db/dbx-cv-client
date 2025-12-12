"""Streaming client for ingesting records to Databricks Change Vault."""

import time
import uuid

from zerobus.sdk import TableProperties
from zerobus.sdk.aio import ZerobusSdk

import dbx_cv_client.generated.record_pb2 as record_pb2
from dbx_cv_client.commands.common import CommandOptions


async def run_client(command_options: CommandOptions) -> None:
    """Opens a stream and ingests sample records to the configured table."""
    table_properties = TableProperties(
        command_options.table_name, record_pb2.Raw.DESCRIPTOR
    )

    sdk_handle = ZerobusSdk(
        command_options.server_endpoint,
        command_options.workspace_url,
    )

    stream = await sdk_handle.create_stream(
        command_options.client_id, command_options.client_secret, table_properties
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
