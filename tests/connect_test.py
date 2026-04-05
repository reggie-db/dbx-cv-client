# import json
# import logging
#
# from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
# from zerobus.sdk.sync import ZerobusSdk
#
# # Configure logging (optional but recommended)
# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# )
#
#
# # Initialize SDK
# sdk = ZerobusSdk(DEST_SERVER_ENTPOINT, DEST_HOST)
#
# # Configure table properties
# table_properties = TableProperties(
#     f"{DEST_CATALOG_NAME}.{DEST_SCHEMA_NAME}.{DEST_TABLE_NAME}"
# )
#
# # Configure stream with JSON record type
# options = StreamConfigurationOptions(record_type=RecordType.JSON)
#
# # Create stream
# stream = sdk.create_stream(
#     DEST_CLIENT_ID, DEST_CLIENT_SECRET, table_properties, options
# )
