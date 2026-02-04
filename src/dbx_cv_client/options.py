"""Common options and utilities shared across CLI commands."""

from dataclasses import dataclass
from typing import Annotated
from urllib.parse import parse_qs, urlparse

import cyclopts
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient

_AZURE_DATABRICKS_DOMAIN = "azuredatabricks.net"


@cyclopts.Parameter(name="*")
@dataclass
class WorkspaceOptions:
    """Databricks workspace configuration."""

    host: Annotated[str, cyclopts.Parameter(env_var="DATABRICKS_HOST")]
    region: Annotated[str, cyclopts.Parameter(env_var="DATABRICKS_REGION")]
    client_id: Annotated[str, cyclopts.Parameter(env_var="DATABRICKS_CLIENT_ID")]
    client_secret: Annotated[
        str, cyclopts.Parameter(env_var="DATABRICKS_CLIENT_SECRET")
    ]
    table_name: Annotated[str, cyclopts.Parameter(env_var="DATABRICKS_TABLE_NAME")]
    zerobus_ip: Annotated[
        str | None, cyclopts.Parameter(env_var="DATABRICKS_ZEROBUS_IP")
    ] = None

    @property
    def workspace_host(self) -> str:
        return self._parse_host(self.host)[0]

    @property
    def workspace_id(self) -> str:
        return self._parse_host(self.host)[1]

    @property
    def workspace_url(self) -> str:
        return f"https://{self.workspace_host}"

    @property
    def server_endpoint(self) -> str:
        return self._server_endpoint(
            self.workspace_host, self.workspace_id, self.region
        )

    @staticmethod
    def _parse_host(value: str) -> tuple[str, str]:
        """Parse URL/host to (host, workspace_id)."""
        raw = value.strip().lower()
        if "://" not in raw:
            raw = f"https://{raw}"

        parsed = urlparse(raw)
        host = parsed.netloc

        if not host:
            raise ValueError(f"Invalid Databricks URL: {value}")

        if host.endswith(f".{_AZURE_DATABRICKS_DOMAIN}"):
            subdomain = host.split(".")[0]
            adb_prefix = "adb-"
            if subdomain.startswith(adb_prefix):
                workspace_id = subdomain[len(adb_prefix) :]
                return (host, workspace_id)
        else:
            query_params = parse_qs(parsed.query)
            workspace_id_list = query_params.get("o")
            if workspace_id_list and workspace_id_list[0]:
                return (host, workspace_id_list[0])

        raise ValueError(f"Unable to extract workspace ID from URL: {value}")

    @staticmethod
    def _server_endpoint(workspace_host: str, workspace_id: str, region: str) -> str:
        """Build Zerobus server endpoint URL."""
        if not region:
            raise ValueError("Region required to construct Zerobus server endpoint")

        prefix = f"{workspace_id}.zerobus.{region}"
        if workspace_host.endswith(f".{_AZURE_DATABRICKS_DOMAIN}"):
            return f"{prefix}.{_AZURE_DATABRICKS_DOMAIN}"
        return f"{prefix}.cloud.databricks.com"


@cyclopts.Parameter(name="*")
@dataclass
class ClientOptions:
    flush_interval: Annotated[
        float | None,
        cyclopts.Parameter(
            env_var="FLUSH_INTERVAL",
            help="Interval for flushing the stream in seconds",
        ),
    ] = None

    max_inflight_records: Annotated[
        int | None,
        cyclopts.Parameter(
            env_var="MAX_INFLIGHT_RECORDS",
            help="Maximum number of records that can be sent before waiting for ack",
        ),
    ] = None

    log_stats_interval: Annotated[
        float | None,
        cyclopts.Parameter(
            env_var="LOG_STATS_INTERVAL",
            help="Interval for logging stats in seconds",
        ),
    ] = 5.0

    fps: Annotated[
        float,
        cyclopts.Parameter(
            env_var="FPS",
            help="Frames per second",
        ),
    ] = 1

    scale: Annotated[
        int,
        cyclopts.Parameter(
            env_var="SCALE",
            help="Scale",
        ),
    ] = 1080

    rtsp_ffmpeg_args: Annotated[
        list[str] | None,
        cyclopts.Parameter(
            name="rtsp-ffmpeg-arg",
            env_var="RTPSP_FFMPEG_ARGS",
            help="Additional FFmpeg arguments for RTSP sources",
            negative_iterable="",
        ),
    ] = None

    frame_multiplier: Annotated[
        int | None,
        cyclopts.Parameter(
            env_var="FRAME_MULTIPLIER",
            help="Sends a frame multiple times",
        ),
    ] = None


@dataclass
class MerakiOptions:
    api_base_url: Annotated[
        str,
        cyclopts.Parameter(
            env_var="MERAKI_API_BASE_URL",
            help="Meraki API base URL",
        ),
    ] = "https://api.meraki.com/api/v1"

    api_token: Annotated[
        str | None,
        cyclopts.Parameter(
            env_var="MERAKI_API_TOKEN",
            help="Meraki API token",
        ),
    ] = None

    vault_url: Annotated[
        str | None,
        cyclopts.Parameter(
            env_var="MERAKI_VAULT_URL",
            help="Meraki Azure Key Vault URL",
        ),
    ] = None

    secret_name: Annotated[
        str | None,
        cyclopts.Parameter(
            env_var="MERAKI_SECRET_NAME",
            help="Meraki Azure Key Vault secret name",
        ),
    ] = None

    async def cisco_meraki_api_key(self) -> str:
        if self.api_token is None and (
            self.vault_url is None or self.secret_name is None
        ):
            raise ValueError(
                "Either api_token or both vault_url and secret_name are required"
            )

        if self.api_token:
            return self.api_token

        credential = DefaultAzureCredential()
        try:
            async with SecretClient(
                vault_url=self.vault_url, credential=credential
            ) as client:
                secret = await client.get_secret(self.secret_name)
                return secret.value
        finally:
            await credential.close()
