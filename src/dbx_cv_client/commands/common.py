"""Common options and utilities shared across CLI commands."""

from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import typer

_AZURE_DATABRICKS_DOMAIN = "azuredatabricks.net"


@dataclass
class WorkspaceOptions:
    """
    Workspace configuration for connecting to Databricks.

    Attributes:
        host: Databricks workspace URL or host.
        region: Cloud region for the Zerobus endpoint.
        client_id: OAuth client ID.
        client_secret: OAuth client secret.
        table_name: Fully qualified table name (catalog.schema.table).
    """

    host: str
    region: str
    client_id: str
    client_secret: str
    table_name: str

    @property
    def workspace_host(self) -> str:
        return _parse_host(self.host)[0]

    @property
    def workspace_id(self) -> str:
        return _parse_host(self.host)[1]

    @property
    def workspace_url(self) -> str:
        return f"https://{self.workspace_host}"

    @property
    def server_endpoint(self) -> str:
        return _server_endpoint(self.workspace_host, self.workspace_id, self.region)


def _parse_host(value: str) -> tuple[str, str]:
    """
    Parses a Databricks workspace URL/host and extracts (host, workspace_id).

    For Azure Databricks: extracts workspace ID from the 'adb-' subdomain prefix.
    For other domains: extracts workspace ID from the 'o' query parameter.
    """
    raw = value.strip().lower()
    if "://" not in raw:
        raw = f"https://{raw}"

    parsed = urlparse(raw)
    host = parsed.netloc

    if not host:
        raise typer.BadParameter(f"Invalid Databricks URL: {value}")

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

    raise typer.BadParameter(f"Unable to extract workspace ID from URL: {value}")


def _server_endpoint(workspace_host: str, workspace_id: str, region: str) -> str:
    """
    Builds the Zerobus server endpoint URL.

    Format: <workspace_id>.zerobus.<region>.<domain>
    """
    if not region:
        raise ValueError("Region required to construct Zerobus server endpoint")

    prefix = f"{workspace_id}.zerobus.{region}"
    if workspace_host.endswith(f".{_AZURE_DATABRICKS_DOMAIN}"):
        return f"{prefix}.{_AZURE_DATABRICKS_DOMAIN}"
    return f"{prefix}.cloud.databricks.com"
