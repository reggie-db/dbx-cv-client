"""Common options and utilities shared across CLI commands."""

from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import typer
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient

_AZURE_DATABRICKS_DOMAIN = "azuredatabricks.net"


@dataclass
class WorkspaceOptions:
    """Databricks workspace configuration."""

    host: str
    region: str
    client_id: str
    client_secret: str
    table_name: str
    zerobus_ip: str | None = None

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

    @staticmethod
    def _server_endpoint(workspace_host: str, workspace_id: str, region: str) -> str:
        """Build Zerobus server endpoint URL."""
        if not region:
            raise ValueError("Region required to construct Zerobus server endpoint")

        prefix = f"{workspace_id}.zerobus.{region}"
        if workspace_host.endswith(f".{_AZURE_DATABRICKS_DOMAIN}"):
            return f"{prefix}.{_AZURE_DATABRICKS_DOMAIN}"
        return f"{prefix}.cloud.databricks.com"


@dataclass
class MerakiOptions:
    """Meraki API configuration."""

    api_base_url: str
    api_token: str | None = None
    vault_url: str | None = None
    secret_name: str | None = None

    async def cisco_meraki_api_key(self) -> str:
        """Get Meraki API key from token or Key Vault."""
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
