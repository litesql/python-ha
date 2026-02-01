"""HA DataSource for managing database connections."""

from dataclasses import dataclass, field
from typing import Optional

from .ha_client import HAClient, HAClientOptions
from .ha_connection import HAConnection, HAConnectionOptions
from .embedded_replicas import EmbeddedReplicasManager, ReplicaOptions


@dataclass
class HADataSourceOptions:
    """Options for HADataSource configuration."""

    url: str
    password: Optional[str] = None
    enable_ssl: bool = False
    timeout: int = 30
    login_timeout: int = 30
    embedded_replicas_dir: Optional[str] = None
    replication_url: Optional[str] = None
    replication_stream: Optional[str] = None
    replication_durable: Optional[str] = None


class HADataSource:
    """
    Data source for managing HA database connections.

    Example:
        ds = HADataSource(HADataSourceOptions(url="litesql://localhost:8080"))
        conn = await ds.get_connection()
        result = await conn.query("SELECT * FROM users")
        await conn.close()
    """

    def __init__(self, options: Optional[HADataSourceOptions] = None):
        """Initialize the data source."""
        self._url = ""
        self._password = ""
        self._enable_ssl = False
        self._timeout = 30
        self._login_timeout = 30
        self._embedded_replicas_dir: Optional[str] = None
        self._replication_url: Optional[str] = None
        self._replication_stream: Optional[str] = None
        self._replication_durable: Optional[str] = None

        if options:
            self._url = options.url
            self._password = options.password or ""
            self._enable_ssl = options.enable_ssl
            self._timeout = options.timeout
            self._login_timeout = options.login_timeout
            self._embedded_replicas_dir = options.embedded_replicas_dir
            self._replication_url = options.replication_url
            self._replication_stream = options.replication_stream
            self._replication_durable = options.replication_durable

    async def get_connection(self) -> HAConnection:
        """
        Get a connection from the data source.

        Returns:
            A new HAConnection instance.
        """
        if (
            self._embedded_replicas_dir
            and self._replication_url
            and self._replication_durable
        ):
            manager = EmbeddedReplicasManager.get_instance()
            await manager.load(
                ReplicaOptions(
                    directory=self._embedded_replicas_dir,
                    nats_url=self._replication_url,
                    stream=self._replication_stream or "ha",
                    durable=self._replication_durable,
                )
            )

        options = HAConnectionOptions(
            url=self._url,
            token=self._password,
            enable_ssl=self._enable_ssl,
            timeout=self._timeout,
            embedded_replicas_dir=self._embedded_replicas_dir,
            replication_url=self._replication_url,
            replication_stream=self._replication_stream,
            replication_durable=self._replication_durable,
        )

        return HAConnection(options)

    async def download_replicas(self, directory: str, override: bool = False) -> None:
        """
        Download all replicas from the HA server.

        Args:
            directory: Directory to save the replicas.
            override: Whether to override existing files.
        """
        client = HAClient(
            HAClientOptions(
                url=self._url,
                token=self._password,
                enable_ssl=self._enable_ssl,
                timeout=self._timeout,
            )
        )

        try:
            await client.download_all_replicas(directory, override)
        finally:
            await client.close()

    @property
    def url(self) -> str:
        """Get the server URL."""
        return self._url

    @url.setter
    def url(self, value: str) -> None:
        """Set the server URL."""
        self._url = value

    @property
    def password(self) -> str:
        """Get the authentication password/token."""
        return self._password

    @password.setter
    def password(self, value: str) -> None:
        """Set the authentication password/token."""
        self._password = value

    @property
    def enable_ssl(self) -> bool:
        """Get SSL enabled status."""
        return self._enable_ssl

    @enable_ssl.setter
    def enable_ssl(self, value: bool) -> None:
        """Set SSL enabled status."""
        self._enable_ssl = value

    @property
    def timeout(self) -> int:
        """Get the query timeout in seconds."""
        return self._timeout

    @timeout.setter
    def timeout(self, value: int) -> None:
        """Set the query timeout in seconds."""
        self._timeout = value

    @property
    def login_timeout(self) -> int:
        """Get the login timeout in seconds."""
        return self._login_timeout

    @login_timeout.setter
    def login_timeout(self, value: int) -> None:
        """Set the login timeout in seconds."""
        self._login_timeout = value

    @property
    def embedded_replicas_dir(self) -> Optional[str]:
        """Get the embedded replicas directory."""
        return self._embedded_replicas_dir

    @embedded_replicas_dir.setter
    def embedded_replicas_dir(self, value: str) -> None:
        """Set the embedded replicas directory."""
        self._embedded_replicas_dir = value

    @property
    def replication_url(self) -> Optional[str]:
        """Get the NATS replication URL."""
        return self._replication_url

    @replication_url.setter
    def replication_url(self, value: str) -> None:
        """Set the NATS replication URL."""
        self._replication_url = value

    @property
    def replication_stream(self) -> Optional[str]:
        """Get the NATS stream name."""
        return self._replication_stream

    @replication_stream.setter
    def replication_stream(self, value: str) -> None:
        """Set the NATS stream name."""
        self._replication_stream = value

    @property
    def replication_durable(self) -> Optional[str]:
        """Get the durable consumer name."""
        return self._replication_durable

    @replication_durable.setter
    def replication_durable(self, value: str) -> None:
        """Set the durable consumer name."""
        self._replication_durable = value
