"""HA Connection for managing database connections."""

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from .ha_client import HAClient, HAClientOptions, ExecutionResult
from .embedded_replicas import EmbeddedReplicasManager


@dataclass
class HAConnectionOptions:
    """Options for HAConnection configuration."""

    url: str
    token: Optional[str] = None
    enable_ssl: bool = False
    timeout: int = 30
    embedded_replicas_dir: Optional[str] = None
    replication_url: Optional[str] = None
    replication_stream: Optional[str] = None
    replication_durable: Optional[str] = None


class HAConnection:
    """
    Represents a connection to the HA database.

    Example:
        options = HAConnectionOptions(url="litesql://localhost:8080")
        conn = HAConnection(options)
        result = await conn.query("SELECT * FROM users")
        await conn.close()
    """

    def __init__(self, options: HAConnectionOptions):
        """Initialize the connection."""
        client_options = HAClientOptions(
            url=options.url,
            token=options.token,
            enable_ssl=options.enable_ssl,
            timeout=options.timeout,
        )
        self._client = HAClient(client_options)
        self._embedded_replica: Optional[sqlite3.Connection] = None
        self._replicas_manager: Optional[EmbeddedReplicasManager] = None
        self._closed = False
        self._auto_commit = True
        self._read_only = False

        if options.embedded_replicas_dir and options.replication_url:
            self._replicas_manager = EmbeddedReplicasManager.get_instance()
            self._embedded_replica = self._replicas_manager.create_connection(
                self._client.replication_id
            )

    async def query(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> ExecutionResult:
        """
        Execute a SELECT query.

        Args:
            sql: The SQL query to execute.
            params: Optional dictionary of parameters.

        Returns:
            ExecutionResult with columns and rows.
        """
        if (
            self._embedded_replica
            and self._replicas_manager
            and self._is_select_query(sql)
            and self._replicas_manager.is_replica_updated(
                self._client.replication_id, self._client.txseq
            )
        ):
            return self._execute_on_replica(sql, params)

        return await self._client.execute_query(sql, params)

    async def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Execute an INSERT/UPDATE/DELETE statement.

        Args:
            sql: The SQL statement to execute.
            params: Optional dictionary of parameters.

        Returns:
            Number of rows affected.
        """
        return await self._client.execute_update(sql, params)

    async def run(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> ExecutionResult:
        """
        Execute any SQL statement.

        Args:
            sql: The SQL statement to execute.
            params: Optional dictionary of parameters.

        Returns:
            ExecutionResult with columns, rows, and rows_affected.
        """
        if (
            self._embedded_replica
            and self._replicas_manager
            and self._is_select_query(sql)
            and self._replicas_manager.is_replica_updated(
                self._client.replication_id, self._client.txseq
            )
        ):
            return self._execute_on_replica(sql, params)

        return await self._client.execute(sql, params)

    def _execute_on_replica(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> ExecutionResult:
        """Execute a query on the local replica."""
        if not self._embedded_replica:
            raise Exception("No embedded replica available")

        cursor = self._embedded_replica.cursor()

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        return ExecutionResult(
            columns=columns,
            rows=[list(row) for row in rows],
        )

    def _is_select_query(self, sql: str) -> bool:
        """Check if the SQL is a read-only query."""
        trimmed = sql.strip().upper()
        return (
            trimmed.startswith("SELECT")
            or trimmed.startswith("PRAGMA")
            or trimmed.startswith("EXPLAIN")
            or trimmed.startswith("WITH")
        )

    async def begin_transaction(self) -> None:
        """Begin a transaction."""
        await self._client.execute_update("BEGIN")
        self._auto_commit = False

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._client.execute_update("COMMIT")
        self._auto_commit = True

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self._client.execute_update("ROLLBACK")
        self._auto_commit = True

    async def set_auto_commit(self, auto_commit: bool) -> None:
        """Set auto-commit mode."""
        if auto_commit == self._auto_commit:
            return

        if auto_commit:
            await self.commit()
        else:
            await self._client.execute_update("BEGIN")

        self._auto_commit = auto_commit

    @property
    def auto_commit(self) -> bool:
        """Get auto-commit mode."""
        return self._auto_commit

    async def set_read_only(self, read_only: bool) -> None:
        """Set read-only mode."""
        pragma = "PRAGMA query_only = 1" if read_only else "PRAGMA query_only = 0"
        await self._client.execute_update(pragma)
        self._read_only = read_only

    @property
    def read_only(self) -> bool:
        """Get read-only mode."""
        return self._read_only

    async def is_valid(self, timeout: int = 5) -> bool:
        """Check if the connection is valid."""
        try:
            await self._client.execute_query("SELECT 1")
            return True
        except Exception:
            return False

    @property
    def catalog(self) -> str:
        """Get the current catalog (database name)."""
        return self._client.replication_id

    async def set_catalog(self, catalog: str) -> None:
        """Set the current catalog (database name)."""
        if not catalog:
            raise ValueError("Catalog cannot be empty")

        self._client.replication_id = catalog

        if self._replicas_manager:
            if self._embedded_replica:
                self._embedded_replica.close()
            self._embedded_replica = self._replicas_manager.create_connection(catalog)

    @property
    def client(self) -> HAClient:
        """Get the underlying HAClient."""
        return self._client

    @property
    def is_closed(self) -> bool:
        """Check if the connection is closed."""
        return self._closed

    async def close(self) -> None:
        """Close the connection."""
        if self._closed:
            return

        await self._client.close()

        if self._embedded_replica:
            self._embedded_replica.close()
            self._embedded_replica = None

        self._closed = True

    async def __aenter__(self) -> "HAConnection":
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        await self.close()
