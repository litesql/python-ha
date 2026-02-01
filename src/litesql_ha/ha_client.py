"""HA Client for communicating with the SQLite HA server via gRPC."""

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import grpc
from google.protobuf.any_pb2 import Any as AnyProto
from google.protobuf.empty_pb2 import Empty

from .client.converter import from_any, to_any


@dataclass
class HAClientOptions:
    """Options for HAClient configuration."""

    url: str
    token: Optional[str] = None
    enable_ssl: bool = False
    timeout: int = 30


@dataclass
class ExecutionResult:
    """Result of a query execution."""

    columns: List[str] = field(default_factory=list)
    rows: List[List[Any]] = field(default_factory=list)
    rows_affected: int = 0


class QueryType:
    """Query type enumeration."""

    UNSPECIFIED = 0
    EXEC_QUERY = 1
    EXEC_UPDATE = 2


@dataclass
class NamedValue:
    """Named parameter value."""

    name: Optional[str] = None
    ordinal: int = 0
    value: Optional[AnyProto] = None


@dataclass
class QueryRequest:
    """Query request message."""

    replication_id: str = ""
    sql: str = ""
    type: int = QueryType.UNSPECIFIED
    params: List[NamedValue] = field(default_factory=list)


@dataclass
class QueryResponse:
    """Query response message."""

    columns: List[str] = field(default_factory=list)
    rows: List[List[AnyProto]] = field(default_factory=list)
    rows_affected: int = 0
    txseq: int = 0
    error: str = ""


class HAClient:
    """
    gRPC client for communicating with the SQLite HA server.

    Example:
        client = HAClient(HAClientOptions(url="litesql://localhost:8080"))
        result = await client.execute_query("SELECT * FROM users")
        client.close()
    """

    def __init__(self, options: HAClientOptions):
        """Initialize the HA client."""
        url = options.url.replace("litesql://", "http://")
        parsed = urlparse(url)

        self._replication_id = parsed.path.lstrip("/")
        self._timeout = options.timeout
        self._token = options.token or ""
        self._enable_ssl = options.enable_ssl
        self._txseq = 0

        host = parsed.hostname or "localhost"
        port = parsed.port or 8080
        address = f"{host}:{port}"

        if options.enable_ssl:
            self._channel = grpc.aio.secure_channel(address, grpc.ssl_channel_credentials())
        else:
            self._channel = grpc.aio.insecure_channel(address)

        self._stub = None
        self._query_stream: Optional[grpc.aio.StreamStreamCall] = None
        self._response_queue: asyncio.Queue[QueryResponse] = asyncio.Queue()

    async def _ensure_stub(self):
        """Ensure the gRPC stub is initialized."""
        if self._stub is None:
            from . import _generated
            self._stub = _generated.DatabaseServiceStub(self._channel)

    async def _get_metadata(self) -> List[Tuple[str, str]]:
        """Get gRPC metadata including authorization."""
        metadata = []
        if self._token:
            metadata.append(("authorization", f"Bearer {self._token}"))
        return metadata

    async def _send_query(
        self,
        sql: str,
        parameters: Optional[Dict[Union[str, int], Any]],
        query_type: int,
    ) -> QueryResponse:
        """Send a query request and wait for response."""
        await self._ensure_stub()

        params = []
        if parameters:
            ordinal = 1
            for key, value in parameters.items():
                nv = {
                    "ordinal": key if isinstance(key, int) else ordinal,
                    "value": to_any(value),
                }
                if not isinstance(key, int):
                    nv["name"] = str(key)
                params.append(nv)
                ordinal += 1

        request = {
            "replication_id": self._replication_id,
            "sql": sql,
            "type": query_type,
            "params": params,
        }

        metadata = await self._get_metadata()

        try:
            async def request_iterator():
                yield request

            responses = self._stub.Query(request_iterator(), metadata=metadata)

            async for response in responses:
                if response.txseq > 0:
                    self._txseq = response.txseq
                return response

        except grpc.RpcError as e:
            raise Exception(f"gRPC error: {e.code()}: {e.details()}") from e

        raise Exception("No response received")

    async def execute_query(
        self,
        sql: str,
        parameters: Optional[Dict[Union[str, int], Any]] = None,
    ) -> ExecutionResult:
        """
        Execute a SELECT query and return results.

        Args:
            sql: The SQL query to execute.
            parameters: Optional dictionary of parameters.

        Returns:
            ExecutionResult containing columns and rows.
        """
        response = await self._send_query(sql, parameters, QueryType.EXEC_QUERY)

        if response.error:
            raise Exception(response.error)

        columns = list(response.result_set.columns) if response.result_set else []
        rows = []

        if response.result_set and response.result_set.rows:
            for row in response.result_set.rows:
                row_data = []
                for value in row.values:
                    row_data.append(from_any(value))
                rows.append(row_data)

        return ExecutionResult(columns=columns, rows=rows)

    async def execute_update(
        self,
        sql: str,
        parameters: Optional[Dict[Union[str, int], Any]] = None,
    ) -> int:
        """
        Execute an INSERT/UPDATE/DELETE statement.

        Args:
            sql: The SQL statement to execute.
            parameters: Optional dictionary of parameters.

        Returns:
            Number of rows affected.
        """
        response = await self._send_query(sql, parameters, QueryType.EXEC_UPDATE)

        if response.error:
            raise Exception(response.error)

        return response.rows_affected

    async def execute(
        self,
        sql: str,
        parameters: Optional[Dict[Union[str, int], Any]] = None,
    ) -> ExecutionResult:
        """
        Execute any SQL statement.

        Args:
            sql: The SQL statement to execute.
            parameters: Optional dictionary of parameters.

        Returns:
            ExecutionResult with columns, rows, and rows_affected.
        """
        response = await self._send_query(sql, parameters, QueryType.UNSPECIFIED)

        if response.error:
            raise Exception(response.error)

        if not response.result_set or not response.result_set.columns:
            return ExecutionResult(rows_affected=response.rows_affected)

        columns = list(response.result_set.columns)
        rows = []

        for row in response.result_set.rows:
            row_data = []
            for value in row.values:
                row_data.append(from_any(value))
            rows.append(row_data)

        return ExecutionResult(columns=columns, rows=rows, rows_affected=response.rows_affected)

    async def download_replica(
        self,
        directory: str,
        replication_id: str,
        override: bool = False,
    ) -> None:
        """
        Download a replica database file.

        Args:
            directory: Directory to save the replica.
            replication_id: The replication ID to download.
            override: Whether to override existing files.
        """
        await self._ensure_stub()

        file_path = os.path.join(directory, replication_id)
        if not override and os.path.exists(file_path):
            return

        os.makedirs(directory, exist_ok=True)
        metadata = await self._get_metadata()

        chunks = []
        request = {"replication_id": replication_id}

        async for response in self._stub.Download(request, metadata=metadata):
            chunks.append(response.data)

        with open(file_path, "wb") as f:
            for chunk in chunks:
                f.write(chunk)

    async def download_all_replicas(self, directory: str, override: bool = False) -> None:
        """
        Download all replica database files.

        Args:
            directory: Directory to save the replicas.
            override: Whether to override existing files.
        """
        ids = await self.get_replication_ids()
        for replication_id in ids:
            await self.download_replica(directory, replication_id, override)

    async def get_replication_ids(self) -> List[str]:
        """
        Get all available replication IDs.

        Returns:
            List of replication IDs.
        """
        await self._ensure_stub()
        metadata = await self._get_metadata()

        response = await self._stub.ReplicationIDs(Empty(), metadata=metadata)
        return list(response.replication_id)

    @property
    def replication_id(self) -> str:
        """Get the current replication ID."""
        return self._replication_id

    @replication_id.setter
    def replication_id(self, value: str) -> None:
        """Set the current replication ID."""
        self._replication_id = value

    @property
    def txseq(self) -> int:
        """Get the current transaction sequence number."""
        return self._txseq

    async def close(self) -> None:
        """Close the client connection."""
        if self._channel:
            await self._channel.close()
