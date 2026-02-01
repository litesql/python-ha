"""LiteSQL HA - High-performance Python client for SQLite HA."""

from .ha_client import HAClient, HAClientOptions, ExecutionResult
from .ha_connection import HAConnection, HAConnectionOptions
from .ha_datasource import HADataSource, HADataSourceOptions
from .embedded_replicas import EmbeddedReplicasManager, ReplicaOptions

__version__ = "1.0.0"
__all__ = [
    "HAClient",
    "HAClientOptions",
    "HAConnection",
    "HAConnectionOptions",
    "HADataSource",
    "HADataSourceOptions",
    "EmbeddedReplicasManager",
    "ReplicaOptions",
    "ExecutionResult",
]
