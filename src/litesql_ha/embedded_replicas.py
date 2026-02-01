"""Embedded replicas manager for local SQLite replicas with NATS synchronization."""

import asyncio
import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, Optional

import nats
from nats.js import JetStreamContext


@dataclass
class ReplicaOptions:
    """Options for replica configuration."""

    directory: str
    nats_url: str
    stream: str
    durable: str


@dataclass
class ReplicaConnection:
    """A connection to a local replica."""

    dsn: str
    conn: sqlite3.Connection
    txseq: int = 0


class EmbeddedReplicasManager:
    """
    Manager for embedded SQLite replicas with NATS synchronization.

    This is a singleton that manages local SQLite replicas and keeps them
    synchronized using NATS JetStream.
    """

    _instance: Optional["EmbeddedReplicasManager"] = None
    _lock = asyncio.Lock()

    def __new__(cls):
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the manager."""
        if self._initialized:
            return

        self._replicas: Dict[str, ReplicaConnection] = {}
        self._nats_conn: Optional[nats.NATS] = None
        self._jetstream: Optional[JetStreamContext] = None
        self._subscriptions: Dict[str, any] = {}
        self._update_task: Optional[asyncio.Task] = None
        self._running = False
        self._initialized = True

    @classmethod
    def get_instance(cls) -> "EmbeddedReplicasManager":
        """Get the singleton instance."""
        return cls()

    async def load(self, options: ReplicaOptions) -> None:
        """
        Load replicas from a directory and connect to NATS for replication.

        Args:
            options: Replica configuration options.
        """
        directory = options.directory
        nats_url = options.nats_url
        stream = options.stream
        durable = options.durable

        if not os.path.exists(directory) or not os.path.isdir(directory):
            raise ValueError(f"Invalid directory: {directory}")

        self._nats_conn = await nats.connect(nats_url)
        self._jetstream = self._nats_conn.jetstream()

        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)

            if not os.path.isfile(file_path) or filename in self._replicas:
                continue

            if not self._is_sqlite_file(file_path):
                continue

            try:
                conn = sqlite3.connect(file_path, check_same_thread=False)
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA temp_store = MEMORY")
                conn.execute("PRAGMA busy_timeout = 5000")

                txseq = 0
                try:
                    cursor = conn.execute(
                        "SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1"
                    )
                    row = cursor.fetchone()
                    if row:
                        txseq = row[0]
                except sqlite3.OperationalError:
                    pass

                self._replicas[filename] = ReplicaConnection(
                    dsn=file_path,
                    conn=conn,
                    txseq=txseq,
                )

                await self._subscribe_to_replication(filename, stream, durable)

            except Exception as e:
                print(f"Failed to load replica {filename}: {e}")

        self._start_txseq_updater()

    async def _subscribe_to_replication(
        self,
        replica_name: str,
        stream: str,
        durable: str,
    ) -> None:
        """Subscribe to replication stream for a replica."""
        if not self._jetstream:
            return

        subject = f"{stream}.{replica_name.replace('.', '_')}"

        try:
            sub = await self._jetstream.pull_subscribe(
                subject,
                durable=durable,
                stream=stream,
            )
            self._subscriptions[replica_name] = sub

            asyncio.create_task(self._process_messages(replica_name, sub))

        except Exception as e:
            print(f"Failed to subscribe to replication for {replica_name}: {e}")

    async def _process_messages(self, replica_name: str, subscription) -> None:
        """Process replication messages for a replica."""
        while self._running:
            try:
                messages = await subscription.fetch(10, timeout=5)
                for msg in messages:
                    await self._apply_replication_message(replica_name, msg.data)
                    await msg.ack()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"Error processing messages for {replica_name}: {e}")
                await asyncio.sleep(1)

    async def _apply_replication_message(
        self,
        replica_name: str,
        data: bytes,
    ) -> None:
        """Apply a replication message to a replica."""
        replica = self._replicas.get(replica_name)
        if not replica:
            return

        try:
            import json
            message = json.loads(data.decode("utf-8"))

            if "sql" in message:
                replica.conn.execute(message["sql"])
                replica.conn.commit()

            if "txseq" in message:
                replica.txseq = message["txseq"]

        except Exception as e:
            print(f"Error applying replication message: {e}")

    def _start_txseq_updater(self) -> None:
        """Start the background task to update txseq."""
        if self._update_task is not None:
            return

        self._running = True

        async def update_loop():
            while self._running:
                await asyncio.sleep(5)
                for name, replica in self._replicas.items():
                    try:
                        cursor = replica.conn.execute(
                            "SELECT received_seq FROM ha_stats ORDER BY updated_at DESC LIMIT 1"
                        )
                        row = cursor.fetchone()
                        if row:
                            replica.txseq = row[0]
                    except Exception:
                        pass

        self._update_task = asyncio.create_task(update_loop())

    def get_replica(self, db_name: str) -> Optional[ReplicaConnection]:
        """
        Get a replica connection by database name.

        Args:
            db_name: The database name.

        Returns:
            ReplicaConnection if found, None otherwise.
        """
        if len(self._replicas) == 1 and (not db_name or db_name == ""):
            return next(iter(self._replicas.values()))
        return self._replicas.get(db_name)

    def create_connection(self, db_name: str) -> Optional[sqlite3.Connection]:
        """
        Create a new read-only connection to a replica.

        Args:
            db_name: The database name.

        Returns:
            A new SQLite connection or None.
        """
        replica = self.get_replica(db_name)
        if not replica:
            return None

        conn = sqlite3.connect(f"file:{replica.dsn}?mode=ro", uri=True, check_same_thread=False)
        return conn

    def is_replica_updated(self, db_name: str, txseq: int) -> bool:
        """
        Check if a replica is up to date with the given txseq.

        Args:
            db_name: The database name.
            txseq: The transaction sequence to compare.

        Returns:
            True if the replica is up to date.
        """
        replica = self.get_replica(db_name)
        if not replica:
            return False
        return replica.txseq >= txseq

    def _is_sqlite_file(self, file_path: str) -> bool:
        """Check if a file is a SQLite database."""
        try:
            if os.path.getsize(file_path) < 100:
                return False

            with open(file_path, "rb") as f:
                header = f.read(16)
                return header.startswith(b"SQLite format 3")
        except Exception:
            return False

    async def close(self) -> None:
        """Stop all replicas and close connections."""
        self._running = False

        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
            self._update_task = None

        for sub in self._subscriptions.values():
            await sub.unsubscribe()
        self._subscriptions.clear()

        for replica in self._replicas.values():
            replica.conn.close()
        self._replicas.clear()

        if self._nats_conn:
            await self._nats_conn.close()
            self._nats_conn = None
