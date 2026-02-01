# litesql-ha

A high-performance Python client for [SQLite HA](https://github.com/litesql/ha) with built-in replication and failover support.

## Overview

`litesql-ha` is a Python client that brings enterprise-grade high availability to SQLite databases. It seamlessly integrates with Python applications while providing automatic failover, embedded replicas for read optimization, and replication support through NATS messaging.

## Features

- **High Availability**: Automatic failover and connection recovery for uninterrupted database access
- **Embedded Replicas**: Download and query read-only replicas locally for improved read performance
- **Replication Support**: Real-time synchronization using NATS messaging
- **Async/Await Support**: Full async support using asyncio
- **Type Hints**: Complete type annotations for better IDE support
- **Lightweight**: Minimal overhead with efficient resource usage

## Installation

```bash
pip install litesql-ha
```

Or install from source:

```bash
pip install -e .
```

## Quick Start

### 1. Start the HA Server

[Download the HA server](https://litesql.github.io/ha/downloads/) compatible with your operating system.

```sh
ha mydatabase.sqlite
```

### 2. Create a DataSource

```python
import asyncio
from litesql_ha import HADataSource, HADataSourceOptions

async def main():
    ds = HADataSource(HADataSourceOptions(
        url="litesql://localhost:8080"
    ))

    conn = await ds.get_connection()

    # Execute queries
    result = await conn.query("SELECT * FROM users")
    print(result.columns)  # ['id', 'name', 'email']
    print(result.rows)     # [[1, 'Alice', 'alice@example.com'], ...]

    await conn.close()

asyncio.run(main())
```

### 3. Execute Queries

```python
# SELECT query
result = await conn.query("SELECT * FROM users WHERE id = ?", {1: 5})

# INSERT/UPDATE/DELETE
rows_affected = await conn.execute(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    {1: "Bob", 2: "bob@example.com"}
)

# Any SQL statement
result = await conn.run("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)")
```

## Advanced Configuration

### Using Embedded Replicas for Read Optimization

Embedded replicas allow you to download and query read-only copies of your database locally, improving read performance. All write operations are automatically routed to the HA server.

```python
from litesql_ha import HADataSource, HADataSourceOptions, HAClient, HAClientOptions

replica_dir = "/path/to/replicas"

# Download replicas from the HA server
client = HAClient(HAClientOptions(
    url="http://localhost:8080",
    token="your-secret-key",
))
await client.download_all_replicas(replica_dir, override=True)
await client.close()

# Configure the DataSource to use embedded replicas
ds = HADataSource(HADataSourceOptions(
    url="litesql://localhost:8080",
    embedded_replicas_dir=replica_dir,
    replication_url="nats://localhost:4222",
    replication_durable="unique_replica_name",
))

conn = await ds.get_connection()

# Read queries automatically use local replicas when up-to-date
result = await conn.query("SELECT * FROM users")
```

**Benefits:**
- Faster read queries by querying local replicas
- Reduced network latency
- Automatic synchronization via NATS
- Write operations still go through the central HA server for consistency

### Connection Options

```python
ds = HADataSource(HADataSourceOptions(
    url="litesql://localhost:8080",       # HA server URL
    password="secret-token",               # Authentication token
    enable_ssl=True,                       # Enable SSL/TLS
    timeout=30,                            # Query timeout in seconds
    login_timeout=30,                      # Connection timeout in seconds
    embedded_replicas_dir="/replicas",     # Local replicas directory
    replication_url="nats://localhost:4222",  # NATS server URL
    replication_stream="ha",               # NATS stream name
    replication_durable="my-app",          # Durable consumer name
))
```

### Transactions

```python
conn = await ds.get_connection()

await conn.begin_transaction()
try:
    await conn.execute("INSERT INTO users (name) VALUES (?)", {1: "Alice"})
    await conn.execute("INSERT INTO users (name) VALUES (?)", {1: "Bob"})
    await conn.commit()
except Exception:
    await conn.rollback()
    raise

await conn.close()
```

### Context Manager

```python
async with await ds.get_connection() as conn:
    result = await conn.query("SELECT * FROM users")
    # Connection is automatically closed when exiting the context
```

### Direct Client Usage

For more control, you can use the HAClient directly:

```python
from litesql_ha import HAClient, HAClientOptions

client = HAClient(HAClientOptions(
    url="litesql://localhost:8080/mydb",
    token="secret",
))

# Execute queries
result = await client.execute_query("SELECT * FROM users")

# Execute updates
rows_affected = await client.execute_update("DELETE FROM users WHERE id = ?", {1: 5})

# Download replicas
await client.download_replica("/replicas", "mydb", override=False)

await client.close()
```

## API Reference

### HADataSource

| Method | Description |
|--------|-------------|
| `get_connection()` | Get a new connection |
| `download_replicas(dir, override)` | Download all replicas to a directory |

### HAConnection

| Method | Description |
|--------|-------------|
| `query(sql, params)` | Execute a SELECT query |
| `execute(sql, params)` | Execute INSERT/UPDATE/DELETE |
| `run(sql, params)` | Execute any SQL statement |
| `begin_transaction()` | Start a transaction |
| `commit()` | Commit the transaction |
| `rollback()` | Rollback the transaction |
| `set_auto_commit(auto_commit)` | Set auto-commit mode |
| `set_read_only(read_only)` | Set read-only mode |
| `set_catalog(catalog)` | Switch database |
| `is_valid(timeout)` | Check connection validity |
| `close()` | Close the connection |

### HAClient

| Method | Description |
|--------|-------------|
| `execute_query(sql, params)` | Execute SELECT query |
| `execute_update(sql, params)` | Execute INSERT/UPDATE/DELETE |
| `execute(sql, params)` | Execute any statement |
| `download_replica(dir, id, override)` | Download a single replica |
| `download_all_replicas(dir, override)` | Download all replicas |
| `get_replication_ids()` | Get available database names |
| `close()` | Close the client |

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/litesql/python-ha.git
cd python-ha

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check src

# Run type checking
mypy src
```

### Generate gRPC Stubs (Optional)

If you want to regenerate the gRPC stubs from the proto file:

```bash
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src/litesql_ha/_generated \
    --grpc_python_out=./src/litesql_ha/_generated \
    ./proto/sql.proto
```

## Troubleshooting

### Connection Issues

If you're unable to connect to the HA server:
- Verify the HA server is running on the specified host and port
- Check network connectivity and firewall rules
- Ensure the URL format is correct: `litesql://hostname:port`

### Replica Synchronization

If replicas are not syncing:
- Verify the NATS server is running at the configured `replication_url`
- Check that the `replication_durable` name is unique across your application instances
- Ensure the HA server has appropriate permissions to write to the replica directory

## License

This project is licensed under the Apache v2 License - see the [LICENSE](LICENSE) file for details.
