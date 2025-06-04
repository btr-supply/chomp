# Storage Adapters

Chomp's pluggable storage adapter system for various database backends.

## Overview

Storage adapters implement the unified `Tsdb` interface defined in `src/model.py`. This allows seamless switching between database backends. Redis is used for caching, pubsub, and synchronization of instances.

## Base Interface

All adapters implement the abstract `Tsdb` base class with these core methods:
- `connect()`, `ping()`, `close()`
- `create_table()`, `insert()`, `insert_many()`
- `fetch()`, `fetch_batch()`, `list_tables()`

## Currently Active Implementations

### TDengine (`src/adapters/tdengine.py`) - Primary
- **Class**: `Taos(Tsdb)`
- **Purpose**: High-performance time-series database
- **Features**: Columnar storage, built-in compression, SQL interface
- **Configuration**: `TAOS_HOST`, `TAOS_PORT`, `TAOS_DB`, `DB_RW_USER`, `DB_RW_PASS`
- **Status**: ✅ **Extensively tested and production-ready**

### SQLite (`src/adapters/sqlite.py`)
- **Class**: `SQLite(Tsdb)`
- **Purpose**: Lightweight embedded SQL database
- **Features**: File-based storage, ACID transactions, no server required
- **Configuration**: Database file path configuration

### ClickHouse (`src/adapters/clickhouse.py`)
- **Class**: `ClickHouse(Tsdb)`
- **Purpose**: Column-oriented database for analytical workloads
- **Features**: Vectorized query execution, compression, distributed architecture
- **Configuration**: Standard database connection parameters

### DuckDB (`src/adapters/duckdb.py`)
- **Class**: `DuckDB(Tsdb)`
- **Purpose**: In-process analytical database
- **Features**: Time-series support with time_bucket, excellent analytics performance
- **Configuration**: Embedded database configuration

## Available but Disabled Implementations

The following adapters are implemented but commented out in `main.py`:

### TimescaleDB (`src/adapters/timescale.py`)
- **Class**: `TimescaleDb(Tsdb)`
- **Purpose**: PostgreSQL extension for time-series
- **Features**: Hypertables, continuous aggregates, compression policies, time_bucket aggregation
- **Configuration**: `TIMESCALE_HOST`, `TIMESCALE_PORT`, `TIMESCALE_DB`

### InfluxDB (`src/adapters/influxdb.py`)
- **Class**: `InfluxDb(Tsdb)`
- **Purpose**: Purpose-built time-series database
- **Features**: Line protocol, retention policies, continuous queries
- **Configuration**: InfluxDB-specific connection parameters

### MongoDB (`src/adapters/mongodb.py`)
- **Class**: `MongoDb(Tsdb)`
- **Purpose**: Document store with time-series collections
- **Features**: Flexible schema, aggregation pipeline
- **Configuration**: `MONGO_HOST`, `MONGO_PORT`, `MONGO_DB`

### QuestDB (`src/adapters/questdb.py`)
- **Class**: `QuestDb(Tsdb)`
- **Purpose**: High-performance time-series database
- **Features**: SQL interface, columnar storage, fast ingestion
- **Configuration**: QuestDB connection parameters

### OpenTSDB (`src/adapters/opentsdb.py`)
- **Class**: `OpenTsdb(Tsdb)`
- **Purpose**: Scalable time-series data store
- **Features**: HBase backend, metric-based storage
- **Configuration**: OpenTSDB server configuration

### VictoriaMetrics (`src/adapters/victoriametrics.py`)
- **Class**: `VictoriaMetrics(PrometheusAdapter)`
- **Purpose**: Prometheus-compatible time-series database
- **Features**: High compression, fast queries, Prometheus compatibility
- **Configuration**: VictoriaMetrics server parameters

### Prometheus (`src/adapters/prometheus.py`)
- **Class**: `PrometheusAdapter(Tsdb)`
- **Purpose**: Monitoring and alerting toolkit
- **Features**: Pull-based metrics collection, PromQL query language
- **Configuration**: Prometheus server connection

### KX (`src/adapters/kx.py`)
- **Class**: `Kdb(Tsdb)`
- **Purpose**: High-performance analytics database
- **Features**: Vector operations, time-series optimization, q language
- **Configuration**: KX connection parameters

## Additional Storage Systems

### Redis (Caching & PubSub)
- **Purpose**: Caching, real-time messaging, and instance synchronization
- **Features**: In-memory storage, pub/sub messaging, distributed locking
- **Configuration**: `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_MAX_CONNECTIONS`
- **Status**: ✅ **Extensively tested and production-ready**
- **Usage**: Used via `RedisProxy` for caching, rate limiting, and WebSocket forwarding

## Adapter Selection

The active adapter is controlled by the `TSDB_ADAPTER` environment variable. Currently supported:
- `tdengine` (default)
- `sqlite`
- `clickhouse`
- `duckdb`

Other adapters exist in code but are commented out in `main.py`.

## Configuration

Adapters are selected at runtime in `main.py` via `get_adapter_class()`. Connection details are configured through adapter-specific environment variables.

## Testing Status

⚠️ **Important**: Only **TDengine** and **Redis** have been extensively tested in production environments. Other adapters should be considered experimental and may require additional testing and validation before production use.
