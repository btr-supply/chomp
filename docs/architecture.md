# Chomp Architecture

## Overview

Chomp is a lightweight, multimodal data ingester for Web2 and Web3 sources. It follows a modular, event-driven architecture supporting clustered deployments and real-time data streaming.

## Core Components

### System Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Ingesters    │    │     Server      │    │   Data Layer    │
│                 │    │                 │    │                 │
│ • HTTP API      │◄──►│ • FastAPI       │◄──►│ • TDengine      │
│ • WebSocket     │    │ • WebSocket     │    │ • Redis Cache   │
│ • Web Scraper   │    │ • Rate Limiter  │    │ • Adapters      │
│ • EVM Caller    │    │ • Middleware    │    │                 │
│ • EVM Logger    │    │                 │    │                 │
│ • Processors    │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow
1. **Configuration**: YAML-based ingester definitions (`config-schema.yml`)
2. **Scheduling**: Cron-based job scheduling with Redis coordination (`schedule.py`)
3. **Ingestion**: Multi-protocol data retrieval (`src/ingesters/`)
4. **Transformation**: Field-level data processing (`transform.py`)
5. **Storage**: Time-series database persistence (`src/adapters/`)
6. **API**: Real-time HTTP/WebSocket data access (`src/server/`)

## Key Design Patterns

### 1. Resource-Field Model
- **Resources**: Data sources or collections (`model.py:Resource`)
- **Fields**: Individual data points within resources (`model.py:ResourceField`)
- **Ingesters**: Combine resources with collection strategies (`model.py:Ingester`)

### 2. Modular Ingester Types
Each type specializes in specific data sources:
- **scrapper**: Static/dynamic web scraping
- **http_api**: REST API polling
- **ws_api**: WebSocket streaming
- **evm_caller**: Smart contract view calls
- **evm_logger**: Event log monitoring
- **processor**: Post-processing of ingested data

### 3. Proxy Pattern
Core services use thread-safe proxies (`proxies.py`):
- `TsdbProxy`: Database connection pooling
- `RedisProxy`: Cache and pubsub management
- `Web3Proxy`: EVM RPC connection handling
- `ConfigProxy`: Configuration management

### 4. Plugin Architecture
Adapters provide pluggable database backends (`src/adapters/`):
- **Primary**: TDengine (columnar, time-series optimized)
- **Available**: TimescaleDB, InfluxDB, MongoDB, ClickHouse
- **Interface**: Abstract `Tsdb` base class (`model.py:Tsdb`)

## Clustering & Synchronization

### Job Distribution
- **Task Claiming**: Redis-based distributed locking (`cache.py:ensure_claim_task`)
- **Load Balancing**: Automatic job distribution across instances
- **Failover**: Unclaimed tasks picked up by available workers
- **State Sync**: Shared configuration and execution state

## Data Transformation Pipeline

### Processing Stages
1. **Raw Data Extraction**: Protocol-specific adapters
2. **Field Processing**: Type conversion and validation
3. **Transformation Engine**: Mathematical expressions, built-in functions, custom code
4. **Storage Optimization**: Batch insertions, columnar storage

### Transformation Features
- Cross-field dependency resolution (`transform.py`)
- Mathematical expressions with field references
- Built-in functions (round, abs, mean, etc.)
- Custom Python lambda functions

## Server Architecture

### FastAPI Application (`src/server/`)
- Multi-version API support (`/`, `/v1`, `/v1.1`)
- Automatic endpoint generation based on ingesters
- WebSocket real-time streaming (`forwarder.py`)

### Middleware Stack (`middlewares/`)
1. **CORS**: Domain-based access control
2. **Rate Limiting**: Multi-tier request/bandwidth/points limits
3. **Compression**: GZip for responses > 1KB

### WebSocket Forwarding
Real-time data streaming via Redis pubsub (`routers/forwarder.py`)

## Configuration System

### Schema Validation (`config-schema.yml`)
- Yamale-based schema enforcement
- Type safety for all fields
- Inheritance patterns for DRY configuration

### Environment Management
Multi-layer configuration: defaults → `.env` → CLI args → runtime validation

## Performance Characteristics

### Scalability
- **Horizontal**: Add more ingester instances
- **Vertical**: Increase `max_jobs` per instance
- **Storage**: Columnar database optimization
- **Network**: Connection pooling and reuse

### Resource Efficiency
- **Memory**: Streaming data processing
- **CPU**: Async/await concurrency model
- **I/O**: Batch operations and connection pooling
- **Storage**: Compressed time-series data

## Security Model

### Rate Limiting (`limiter.py`)
Multi-dimensional limits: requests, bandwidth, points per minute/hour/day

### Access Control
- IP-based whitelisting/blacklisting
- Domain-restricted CORS policies
- User identification via headers/IPs

## Error Handling & Reliability

### Retry Mechanisms
- Configurable retry counts and cooldowns (`state.py:args`)
- Exponential backoff for transient failures
- Circuit breaker patterns for persistent failures

### Monitoring
- Comprehensive logging (`utils/`)
- Health check endpoints (`/ping`)
- Performance metrics collection

### Data Integrity
- Transactional database operations
- Schema validation at ingestion time
- Duplicate detection and handling

## Extension Points

### Planned Enhancements
1. **Additional Protocols**: FIX API, more blockchain networks
2. **Storage Backends**: Complete adapter implementations
3. **Streaming**: Real-time processing pipelines
4. **ML Integration**: Anomaly detection and forecasting
5. **Observability**: Metrics, tracing, and alerting

### Customization
- Custom transformer functions (`transform.py`)
- Protocol-specific adapters (`src/adapters/`)
- Database backend implementations
- Middleware components (`src/server/middlewares/`)
- Authentication providers
