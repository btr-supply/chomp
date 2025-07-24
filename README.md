<div align="center">
  <img style="border-radius=25px; max-height=250px;" height="400" src="./banner.png" />
  <h1>Chomp</h1>
  <p>
    <strong>Low-code, lightweight, multi-modal data ingester</strong>
  </p>
  <p>
    <a href="https://cho.mp/docs"><img alt="Docs" src="https://img.shields.io/badge/cho.mp-Docs-blue?style=social&logo=react"></a>
    <a href="https://t.me/chomp_ingester"><img alt="Chomp" src="https://img.shields.io/badge/Chomp--white?style=social&logo=telegram"></a>
    <a href="https://opensource.org/licenses/MIT"><img alt="License" src="https://img.shields.io/github/license/btr-supply/chomp?style=social" /></a>
  </p>
</div>

## Overview

Chomp is a **low-code data ingestion platform** that retrieves, transforms, and archives data from Web2 and Web3 sources using YAML configuration files. The **cho.mp** web interface provides no-code configuration and monitoring.

### Core Capabilities
- **High-Throughput**: Process millions of data points per day (hundreds of millions with decent hardware)
- **Rapid Deployment**: Start multi-node clusters in minutes
- **Lightweight**: Runs on Raspberry Pi 4 to enterprise clusters
- **Multi-Modal**: HTTP APIs, WebSockets, EVM, SVM (Solana), Sui
- **High-Performance Stack**: Built on Redis (caching/streaming) and TDengine (high-compression time-series storage)

### Scaling Capabilities
- **Vertical Scaling**: Powerful instances handle hundreds of millions of data points per day
- **Horizontal Scaling**: Nodes automatically sync via cluster Redis, preventing resource competition and redundant ingestion

### What Chomp Is
- YAML-configured ETL pipelines
- Self-hostable with built-in clustering
- Real-time data streaming platform
- Battle-tested at BTR Supply across 50+ exchanges

### What Chomp Is Not
- A graph indexer (focuses on timeseries data)
- An all-purpose task scheduler

## Architecture

### Core Components
- **Ingesters**: Multi-protocol data source connectors
- **Actions**: ETL pipeline orchestration (Schedule → Load → Transform → Store)
- **Services**: Core business logic and coordination
- **Adapters**: Database and cache system integrations
- **API**: FastAPI server with REST and WebSocket endpoints
- **UI**: Web interface for configuration and monitoring

### Technology Stack
- **Runtime**: Python 3.12+ with asyncio
- **Database**: TDengine (primary - high-compression, high-speed time-series), adapters for TimescaleDB, InfluxDB, QuestDB, MongoDB
- **Cache/Coordination**: Redis for caching, pubsub, distributed locking, and cluster communication
- **Data Processing**: Polars for high-performance data manipulation
- **API**: FastAPI with automatic OpenAPI documentation
- **Frontend**: React/Next.js with TypeScript and MUI
- **Deployment**: Docker or bare-metal with clustering

### Performance Foundation
The lightweight and high-speed capabilities are enabled by:
- **Redis**: Caching, real-time streaming, and cluster coordination
- **TDengine**: High-compression, high-speed time-series storage and retrieval
- **Cluster Sync**: Automatic node synchronization prevents resource competition

## Web Interface

The cho.mp web interface connects to any Chomp backend:

- **Public Instances**: Curated list from [directory.json](./directory.json)
- **Custom Backends**: User-added instances
- **Local Development**: Default localhost:40004 support
- **Self-Hosted**: Full compatibility with private deployments

### Authentication
- **Static Tokens**: Admin access tokens
- **Web3 Wallets**: EVM, SVM, Sui signatures
- **OAuth2**: GitHub, Twitter/X (self-hosted deployments only)

*NB: OAuth2 requires fixed callback URLs, so it's not available on the public cho.mp site.*

## Features

### Data Ingestion
- **HTTP APIs**: REST endpoints with retry and rate limiting
- **WebSockets**: Real-time streaming connections
- **EVM Chains**: Event logs, contract calls, block data
- **SVM Chains**: Solana program logs and account monitoring
- **Sui Network**: Move-based smart contract interactions

### Data Processing
- **Field Extraction**: JSONPath and custom selectors
- **Type Conversion**: Automatic type inference and validation
- **Transformations**: Built-in functions and mathematical expressions
- **Aggregation**: Cross-field dependencies and computed metrics

### Web Interface Features
- **Visual Configuration**: WYSIWYG YAML editing with inheritance support using Eemeli Aro's YAML library (best for preserving anchors and round-trip editing) or raw file editing with Prism editor
- **Real-time Testing**: "Try" configuration with live preview
- **Data Visualization**: Interactive charts and tables for timeseries data
- **Resource Monitoring**: Live ingester health and performance metrics
- **User Management**: Access control and rate limiting

### Operational Features
- **Native Clustering**: Automatic job distribution across instances
- **Rate Limiting**: Built-in throttling with exponential backoff
- **Health Monitoring**: Service health checks and automatic failover
- **Configuration Management**: Hot-reloading YAML configurations

## Quick Start

```bash
# Test full setup
make full-setup

# Development setup
pip install -e .
cp examples/dex-trades.yml ingesters/my-config.yml
python -m chomp.main --config ingesters/my-config.yml
```

## Configuration

YAML-based configuration for data sources and processing:

```yaml
http_api:
  - name: PriceIngester
    resource_type: timeseries
    target: https://api.example.com/prices
    interval: m1
    fields:
      - name: price
        type: float64
        selector: .data.price
        transformers: ["round6"]
      - name: volume
        type: float64
        selector: .data.volume
```

## Framework Comparison

| Feature | Chomp | Ponder.sh | The Graph |
|---|---|---|---|
| **Configuration** | YAML + Web UI | TypeScript | GraphQL Schema |
| **Interface** | cho.mp or self-hosted | Self-hosted only | Hosted/CLI |
| **Real-time API** | HTTP + WebSocket | HTTP + GraphQL | GraphQL |
| **Data Sources** | HTTP, WS, EVM, SVM, Sui | EVM | EVM |
| **Deployment** | Self-hosted/Hosted | Self-hosted | Self-hosted/Hosted |
| **Data Types** | Timeseries | Relational | Graph |
| **Storage** | TDengine, TimescaleDB, InfluxDB | PostgreSQL | IPFS + PostgreSQL |
| **Latency** | Low | Medium | High |

## Contributing

High-priority areas:
- **GraphQL Adapter**: Expose stored data via GraphQL
- **Database Adapters**: Add support for more databases
- **Visual Configuration**: Enhance WYSIWYG editor
- **Template Library**: Pre-built configurations for common use cases

**Development Guide**: [CONTRIBUTING.md](./CONTRIBUTING.md)

## License

MIT License - see [LICENSE](./LICENSE)

**Community**: [Telegram](https://t.me/chomp_ingester) | **Web UI**: [cho.mp](https://cho.mp)
