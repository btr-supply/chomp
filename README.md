<div align="center">
  <img style="border-radius=25px; max-height=250px;" height="400" src="./banner.png" />
  <h1>Chomp</h1>
  <p>
    <strong>Lightweight, multimodal data ingester for Web2/Web3 sources</strong>
  </p>
  <p>
    <a href="https://t.me/chomp_ingester"><img alt="Chomp" src="https://img.shields.io/badge/Chomp--white?style=social&logo=telegram"></a>
    <a href="https://opensource.org/licenses/MIT"><img alt="License" src="https://img.shields.io/github/license/btr-supply/chomp?style=social" /></a>
  </p>
</div>

## Overview

Chomp is a productivity-first, highly modular data ingester that retrieves, transforms and archives data from Web2 and Web3 sources. It enables anyone to set up a data backend and ETL pipelines in minutes using simple YAML configuration files.

### Key Characteristics

**What Chomp Is:**
- **Productivity-First**: YAML-configured ETL pipelines that deploy in minutes
- **Multimodal**: Supports HTTP APIs, WebSockets, EVM chains, SVM chains (Solana), Sui, and more
- **Lightweight**: Self-hostable on a Raspberry Pi 4, cluster-native with built-in sync
- **Real-time**: Faster alternative to Ponder and TheGraph for live protocol tracking
- **Battle-tested**: Powers BTR Supply's automated liquidity management across 44 CEXs and 7+ chains

**What Chomp Is Not:**
- A graph indexer (specializes in 1-dimensional timeseries data ingestion)
- An all-purpose task scheduler (focused on data ingestion and ETL pipelines)

## Architecture

### Core Components

- **[Ingesters](./docs/ingesters/)**: Multi-protocol data source connectors
- **[Actions](./docs/actions.md)**: ETL pipeline orchestration (Schedule → Load → Transform → Store)
- **[Services](./docs/services.md)**: Core business logic and coordination
- **[Adapters](./docs/adapters/)**: Database and cache system integrations
- **[API](./docs/api.md)**: FastAPI server with REST and WebSocket endpoints

### Technology Stack

- **Runtime**: Python 3.11+ with asyncio-based concurrency
- **Database**: TDengine (primary), with adapters for Timescale, MongoDB, InfluxDB
- **Cache/Coordination**: Redis for caching, pubsub, and distributed locking
- **Data Processing**: Polars for high-performance data manipulation
- **API Framework**: FastAPI with automatic OpenAPI documentation
- **Deployment**: Docker containerization with clustering support

## Features

### Data Ingestion
- **HTTP APIs**: REST endpoints with automatic retry and rate limiting
- **WebSockets**: Real-time streaming data connections
- **EVM Chains**: Event logs, contract calls, and block data
- **SVM Chains**: Solana program logs and account monitoring
- **Sui Network**: Move-based smart contract interactions
- **FIX Protocol**: Financial market data feeds (🚧 in development)

### Data Processing
- **Field Extraction**: JSONPath and custom selectors
- **Type Conversion**: Automatic type inference and validation
- **Transformations**: Built-in functions and mathematical expressions
- **Aggregation**: Cross-field dependencies and computed metrics

### Operational Features
- **Native Clustering**: Automatic job distribution across instances
- **Rate Limiting**: Built-in throttling with exponential backoff
- **Health Monitoring**: Service health checks and automatic failover
- **Configuration Management**: Hot-reloading YAML configurations

## Quick Start

### Framework Testing
```bash
# Test full setup (clustered ingesters + server)
sudo bash ./full_setup.sh

# Or using Make
make full-setup
```

### Development Setup
```bash
# Install dependencies
pip install -e .

# Configure ingesters
cp examples/basic.yml ingesters/my-config.yml

# Start ingestion
python -m chomp.main --config ingesters/my-config.yml
```

## Configuration

Chomp uses YAML files to define data sources and processing pipelines:

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

evm_logs:
  - name: UniswapV3
    resource_type: timeseries
    target: wss://mainnet.infura.io/ws/v3/YOUR_KEY
    contracts:
      - address: "0x1f98431c8ad98523631ae4a59f267346ea31f984"
        events: ["PoolCreated"]
```

**For complete configuration reference**: [Deployment Guide](./docs/deployment.md)

## Documentation

### Getting Started
- **📖 [Overview](./docs/overview.md)** - Complete guide navigation and quick start paths
- **🚀 [Deployment](./docs/deployment.md)** - Installation, configuration, and production setup
- **⚙️ [Configuration](./docs/configuration.md)** - YAML setup and ingester configuration

### Technical Reference
- **🏗️ [Architecture](./docs/architecture.md)** - System design, components, and data flow
- **🔌 [API Reference](./docs/api.md)** - REST and WebSocket API documentation
- **⚡ [Actions](./docs/actions.md)** - ETL pipeline and workflow orchestration
- **🛠️ [Services](./docs/services.md)** - Core services and business logic

### Integration Guides
- **🔗 [Ingesters](./docs/ingesters/)** - Data source connectors and protocols
- **💾 [Storage Adapters](./docs/adapters/storage.md)** - Database backends and configuration
- **⚡ [Cache System](./docs/adapters/cache.md)** - Redis-based caching and coordination

### Development
- **🧪 [Testing](./docs/testing.md)** - Test suites and quality assurance
- **📊 [Monitoring](./docs/monitoring.md)** - Observability and debugging tools

## Framework Comparison

| Feature | Chomp | Ponder.sh | The Graph |
|---------|-------|-----------|-----------|
| **Configuration** | YAML | TypeScript | GraphQL Schema |
| **Real-time API** | HTTP + WebSocket | HTTP + GraphQL | GraphQL |
| **Data Sources** | HTTP + WS + EVM + SVM + Sui | EVM | EVM |
| **Deployment** | Self-hosted | Self-hosted | Hosted/Decentralized |
| **Data Types** | Timeseries focus | Relational | Graph |
| **Latency** | Low | Medium | High |

## Performance Considerations

- **Rate Limits**: Respect API and RPC endpoint limits (WebSocket/FIX connections typically exempt)
- **Storage Growth**: Timeseries data can grow rapidly; plan storage and use compression
- **Memory Usage**: Configure batch sizes and connection pools based on available resources
- **Network**: Use Redis clustering for distributed setups across data centers

## Contributing

Chomp welcomes contributions! Areas needing development:

### High Priority
- **GraphQL Adapter**: Expose stored data using [Strawberry GraphQL](https://strawberry.rocks/)
- **Management UI**: Dashboard for configuring ingesters and monitoring data feeds
- **Database Adapters**: Expand beyond TDengine to Timescale, InfluxDB, kdb/kx

### Medium Priority
- **Performance Optimization**: Better I/O handling, threading, and data transformers
- **Additional Protocols**: More blockchain networks and financial data feeds
- **Monitoring**: Enhanced observability and debugging capabilities

### Future Exploration
- **Rust Port**: High-performance rewrite for extreme throughput requirements
- **AI Integration**: Machine learning pipelines for anomaly detection and prediction

**Development Guide**: [CONTRIBUTING.md](./CONTRIBUTING.md)

## License

This project is licensed under the MIT License - see [LICENSE](./LICENSE) for details.

**Community**: [Telegram](https://t.me/chomp_ingester)
