<div align="center">
  <img style="border-radius=25px; max-height=250px;" height="400" src="./banner.png" />
  <!-- <h1>Chomp</h1> -->
  <p>
    <a href="https://t.me/chomp_ingester"><img alt="Chomp" src="https://img.shields.io/badge/Chomp--white?style=social&logo=telegram"></a>
    <a href="https://twitter.com/AstrolabDAO"><img alt="Twitter Follow" src="https://img.shields.io/twitter/follow/AstrolabDAO?label=@AstrolabDAO&style=social"></a>
    <a href="https://btr.supply/docs"><img alt="BTR Docs" src="https://img.shields.io/badge/btr_docs-F9C3B3" /></a>
    <a href="https://opensource.org/licenses/MIT"><img alt="License" src="https://img.shields.io/github/license/btr-supply/chomp?style=social" /></a>
  </p>
  <!-- <p>
    <strong>by <a href="https://astrolab.fi">Astrolab DAO</a> & friends</strong>
  </p> -->
</div>

## Overview

**A lightweight, multimodal data ingester for Web2/Web3 sources**

Chomp is a productivity-first, highly modular data ingester that retrieves, transforms and archives data from Web2 and Web3 sources. This fork is specifically tailored for **BTR Supply**, an automated liquidity manager (ALM) for Uniswap v3/v4.

#### Chomp is:
- A productivity-first, highly modular data ingester that retrieves, transforms and archives data from Web2 and Web3 sources.
It allows anyone to set up a data back-end and ETL pipelines in minutes, all from a simple YAML configuration file.
- Lightweight, you can self-host Chomp on a Raspberry Pi 4, its built-in sync makes it cluster-native.
- A faster alternative to [Ponder](https://ponder.sh/) and [TheGraph](https://thegraph.com/) if you need to track your protocol's activity in real time.
- Plug and play, test it now! `bash ./full_setup.sh`
- An out-of-the-box http and websocket API to expose all your ingested data

#### Chomp is not:
- A graph indexer, it specializes in 1-dimensional timeseries data ingestion.
Graph indexing and API generation can however easily be implemented on top of it. (cf. [Contributing](#contributing))
- An all purpose task scheduler, it is a generic data back-end that deploys light ETL pipelines.
If you need domain specific task automation and consider using Chomp as all-purpose scheduler, [let's discuss it!](https://t.me/chomp_ingester)

## Features

- **Multimodal Ingestion**: HTTP APIs, WebSockets, EVM chains (eg. Ethereum), SVM chains (eg. Solana), Sui, and more
- **Low Code Configuration**: Simple YAML files define entire data pipelines
- **Native Clustering**: Spawn multiple instances with automatic job distribution
- **Real-time API**: FastAPI server with WebSocket streaming
- **Database Adapters**: TDengine (primary), with support for Timescale, MongoDB, InfluxDB, etc.
- **Rate Limiting**: Built-in rate limiting and retry mechanisms
- **Data Transformation**: Powerful field transformation and aggregation capabilities

## Use Cases

- **Web3 DApp Backends:** Set up a data backend for your decentralized app in minutes, [EVM or not](./docs/ingesters/).
- **Data Aggregators:** Ingest and consolidate data from various on-chain and off-chain sources for analysis.
- **Homelab Stuff:** Integrate and manage data ingestion for home servers and IoT devices.
- **Mass Metrics Ingestion:** Gather and process metrics from diverse systems and platforms, all at once.
- **BTR Supply Context:** This fork provides liquidity data ingestion for automated liquidity management on Uniswap v3/v4.

## Quick Start

Test a full setup (clustered ingesters + server), all at once:

```bash
sudo bash ./full_setup.sh
```

*Or using the Makefile:*
```bash
make full-setup
```

## Documentation

📖 **[Documentation Overview](./docs/overview.md)** - Complete guide navigation and quick start paths

**Core Documentation:**
- **[Architecture](./docs/architecture.md)** - System design, components, and data flow
- **[Deployment Guide](./docs/deployment.md)** - Installation, configuration, and production setup
- **[API Reference](./docs/api.md)** - REST and WebSocket API documentation
- **[Services](./docs/services.md)** - Core services and business logic
- **[Actions](./docs/actions.md)** - ETL pipeline and workflow orchestration
- **[Storage Adapters](./docs/adapters/storage.md)** - Database backends and configuration
- **[Cache System](./docs/adapters/cache.md)** - Redis-based caching and coordination

## Configuration

Chomp uses YAML configuration files to define data sources and processing pipelines. See [Deployment Guide](./docs/deployment.md) for complete setup instructions.

**Basic Example:**
```yaml
http_api:
  - name: ExampleIngester
    resource_type: timeseries
    target: http://example.com/api
    interval: m1
    fields:
      - name: price
        type: float64
        selector: .data.price
        transformers: ["round6"]
```

## Disclaimers

- **Work in Progress:** As per the [licence](./LICENCE), the project is under development. All of its aspects may change and improve over time.
- **Rate Limits:** Mind RPC and HTTP endpoints rate limits. This usually does not apply to WebSocket and FIX connections.
- **Data Storage Size:** Ingestion table sizes can grow rapidly in expansive setups (many resources/fields and short intervals). Ensure adequate storage planning, db schemas and compression settings (or just use the [default TDengine+Redis setup](./setup/Dockerfile.db)).

## Comparison with Similar Tools

| Feature | Chomp | Ponder.sh | The Graph |
|---------|-------|-----------|-----------|
| **___ APIs ___** |
| HTTP API | ✔️ | ❌ | ❌ |
| WS API | ✔️ | ❌ | ❌ |
| SQL API | partial | ✔️ | ❌ |
| GraphQL API | ❌ | ✔️ | ✔️ |
| **___ Ingesters ___** |
| HTTP API | ✔️ | ❌ | ❌ |
| WS API | ✔️ | ❌ | ❌ |
| FIX API | 🚧 | ❌ | ❌ |
| EVM Logs | ✔️ | ✔️ | ✔️ |
| EVM Reads | ✔️ | ✔️ | ❌ |
| EVM Call Traces | 🚧 | ✔️ | ✔️ |
| EVM Blocks | ❌ | ❌ | ✔️ |
| SVM Reads | ✔️ | ❌ | ✔️ |
| SVM Logs | 🚧 | ❌ | ✔️ |
| Sui Reads | ✔️| ❌ | ❌ |
| Sui Logs | 🚧 | ❌ | ❌ |
| Aptos Reads | 🚧 | ❌ | ❌ |
| Aptos Logs | 🚧 | ❌ | ❌ |
| Ton Reads | 🚧 | ❌ | ❌ |
| Ton Logs | 🚧 | ❌ | ❌ |
| **__ Features __** |
| Dashboard | ✔️ | ❌ | ✔️ |
| No-code Schema Declaration | ✔️ | ❌ | ✔️ |
| Auto-Scaling | ✔️ | ❌ | ✔️ |

For detailed comparisons with Ponder.sh and The Graph, see [Architecture](./docs/architecture.md).

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed development guidelines.

Contributions are much welcome!

Chomp is currently mostly an multimodal ingester, and lacks:
- [GraphQL](https://graphql.org/) adapter - expose stored data similarly to [The Graph](https://thegraph.com/) using [Strawberry](https://strawberry.rocks/)
- UI to explore running nodes, configure ingesters configurations, monitor data feeds
- Adapters - currently [TDengine](https://tdengine.com/) was our focus for performance and stability purposes, but new database adapters can very easily be added to [./src/adapters](./src/adapters), we are already looking at [Timescale](https://www.timescale.com/), [Influx](https://www.influxdata.com/), [kdb/kx](https://kx.com/) and others
- Performance profiling and optimization (better IO, threading, transformers, or even a [Rust](https://www.rust-lang.org/) port)

## License
This project is licensed under the MIT License, use at will. ❤️
