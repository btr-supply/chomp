# TODO: we should have 1 doc file per adapter (eg. http_api, evm_caller etc.) from src/ingesters

# Ingesters Overview

Chomp supports multiple ingester types for different data sources. Each ingester type specializes in specific protocols and data collection strategies.

## Available Ingester Types

- **scrapper**: Static/dynamic web scraping with XPath/CSS selectors
- **http_api**: REST API polling with JSON response parsing
- **ws_api**: WebSocket streaming for real-time data feeds
- **evm_caller**: Smart contract view calls on EVM chains
- **evm_logger**: Event log monitoring on EVM chains
- **processor**: Post-processing of ingested data

## Configuration

Each ingester is configured through YAML files with type-specific attributes:

```yaml
http_api:
  - name: "example_api"
    target: "https://api.example.com/data"
    interval: "*/30 * * * * *"
    fields:
      - name: "value"
        type: "float64"
        selector: ".data.value"
```

For detailed configuration examples and parameters, see the `examples/` directory and [Deployment Guide](../deployment.md).

## TODO: Detailed Documentation Needed

The following ingester types need comprehensive documentation:
- `scrapper.md` - Web scraping configuration and selectors
- `http_api.md` - REST API ingestion patterns and transformations
- `ws_api.md` - WebSocket streaming setup and real-time handling
- `evm_caller.md` - Smart contract integration and ABI handling
- `evm_logger.md` - Event monitoring and chain synchronization
- `processor.md` - Data post-processing and aggregation workflows
