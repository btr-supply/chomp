# TODO: we should have 1 doc file per adapter (eg. http_api, evm_caller etc.) from src/ingesters

# Ingesters Overview

Chomp supports multiple ingester types for different data sources. Each ingester type specializes in specific protocols and data collection strategies.

## Available Ingester Types

- **scrapper**: Static/dynamic web scraping with XPath/CSS selectors
- **http_api**: REST API polling with JSON response parsing
- **ws_api**: WebSocket streaming for real-time data feeds
- **evm_caller**: Smart contract view calls on EVM chains (eg. Ethereum)
- **evm_logger**: Event log monitoring on EVM chains
- **svm_caller**:  Smart contract view calls on SVM chains (eg. Solana)
- **sui_caller**:  Smart contract view calls on Sui
- **processor**: Post-processing of ingested data

## Configuration

### Single vs Multiple Configuration Files

Chomp supports both single and multiple configuration files:

```bash
# Single configuration file
INGESTER_CONFIGS=examples/diverse.yml

# Multiple configuration files (comma-delimited)
INGESTER_CONFIGS=ingesters/cexs.yml,ingesters/evm_dexs.yml,ingesters/processors.yml
```

### Configuration File Structure

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

### Important Configuration Behavior

**Namespace Isolation**: Each configuration file operates as an isolated namespace:

- **Single Instance Scope**: An ingester instance cannot pick up jobs from multiple configuration files
- **File-Based Namespacing**: Each config file should be considered a specific namespace (identified by the YAML filename)
- **Job Distribution**: While jobs are distributed across multiple ingester instances, each instance only processes jobs from its assigned configuration namespace
- **Clustering**: Multiple instances can be started for the same configuration file to scale horizontally

This design ensures clear separation of concerns and prevents configuration conflicts between different ingester categories (e.g., CEX vs DEX vs processors).

For detailed configuration examples and parameters, see the `examples/` directory and [Deployment Guide](../deployment.md).

## TODO: Detailed Documentation Needed

The following ingester types need comprehensive documentation:
- `scrapper.md` - Web scraping configuration and selectors
- `http_api.md` - REST API ingestion patterns and transformations
- `ws_api.md` - WebSocket streaming setup and real-time handling
- `evm_caller.md` - Smart contract integration and ABI handling
- `evm_logger.md` - Event monitoring and chain synchronization
- `processor.md` - Data post-processing and aggregation workflows
