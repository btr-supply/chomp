# Chomp Configuration Guide

This guide covers all aspects of configuring Chomp ingester instances, divided into two main areas:

1. **Instance Configuration** - Environment variables (.env) and CLI parameters
2. **Ingester Configuration** - YAML-based ingester definitions

---

## 1. Instance Configuration

### Environment Variables (.env)

Chomp supports configuration through environment variables and `.env` files. Create a `.env` file in your project root or use environment variables directly.

#### Database Configuration

**TDengine (Default)**
```env
# TDengine connection settings
TSDB_ADAPTER=tdengine
DB_HOST=localhost
DB_PORT=40002
DB_HTTP_PORT=40003
DB_NAME=chomp
DB_ROOT_USER=root
DB_ROOT_PASS=pass
DB_RW_USER=rw
DB_RW_PASS=pass
```

**Alternative Database Adapters**
```env
# ClickHouse
TSDB_ADAPTER=clickhouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=default

# InfluxDB
TSDB_ADAPTER=influxdb
INFLUXDB_HOST=localhost
INFLUXDB_PORT=8086
INFLUXDB_BUCKET=chomp
INFLUXDB_ORG=myorg
INFLUXDB_TOKEN=mytoken

# TimescaleDB
TSDB_ADAPTER=timescale
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5432
TIMESCALE_DB=postgres

# MongoDB
TSDB_ADAPTER=mongodb
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DB=chomp

# SQLite
TSDB_ADAPTER=sqlite
SQLITE_DB=./chomp.db

# Other supported adapters:
# questdb, opentsdb, victoriametrics, duckdb, kx
```

#### Redis Configuration

```env
# Redis connection settings
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_NS=chomp
REDIS_MAX_CONNECTIONS=65536
REDIS_MASTER_HOST=localhost
REDIS_MASTER_PORT=6379
```

#### Application Runtime Configuration

```env
# Core runtime settings
MAX_JOBS=15                    # Max concurrent ingester jobs
MAX_RETRIES=5                  # Max retries per ingester event
RETRY_COOLDOWN=2               # Sleep time between retries (seconds)
VERBOSE=false                  # Enable debug logging
THREADED=true                  # Run jobs in separate threads
MONITORED=false                # Enable system monitoring
PERPETUAL_INDEXING=false       # Perpetual blockchain indexing

# Instance identification
PROC_ID=chomp-{random}         # Unique instance identifier
UID_MASKS_FILE=uid-masks       # Path to UID masks file
WORKDIR=.                      # Working directory

# Server configuration
HOST=127.0.0.1                # FastAPI server host
PORT=40004                     # FastAPI server port
WS_PING_INTERVAL=30            # WebSocket ping interval
WS_PING_TIMEOUT=20             # WebSocket ping timeout

# Logging
LOGFILE=out.log                # Log file path
```

#### Docker & Development Configuration

```env
# Docker deployment settings
DOCKER_NET=chomp-net
DB_IMAGE=chomp-db:latest
CORE_IMAGE=chomp-core:latest
DB_CONTAINER=chomp-db
API_CONTAINER=chomp-api

# Environment mode
MODE=dev                       # dev|prod
DEPLOYMENT=docker              # docker|local
ENV_FILE=.env.dev              # Environment file path
```

### Command Line Interface

All environment variables can also be set via CLI arguments. CLI arguments take precedence over environment variables.

#### Common Runtime Arguments

```bash
# Environment and logging
-e, --env .env                 # Environment file path
-v, --verbose                  # Enable verbose/debug output
-i, --proc_id IDENTIFIER       # Unique instance identifier

# Performance tuning
-r, --max_retries 5            # Max ingester retries per event
-rc, --retry_cooldown 2        # Min sleep time between retries (seconds)
-t, --threaded                 # Run jobs/routers in separate threads
-j, --max_jobs 15              # Max ingester jobs to run concurrently

# Database and monitoring
-a, --tsdb_adapter tdengine    # Timeseries database adapter
-m, --monitored                # Enable monitoring for all ingesters
-uidf, --uid_masks_file PATH   # Path to UID masks file
```

#### Ingester Runtime Arguments

```bash
# Ingester-specific settings
-p, --perpetual_indexing       # Perpetually listen for new blocks
-c, --ingester_configs FILES   # Comma-delimited list of YAML config files
```

#### Server Runtime Arguments

```bash
# Server mode
-s, --server                   # Run as server (ingester by default)
-sh, --host 127.0.0.1         # FastAPI server host
-sp, --port 40004             # FastAPI server port

# WebSocket configuration
-wpi, --ws_ping_interval 30    # WebSocket ping interval
-wpt, --ws_ping_timeout 20     # WebSocket ping timeout

# Health checks
-pi, --ping                    # Ping DB and cache for readiness
```

#### Usage Examples

```bash
# Basic ingester with custom config
python main.py -c "ingesters/crypto.yml"

# Multiple config files
python main.py -c "ingesters/dex.yml,ingesters/cex.yml,ingesters/defi.yml"

# Development mode with verbose logging
python main.py -v -c "examples/diverse.yml"

# Production server mode
python main.py --server --host 0.0.0.0 --port 8080

# Custom database adapter
python main.py -a clickhouse -c "ingesters/crypto.yml"

# Health check
python main.py --ping
```

---

## 2. Ingester Configuration

### YAML Schema Overview

Chomp uses YAML files to define ingester configurations. Each file can contain multiple ingester types, and multiple files can be specified for organizational purposes.

#### Configuration Structure

```yaml
# Main ingester categories
scrapper: []        # Web scraping ingesters
http_api: []        # REST API ingesters
ws_api: []          # WebSocket API ingesters
fix_api: []         # FIX protocol ingesters
evm_caller: []      # Ethereum-compatible view calls
evm_logger: []      # Ethereum-compatible event logs
svm_caller: []      # Solana view calls
svm_logger: []      # Solana event logs
sui_caller: []      # Sui view calls
sui_logger: []      # Sui event logs
aptos_caller: []    # Aptos view calls
aptos_logger: []    # Aptos event logs
ton_caller: []      # TON view calls
ton_logger: []      # TON event logs
cosmos_caller: []   # Cosmos view calls
cosmos_logger: []   # Cosmos event logs
processor: []       # Data processors
```

### Ingester Definition Schema

#### Required Fields

```yaml
name: string                   # Unique ingester name
interval: string               # Execution interval (s2, s5, s10, s20, s30, m1, m2, m5, m10, m15, m30, h1, h4, h6, h12, D1, D2, D3, W1, M1, Y1)
resource_type: string          # Data storage type: "timeseries", "value", "series"
fields: []                     # Array of field definitions
```

#### Optional Fields

```yaml
target: string                 # URL or chain_id:address (inherited by fields if not specified)
selector: string               # CSS selector, XPath, event signature, or method signature
type: string                   # Data type (inherited by fields if not specified)
probability: float             # Execution probability (0-1)
pre_transformer: string        # Pre-processing transformer
handler: string                # Custom event handler (Python code)
reducer: string                # Data reduction function
headers: object                # HTTP headers
params: mixed                  # Parameters (object, array, or string)
transient: boolean             # Whether to cache but not store in time series
transformers: []               # Array of transformation functions
tags: []                       # Array of tags for categorization
```

#### Data Types

```yaml
# Numeric types
int8, uint8, int16, uint16, int32, uint32, int64, uint64
float32, ufloat32, float64, ufloat64

# Other types
bool, timestamp, string, binary, varbinary
```

#### Execution Intervals

```yaml
# Seconds: s2, s5, s10, s20, s30
# Minutes: m1, m2, m5, m10, m15, m30
# Hours: h1, h4, h6, h12
# Days: D1, D2, D3
# Weeks: W1
# Months: M1
# Years: Y1
```

### Field Configuration

Fields inherit properties from their parent ingester unless explicitly overridden.

```yaml
fields:
  - name: field_name           # Required: Field name
    type: float64              # Data type (inherits from parent if not set)
    selector: .price           # Data selector (inherits from parent if not set)
    target: https://api.com    # Override parent target
    transformers:              # Data transformations
      - "float({self})"
      - "round({self}, 2)"
    transient: true            # Don't store in time series (cache only)
    tags: ["price", "crypto"]  # Field tags
```

### Configuration Examples

#### REST API Ingester

```yaml
http_api:
  - name: CoingeckoFeeds
    interval: s30
    resource_type: timeseries
    target: https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd
    type: float64
    fields:
      - name: BTC_USD
        selector: .bitcoin.usd
        transformers: ["round({self}, 2)"]
      - name: ETH_USD
        selector: .ethereum.usd
        transformers: ["round({self}, 2)"]
```

#### WebSocket Ingester

```yaml
ws_api:
  - name: BinanceFeeds
    interval: s10
    resource_type: timeseries
    type: float64
    handler: |
      def h(msg, epochs):
        # Custom message processing logic
        price = float(msg['p'])
        return {"price": price}
    fields:
      - name: BTCUSDT_PRICE
        target: wss://stream.binance.com:9443/ws/btcusdt@trade
        selector: price
```

#### EVM Blockchain Ingester

```yaml
evm_caller:
  - name: ChainlinkFeeds
    interval: s30
    resource_type: timeseries
    type: float64
    fields:
      - name: BTC_USD_PRICE
        target: "1:0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"  # chainId:address
        selector: latestRoundData()((uint80,int256,uint256,uint256,uint80))
        transformers: ["{self}[1] / 1e8"]  # Convert from 8 decimals
```

#### Web Scraping Ingester

```yaml
scrapper:
  - name: CoinMarketCapScraper
    interval: m5
    resource_type: value
    target: https://coinmarketcap.com/currencies/bitcoin/
    fields:
      - name: bitcoin_price
        type: string
        selector: ".priceValue"  # CSS selector
      - name: market_cap
        type: string
        selector: "//div[@class='statsValue']"  # XPath selector
```

#### Event Log Ingester

```yaml
evm_logger:
  - name: UniswapSwaps
    target: "1:0xa0b86a33e6441bd93a0d6e2c9e0a38c56f4f5e4"  # Uniswap V3 Pool
    selector: Swap(indexed address,indexed address,int256,int256,uint160,uint128,int24)
    resource_type: timeseries
    interval: s30
    fields:
      - name: sender
        type: string
        transformers: ["{self}[0]"]
      - name: amount0
        type: float64
        transformers: ["{self}[2] / 1e18"]
      - name: amount1
        type: float64
        transformers: ["{self}[3] / 1e6"]
```

### Advanced Features

#### Transformers

Transformers process field data using Python expressions:

```yaml
transformers:
  - "float({self})"                    # Convert to float
  - "round({self}, 2)"                 # Round to 2 decimals
  - "{self} * 1000"                    # Mathematical operations
  - "{self} / {OTHER_FIELD}"           # Reference other fields
  - "max(0, {self})"                   # Built-in functions
  - "'{self}'.upper()"                 # String operations
```

#### Conditional Execution

```yaml
probability: 0.1  # Execute only 10% of the time (useful for sampling)
```

#### Transient Fields

```yaml
transient: true   # Cache the field but don't store in time series
                  # Useful for intermediate calculations
```

#### Custom Handlers

```yaml
handler: |
  def h(msg, epochs):
    # Custom Python code for processing messages
    # msg: current message
    # epochs: historical data for calculations
    return processed_data
```

### Multiple Configuration Files

Organize ingesters across multiple files for better management:

```bash
# Separate by exchange type
python main.py -c "ingesters/cex.yml,ingesters/dex.yml"

# Separate by blockchain
python main.py -c "ingesters/ethereum.yml,ingesters/solana.yml,ingesters/polygon.yml"

# Separate by data type
python main.py -c "ingesters/prices.yml,ingesters/volumes.yml,ingesters/events.yml"
```

**Important**: Each configuration file operates as an isolated namespace. A single ingester instance can only process ingesters from one configuration file at a time.

### Configuration Validation

Chomp validates all configurations against the schema defined in `chomp/src/config-schema.yml` using yamale. Invalid configurations will prevent startup with detailed error messages.

### Best Practices

1. **Use meaningful names**: Choose descriptive ingester and field names
2. **Set appropriate intervals**: Balance data freshness with API rate limits
3. **Use transient fields**: For intermediate calculations that don't need storage
4. **Organize by domain**: Group related ingesters in the same configuration file
5. **Tag your data**: Use tags for better categorization and querying
6. **Test transformers**: Validate transformation logic before deployment
7. **Monitor rate limits**: Set appropriate intervals to respect API rate limits
8. **Use probability**: For high-frequency sampling or load testing

---

For additional examples, see:
- `chomp/examples/diverse.yml` - Comprehensive examples across all ingester types
- `ingesters/evm_callers/avalanche.yml` - Advanced EVM configuration patterns
