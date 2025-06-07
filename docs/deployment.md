# Chomp Deployment Guide

Chomp is a lightweight, multimodal data ingester for Web2 and Web3 sources with support for multiple databases and optional dependencies.

## Dependency Management

Chomp uses a sophisticated lazy-loading system for optional dependencies to support lean deployments:

### Core Dependencies (Always Required)
- FastAPI + Uvicorn (web server)
- TDengine client (taospy)
- Redis client
- Polars + PyArrow (data processing)
- Basic utilities (orjson, pyyaml, httpx, etc.)

### Optional Dependencies (Lazy-Loaded)
- **Web Scraping** (`chomp[scraper]`): BeautifulSoup4, lxml, Playwright
- **EVM Blockchains (eg. Ethereum)** (`chomp[evm]`): web3.py, multicall, eth-utils
- **SVM Blockchains (eg. Solana)** (`chomp[svm]`): Solana SDK, Solders
- **SUI** (`chomp[sui]`): SUI client
- **Aptos** (`chomp[aptos]`): Aptos client
- **TON** (`chomp[ton]`): TON client
- **Database Adapters**: TDengine, TimescaleDB, ClickHouse, DuckDB, MongoDB, InfluxDB, SQLite, etc.

### Installation Examples
```bash
# Minimal install (core only)
pip install chomp

# With web scraping
pip install chomp[scraper]

# With EVM support
pip install chomp[evm]

# Everything
pip install chomp[all]

# Custom combination
pip install chomp[scraper,evm,tdengine]
```

**Important**: Missing optional dependencies will cause helpful error messages only when those specific features are used, allowing different deployments to install only what they need.

## Prerequisites

### System Requirements

- **Operating System**: Linux (Ubuntu 20.04+), macOS, Windows (with WSL2)
- **Memory**: 1GB RAM minimum, 4GB+ recommended for production (based on ingesters config and db choice)
- **Storage**: 1GB+ available disk space (varies based on ingestion volume)
- **Python**: 3.11.x (required version)
- **Docker**: 21.0+ (for containerized deployment)

### Required Tools

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3.11 python3.11-dev python3-pip docker.io redis-tools

# macOS (with Homebrew)
brew install python@3.11 docker redis

# Install UV package manager (recommended)
pip install uv
```

## Deployment Methods

### Method 1: Quick Setup (Recommended for Testing)

The fastest way to get started with a complete setup:

```bash
# Clone repository (if working independently)
git clone https://github.com/AstrolabDAO/chomp.git
cd chomp

# Full containerized setup with all components
sudo make full-setup

# Or using the bash script directly
sudo bash scripts/full_setup.sh
```

This command:
1. Builds Docker images for database and core components
2. Sets up TDengine + Redis backend
3. Starts ingester cluster and API server
4. Configures networking and health checks

### Method 2: Container-Only Deployment

#### Database Setup

Set up the TDengine + Redis backend:

```bash
# Build and start database containers
sudo make db-setup

# Or manually
sudo bash scripts/db_setup.sh
```

This creates:
- TDengine server on port 40002 (default)
- TDengine HTTP adapter on port 40003
- Redis server on port 40001 (default)
- Shared Docker network: `chomp-net`

#### Core Application Setup

Deploy the ingester and API components:

```bash
# Build and start core application
sudo make core-setup

# Or for API server only
sudo make api-setup
```

### Method 3: Local Development Setup

#### Install Dependencies

```bash
# Install all dependencies (default: includes all EXTRA packages)
make install

# Install specific extras only
make install EXTRA=default
make install EXTRA=web2,evm
make install EXTRA=all-adapters
```

The `default` extra includes:
- TDengine adapter (`taospy`)
- Web2 scraping tools (`playwright`, `beautifulsoup4`, `lxml`)
- EVM blockchain tools (`web3`, `multicall`, `eth-utils`)
- Solana blockchain tools (`solana`, `solders`)

#### Database Backend

You can either:

1. **Use containerized database** (recommended):
   ```bash
   sudo make db-setup
   ```

2. **Install locally**:
   ```bash
   # TDengine (Ubuntu/Debian)
   wget https://www.taosdata.com/assets-download/3.0/TDengine-server-3.2.3.0-Linux-x64.tar.gz
   tar -xzf TDengine-server-3.2.3.0-Linux-x64.tar.gz
   cd TDengine-server-3.2.3.0/
   sudo ./install.sh

   # Redis
   sudo apt install redis-server
   ```

#### Local Runtime

```bash
# Start development environment (local)
make run dev local

# Start development environment (docker, default)
make run

# Start production environment
make run prod

# Start without API
make run dev noapi

# Check services
make monitor
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Database Configuration
TSDB_ADAPTER=tdengine
DB_HOST=localhost
DB_PORT=40002
DB_HTTP_PORT=40003
DB_ROOT_USER=root
DB_ROOT_PASS=pass
DB_RW_USER=rw
DB_RW_PASS=pass

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=40001
REDIS_MASTER_HOST=localhost
REDIS_MASTER_PORT=40001

# Application Configuration
MAX_JOBS=15
VERBOSE=false
THREADED=true
MAX_RETRIES=5
RETRY_COOLDOWN=2

# Server Configuration
HOST=127.0.0.1
PORT=40004
WS_PING_INTERVAL=30
WS_PING_TIMEOUT=20

# Docker Configuration (for scripts)
DOCKER_NET=chomp-net
DB_IMAGE=chomp-db:latest
CORE_IMAGE=chomp-core:latest
DB_CONTAINER=chomp-db-1
CORE_CONTAINER=chomp-core
```

### Ingester Configuration

#### Multiple Configuration Files

Chomp supports comma-delimited lists of configuration files, allowing you to organize ingesters by category:

```bash
# Single config file
INGESTER_CONFIGS=examples/diverse.yml

# Multiple config files
INGESTER_CONFIGS=ingesters/cexs.yml,ingesters/evm_dexs.yml,ingesters/processors.yml
```

**Important Configuration Behavior:**

- **Namespace Isolation**: Each configuration file works as an isolated namespace
- **Single Instance Limitation**: An ingester instance cannot pick up jobs from multiple configuration files
- **File-Based Clustering**: Each config file should be considered a specific namespace (named after the YAML file)
- **Job Distribution**: Jobs are distributed across instances, but each instance processes jobs from the same configuration namespace

#### Configuration File Structure

Create YAML files to define data sources (see `examples/` directory):

```yaml
# examples/simple.yml
ingesters:
  - name: "bitcoin_price"
    ingester_type: "http_api"
    endpoint: "https://api.coinbase.com/v2/exchange-rates?currency=BTC"
    interval: "*/30 * * * * *"  # Every 30 seconds
    fields:
      - path: "data.rates.USD"
        name: "btc_usd_price"
        type: "float"
```

#### Configuration Examples

```bash
# Start with specific config files
INGESTER_CONFIGS="config1.yml,config2.yml" make run

# Start with BTR configs (CEX, DEX, processors)
INGESTER_CONFIGS="ingesters/cexs.yml,ingesters/evm_dexs.yml,ingesters/processors.yml" make run

# Start production with custom configs
INGESTER_CONFIGS="prod-configs.yml" make run prod

# Start with debug logging enabled
make debug dev local     # Local with verbose debug logs
make debug dev docker    # Docker with verbose debug logs
```

## Scaling and Clustering

### Horizontal Scaling

Chomp supports native clustering with automatic job distribution:

```bash
# Start multiple ingester instances
for i in {1..5}; do
  uv run python main.py -e .env -c examples/diverse.yml -j 5 &
  sleep 2
done
```

### Container Clustering

```bash
# Scale with Docker using the lean setup
make run prod

# Monitor cluster status
make monitor
```

### Load Balancing

- Jobs are automatically distributed across instances using Redis-based coordination
- Each instance claims unclaimed tasks to prevent duplication
- Failed jobs are retried with exponential backoff

## Monitoring and Operations

### Health Checks

```bash
# Check database connectivity
make ping

# Comprehensive health check
make health-check

# View logs
make logs

# Monitor running services
make monitor
```

### Resource Monitoring

```bash
# Container status
docker ps --filter "label=project=Chomp"

# Resource usage
docker stats $(docker ps --filter "label=project=Chomp" --format "{{.Names}}")

# Database performance
redis-cli -h localhost -p 40001 INFO stats
taos -h localhost -P 40002 -s "SHOW DATABASES;"
```

### Maintenance

```bash
# Stop all services
make stop-all

# Clean up containers and networks
make cleanup

# Restart services
make full-setup
```

## Production Deployment Considerations

### Security

1. **Change default passwords** in production:
   ```env
   DB_ROOT_PASS=your_secure_password
   DB_RW_PASS=your_secure_rw_password
   ```

2. **Network isolation**: Use Docker networks or VPNs
3. **Firewall configuration**: Restrict access to database ports
4. **TLS/SSL**: Configure HTTPS for API endpoints in production

### Performance Tuning

1. **TDengine optimization**:
   - Adjust cache settings based on available memory
   - Configure compression levels for storage efficiency
   - Set appropriate retention policies

2. **Redis optimization**:
   - Configure memory limits and eviction policies
   - Adjust persistence settings based on requirements

3. **Application tuning**:
   - Set `MAX_JOBS` based on CPU cores and I/O capacity
   - Tune retry settings for external service reliability
   - Configure rate limits for external APIs

### Storage Management

- **Database retention**: Configure TDengine retention policies
- **Log rotation**: Set up log rotation for application and database logs
- **Backup strategy**: Implement regular backups for critical data

## API Usage

### HTTP API

Access ingested data via REST endpoints:

```bash
# List available resources
curl http://localhost:40004/resources

# Get time series data
curl "http://localhost:40004/data/bitcoin_price?start=2024-01-01&end=2024-01-02"
```

### WebSocket API

Real-time data streaming:

```javascript
const ws = new WebSocket('ws://localhost:40004/ws');
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Real-time data:', data);
};
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 40001-40004 are available
2. **Permission denied**: Use `sudo` for Docker commands
3. **Memory issues**: Increase Docker memory limits
4. **Network connectivity**: Check Docker network configuration

### Debug Mode

Chomp supports conditional debug logging through the `VERBOSE` environment variable:

```bash
# Using debug commands (recommended)
make debug dev local     # Overrides VERBOSE=true for local execution
make debug dev docker    # Overrides VERBOSE=true for Docker execution

# Manual verbose logging
VERBOSE=true uv run python main.py -e .env -c examples/diverse.yml

# Or using the -v flag
uv run python main.py -v -e .env -c examples/diverse.yml

# Check container logs
docker logs chomp-db-1
docker logs chomp-core-1
```

The debug mode enables detailed logging for:
- Database connection details and SQL query execution
- Task claiming and scheduling operations
- Data transformation steps
- Network request details
- Redis operations and job coordination

### Support

- GitHub Issues: [Report bugs and request features](https://github.com/AstrolabDAO/chomp/issues)
- Telegram: [Community support](https://t.me/chomp_ingester)
- Discord: [Astrolab DAO](https://discord.gg/xEEHAY2v5t)

## Next Steps

After successful deployment:

1. **Configure your data sources**: Create YAML configurations for your specific use case
2. **Set up monitoring**: Implement alerting for failed ingestions
3. **Scale as needed**: Add more instances based on data volume
4. **Integrate with your application**: Use the HTTP/WebSocket APIs to consume data

For specific adapter configurations and advanced features, refer to the source code in `src/adapters/` and example configurations in `examples/`.
