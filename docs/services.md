# Services

Core services that power Chomp's data processing and API capabilities.

## Overview

Services follow the `ServiceResponse = tuple[str, any]` pattern where the first element is an error message (empty if success) and the second is the result data.

## Available Services

### Rate Limiter (`src/services/limiter.py`)
- **Purpose**: Multi-dimensional rate limiting for API endpoints
- **Key Functions**:
  - `check_limits(user, path)` - Validates request limits
  - `get_user_limits(user)` - Retrieves current usage
  - `increment_counters(user, response_size, ppr)` - Updates counters post-request
- **Features**: Points-based system, multiple time windows (minute/hour/day), Redis-backed persistence

### Data Loader (`src/services/loader.py`)
- **Purpose**: Resource management, schema loading, and data retrieval
- **Key Functions**:
  - `get_resources()`, `parse_resources()` - Resource discovery and parsing
  - `get_schema()`, `get_last_values()`, `get_history()` - Data access
  - `parse_resources_fields()` - Field parsing and validation
- **Features**: Scope-based filtering, quote currency conversion, format transformation

### Time Series Analysis (`src/services/ts_analysis.py`)
- **Purpose**: Statistical analysis and metrics for time series data
- **Key Functions**:
  - `get_volatility()`, `get_trend()`, `get_momentum()` - Technical indicators
  - `get_all()` - Comprehensive analysis with configurable periods
  - `ensure_df()`, `add_metrics()` - Data preparation and enrichment
- **Features**: Polars DataFrame integration, rolling window calculations, multiple analysis types

### Data Converter (`src/services/converter.py`)
- **Purpose**: Currency/asset conversion and peg analysis
- **Key Functions**:
  - `convert(pair, base_amount, quote_amount)` - Currency conversion
  - `pegcheck(pair, factor, max_deviation)` - Peg deviation analysis
- **Features**: Automatic pair resolution, precision control, deviation detection

### Status Checker (`src/services/status_checker.py`)
- **Purpose**: System health monitoring and ping functionality
- **Key Functions**:
  - `check_status(req, utc_time)` - Health check with optional time validation
- **Features**: UTC time synchronization, system status reporting

### Gatekeeper (`src/services/gatekeeeper.py`)
- **Purpose**: Authentication and user identification
- **Key Functions**:
  - `requester_id(request)` - Extract user ID from request headers
- **Features**: IP-based identification, request header parsing

## Service Integration

### Error Handling
All services use the `ServiceResponse` pattern for consistent error handling and result delivery.

### State Management
Services access shared resources through the global `state` module including database connections, Redis cache, and configuration.

### Configuration
Services are configured through environment variables and runtime parameters, with hot-reloading support for dynamic updates.
