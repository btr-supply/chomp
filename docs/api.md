# API

Chomp's FastAPI-based REST and WebSocket APIs for accessing ingested data.

## Overview

The API server provides access to ingested data through:
- REST endpoints for data retrieval (`src/server/routers/retriever.py`)
- WebSocket connections for real-time streaming (`src/server/routers/forwarder.py`)
- Rate limiting and middleware (`src/server/middlewares/`)

## Configuration

### Server Settings
```bash
HOST=127.0.0.1                    # Server bind address
PORT=40004                        # Server port
WS_PING_INTERVAL=30               # WebSocket ping interval
WS_PING_TIMEOUT=20                # WebSocket ping timeout
```

### Server Startup
Multi-version support with middleware stack:
- Rate limiting (`src/server/middlewares/limiter.py`)
- CORS handling
- Response compression

## REST Endpoints (`retriever.py`)

### Core Endpoints
- `GET /ping` - Health check with optional UTC time parameter
- `GET /resources` - Resource discovery and schema information
- `GET /last/{resources}` - Latest data values with quote conversion
- `GET /history/{resources}` - Historical data with time-based filtering
- `GET /limits` - User rate limit status

### Analysis Endpoints
- `GET /analysis/{resources}` - Statistical analysis with configurable periods
- `GET /volatility/{resources}` - Volatility calculations
- `GET /trend/{resources}` - Trend analysis
- `GET /momentum/{resources}` - Momentum indicators
- `GET /oprange/{resources}` - Operating range analysis

### Conversion Endpoints
- `GET /convert/{pair}` - Currency/asset conversion
- `GET /pegcheck/{pair}` - Peg deviation analysis

### Query Parameters
- `resources`: Resource names (comma-separated paths)
- `fields`: Specific fields to retrieve
- `from_date`/`to_date`: Time range filtering
- `interval`: Aggregation interval
- `format`: Response format (`json:row`, `json:col`, `csv`, `parquet`)
- `precision`: Decimal precision for numbers
- `quote`: Quote currency for conversions

## WebSocket API (`forwarder.py`)

### Real-Time Streaming
- WebSocket endpoint: `ws://host:port/ws/{resource}`
- Redis pubsub integration for scalable distribution
- Automatic connection management and heartbeat

### Message Types
Real-time data updates, status notifications, and error messages pushed to subscribed clients.

## Rate Limiting (`limiter.py`)

### Multi-dimensional Limits
- Points-based system (different endpoints cost different points)
- Requests per minute/hour/day tracking
- Size-based limits for large responses
- Redis-backed counters for distributed limiting

### Implementation
Uses decorator `@limit(points=N)` on endpoints with user identification and automatic counter updates.

## Response Handling (`responses.py`)

### Unified Response Format
- `ApiResponse` class for consistent formatting
- Format conversion (JSON, CSV, Parquet)
- Error handling with service response pattern
- Compression and caching headers

## Data Services Integration

### Service Layer
Endpoints delegate to services in `src/services/`:
- `loader.py` - Data loading and resource management
- `converter.py` - Currency conversion
- `ts_analysis.py` - Time series analysis
- `limiter.py` - Rate limiting
- `gatekeeeper.py` - Authentication

### Service Response Pattern
All services return `ServiceResponse = tuple[str, any]` where first element is error message (empty if success) and second is result data.

## Configuration

API behavior is controlled through environment variables and the services layer, with automatic registration of available resources based on configured ingesters.
