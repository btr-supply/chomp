# Chomp API Documentation

## Overview

Chomp is a FastAPI-based cryptocurrency data aggregation and analysis platform that provides real-time price feeds, historical data, and various financial analysis tools. The API runs on port 40004 and provides both REST endpoints and WebSocket streaming capabilities.

## Base URL
```
http://localhost:40004
```

## Authentication
Currently no authentication is required for API access.

## Rate Limiting
The API implements rate limiting through custom middleware to prevent abuse.

---

## Core Endpoints

### Health Check

#### GET `/ping`
Returns server health status and basic information.

**Response:**
```json
{
  "name": "chomp",
  "version": "1.1.0",
  "status": "OK",
  "ping_ms": 5,
  "server_time": 1749199306650,
  "id": "127.0.0.1",
  "ip": "127.0.0.1"
}
```

### System Information

#### GET `/limits`
Returns current rate limiting configuration.

**Response:**
```json
{
  "rate_limit": {
    "requests_per_minute": 60,
    "requests_per_hour": 1000
  }
}
```

#### GET `/resources`
Returns server resource utilization.

**Response:**
```json
{
  "cpu_percent": 23.4,
  "memory_percent": 67.8,
  "disk_usage": 45.2
}
```

---

## Data Schema

#### GET `/schema`
Returns the complete database schema showing all available data feeds and their fields.

**Response:**
```json
{
  "tables": {
    "XtComFeeds": ["USDT", "DAI", "TUSD", "USDM", "USDC", "BTC", "BCH", "LTC", "XRP", "ETH", "BNB", "OP", "ARB", "SOL", "TON", "SUI", "APT", "ADA", "DOT", "KSM", "AVAX", "S", "GLMR", "GNO", "LDO", "LINK", "MKR", "CAKE", "AAVE", "COMP", "CRV", "SUSHI", "UNI", "PENDLE", "MORPHO", "DOGE", "PEPE", "BONK", "FLOKI", "SHIB", "VIRTUAL", "THE", "ENA", "SCR", "BRETT", "VELO", "AIXBT", "AERO", "LUNA", "ZK", "MNT"],
    "ToobitFeeds": ["USDT", "FDUSD", "USDC", "BTC", "BCH", "LTC", "XRP", "ETH", "BNB", "OP", "ARB", "POL", "SOL", "TON", "SUI", "APT", "ADA", "DOT", "AVAX", "S", "AAVE", "COMP", "CRV", "SUSHI", "UNI", "PENDLE", "DOGE", "PEPE", "BONK", "FLOKI", "SHIB", "THE", "ENA", "LDO", "LINK", "MKR", "CAKE"],
    "WhiteBitFeeds": ["USDT", "DAI", "TUSD", "USDC", "BTC", "BCH", "LTC", "XRP", "ETH", "OP", "ARB", "POL", "SOL", "TON", "SUI", "APT", "ADA", "DOT", "KSM", "AVAX", "S", "GLMR", "GNO", "LDO", "LINK", "MKR", "CAKE", "AAVE", "COMP", "CRV", "SUSHI", "UNI", "PENDLE", "DOGE", "PEPE", "BONK", "FLOKI", "SHIB", "ENA", "SCR", "ZK"],
    "UpbitFeeds": ["USDT", "TUSD", "USDC", "BTC", "BCH", "XRP", "ETH", "SOL", "ADA", "MNT", "UNI", "DOGE", "PEPE", "BONK", "BRETT"],
    "TrubitFeeds": ["USDT", "USDD", "USDP", "USDC", "BTC", "BCH", "LTC", "XRP", "ETH", "BNB", "OP", "ARB", "SOL", "TON", "SUI", "APT", "ADA", "DOT", "KSM", "AVAX", "S", "ZK", "AAVE", "COMP", "CRV", "SUSHI", "UNI", "PENDLE", "DOGE", "PEPE", "BONK", "FLOKI", "SHIB", "LDO", "LINK", "MKR", "CAKE"]
  }
}
```

---

## Data Retrieval

### Historical Data

#### GET `/history/{feed_name}`
Returns historical price data for specified fields from a data feed.

**Parameters:**
- `feed_name` (path): Name of the data feed (e.g., XtComFeeds, ToobitFeeds, etc.)
- `fields` (query): Comma-separated list of cryptocurrency symbols (e.g., BTC,ETH)
- `limit` (query): Maximum number of records to return (default: 100)
- `start_time` (query, optional): Start timestamp for data range
- `end_time` (query, optional): End timestamp for data range

**Example Request:**
```
GET /history/XtComFeeds?fields=BTC&limit=5
```

**Response:**
```json
{
  "status": "success",
  "data": [
    {
      "ts": "2025-06-06T08:40:00.000Z",
      "BTC": 103455.9363
    },
    {
      "ts": "2025-06-06T08:39:50.000Z",
      "BTC": 103421.1319
    },
    {
      "ts": "2025-06-06T08:39:40.000Z",
      "BTC": 103400.389
    },
    {
      "ts": "2025-06-06T08:39:30.000Z",
      "BTC": 103445.5969
    },
    {
      "ts": "2025-06-06T08:39:20.000Z",
      "BTC": 103476.7185
    }
  ],
  "count": 5,
  "feed": "XtComFeeds",
  "fields": ["BTC"]
}
```

### Latest Data

#### GET `/last/{feed_name}`
Returns the most recent data point for all fields in a data feed.

**Parameters:**
- `feed_name` (path): Name of the data feed

**Example Request:**
```
GET /last/XtComFeeds
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "ts": "2025-06-06T08:40:50.000Z",
    "USDT": 1.0006,
    "DAI": 1.0002,
    "TUSD": 0.998799,
    "USDM": 1.0012,
    "USDC": 1.0,
    "BTC": 103533.8831,
    "BCH": 387.4323,
    "LTC": 84.1705,
    "XRP": 2.1345,
    "ETH": 2462.3365,
    "BNB": 642.4853,
    "OP": 0.5994,
    "ARB": 0.3336,
    "SOL": 147.7786,
    "TON": 3.1019,
    "SUI": 3.0624,
    "APT": 4.6028,
    "ADA": 0.6394,
    "DOT": 3.8793,
    "KSM": 15.5693,
    "AVAX": 19.4016,
    "S": 0.3718,
    "GLMR": 0.0749,
    "GNO": 123.0738,
    "LDO": 0.7695,
    "LINK": 13.1879,
    "MKR": 1775.0644,
    "CAKE": 2.2984,
    "AAVE": 249.7197,
    "COMP": 49.6097,
    "CRV": 0.6213,
    "SUSHI": 0.6304,
    "UNI": 6.0166,
    "PENDLE": 4.0764,
    "MORPHO": 1.2689,
    "DOGE": 0.175055,
    "PEPE": 0.0000110566,
    "BONK": 0.000015039,
    "FLOKI": 0.0000842005,
    "SHIB": 0.0000122674,
    "VIRTUAL": 1.7,
    "THE": 0.2204,
    "ENA": 0.2937,
    "SCR": 0.2742,
    "BRETT": 0.046838086,
    "VELO": 0.0488,
    "AIXBT": 0.17570536,
    "AERO": 0.4987,
    "LUNA": 0.16279762,
    "ZK": 0.0529,
    "MNT": 0.6348
  },
  "feed": "XtComFeeds"
}
```

---

## Financial Tools

### Currency Conversion

#### GET `/convert/{source_pair}-{target_pair}`
Converts amounts between different cryptocurrency pairs across exchanges.

**Parameters:**
- `source_pair` (path): Source exchange and currency (e.g., XtComFeeds.BTC)
- `target_pair` (path): Target exchange and currency (e.g., WhiteBitFeeds.USDC)
- `base_amount` (query): Amount to convert (default: 1.0)

**Example Request:**
```
GET /convert/XtComFeeds.BTC-WhiteBitFeeds.USDC?base_amount=0.1
```

**Response:**
```json
{
  "status": "success",
  "conversion": {
    "source": {
      "exchange": "XtComFeeds",
      "currency": "BTC",
      "amount": 0.1,
      "price": 103533.8831
    },
    "target": {
      "exchange": "WhiteBitFeeds",
      "currency": "USDC",
      "amount": 10353.38831,
      "price": 1.0
    },
    "rate": 103533.8831,
    "timestamp": "2025-06-06T08:40:50.000Z"
  }
}
```

### Peg Stability Check

#### GET `/pegcheck/{pair1}-{pair2}`
Checks price stability between two stablecoin pairs across exchanges.

**Parameters:**
- `pair1` (path): First exchange and currency pair (e.g., XtComFeeds.USDC)
- `pair2` (path): Second exchange and currency pair (e.g., WhiteBitFeeds.USDC)

**Example Request:**
```
GET /pegcheck/XtComFeeds.USDC-WhiteBitFeeds.USDC
```

**Response:**
```json
{
  "status": "success",
  "peg_check": {
    "pair1": {
      "exchange": "XtComFeeds",
      "currency": "USDC",
      "price": 1.0
    },
    "pair2": {
      "exchange": "WhiteBitFeeds",
      "currency": "USDC",
      "price": 1.0
    },
    "difference": 0.0,
    "percentage_diff": 0.0,
    "is_pegged": true,
    "tolerance": 0.01,
    "timestamp": "2025-06-06T08:40:50.000Z"
  }
}
```

### Volatility Analysis

#### GET `/volatility/{feed_name}`
Returns volatility analysis for specified fields.

**Parameters:**
- `feed_name` (path): Name of the data feed
- `fields` (query): Comma-separated list of cryptocurrency symbols
- `limit` (query): Number of data points to analyze (default: 100)
- `window` (query): Analysis window in minutes (default: 60)

**Example Request:**
```
GET /volatility/XtComFeeds?fields=BTC&limit=10
```

**Note:** This endpoint currently has a JSON serialization issue with DataFrame objects that needs to be resolved.

---

## Real-time Data Streaming

### WebSocket Connection

#### WebSocket `/ws`
Provides real-time streaming of price data updates.

**Connection:**
```javascript
const ws = new WebSocket('ws://localhost:40004/ws');
```

**Subscribe to Data Feeds:**
```json
{
  "action": "subscribe",
  "topics": ["chomp:XtComFeeds", "chomp:ToobitFeeds"]
}
```

**Unsubscribe from Data Feeds:**
```json
{
  "action": "unsubscribe",
  "topics": ["chomp:XtComFeeds"]
}
```

**Example Messages:**
```json
{
  "success": true,
  "subscribed": ["chomp:XtComFeeds"]
}
```

```json
{
  "USDT": 1.0006,
  "DAI": 1.0002,
  "TUSD": 0.998799,
  "USDM": 1.0012,
  "USDC": 1.0,
  "BTC": 103533.8831,
  "BCH": 387.4323,
  "LTC": 84.1705,
  "XRP": 2.1345,
  "ETH": 2462.3365,
  "BNB": 642.4853,
  "OP": 0.5994,
  "ARB": 0.3336,
  "SOL": 147.7786,
  "TON": 3.1019,
  "SUI": 3.0624,
  "APT": 4.6028,
  "ADA": 0.6394,
  "DOT": 3.8793,
  "KSM": 15.5693,
  "AVAX": 19.4016
}
```

---

## Error Handling

### Standard Error Response Format
```json
{
  "status": "error",
  "error": {
    "code": "INVALID_FEED",
    "message": "Feed 'InvalidFeed' not found",
    "details": "Available feeds: XtComFeeds, ToobitFeeds, WhiteBitFeeds, UpbitFeeds, TrubitFeeds"
  }
}
```

### Common Error Codes
- `400 Bad Request`: Invalid parameters or malformed request
- `404 Not Found`: Resource not found (invalid feed name, etc.)
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: Server-side error

---

## Data Feeds

### Available Exchanges
1. **XtComFeeds** - 50 cryptocurrency pairs
2. **ToobitFeeds** - 37 cryptocurrency pairs
3. **WhiteBitFeeds** - 41 cryptocurrency pairs
4. **UpbitFeeds** - 15 cryptocurrency pairs
5. **TrubitFeeds** - 35 cryptocurrency pairs

### Update Frequency
- Data is updated every 10 seconds for all feeds
- WebSocket streams provide real-time updates as data changes
- Historical data is stored with millisecond precision timestamps

---

## Testing

### WebSocket Test
A WebSocket test script is available at `chomp/tests/test_websocket.py`:

```bash
cd chomp
uv run python tests/test_websocket.py
```

This script demonstrates:
- Connecting to the WebSocket endpoint
- Subscribing to data feeds
- Receiving real-time data
- Unsubscribing from feeds
- Proper connection handling

---

## Technical Analysis Endpoints

### General Analysis

#### GET `/analysis/{resources:path}`
Returns comprehensive time series analysis including volatility, trend, and momentum metrics for specified resources and fields.

**Parameters:**
- `resources` (path): Resource name(s) (e.g., USDC, BTC)
- `fields` (query): Comma-separated list of field names (default: "")
- `from_date` (query): Start date for analysis period
- `to_date` (query): End date for analysis period
- `periods` (query): Comma-separated list of periods for calculations (default: "20")
- `precision` (query): Decimal precision for results (default: 6)
- `quote` (query): Quote currency for conversions
- `format` (query): Output format (default: "json:row")

**Example Request:**
```
GET /analysis/USDC?fields=idx&periods=5,10,20
```

**Note:** This endpoint is currently experiencing technical issues and may return errors.

### Volatility Analysis

#### GET `/volatility/{resources:path}`
Returns volatility analysis for specified fields including standard deviation, weighted standard deviation, and Average True Range (ATR) metrics.

**Parameters:**
- `resources` (path): Resource name(s) (e.g., USDC, BTC)
- `fields` (query): Field names to analyze (default: "idx")
- `from_date` (query): Start date for analysis period
- `to_date` (query): End date for analysis period
- `periods` (query): Comma-separated list of periods for calculations (default: "20")
- `precision` (query): Decimal precision for results (default: 6)
- `quote` (query): Quote currency for conversions
- `format` (query): Output format (default: "json:row")

**Example Request:**
```
GET /volatility/USDC?fields=idx&periods=5,10
```

**Note:** This endpoint is currently experiencing serialization issues and may return errors.

### Trend Analysis

#### GET `/trend/{resources:path}`
Returns trend analysis including Simple Moving Average (SMA), Exponential Moving Average (EMA), linear regression, and other trend indicators.

**Parameters:**
- `resources` (path): Resource name(s) (e.g., USDC, BTC)
- `fields` (query): Field names to analyze (default: "")
- `from_date` (query): Start date for analysis period
- `to_date` (query): End date for analysis period
- `periods` (query): Comma-separated list of periods for calculations (default: "20")
- `precision` (query): Decimal precision for results (default: 6)
- `quote` (query): Quote currency for conversions
- `format` (query): Output format (default: "json:row")

**Example Request:**
```
GET /trend/USDC?fields=idx&periods=20,50
```

**Note:** This endpoint is currently experiencing serialization issues and may return errors.

### Momentum Analysis

#### GET `/momentum/{resources:path}`
Returns momentum analysis including Rate of Change (ROC), RSI, MACD, Stochastic oscillator, and other momentum indicators.

**Parameters:**
- `resources` (path): Resource name(s) (e.g., USDC, BTC)
- `fields` (query): Field names to analyze (default: "")
- `from_date` (query): Start date for analysis period
- `to_date` (query): End date for analysis period
- `periods` (query): Comma-separated list of periods for calculations (default: "20")
- `precision` (query): Decimal precision for results (default: 6)
- `quote` (query): Quote currency for conversions
- `format` (query): Output format (default: "json:row")

**Example Request:**
```
GET /momentum/USDC?fields=idx&periods=14,21
```

**Note:** This endpoint is currently experiencing serialization issues and may return errors.

### Operating Range Analysis

#### GET `/oprange/{resources:path}`
Returns operating range analysis including min/max values, current range position, and Average True Range (ATR) calculations.

**Parameters:**
- `resources` (path): Resource name(s) (e.g., USDC, BTC)
- `fields` (query): Field names to analyze (default: "")
- `from_date` (query): Start date for analysis period
- `to_date` (query): End date for analysis period
- `precision` (query): Decimal precision for results (default: 6)
- `quote` (query): Quote currency for conversions
- `format` (query): Output format (default: "json:row")

**Example Request:**
```
GET /oprange/USDC?fields=idx
```

**Response:**
```json
{
  "columns": [
    "ts",
    "idx",
    "idx:min",
    "idx:max",
    "idx:range",
    "idx:range_position",
    "idx:atr_14",
    "idx:current"
  ],
  "types": [
    "int64",
    "float64",
    "float64",
    "float64",
    "float64",
    "null",
    "null",
    "float64"
  ],
  "data": [
    [
      1748903390000.0,
      1.0,
      1.0,
      1.0,
      0.0,
      null,
      null,
      1.0
    ]
  ]
}
```

---

## User Interface Endpoints

### Root/Home Page

#### GET `/`
#### GET `/index`
Returns the main landing page for the Chomp API with basic information and navigation links.

**Response:** HTML page with API information, version, and links to resources.

### API Documentation Page

#### GET `/docs`
Returns an interactive API documentation page with Swagger UI interface.

**Response:** HTML page with Swagger UI for API exploration and testing.

## Notes

1. All timestamps are in ISO 8601 format with millisecond precision
2. Price data is provided as floating-point numbers
3. The API uses TDengine as the underlying time-series database
4. Redis is used for caching and real-time data distribution
5. Rate limiting is enforced per IP address
6. **Technical Analysis Endpoints**: Several analysis endpoints (`/analysis`, `/volatility`, `/trend`, `/momentum`) are currently experiencing DataFrame serialization issues that prevent proper JSON responses. These need to be fixed.
7. The `/oprange` endpoint works correctly and returns proper JSON responses
8. Some field names containing dots (like "Binance.1") may cause SQL syntax errors in analysis queries
