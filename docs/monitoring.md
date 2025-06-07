# Chomp Monitoring System

Chomp provides a **lean, two-level monitoring system**: **request-level** and **instance-level** monitoring with minimal overhead and unique table separation for cluster deployments.

## Overview

**Core Features**:
- Request performance tracking (latency, throughput, status codes)
- Instance health monitoring (CPU, memory, disk I/O)
- Geolocation and ISP detection for instances
- Time series storage + Redis caching
- **Unique tables per resource/instance** - prevents cluster conflicts
- **Transient fields** - cached metadata without time series bloat

## Architecture

### Naming Convention

All monitoring follows consistent patterns to prevent table conflicts:
- **Resource monitors**: `{resource_name}_monitor` (e.g., `BinanceETH_monitor`)
- **Instance monitors**: `{instance_name}_monitor` (e.g., `chomp_worker_01_monitor`)

### Core Components

1. **Monitor Class**: Simple request timer
2. **Instance**: IP detection + geolocation with 6-hour caching
3. **System Monitor Ingester**: Collects vitals every 30s when `--monitored`
4. **Storage Integration**: Automatic collection via standard `store()` flow

## Request Monitoring

### Basic Usage

```python
# Automatic monitoring in ingesters
if monitor:
  monitor.start_timer()

# ... fetch data ...

if monitor:
  monitor.stop_timer(response_bytes, status_code)
```

### RequestVitals

```python
@dataclass
class RequestVitals:
  instance_name: str = ""
  field_count: int = 0
  latency_ms: float = 0.0
  response_bytes: int = 0
  status_code: Optional[int] = None
```

**Storage**: Automatically stored in `{resource_name}_monitor` table when `store()` detects vitals.

## Instance Monitoring

### System Vitals

Collected every **30 seconds** with `python main.py --monitored`:

```python
@dataclass
class InstanceVitals:
  instance_name: str = ""
  resources_count: int = 0
  cpu_usage: float = 0.0     # % (non-blocking measurement)
  memory_usage: float = 0.0  # bytes
  disk_usage: float = 0.0    # bytes/second rate
```

### Instance Identity & Geolocation

Each instance includes cached location data (6-hour TTL):

```python
@dataclass
class Instance:
  # Core identity
  uid: str              # Internal unique ID
  pid: int              # Process ID
  hostname: str         # Server hostname
  name: str             # Human-friendly name from UID masks
  ipv4: str            # External IPv4 address
  ipv6: str            # External IPv6 address

  # Geolocation (cached, transient)
  coordinates: str      # "lat,lon" format
  timezone: str         # e.g., "Asia/Ho_Chi_Minh"
  country_code: str     # e.g., "VN"
  location: str         # e.g., "Da Nang, Da Nang, Vietnam"
  isp: str             # e.g., "Viettel Group"
```

**APIs Used**:
- `https://api4.ipify.org/?format=json` (IPv4)
- `https://api6.ipify.org/?format=json` (IPv6)
- `http://ip-api.com/json/{ip}` (geolocation)

**Transient Fields**: Geolocation data is cached in Redis but **not stored** in time series database every 30s.

## Storage & Retrieval

### Automatic Storage

All monitoring uses standard ingester flow:

1. **Request vitals**: Auto-stored when `monitor.vitals` exists
2. **Instance vitals**: Stored by system monitor ingester
3. **Redis cache**: Latest values cached with resource names
4. **Time series**: Historical data in dedicated tables per resource/instance

### API Endpoints

Use existing generic endpoints:

```bash
# Latest data
GET /last/{resource_name}_monitor        # Request monitoring
GET /last/{instance_name}_monitor        # Instance monitoring
GET /last/BinanceETH_monitor,chomp_worker_01_monitor  # Combined

# Historical data
GET /history/{instance_name}_monitor?from_date=2024-01-01&interval=h1

# Schema
GET /schema/{instance_name}_monitor,{resource_name}_monitor
```

### Example Response

**Instance Monitor** (`/last/chomp_worker_01_monitor`):
```json
{
  "ts": "2024-01-01T12:00:00Z",
  "instance_name": "chomp-worker-01",
  "resources_count": 15,
  "cpu_usage": 25.5,
  "memory_usage": 1073741824,
  "disk_usage": 1048576.5,
  "coordinates": "16.0685,108.2215",
  "timezone": "Asia/Ho_Chi_Minh",
  "country_code": "VN",
  "location": "Da Nang, Da Nang, Vietnam",
  "isp": "Viettel Group",
  "date": "2024-01-01T12:00:00Z"
}
```

**Resource Monitor** (`/last/BinanceETH_monitor`):
```json
{
  "ts": "2024-01-01T12:00:00Z",
  "instance_name": "BinanceETH_Ingester",
  "field_count": 5,
  "latency_ms": 150.5,
  "response_bytes": 2048,
  "status_code": 200,
  "date": "2024-01-01T12:00:00Z"
}
```

## Configuration

### Command Line

```bash
# Enable instance monitoring
python main.py --monitored

# Verbose logging (shows monitoring operations)
python main.py --verbose --monitored
```

### Performance Optimizations

- **Non-blocking CPU**: Uses `psutil.cpu_percent(interval=None)`
- **Proper I/O rates**: Time-delta calculations for bytes/second
- **6-hour caching**: IP/geolocation cached to minimize API calls
- **Transient fields**: Location data cached but not stored in time series
- **Error isolation**: Individual try-catch prevents cascade failures

## Implementation Details

### Instance Monitor Creation

```python
def create_instance_monitor() -> 'Ingester':
  instance_name = state.instance.name
  monitor_name = f"{instance_name}_monitor"

  return Ingester(
    name=monitor_name,
    resource_type="timeseries",
    ingester_type="monitor",
    interval="s30",
    fields=[
      ResourceField(name="ts", type="timestamp"),
      ResourceField(name="instance_name", type="string"),
      ResourceField(name="resources_count", type="int32"),
      ResourceField(name="cpu_usage", type="float64"),
      ResourceField(name="memory_usage", type="float64"),
      ResourceField(name="disk_usage", type="float64"),
      # Geolocation (transient)
      ResourceField(name="coordinates", type="string", transient=True),
      ResourceField(name="timezone", type="string", transient=True),
      ResourceField(name="country_code", type="string", transient=True),
      ResourceField(name="location", type="string", transient=True),
      ResourceField(name="isp", type="string", transient=True),
    ]
  )
```

### Resource Monitor Creation

```python
# In store.py - automatic per-resource monitor creation
resource_monitor_name = f"{ingester.name}_monitor"
state.resource_monitor_ingesters[resource_monitor_name] = Ingester(
  name=resource_monitor_name,
  resource_type="timeseries",
  fields=[
    ResourceField(name="ts", type="timestamp"),
    ResourceField(name="instance_name", type="string"),
    ResourceField(name="field_count", type="int32"),
    ResourceField(name="latency_ms", type="float64"),
    ResourceField(name="response_bytes", type="int64"),
    ResourceField(name="status_code", type="int32"),
  ]
)
```

## Troubleshooting

**No monitoring data**: Check `--monitored` flag and Redis/DB connectivity
**Missing request vitals**: Ensure `monitor.start_timer()` and `monitor.stop_timer()` calls
**High CPU from monitoring**: Verify non-blocking CPU measurement (should use `interval=None`)
**Geolocation missing**: Check network access to `ipify.org` and `ip-api.com`

Use `--verbose` flag to see detailed monitoring operations in logs.

## Design Goals Achieved

- **<1ms overhead** per request operation
- **Non-blocking measurements** prevent delays
- **Essential metrics only** - no profiling bloat
- **Cluster-safe** - unique tables per resource/instance
- **Cached metadata** - rich instance context without time series pollution
- **Standard integration** - reuses existing storage/retrieval infrastructure
