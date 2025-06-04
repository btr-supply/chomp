# Cache System

Chomp's Redis-based caching and coordination system for distributed operations.

## Overview

Redis serves as the primary cache and coordination mechanism, handling:
- Task coordination and distributed locking
- Data caching and pubsub messaging
- Rate limiting and session management

## Core Implementation (`src/cache.py`)

### Key Functions
- **Task Coordination**: `claim_task()`, `is_task_claimed()`, `ensure_claim_task()`
- **Caching**: `cache()`, `get_cache()`, `cache_batch()`
- **Pubsub**: `pub()`, `sub()`
- **Registry**: `register_ingester()`, `get_registered_ingesters()`

### Key Namespacing
Uses `REDIS_NS` environment variable (default: `"chomp"`) with structured patterns:
- `chomp:claims:{ingester_id}` - Task ownership
- `chomp:cache:{name}` - General caching
- `chomp:ingesters:{name}` - Ingester registry
- `chomp:locks:ingesters` - Registry locks

## Configuration

### Environment Variables
```bash
REDIS_NS=chomp           # Namespace prefix
REDIS_HOST=localhost     # Redis server host
REDIS_PORT=40001        # Redis server port
REDIS_DB=0              # Redis database number
```

### Connection Management
Redis connection is managed through `src/proxies.py` with connection pooling and automatic reconnection.

## Task Coordination

### Distributed Locking
- Uses Redis SETEX with NX flag for atomic claims
- Automatic expiration based on ingester intervals
- Registry locks prevent race conditions during ingester registration

### Clustering Support
- Process identification via `proc_id`
- Probabilistic task execution via `probablity` field
- Heartbeat monitoring for task health

## Data Management

### Caching Strategies
- General purpose caching with configurable TTL
- Batch operations for performance
- Pickle support for complex objects
- Registry caching for ingester configurations

### Ingester Registry
- Centralized registry of all ingesters
- Scope-based serialization (see `src/model.py` Scope enum)
- Hot-reloading of configurations
- Dependency resolution between ingesters

## Performance Features

- Connection pooling through Redis proxy
- Batch operations with pipelines
- Configurable TTL per cache operation
- Memory-efficient serialization options
