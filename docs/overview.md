# Chomp Documentation Overview

Comprehensive documentation for the Chomp data ingestion framework covering architecture, deployment, APIs, and data processing.

## Documentation Structure

### 🏗️ **Core Architecture**
- **[Architecture](./architecture.md)** - System design, components, and extension points
- **[Services](./services.md)** - Business logic services and interactions
- **[Actions](./actions.md)** - ETL pipeline and workflow orchestration

### 🚀 **Setup & Operations**
- **[Deployment Guide](./deployment.md)** - Installation, configuration, and production setup

### 🔌 **APIs & Integration**
- **[API Reference](./api.md)** - REST and WebSocket endpoints with examples

### 📊 **Data Layer**
- **[Storage Adapters](./adapters/storage.md)** - Database backends and configuration
- **[Cache System](./adapters/cache.md)** - Redis caching and coordination

### 🔄 **Data Ingestion**
- **[Ingesters Overview](./ingesters/overview.md)** - Data source types and configuration

## Quick Start Paths

**For Developers**: [Architecture](./architecture.md) → [Services](./services.md) → [Storage Adapters](./adapters/storage.md)

**For Operators**: [Deployment Guide](./deployment.md) → [API Reference](./api.md) → [Cache System](./adapters/cache.md)

**For Integrators**: [API Reference](./api.md) → [Ingesters Overview](./ingesters/overview.md) → [Architecture](./architecture.md)

## Documentation Status

- ✅ **Complete**: Architecture, deployment, APIs, storage adapters, cache system, services
- 🚧 **In Progress**: Detailed ingester type documentation
- 📋 **Planned**: Performance guides, troubleshooting, custom adapter tutorials
