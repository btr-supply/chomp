# Actions

Core actions that handle Chomp's data ingestion workflow orchestration.

## Overview

Actions form the ETL (Extract, Transform, Load) pipeline. Each action represents a distinct phase in data processing:

Schedule → Load → Transform → Store

## Available Actions

### Schedule (`src/actions/schedule.py`)
- **Purpose**: Orchestrates execution timing and coordination of ingester jobs across clustered instances
- **Key Functions**: `schedule(ingester)`, `scheduler.start(threaded)`
- **Features**: Converts interval notation to cron expressions, Redis-based distributed locking, jitter implementation

### Load (`src/actions/load.py`)
- **Purpose**: Handles data extraction from configured sources
- **Key Functions**: `load_data(ingester)`
- **Features**: Supports all ingester types, automatic type delegation, retry logic

### Transform (`src/actions/transform.py`)
- **Purpose**: Converts raw data into structured, validated formats
- **Key Functions**: Field extraction, type conversion, transformer chain execution
- **Features**: Built-in functions, mathematical expressions, cross-field dependencies

### Store (`src/actions/store.py`)
- **Purpose**: Persists transformed data to the configured database backend
- **Key Functions**: `store_data(ingester, transformed_data)`
- **Features**: Automatic table creation, batch insertions, schema management

## Configuration

Actions are configured through environment variables and ingester-specific settings. Threading, retry limits, and performance tuning can be adjusted per ingester type.
