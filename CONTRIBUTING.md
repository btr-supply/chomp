# Contributing to Chomp

Thank you for your interest in contributing to Chomp! This document provides guidelines for contributing to the Chomp data ingestion framework. All participants are expected to be respectful and inclusive in all interactions. Harassment of any kind is not tolerated.

## Table of Contents

- [Project Overview](#project-overview)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Branch Structure](#branch-structure)
- [Naming Conventions](#naming-conventions)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Architecture Guidelines](#architecture-guidelines)

## Project Overview

Chomp is a lightweight, multimodal data ingester for Web2/Web3 sources with Python 3.11, asyncio, TDengine storage, Redis coordination, and FastAPI + WebSocket APIs.

## Getting Started

### Prerequisites

- **Unix-like system** (macOS/Linux)
- **Git, Python >= 3.11, UV, Docker**

### Setup

```bash
git clone https://github.com/btr-supply/chomp.git
cd chomp
make install-deps
make dev-setup
```

## Development Workflow

We follow a simplified gitflow workflow:

```
main     ← production-ready, stable releases
└── dev  ← active development (features, fixes merged here)
    └── feat/new-adapter
    └── fix/connection-issue
    └── docs/update-readme
```

### Branch Structure

- **`main`** - Production branch with stable, tested code
- **`dev`** - Active development branch
- **Feature/Fix branches** - Created from `dev`, merged back via PR

### Naming Conventions

| Type     | Example Branch          | Example Commit                    | Description                          |
|----------|-------------------------|-----------------------------------|--------------------------------------|
| **feat** | `feat/live-data`        | `[feat] Add real-time feeds`      | New features, improvements, updates  |
| **fix**  | `fix/chart-leak`        | `[fix] Resolve memory leak`       | Bug fixes, issues                    |
| **refac**| `refac/stores`          | `[refac] Optimize state`          | Refactors for style or performance   |
| **docs** | `docs/examples`         | `[docs] Add examples`             | Docs, comments, translations, README |
| **ops**  | `ops/deps`              | `[ops] Update dependencies`       | CI/CD, dependencies, scripts, chores |

### Pull Request Process

1. Ensure `make build` passes locally (format, lint, test)
2. Update documentation for new features/API changes
3. Use proper commit format in PR title
4. Reference related issues and provide clear change description
5. Wait for maintainer review

## Coding Standards

### Python Standards

- **Formatting**: `black` and `ruff format`
- **Linting**: `ruff` checks
- **Type Hints**: Use for all function parameters and return values
- **Docstrings**: Google-style for all public functions and classes
- **Environment Variables**: Always import `os.environ` as `env` for ease of use: `from os import environ as env`

```python
def process_data(data: Dict[str, Any], timeout: float = 5.0) -> List[Dict[str, Any]]:
    """Process raw data from API response.

    Args:
        data: Raw API response data
        timeout: Processing timeout in seconds

    Returns:
        List of processed data records

    Raises:
        TimeoutError: If processing exceeds timeout
        ValueError: If data format is invalid
    """
    pass
```

### Configuration Standards

- **YAML**: 2-space indentation, snake_case fields
- **Comments**: Document complex configurations inline

### Component Guidelines

**Database Adapters**:
1. Inherit from `BaseAdapter` in `src/adapters/base.py`
2. Implement all required methods with error handling
3. Include connection pooling if applicable

**Ingesters**:
1. Follow patterns in `src/ingesters/`
2. Use async/await for I/O operations
3. Implement rate limiting and handle connection failures
4. Support clustering via Redis coordination

## Testing

### Structure

```
tests/
├── unit/           # Unit tests for individual components
├── integration/    # Integration tests with external services
├── fixtures/       # Test data and configuration files
└── conftest.py     # Pytest configuration and fixtures
```

### Writing Tests

- Use `pytest` with descriptive test names
- Mock external dependencies in unit tests
- Include both positive and negative test cases

### Running Tests

```bash
make test                           # Run all tests
pytest tests/unit/test_adapters.py -v  # Run specific test file
pytest --cov=src tests/            # Run with coverage
```

## Architecture Guidelines

### Adding New Components

**Database Adapters**:
1. Create new file in `src/adapters/`
2. Inherit from `BaseAdapter`
3. Implement connection management and add to registry
4. Update documentation

**Data Sources**:
1. Create ingester in `src/ingesters/`
2. Define YAML configuration schema with validation
3. Implement data transformation logic
4. Update configuration schema

### Performance Considerations

- Use connection pooling and asyncio for concurrency
- Implement backpressure handling and exponential backoff
- Monitor memory usage and profile critical paths

### Error Handling

- Use structured logging with appropriate levels
- Implement graceful failure handling for external services
- Provide meaningful error messages with proper retry logic

### Configuration Example

```yaml
# Example ingester configuration
http_api:
  - name: ExampleAPI           # Unique identifier
    interval: s30              # Polling interval (s=seconds, m=minutes)
    target: "https://api.example.com/data"
    timeout: 30                # Request timeout in seconds
    fields:
      - name: price            # Field name (becomes database column)
        type: float64          # Data type for storage
        selector: ".data.price"  # JSONPath selector
        transformers: ["round6"] # Optional transformations
    headers:                   # Optional HTTP headers
      User-Agent: "Chomp/1.1.0"
    retry_config:              # Optional retry configuration
      max_retries: 3
      backoff_factor: 2
```

### Development Tools

```bash
# Development
make install-deps    # Install all dependencies
make format         # Format code
make lint           # Lint code
make test           # Run tests
make build          # Complete build process

# Infrastructure
make db-setup       # Start databases
make full-setup     # Complete setup

# Runtime
make start-ingester # Start ingester locally
make start-server   # Start API server
make health-check   # Check system health
```

### Documentation Standards

- Document all public APIs with docstrings and examples
- Keep comments up to date with code changes
- Document design decisions and data flow
- Provide deployment guidelines and performance considerations

## Getting Help

- **Issues**: Use GitHub Issues for bug reports and feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Documentation**: Check README.md and inline documentation
- **Examples**: See `examples/` directory for configuration samples

## License

By contributing to Chomp, you agree that your contributions will be licensed under the MIT License.
