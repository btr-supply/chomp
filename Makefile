# Chomp Makefile - Lean Data Ingestion Framework
.PHONY: help setup run ingesters api stop cleanup logs monitor monitor-local format lint check-name pre-commit

# Variables
MAX_JOBS ?= 15

# Default target
help:
	@echo "Chomp - Lean Data Ingestion Framework"
	@echo ""
	@echo "Commands:"
	@echo "  setup [deps|images|all] - Install dependencies and/or build images"
	@echo "  run [MODE] [DEPLOYMENT] [API] - Start services"
	@echo "  debug [MODE] [DEPLOYMENT] [API] - Start services with verbose debug logging"
	@echo "  api [MODE] [DEPLOYMENT] - Start API server only"
	@echo "  ingesters [MODE] [DEPLOYMENT] - Start ingesters only"
	@echo "  stop - Stop all services"
	@echo "  cleanup - Stop and remove all containers/data"
	@echo "  logs [container] - Show service logs"
	@echo "  monitor - Monitor running services (Docker)"
	@echo "  monitor-local - Monitor local services and logs"
	@echo "  format - Format code with yapf"
	@echo "  lint - Lint code with ruff and mypy"
	@echo "  check-name - Validate branch/commit naming"
	@echo "  pre-commit - Run format + lint + check-name"
	@echo ""
	@echo "Run Arguments:"
	@echo "  MODE: dev|prod (default: dev)"
	@echo "  DEPLOYMENT: local|docker (default: docker)"
	@echo "  API: api|noapi (default: api)"
	@echo ""
	@echo "Examples:"
	@echo "  make run              # dev docker with API"
	@echo "  make run prod         # prod docker with API"
	@echo "  make run dev local    # dev local with API"
	@echo "  make run prod noapi   # prod docker without API"
	@echo "  make debug dev local  # dev local with API and verbose debug logs"
	@echo "  make debug dev docker # dev docker with API and verbose debug logs"
	@echo ""
	@echo "Environment:"

	@echo "  MAX_JOBS - Workers per service (default: 15)"

# Setup command
setup:
	@bash scripts/setup.sh $(filter-out setup,$(MAKECMDGOALS))

# Universal run command
run:
	@bash scripts/run.sh $(filter-out run,$(MAKECMDGOALS))

# Debug command with verbose flag
debug:
	@VERBOSE=true bash scripts/run.sh $(filter-out debug,$(MAKECMDGOALS))

# Service-specific commands
ingesters:
	@bash scripts/services.sh ingester $(filter-out ingesters,$(MAKECMDGOALS))

api:
	@bash scripts/services.sh api $(filter-out api,$(MAKECMDGOALS))

database:
	@bash scripts/database.sh $(filter-out database,$(MAKECMDGOALS))

# Operations
stop:
	@bash scripts/stop.sh

cleanup:
	@bash -c "source scripts/utils.sh && docker_cleanup_all"

logs:
	@bash -c "source scripts/utils.sh && docker_show_logs $(filter-out logs,$(MAKECMDGOALS))"

monitor:
	@bash -c "source scripts/utils.sh && docker_monitor"

monitor-local:
	@bash scripts/monitor_local.sh

# Pre-commit commands
format:
	@bash scripts/format.sh chomp

lint:
	@bash scripts/lint.sh chomp

check-name:
	@echo "Validating branch and commit naming..."
	@uv run python scripts/check_name.py -b -c

pre-commit: format lint check-name
	@echo "✅ Pre-commit checks completed successfully"

# Allow passing arguments to targets
%:
	@:
