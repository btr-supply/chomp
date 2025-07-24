# Chomp Makefile - Lean Data Ingestion Framework
.PHONY: help setup run ingesters api stop cleanup logs monitor monitor-local format lint check-name pre-commit restart status db-start db-stop db-restart

# Variables
MAX_JOBS ?= 6

# Default target
help:
	@echo "Chomp - Lean Data Ingestion Framework"
	@echo ""
	@echo "Commands:"
	@echo "  setup [deps|images|all] - Install dependencies and/or build images"
	@echo "  run [MODE] [DEPLOYMENT] [API] [keep-db] - Start services"
	@echo "  debug [MODE] [DEPLOYMENT] [API] [keep-db] - Start services with verbose debug logging"
	@echo "  api [MODE] [DEPLOYMENT] - Start API server only"
	@echo "  ingesters [MODE] [DEPLOYMENT] - Start ingesters only"
	@echo "  stop [keep-db] - Stop all services (optionally preserve database)"
	@echo "  cleanup - Stop and remove all containers/data"
	@echo "  logs [container] - Show service logs"
	@echo "  monitor - Monitor running services (Docker)"
	@echo "  monitor-local - Monitor local services and logs"
	@echo "  format - Format code with yapf"
	@echo "  lint - Lint code with ruff and mypy"
	@echo "  check-name - Validate branch/commit naming"
	@echo "  pre-commit - Run format + lint + check-name"
	@echo "  restart [keep-db] - Restart all services (optionally preserve database)"
	@echo "  status - Display current runtime information"
	@echo ""
	@echo "Database Commands:"
	@echo "  run [MODE] [DEPLOYMENT] db - Start database only"
	@echo "  stop db - Stop database only"
	@echo "  restart db - Restart database only"
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
	@echo "  make run dev db       # start database only (always docker)"
	@echo "  make stop db          # stop database only"
	@echo "  make restart db       # restart database only"
	@echo "  make debug dev local  # dev local with API and verbose debug logs"
	@echo "  make debug dev docker # dev docker with API and verbose debug logs"
	@echo "  make run dev local keep-db # start services without restarting database"
	@echo "  make stop keep-db     # stop services but preserve database"
	@echo "  make restart keep-db  # restart services but preserve database"
	@echo ""
	@echo "Environment:"

	@echo "  MAX_JOBS - Max concurrent resources ingested by this instance (default: 6)"

# Setup command
setup:
	@bash scripts/setup.sh $(filter-out setup,$(MAKECMDGOALS))

# Database-specific commands
DB_ARGS := $(filter-out db-start db-stop db-restart,$(MAKECMDGOALS))
db-start:
	@echo "🗄️ Starting database..."
	@bash scripts/database.sh $(DB_ARGS)

db-stop:
	@echo "🗄️ Stopping database..."
	@bash scripts/stop.sh --db-only $(DB_ARGS)

db-restart: db-stop db-start
	@echo "✅ Database restarted"

# Universal run command with database detection
RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
run:
	@if echo "$(RUN_ARGS)" | grep -q "\bdb\b"; then \
		$(MAKE) db-start $(filter-out db,$(RUN_ARGS)); \
	else \
		VERBOSE=true bash scripts/run.sh $(shell echo "$(RUN_ARGS)" | sed 's/keep-db/--keep-db/g'); \
	fi

# Debug command with verbose flag and database detection
DEBUG_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
debug:
	@if echo "$(DEBUG_ARGS)" | grep -q "\bdb\b"; then \
		$(MAKE) db-start $(filter-out db,$(DEBUG_ARGS)); \
	else \
		VERBOSE=true bash scripts/run.sh $(shell echo "$(DEBUG_ARGS)" | sed 's/keep-db/--keep-db/g'); \
	fi

# Service-specific commands
ingesters:
	@bash scripts/services.sh ingester $(filter-out ingesters,$(MAKECMDGOALS))

api:
	@bash scripts/services.sh api $(filter-out api,$(MAKECMDGOALS))

database:
	@bash scripts/database.sh $(filter-out database,$(MAKECMDGOALS))

# Operations with database detection
STOP_ARGS := $(filter-out stop,$(MAKECMDGOALS))
stop:
	@if echo "$(STOP_ARGS)" | grep -q "\bdb\b"; then \
		$(MAKE) db-stop $(filter-out db,$(STOP_ARGS)); \
	else \
		echo "Stopping Chomp cluster..."; \
		bash scripts/stop.sh $(shell echo "$(STOP_ARGS)" | sed 's/keep-db/--keep-db/g'); \
	fi

RESTART_ARGS := $(filter-out restart,$(MAKECMDGOALS))
restart:
	@if echo "$(RESTART_ARGS)" | grep -q "\bdb\b"; then \
		$(MAKE) db-restart $(filter-out db,$(RESTART_ARGS)); \
	else \
		echo "Restarting Chomp cluster..."; \
		bash scripts/restart.sh $(shell echo "$(RESTART_ARGS)" | sed 's/keep-db/--keep-db/g'); \
	fi

status:
	@uv run python scripts/status.py

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
