# Chomp Makefile - BTR Supply Data Ingestion Framework
.PHONY: help install format lint test build clean
.PHONY: db-setup core-setup api-setup full-setup health-check ping
.PHONY: start-ingester start-server start-cluster stop-cluster stop-all cleanup monitor logs
.PHONY: build-images dev-setup ci-test push-images

# Variables
ENV_FILE ?= .env
CONFIG_FILE ?= examples/diverse.yml
MAX_JOBS ?= 8
REGISTRY ?=

# Default target
help:
	@echo "Chomp - Data Ingestion Framework"
	@echo ""
	@echo "Dev: install format lint test build clean pre-commit"
	@echo "Git Hooks: validate-commit-msg validate-branch-name pre-push"
	@echo "Docker: build-images db-setup core-setup api-setup full-setup"
	@echo "Runtime: start-ingester start-server start-cluster stop-cluster ping health-check"
	@echo "Ops: stop-all cleanup monitor logs"
	@echo "Utils: dev-setup ci-test push-images"

# Development
install:
	@bash scripts/install_deps.sh $(EXTRA)

format:
	@bash scripts/format_code.sh

lint:
	@bash scripts/lint.sh

test:
	@bash scripts/test.sh

build: format lint test
	@echo "Build completed."

clean:
	@bash scripts/clean.sh

# Git Hook Validations
pre-commit: format lint

validate-commit-msg:
	@echo "Validating commit message format..."
	@uv run --active python scripts/check_name.py -c

validate-branch-name:
	@echo "Validating current branch name format..."
	@uv run --active python scripts/check_name.py -b

pre-push:
	@echo "Validating format of commits+branch name to be pushed..."
	@uv run --active python scripts/check_name.py -p

# Docker
build-images:
	@bash scripts/build_images.sh

db-setup:
	@sudo bash scripts/db_setup.sh

core-setup:
	@sudo bash scripts/core_setup.sh

api-setup:
	@sudo bash scripts/api_setup.sh

full-setup:
	@sudo bash scripts/full_setup.sh

# Runtime
start-ingester:
	@ENV_FILE=$(ENV_FILE) CONFIG_FILE=$(CONFIG_FILE) MAX_JOBS=$(MAX_JOBS) bash scripts/start_ingester.sh

start-server:
	@ENV_FILE=$(ENV_FILE) bash scripts/start_server.sh

start-cluster:
	@ENV_FILE=$(ENV_FILE) CONFIG_FILE=$(CONFIG_FILE) MAX_JOBS=$(MAX_JOBS) bash scripts/start_cluster.sh

stop-cluster:
	@bash scripts/stop_cluster.sh

ping health-check:
	@ENV_FILE=$(ENV_FILE) bash scripts/health_check.sh

# Operations
stop-all:
	@bash scripts/stop_all.sh

cleanup: stop-all
	@docker ps -a --format '{{.Names}}' | grep "^chomp" | xargs -r docker rm 2>/dev/null || true
	@docker network ls --format '{{.Name}}' | grep "^chomp_net" | xargs -r docker network rm 2>/dev/null || true

monitor:
	@bash scripts/monitor.sh

logs:
	@bash scripts/show_logs.sh

# Utilities
dev-setup: install db-setup
	@echo "Dev environment ready. Try: make start-ingester"

ci-test: install lint test

push-images: build-images
	@[ -n "$(REGISTRY)" ] || { echo "Error: REGISTRY not set"; exit 1; }
	@docker tag chomp-core:latest $(REGISTRY)/chomp-core:latest
	@docker tag chomp-db:latest $(REGISTRY)/chomp-db:latest
	@docker push $(REGISTRY)/chomp-core:latest $(REGISTRY)/chomp-db:latest
