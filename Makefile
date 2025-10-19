PYTHONPATH=$(PWD)
API_URL=http://localhost:8000/openapi.json

.PHONY: help for datasets generator
.DEFAULT_GOAL := help

help: ## Show helper
	@echo "Usage: make <command>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

clean: ## Clean all __pycache__ and .pytest_cache
	docker compose down

	@echo "Clean client..."
	@find . -type d -name "node_modules" -prune -print -exec rm -rf {} +

	@echo "Clean server..."
	@find . -type d -name ".venv" -prune -print -exec rm -rf {} +
	@find . -type d -name "__pycache__" -prune -print -exec rm -rf {} +
	@find . -type d -name ".pytest_cache" -prune -print -exec rm -rf {} +
	@find . -type d -name ".ruff_cache" -prune -print -exec rm -rf {} +

setup-client: ## Setup client
	echo "Setting up client..."
	cd apps/client && \
		pnpm install && \
		export OPENAPI_INPUT="./openapi/openapi.json" && \
		pnpm run api:generate

setup-server: ## Setup server
	echo "Setting up server..."
	cd apps/server && \
		uv venv --clear && \
		source .venv/bin/activate && \
		uv sync

install: ## Install dependencies
	echo "Installing dependencies..."
	cd apps/server && PYTHONPATH=$(PWD)/apps/server uv sync --all-groups --dev

run-test: install ## Run all tests
	echo "Running tests..."
	cd apps/server && PYTHONPATH=$(PWD)/apps/server uv run pytest -s -v -o log_cli=true -o log_cli_level=DEBUG tests/ --cov=app --cov=schemas --cov=broker --cov=services --cov-report=term-missing

run-test-file: install ## Run tests in a specific file, e.g., make run-test-file FILE=tests/test_example.py
	echo "Running tests $(FILE)..."
	cd apps/server && PYTHONPATH=$(PWD)/apps/server uv run pytest -s -v $(FILE) -o log_cli=true -o log_cli_level=DEBUG --cov=app --cov=schemas --cov=broker --cov=services --cov-report=term-missing

lint: install 
	echo "Linting code..."
	cd apps/server && PYTHONPATH=$(PWD)/apps/server uv run ruff check .

up-server: ## Start backend server
	echo "Starting backend..."
	cd apps/server && PYTHONPATH=$(PWD)/apps/server uv run uvicorn app.main:app --host 0.0.0.0 --port 5000 --reload

openapi: clean setup-server ## Generate openapi client
	@echo "Retrieve openapi.json from $(API_URL)"
	@curl $(API_URL) -o ./apps/client/openapi/openapi.json
	@echo "Generate api client from openapi.json"
	docker compose build client
# 	@make build-client

start: clean setup-server setup-client ## Start full stack application
	docker compose up --watch

alembic-revision: install ## Run alembic revision
	echo "Running alembic revision..."
	cd apps/server && PYTHONPATH=$(PWD)/apps/server uv run alembic revision --autogenerate -m "$(MESSAGE)"