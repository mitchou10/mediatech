PYTHONPATH=$(PWD)

.PHONY: help for datasets generator
.DEFAULT_GOAL := help

help: ## Show helper
	@echo "Usage: make <command>"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

clean: ## Clean all __pycache__ and .pytest_cache
	
	@echo "Clean client..."
	@find . -type d -name "node_modules" -prune -print -exec rm -rf {} +

	@echo "Clean server..."
	@find . -type d -name ".venv" -prune -print -exec rm -rf {} +
	@find . -type d -name "__pycache__" -prune -print -exec rm -rf {} +
	@find . -type d -name ".pytest_cache" -prune -print -exec rm -rf {} +
	@find . -type d -name ".ruff_cache" -prune -print -exec rm -rf {} +

install: ## Install dependencies
	echo "Installing dependencies..."
	uv sync --all-groups --dev

run-test: install ## Run all tests
	echo "Running tests..."
	PYTHONPATH=$(PWD) uv run pytest -s -v -o log_cli=true -o log_cli_level=DEBUG tests/ --cov=src --cov-report=term-missing

run-test-file: install ## Run tests in a specific file, e.g., make run-test-file FILE=tests/test_example.py
	echo "Running tests $(FILE)..."
	PYTHONPATH=$(PWD) uv run pytest -s -v $(FILE) -o log_cli=true -o log_cli_level=DEBUG --cov=src --cov-report=term-missing

lint: install 
	echo "Linting code..."
	PYTHONPATH=$(PWD) uv run ruff check .


alembic-revision: install ## Run alembic revision
	echo "Running alembic revision..."
	PYTHONPATH=$(PWD) uv run alembic revision --autogenerate -m "$(MESSAGE)"