.PHONY: help install install-dev install-test setup clean test test-unit test-integration test-coverage lint format type-check security build build-docker up down logs deps ci-check docker-build docker-push release

# Default help target
help:
	@echo "DataFlow Pro - Make Commands"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make install           - Install package"
	@echo "  make install-dev     - Install dev dependencies"
	@echo "  make install-test   - Install test dependencies"
	@echo "  make setup          - Full setup (install + pre-commit)"
	@echo "  make clean          - Clean build artifacts"
	@echo ""
	@echo "Development:"
	@echo "  make test           - Run all tests"
	@echo "  make test-unit      - Run unit tests"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-coverage - Run tests with coverage"
	@echo "  make lint          - Run linters"
	@echo "  make format       - Format code"
	@echo "  make type-check   - Type check code"
	@echo "  make security     - Run security scans"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build  - Build Docker image"
	@echo "  make docker-push  - Push Docker image"
	@echo "  make up           - Start services"
	@echo "  make down         - Stop services"
	@echo "  make logs         - View logs"
	@echo ""
	@echo "Release:"
	@echo "  make build        - Build package"
	@echo "  make release     - Create release"

# Installation
install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-test:
	pip install -e ".[test]"

setup: install-dev
	pre-commit install
	@echo "Setup complete!"

# Cleaning
clean:
	rm -rf build/ dist/ *.egg-info
	rm -rf .coverage htmlcov/ .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.so" -delete

# Testing
test: test-unit test-integration

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-coverage:
	pytest tests/ -v --cov=dataflow_pro --cov-report=html --cov-report=term-missing

# Linting
lint:
	ruff check src/ tests/

format:
	black src/ tests/
	isort src/ tests/

type-check:
	mypy src/

security:
	safety check || true
	bandit -r src/ || true

# Docker
docker-build:
	docker build -t dataflow-pro:latest .

docker-push:
	docker push dataflow-pro:latest

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

# Build package
build:
	python -m build

# Release
release:
	git tag -a v$$(python -c "import dataflow_pro; print(dataflow_pro.__version__)") -m "Release $$ (python -c 'import dataflow_pro; print(dataflow_pro.__version__)')"
	git push origin --tags

# CI check (run locally before pushing)
ci-check: lint type-check test-unit security