.PHONY: help install dev run test docker-build docker-run docker-compose-up docker-compose-down clean

PORT ?= 5005
IMAGE_NAME ?= hdfs-gateway:latest

help:
	@echo "Available commands:"
	@echo "  make install             Install dependencies"
	@echo "  make dev                 Run FastAPI in development mode with reload (Port $(PORT))"
	@echo "  make run                 Run FastAPI in production mode (Port $(PORT))"
	@echo "  make test                Run test suite"
	@echo "  make docker-build        Build Docker container image"
	@echo "  make docker-run          Run Docker container on port $(PORT)"
	@echo "  make docker-compose-up   Start services using Docker Compose"
	@echo "  make docker-compose-down Stop Docker Compose services"
	@echo "  make clean               Remove cached Python bytecode"

install:
	pip install -r requirements-dev.txt

dev:
	uvicorn app.main:app --host 0.0.0.0 --port $(PORT) --reload

run:
	uvicorn app.main:app --host 0.0.0.0 --port $(PORT)

test:
	pytest -v tests/

docker-build:
	docker build -t $(IMAGE_NAME) .

docker-run:
	docker run -p $(PORT):$(PORT) --env-file .env $(IMAGE_NAME)

docker-compose-up:
	docker compose up --build -d

docker-compose-down:
	docker compose down

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	rm -rf .pytest_cache .coverage htmlcov
