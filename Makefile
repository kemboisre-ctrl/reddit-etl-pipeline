# =============================================================================
# MAKEFILE — Common commands for the Reddit ETL Pipeline (TaskFlow + LocalExecutor)
# =============================================================================

.PHONY: help build init up down logs test clean

help:
	@echo "Available commands:"
	@echo "  make build    - Build Docker images"
	@echo "  make init     - Initialize Airflow DB (first time)"
	@echo "  make up       - Start all services"
	@echo "  make down     - Stop all services"
	@echo "  make logs     - Tail scheduler logs"
	@echo "  make test     - Run Python unit tests"
	@echo "  make clean    - Remove containers and volumes"

build:
	docker-compose build

init:
	docker-compose up airflow-init

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f airflow-scheduler

test:
	python -m pytest tests/ -v

clean:
	docker-compose down -v
	rm -rf logs/*