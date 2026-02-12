.PHONY: help venv install up down logs jupyter test-spark test-mysql

help:
	@echo "Available commands:"
	@echo "  make venv         - Create virtual environment"
	@echo "  make install      - Install Python requirements"
	@echo "  make up           - Start Docker services"
	@echo "  make down         - Stop Docker services"
	@echo "  make logs         - View MySQL logs"
	@echo "  make jupyter      - Launch Jupyter Lab"
	@echo "  make test-spark   - Test Spark installation"
	@echo "  make test-mysql   - Test MySQL connection"

venv:
	python3 -m venv .venv

install:
	. .venv/bin/activate && pip install -r requirements.txt

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker logs -f bigdata-mysql

jupyter:
	. .venv/bin/activate && jupyter lab

test-spark:
	. .venv/bin/activate && python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.getOrCreate(); print(s.range(5).count()); s.stop()"

test-mysql:
	. .env && mysql -h 127.0.0.1 -P $${MYSQL_PORT} -u$${MYSQL_USER} -p$${MYSQL_PASSWORD} $${MYSQL_DATABASE} -e "SELECT 1;"
