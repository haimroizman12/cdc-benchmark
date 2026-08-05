COMPOSE_DB = docker compose --env-file .env -f docker/docker-compose.db.yml
COMPOSE_DBZ = docker compose --env-file .env -f docker/docker-compose.db.yml -f docker/docker-compose.debezium.yml
SQLCMD = /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$$MSSQL_SA_PASSWORD" -C

RATE ?= 100
DURATION ?= 60
MIX ?= 70/20/10
SEED_ROWS ?= 0

.PHONY: env up down mssql-schema clean bench-build selftest \
	debezium-up debezium-down debezium-bench
env:
	@test -f .env || cp .env.example .env

up: env
	$(COMPOSE_DB) up -d
	@echo "waiting for mssql to accept connections..."
	@until $(COMPOSE_DB) exec -T mssql bash -lc '$(SQLCMD) -Q "SELECT 1"' >/dev/null 2>&1; do sleep 3; done
	@$(MAKE) mssql-schema
	@echo "databases ready."

mssql-schema:
	$(COMPOSE_DB) exec -T mssql bash -lc '$(SQLCMD) -i /sql/mssql_init.sql'

down:
	$(COMPOSE_DB) down

clean:
	$(COMPOSE_DB) down -v
	rm -rf results/*.json results/*.csv

bench-build:
	docker build -f docker/bench.Dockerfile -t cdc-bench .

selftest: bench-build
	docker run --rm --network cdc-bench --env-file .env -e PG_HOST=postgres -e MSSQL_HOST=mssql \
	  -v $$PWD/results:/app/results cdc-bench \
	  --tool selftest --rate $(RATE) --duration 10 --mix 100/0/0

debezium-up: up
	$(COMPOSE_DBZ) up -d --build
	@echo "waiting for Kafka Connect REST..."
	@until curl -sf localhost:8083/ >/dev/null; do sleep 2; done
	curl -sf -X POST -H "Content-Type: application/json" --data @connectors/pg-source.json localhost:8083/connectors
	curl -sf -X POST -H "Content-Type: application/json" --data @connectors/mssql-sink.json localhost:8083/connectors
	@echo "connectors registered."

debezium-down:
	-curl -sf -X DELETE localhost:8083/connectors/mssql-sink
	-curl -sf -X DELETE localhost:8083/connectors/pg-source
	$(COMPOSE_DBZ) down

debezium-bench: bench-build
	docker run --rm --network cdc-bench --env-file .env -e PG_HOST=postgres -e MSSQL_HOST=mssql \
	  -v $$PWD/results:/app/results cdc-bench \
	  --tool debezium --rate $(RATE) --duration $(DURATION) --mix $(MIX) --seed-rows $(SEED_ROWS)
