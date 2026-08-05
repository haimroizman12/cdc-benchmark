COMPOSE_DB = docker compose --env-file .env -f docker/docker-compose.db.yml
SQLCMD = /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$$MSSQL_SA_PASSWORD" -C

.PHONY: env up down mssql-schema clean
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
