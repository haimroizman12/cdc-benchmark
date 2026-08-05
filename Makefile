COMPOSE_DB = docker compose --env-file .env -f docker/docker-compose.db.yml
COMPOSE_DBZ = docker compose --env-file .env -f docker/docker-compose.db.yml -f docker/docker-compose.debezium.yml
SQLCMD = /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$$MSSQL_SA_PASSWORD" -C

RATE ?= 100
DURATION ?= 60
MIX ?= 70/20/10
SEED_ROWS ?= 0
GRACE ?= 30

.PHONY: env up down mssql-schema clean bench-build selftest \
	debezium-up debezium-down debezium-bench \
	airbyte-install airbyte-up airbyte-down airbyte-bench \
	report demo
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
	@set -a; . ./.env; set +a; \
	echo "resetting dbo.source_events to the harness shape (the Airbyte leg, if it ran,"; \
	echo "leaves an Airbyte-owned table the JDBC sink can't write to — makes demo idempotent)..."; \
	$(COMPOSE_DB) exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa \
	  -P "$$MSSQL_SA_PASSWORD" -C -Q "USE target_db; DROP TABLE IF EXISTS dbo.source_events;"
	@$(MAKE) mssql-schema
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
	  --tool debezium --rate $(RATE) --duration $(DURATION) --mix $(MIX) --seed-rows $(SEED_ROWS) --grace $(GRACE)

# Installs abctl. Prefers the official one-line installer; if it is unreachable
# (it has returned Cloudflare 526s), falls back to the pinned GitHub release.
ABCTL_VERSION ?= v0.30.4
airbyte-install:
	@command -v abctl >/dev/null && exit 0; \
	echo "installing abctl..."; \
	curl -LsfS https://connect.airbyte.com/v1/install | bash || { \
	  echo "official installer failed; falling back to GitHub release $(ABCTL_VERSION)"; \
	  os=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
	  arch=$$(uname -m); [ "$$arch" = "x86_64" ] && arch=amd64; [ "$$arch" = "aarch64" ] && arch=arm64; \
	  mkdir -p $$HOME/.local/bin; \
	  curl -LsfS "https://github.com/airbytehq/abctl/releases/download/$(ABCTL_VERSION)/abctl-$(ABCTL_VERSION)-$$os-$$arch.tar.gz" \
	    | tar -xz -C /tmp; \
	  cp /tmp/abctl-$(ABCTL_VERSION)-$$os-$$arch/abctl $$HOME/.local/bin/abctl && chmod +x $$HOME/.local/bin/abctl; \
	  echo "installed to $$HOME/.local/bin/abctl (ensure it is on PATH)"; \
	}

# Airbyte's ingress binds a host port; 8000 clashes with a local dev API here, so
# default to 8010. Override with AIRBYTE_PORT=. configure.py talks to the PUBLIC API.
AIRBYTE_PORT ?= 8010
AIRBYTE_URL ?= http://localhost:$(AIRBYTE_PORT)/api/public/v1
# Shell that resolves abctl app credentials into env vars for configure.py.
AB_CREDS = creds=$$(abctl local credentials 2>/dev/null); \
	cid=$$(echo "$$creds" | sed -n 's/.*Client-Id: *\([0-9a-f-]*\).*/\1/p'); \
	csec=$$(echo "$$creds" | sed -n 's/.*Client-Secret: *\([0-9A-Za-z]*\).*/\1/p')

airbyte-up: up airbyte-install
	abctl local install --port $(AIRBYTE_PORT)
	@echo "airbyte up at http://localhost:$(AIRBYTE_PORT)"
	@set -a; . ./.env; set +a; \
	echo "wiring DB containers onto abctl's kind network (Airbyte's connector pods run"; \
	echo "in the kind cluster, a separate network from the cdc-bench rig)..."; \
	pg=$$($(COMPOSE_DB) ps -q postgres); ms=$$($(COMPOSE_DB) ps -q mssql); \
	docker network connect kind $$pg 2>/dev/null || true; \
	docker network connect kind $$ms 2>/dev/null || true; \
	pgip=$$(docker inspect $$pg --format '{{(index .NetworkSettings.Networks "kind").IPAddress}}'); \
	msip=$$(docker inspect $$ms --format '{{(index .NetworkSettings.Networks "kind").IPAddress}}'); \
	echo "  postgres kind IP=$$pgip  mssql kind IP=$$msip"; \
	echo "creating Postgres CDC publication + replication slot (idempotent)..."; \
	$(COMPOSE_DB) exec -T postgres psql -U $$POSTGRES_USER -d $$POSTGRES_DB \
	  -c "CREATE PUBLICATION airbyte_pub FOR TABLE source_events;" 2>/dev/null || true; \
	$(COMPOSE_DB) exec -T postgres psql -U $$POSTGRES_USER -d $$POSTGRES_DB \
	  -c "SELECT pg_create_logical_replication_slot('airbyte_slot','pgoutput');" 2>/dev/null || true; \
	echo "dropping dbo.source_events so Airbyte OWNS its target table (destination-mssql"; \
	echo "2.2.20 crashes introspecting a pre-created table: 'No enum constant MssqlType.*')..."; \
	$(COMPOSE_DB) exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa \
	  -P "$$MSSQL_SA_PASSWORD" -C -Q "USE target_db; DROP TABLE IF EXISTS dbo.source_events;" || true; \
	$(AB_CREDS); \
	AIRBYTE_URL=$(AIRBYTE_URL) AIRBYTE_CLIENT_ID=$$cid AIRBYTE_CLIENT_SECRET=$$csec \
	  AB_SRC_HOST=$$pgip AB_SRC_PORT=5432 AB_DST_HOST=$$msip AB_DST_PORT=1433 \
	  python3 airbyte/configure.py setup; \
	echo "priming one sync so Airbyte CREATES dbo.source_events (it owns the table and"; \
	echo "only creates it on first sync; the bench's target reset needs it to exist)..."; \
	AIRBYTE_URL=$(AIRBYTE_URL) AIRBYTE_CLIENT_ID=$$cid AIRBYTE_CLIENT_SECRET=$$csec \
	  python3 airbyte/configure.py sync

airbyte-down:
	abctl local uninstall

# Runs the harness AND drives back-to-back syncs for the WHOLE window (load + grace):
# Airbyte is batch, so its tail arrives during the drain and the sync loop must keep
# firing through GRACE or completeness collapses. --grace is passed to the harness too.
airbyte-bench: bench-build
	@$(AB_CREDS); \
	( end=$$(( $$(date +%s) + $(DURATION) + $(GRACE) )); while [ $$(date +%s) -lt $$end ]; do \
	    AIRBYTE_URL=$(AIRBYTE_URL) AIRBYTE_CLIENT_ID=$$cid AIRBYTE_CLIENT_SECRET=$$csec \
	    python3 airbyte/configure.py sync; done ) & \
	docker run --rm --network cdc-bench --env-file .env -e PG_HOST=postgres -e MSSQL_HOST=mssql \
	  -v $$PWD/results:/app/results cdc-bench \
	  --tool airbyte --rate $(RATE) --duration $(DURATION) --mix $(MIX) --seed-rows $(SEED_ROWS) --grace $(GRACE)

# End-to-end demo: run BOTH tools one at a time (they need different target-table
# shapes and must never run simultaneously), then print the comparison table.
# Airbyte gets a longer grace because its batch tail arrives over many seconds.
demo:
	$(MAKE) debezium-up
	$(MAKE) debezium-bench RATE=50 DURATION=20 MIX=100/0/0
	$(MAKE) debezium-down
	$(MAKE) airbyte-up
	$(MAKE) airbyte-bench RATE=20 DURATION=60 MIX=100/0/0 GRACE=300
	$(MAKE) airbyte-down
	$(MAKE) report

# Renders the comparison table from the latest results JSON per tool.
report: bench-build
	@docker run --rm -v $$PWD/results:/app/results -v $$PWD/bench:/app/bench \
	  --entrypoint python cdc-bench -c "import json,glob; \
from bench.report import render_table; \
latest={}; \
[latest.__setitem__(json.load(open(f)).get('tool','?'), json.load(open(f))) for f in sorted(glob.glob('results/*.json'))]; \
print(render_table(latest))"
