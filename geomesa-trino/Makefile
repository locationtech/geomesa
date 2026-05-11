APP    := tools
VENV   := $(APP)/.venv
PYTHON ?= $(VENV)/bin/python

.DEFAULT_GOAL := help

.PHONY: help install-trino \
        up-trino down-trino destroy-trino logs-trino purge-trino \
        bench-trino bench-geomesa \
        ingest-demo-data ingest-multigeom-demo ingest-tdrive ingest-tdrive-all ingest-geolife ingest-ais \
        test integration-test clean \
        _ingest-demo-data-trino \
        _ingest-tdrive-trino _ingest-tdrive-all-trino _ingest-geolife-trino _ingest-ais-trino

help:
	@echo ""
	@echo "  Trino stack (defaults to COMPOSE_FILE=docker-compose.local.yml)"
	@echo "    install-trino    Build plugin JAR + install Python deps (one-time setup)"
	@echo "    up-trino         Start stack, wait for Trino"
	@echo "    down-trino       Stop stack (MinIO data preserved)"
	@echo "    destroy-trino    Stop stack and delete all data volumes"
	@echo "    logs-trino       Tail Trino logs"
	@echo "    purge-trino      Drop all Trino spatial tables (stack stays up)"
	@echo "    (override env:   make up-trino COMPOSE_FILE=docker-compose.aws.yml)"
	@echo ""
	@echo "  Data  (ingests into Trino; skips datasets already present)"
	@echo "    ingest-demo-data         Ingest 100k synthetic observations + 1k regions"
	@echo "    ingest-multigeom-demo    Ingest spatial.observations_2geom (multi-geom proof)"
	@echo "    ingest-tdrive            Download + ingest T-Drive 1 zip (~2M rows)"
	@echo "    ingest-tdrive-all   Download + ingest T-Drive all 9 zips (~18M rows)"
	@echo "    ingest-geolife      Download + ingest GeoLife (~25M rows)"
	@echo "    ingest-ais          Download + ingest AIS Zone 17 Jan 2017 (~5M rows)"
	@echo ""
	@echo "  Benchmark  (auto-adapts to COMPOSE_FILE — local runs full matrix, aws runs envelope probe)"
	@echo "    bench-trino    Run SQL spatial benchmark (spatial_iceberg vs iceberg)"
	@echo "    bench-geomesa  Run GeoMesa DataStore benchmark (full read path: CQL → features)"
	@echo "    (aws override: make bench-trino COMPOSE_FILE=docker-compose.aws.yml BENCH_TABLE=rawob_harrisnextgen)"
	@echo ""
	@echo "  Build"
	@echo "    test              Run Java unit tests"
	@echo "    integration-test  Run ITs (requires running stack + ingested demo data)"
	@echo "                      Override scope: make integration-test IT=Z2PruningIT#methodName"
	@echo "    clean             Remove Maven target/ dirs and plugin JAR"
	@echo ""

# ── Trino stack ───────────────────────────────────────────────────────────────

# The generic docker-compose.yml is intentionally absent — pick local (MinIO +
# iceberg-rest + Trino) or aws (Trino only, reading from the shared AWS
# iceberg-rest + S3). Override per-invocation:
#   make up-trino COMPOSE_FILE=docker-compose.aws.yml
# The aws variant also requires a populated .env file (see .env.template).
COMPOSE_FILE ?= docker-compose.local.yml
COMPOSE      := docker compose -f $(COMPOSE_FILE)

install-trino:
	bash build.sh
	uv venv $(VENV)
	uv pip install --python $(VENV)/bin/python -r $(APP)/requirements.txt

up-trino:
	$(COMPOSE) up -d
	@echo "Waiting for Trino..."
	@until $(COMPOSE) logs trino 2>&1 | grep -q "SERVER STARTED"; do sleep 3; done
	@echo "Trino ready."

down-trino:
	$(COMPOSE) down

destroy-trino:
	$(COMPOSE) down -v

logs-trino:
	$(COMPOSE) logs -f trino

purge-trino:
	$(COMPOSE) restart iceberg-rest
	@until curl -sf http://localhost:8181/v1/config >/dev/null 2>&1; do sleep 1; done
	$(PYTHON) $(APP)/purge_tables.py

# ── Data ──────────────────────────────────────────────────────────────────────
#
# Public targets skip datasets already present in Trino.
# Use purge-trino to reset before re-ingesting.
#
# do-ingest args:
#   $(1) Trino table name   $(2) display name   $(3) internal Trino target
define do-ingest
	@if ! curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; then \
	    echo "Trino is not running — run 'make up-trino' first"; exit 1; \
	fi; \
	if docker exec trino trino --execute "SHOW TABLES IN iceberg.spatial" 2>/dev/null | grep -qx '"$(1)"'; then \
	    echo "[Trino] $(2) already ingested — skipping (purge-trino to reset)"; \
	else \
	    $(MAKE) --no-print-directory _ingest-$(3)-trino; \
	fi
endef

ingest-demo-data:
	@if ! curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; then \
	    echo "Trino is not running — run 'make up-trino' first"; exit 1; \
	fi; \
	if docker exec trino trino --execute "SHOW TABLES IN iceberg.spatial" 2>/dev/null | grep -qx '"observations"'; then \
	    echo "[Trino] Demo data already ingested — skipping (purge-trino to reset)"; \
	else \
	    $(MAKE) --no-print-directory _ingest-demo-data-trino; \
	fi

ingest-multigeom-demo:
	$(PYTHON) $(APP)/ingest_synthetic.py --twogeom

ingest-tdrive:
	$(call do-ingest,tdrive,T-Drive,tdrive)

ingest-tdrive-all:
	$(call do-ingest,tdrive,T-Drive,tdrive-all)

ingest-geolife:
	$(call do-ingest,geolife,GeoLife,geolife)

ingest-ais:
	$(call do-ingest,ais,AIS,ais)

# ── Internal ingest targets ───────────────────────────────────────────────────

_ingest-demo-data-trino:
	$(PYTHON) $(APP)/ingest_synthetic.py

_ingest-tdrive-trino:
	$(PYTHON) $(APP)/ingest_tdrive.py

_ingest-tdrive-all-trino:
	$(PYTHON) $(APP)/ingest_tdrive.py --zips 9

_ingest-geolife-trino:
	$(PYTHON) $(APP)/ingest_geolife.py

_ingest-ais-trino:
	$(PYTHON) $(APP)/ingest_ais.py

# ── Benchmark ─────────────────────────────────────────────────────────────────

BENCH_ARGS ?=

# Auto-detect which stack is active: if the running Trino exposes a Glue
# catalog, we're on aws; otherwise (Trino not running, or running with a
# REST catalog) we default to local. COMPOSE_FILE=docker-compose.aws.yml
# still wins as an explicit override — useful before Trino is brought up.
# The shell-out runs once at Make parse time; failure (Trino down) falls
# through to local.
ifeq ($(COMPOSE_FILE),docker-compose.aws.yml)
_ACTIVE_STACK := aws
else
_ACTIVE_STACK := $(shell docker exec trino sh -c "grep -q 'iceberg.catalog.type=glue' /etc/trino/catalog/iceberg.properties" 2>/dev/null && echo aws || echo local)
endif

# Both bench targets auto-adapt to the active stack. On local they run the
# full DATASET_CONFIGS / DatasetConfig matrix; on aws they switch to the
# single-table envelope-matrix probe against the FAA table (what the deleted
# bench-trino-aws / bench-geomesa-aws targets used to do). Override:
#   BENCH_TABLE     — table to probe on aws (default rawob_harrisnextgen)
#   BENCH_NAMESPACE — Iceberg namespace / schema (default trino_test on aws)
#   BENCH_ARGS      — extra args appended to the underlying script invocation
BENCH_TABLE     ?= rawob_harrisnextgen
BENCH_NAMESPACE ?= trino_test

ifeq ($(_ACTIVE_STACK),aws)
_BENCH_TRINO_ARGS   := --envelope-probe $(BENCH_TABLE) --schema $(BENCH_NAMESPACE)
_BENCH_GEOMESA_ARGS := --schema $(BENCH_NAMESPACE) --warmup 1 --runs 1
_BENCH_ENV_PREFIX    = set -a; . ./.env; set +a;
else
_BENCH_TRINO_ARGS   :=
_BENCH_GEOMESA_ARGS :=
_BENCH_ENV_PREFIX    =
endif

bench-trino:
	@$(_BENCH_ENV_PREFIX) $(PYTHON) $(APP)/benchmark.py $(_BENCH_TRINO_ARGS) $(BENCH_ARGS)

bench-geomesa:
	mvn clean install -pl geomesa-trino-benchmark -am -DskipTests -Dlicense.skip=true -q
	@$(_BENCH_ENV_PREFIX) mvn exec:java -pl geomesa-trino-benchmark $(if $(_BENCH_GEOMESA_ARGS)$(BENCH_ARGS),-Dexec.args="$(_BENCH_GEOMESA_ARGS) $(BENCH_ARGS)")



# ── Build ─────────────────────────────────────────────────────────────────────

test:
	mvn test -Dlicense.skip=true

# Override scope: make integration-test IT=ClassName  or  IT=ClassName#methodName
# Pre-reqs: stack up (make up-trino), plugin built (bash build.sh && docker compose
# -f docker-compose.local.yml restart trino), demo data ingested
# (make ingest-demo-data). ITs are tagged @integration so
# `make test` always excludes them; this target is the only path that runs them.
IT ?=
integration-test:
	mvn -pl geomesa-trino-plugin,geomesa-trino-datastore verify -DskipITs=false -Dfailsafe.failIfNoSpecifiedTests=false $(if $(IT),-Dit.test=$(IT))

clean:
	mvn clean
	rm -f dist/geomesa-trino-plugin/*.jar
