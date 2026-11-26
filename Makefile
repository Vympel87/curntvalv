.PHONY: test build proto test-pg pg-up pg-down test-ch ch-up ch-down test-sinks sinks-up sinks-down test-ext chaos failover-up failover-down test-failover test-upgrade

test:
	go vet ./...
	go test -race -count=1 ./...

build:
	go build -o bin/curntvalv ./cmd/curntvalv

# Regenerates internal/pb from proto/. Needs buf, protoc-gen-go and
# protoc-gen-go-grpc on PATH (see docs/DESIGN.md D10 for versions).
proto:
	buf lint
	buf generate

# Tests that need Postgres read CURNTVALV_TEST_PG_URL and are skipped without
# it. test-pg starts a throwaway Postgres 17 in Docker and runs everything.
PG_TEST_URL ?= postgres://postgres:postgres@127.0.0.1:55432/postgres?sslmode=disable

pg-up:
	@docker inspect curntvalv-test-pg >/dev/null 2>&1 || docker run -d --name curntvalv-test-pg \
		-p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=postgres postgres:17 \
		-c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20
	@until docker exec curntvalv-test-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done

pg-down:
	docker rm -f curntvalv-test-pg

test-pg: pg-up
	CURNTVALV_TEST_PG_URL='$(PG_TEST_URL)' $(MAKE) test

# Tests that need ClickHouse read CURNTVALV_TEST_CH_URL and are skipped
# without it. test-ch starts a throwaway ClickHouse in Docker.
CH_TEST_URL ?= http://default:curntvalv@127.0.0.1:58123

ch-up:
	@docker inspect curntvalv-test-ch >/dev/null 2>&1 || docker run -d --name curntvalv-test-ch \
		-p 127.0.0.1:58123:8123 -e CLICKHOUSE_PASSWORD=curntvalv --ulimit nofile=262144:262144 \
		clickhouse/clickhouse-server:25.8
	@until curl -sf '$(CH_TEST_URL)/?query=SELECT%201' >/dev/null; do sleep 1; done

ch-down:
	docker rm -f curntvalv-test-ch

test-ch: ch-up
	CURNTVALV_TEST_CH_URL='$(CH_TEST_URL)' $(MAKE) test

# Tests of the NATS, Redis, Meilisearch and Kafka sinks read
# CURNTVALV_TEST_NATS_URL, _REDIS_URL, _MEILI_URL and _KAFKA_URL and are
# skipped without them. test-sinks starts the four in Docker.
NATS_TEST_URL ?= nats://127.0.0.1:54222
REDIS_TEST_URL ?= redis://127.0.0.1:56379
MEILI_TEST_URL ?= http://:curntvalv-test-key@127.0.0.1:57700
KAFKA_TEST_URL ?= kafka://127.0.0.1:59092
S3_TEST_ENDPOINT ?= http://127.0.0.1:58333
SINKS_ENV = CURNTVALV_TEST_NATS_URL='$(NATS_TEST_URL)' CURNTVALV_TEST_REDIS_URL='$(REDIS_TEST_URL)' \
	CURNTVALV_TEST_MEILI_URL='$(MEILI_TEST_URL)' CURNTVALV_TEST_KAFKA_URL='$(KAFKA_TEST_URL)' \
	CURNTVALV_TEST_S3_ENDPOINT='$(S3_TEST_ENDPOINT)'

sinks-up:
	@docker inspect curntvalv-test-nats >/dev/null 2>&1 || docker run -d --name curntvalv-test-nats \
		-p 127.0.0.1:54222:4222 nats:2.10 -js
	@docker inspect curntvalv-test-redis >/dev/null 2>&1 || docker run -d --name curntvalv-test-redis \
		-p 127.0.0.1:56379:6379 redis:7.4
	@docker inspect curntvalv-test-meili >/dev/null 2>&1 || docker run -d --name curntvalv-test-meili \
		-p 127.0.0.1:57700:7700 -e MEILI_MASTER_KEY=curntvalv-test-key -e MEILI_NO_ANALYTICS=true getmeili/meilisearch:v1.11
	@docker inspect curntvalv-test-kafka >/dev/null 2>&1 || docker run -d --name curntvalv-test-kafka \
		-p 127.0.0.1:59092:9092 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS=PLAINTEXT://:9092,BROKER://:9094,CONTROLLER://:9093 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:59092,BROKER://localhost:9094 -e KAFKA_INTER_BROKER_LISTENER_NAME=BROKER \
		-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
		-e KAFKA_NUM_PARTITIONS=3 apache/kafka:3.9.1
	@docker inspect curntvalv-test-s3 >/dev/null 2>&1 || docker run -d --name curntvalv-test-s3 -p 127.0.0.1:58333:8333 \
		-v $(CURDIR)/internal/sink/testdata/s3.json:/etc/s3.json:ro chrislusf/seaweedfs:3.80 server -s3 -s3.config=/etc/s3.json -dir=/data
	@until docker exec curntvalv-test-redis redis-cli ping >/dev/null 2>&1; do sleep 1; done
	@until docker exec curntvalv-test-s3 sh -c "echo 's3.bucket.create -name curntvalv-test' | weed shell" 2>&1 | grep -qE 'created bucket|already exists'; do sleep 1; done
	@until docker logs curntvalv-test-kafka 2>&1 | grep -q 'Kafka Server started'; do sleep 1; done
	@until curl -sf http://127.0.0.1:57700/health >/dev/null; do sleep 1; done

sinks-down:
	docker rm -f curntvalv-test-nats curntvalv-test-redis curntvalv-test-meili curntvalv-test-kafka curntvalv-test-s3

test-sinks: sinks-up
	$(SINKS_ENV) $(MAKE) test

# Everything, with Postgres, ClickHouse and the other sink targets.
test-ext: pg-up ch-up sinks-up
	CURNTVALV_TEST_PG_URL='$(PG_TEST_URL)' CURNTVALV_TEST_CH_URL='$(CH_TEST_URL)' $(SINKS_ENV) $(MAKE) test

# Random fault injection against a real cluster and Postgres (design §5).
# CHAOS_SEED=N repeats a run's fault schedule.
CHAOS_TIME ?= 2m
chaos: pg-up
	CURNTVALV_TEST_PG_URL='$(PG_TEST_URL)' CURNTVALV_CHAOS='$(CHAOS_TIME)' CURNTVALV_CHAOS_SEED='$(CHAOS_SEED)' \
		go test -count=1 -timeout 60m -run 'TestChaos$$' -v ./cmd/curntvalv/

# Postgres failover (design D7): a Postgres 17 primary and a streaming
# standby that syncs failover slots. A promotion cannot be undone, so every
# scenario gets a fresh pair.
FO_PRIMARY_URL ?= postgres://postgres:postgres@127.0.0.1:55442/postgres?sslmode=disable
FO_STANDBY_URL ?= postgres://postgres:postgres@127.0.0.1:55443/postgres?sslmode=disable
FO_PG = -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20 -c hot_standby_feedback=on
FO_SCENARIOS ?= Safe Diverged HeldBack

failover-up: failover-down
	docker network create curntvalv-fo >/dev/null
	docker run -d --name curntvalv-fo-primary --network curntvalv-fo -p 127.0.0.1:55442:5432 \
		-e POSTGRES_PASSWORD=postgres postgres:17 $(FO_PG) >/dev/null
	@# The entrypoint's first server only listens on a socket; wait for TCP.
	@until docker exec curntvalv-fo-primary psql -h 127.0.0.1 -U postgres -c 'SELECT 1' >/dev/null 2>&1; do sleep 1; done
	docker exec curntvalv-fo-primary bash -c 'echo "host replication all all scram-sha-256" >> "$$PGDATA/pg_hba.conf"'
	docker exec curntvalv-fo-primary psql -q -U postgres -c "SELECT pg_reload_conf()" \
		-c "SELECT pg_create_physical_replication_slot('standby1')" >/dev/null
	docker run -d --name curntvalv-fo-standby --network curntvalv-fo -p 127.0.0.1:55443:5432 \
		--entrypoint bash postgres:17 -c 'set -e; mkdir -p "$$PGDATA"; chown postgres "$$PGDATA"; chmod 700 "$$PGDATA"; \
		gosu postgres pg_basebackup -d "host=curntvalv-fo-primary user=postgres password=postgres dbname=postgres" \
			-D "$$PGDATA" -R -S standby1 -X stream; \
		exec gosu postgres postgres $(FO_PG) -c sync_replication_slots=on' >/dev/null
	@until docker exec curntvalv-fo-standby psql -h 127.0.0.1 -U postgres -c 'SELECT 1' >/dev/null 2>&1; do sleep 1; done

failover-down:
	@docker rm -f curntvalv-fo-primary curntvalv-fo-standby >/dev/null 2>&1 || true
	@docker network rm curntvalv-fo >/dev/null 2>&1 || true

test-failover:
	@for s in $(FO_SCENARIOS); do \
		$(MAKE) --no-print-directory failover-up && \
		CURNTVALV_TEST_PG_PRIMARY_URL='$(FO_PRIMARY_URL)' CURNTVALV_TEST_PG_STANDBY_URL='$(FO_STANDBY_URL)' \
			go test -race -count=1 -run "^TestFailover$$s$$" -v ./internal/source/pg/ || { $(MAKE) --no-print-directory failover-down; exit 1; }; \
	done; $(MAKE) --no-print-directory failover-down

# Upgrades a running cluster from the release UPGRADE_FROM to this build,
# one node at a time, with Postgres changes flowing (TestRollingUpgrade).
# The old binary is built from its tag once and kept in .cache/.
UPGRADE_FROM ?= v0.9.0
UPGRADE_DIR = $(CURDIR)/.cache/upgrade-$(UPGRADE_FROM)

test-upgrade: pg-up
	@test -x $(UPGRADE_DIR)/curntvalv || { \
		rm -rf $(UPGRADE_DIR)/src && git worktree prune && \
		git worktree add --detach $(UPGRADE_DIR)/src $(UPGRADE_FROM) && \
		(cd $(UPGRADE_DIR)/src && go build -o $(UPGRADE_DIR)/curntvalv ./cmd/curntvalv); \
		status=$$?; git worktree remove --force $(UPGRADE_DIR)/src; exit $$status; }
	CURNTVALV_TEST_PG_URL='$(PG_TEST_URL)' CURNTVALV_UPGRADE_FROM_BIN=$(UPGRADE_DIR)/curntvalv \
		go test -count=1 -timeout 20m -run 'TestRollingUpgrade$$' -v ./cmd/curntvalv/
