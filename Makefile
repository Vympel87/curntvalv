.PHONY: test build proto test-pg pg-up pg-down test-ch ch-up ch-down test-ext

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

# Everything, with both Postgres and ClickHouse.
test-ext: pg-up ch-up
	CURNTVALV_TEST_PG_URL='$(PG_TEST_URL)' CURNTVALV_TEST_CH_URL='$(CH_TEST_URL)' $(MAKE) test
