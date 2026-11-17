.PHONY: test build proto test-pg pg-up pg-down

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
