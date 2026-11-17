.PHONY: test build proto

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
