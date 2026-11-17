.PHONY: test build

test:
	go vet ./...
	go test -race -count=1 ./...

build:
	go build -o bin/curntvalv ./cmd/curntvalv
