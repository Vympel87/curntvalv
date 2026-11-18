# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=0.0.0-dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/curntvalv ./cmd/curntvalv \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/curntvalv /usr/local/bin/curntvalv
# Owned by the nonroot user so a fresh named volume mounted here is writable.
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot
# 7000: gRPC (Raft, admin, subscribe); 8080: dashboard and /metrics (--http-listen)
EXPOSE 7000 8080
ENTRYPOINT ["curntvalv"]
