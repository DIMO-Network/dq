# Build context is the dq repo itself (cloudevent is a published public module now —
# no sibling checkout, no replace). Build with `make docker`, or:
#   docker build -f Dockerfile .
FROM golang:1.26-bookworm AS build

RUN useradd -u 10001 dimo

WORKDIR /build
# Resolve modules first so the dep layer caches across source changes (cloudevent is
# public — no auth needed to fetch it).
COPY go.mod go.sum ./
RUN go mod download
COPY . ./

RUN make build

# Pre-install DuckDB extensions (httpfs, aws, spatial, ducklake, postgres) into a fixed directory.
# The distroless runtime has no network or writable home, so extensions must
# be baked into the image. The version/platform subdirectories created here
# match the duckdb library linked into the binary.
ENV DUCKDB_EXTENSION_DIR=/duckdb/extensions
RUN go run ./internal/service/duck/installext -dir "$DUCKDB_EXTENSION_DIR"

# distroless/cc ships glibc + libstdc++ needed by the CGO duckdb bindings.
FROM gcr.io/distroless/cc-debian12 AS final

LABEL maintainer="DIMO <hello@dimo.zone>"

USER nonroot:nonroot

COPY --from=build --chown=nonroot:nonroot /build/bin/dq /
COPY --from=build --chown=nonroot:nonroot /duckdb/extensions /duckdb/extensions

ENV DUCKDB_EXTENSION_DIR=/duckdb/extensions

EXPOSE 8080
EXPOSE 8888

ENTRYPOINT ["/dq"]
