# Requires the cloudevent repo checked out as a sibling until it is released:
# the build context must be the PARENT directory so the `replace => ../cloudevent`
# in go.mod resolves. Build with `make docker` (which sets the parent context), or:
#   docker build -f dq/Dockerfile <parent-dir>
FROM golang:1.26-bookworm AS build

RUN useradd -u 10001 dimo

WORKDIR /build
COPY cloudevent/ ./cloudevent/
COPY dq/ ./dq/
WORKDIR /build/dq

RUN make tidy
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

COPY --from=build --chown=nonroot:nonroot /build/dq/bin/dq /
COPY --from=build --chown=nonroot:nonroot /duckdb/extensions /duckdb/extensions

ENV DUCKDB_EXTENSION_DIR=/duckdb/extensions

EXPOSE 8080
EXPOSE 8888

ENTRYPOINT ["/dq"]
