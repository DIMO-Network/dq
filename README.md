# dq (DIMO Query)

This is the result of a merger of two services: [Telemetry](https://github.com/DIMO-Network/telemetry-api) and [Fetch](https://github.com/DIMO-Network/fetch-api).

## Authentication

This service accepts subject and permission-scoped tokens. These are generally short-lived and must be signed by a trusted issuer. The important claims beyond the standard ones are

```json
{
  "asset": "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
  "permissions": ["privilege:GetNonLocationHistory", "privilege:GetRawData"]
}
```

## Migrating

### From Telemetry

The primary identifier for all queries is now the `subject` string. All `tokenId` parameters should be transformed into subject DIDs. The `did` parameters merely need to be renamed.

Enum values are now all consistently upper snake case, so `ignitionDetection` is now `IGNITION_DETECTION`.

The queries `attestations` and `vinVCLatest` queries have been removed. These can be replicated using queries imported from Fetch with appropriate filter settings. More specifically:

```graphql
# Before
attestations(tokenId: 42)

# After
cloudEvents(
  subject: "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
  filter: {type: "dimo.attestation"}
)
```

```graphql
# Before
vinVCLatest(tokenId: 42)

# After
latestCloudEvent(
  subject: "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
  filter: {type: "dimo.attestation", dataversion: "vin/v1.0"}
)
```

### From Fetch

The queries `indexes` and `latestIndex` have been removed. If a client does not want to incur the cost of loading the referenced documents, then it should not request the fields `data` or `dataBase64`. Note that we no longer expose the `indexKey` field, since clients have no way of making use of it.

## Storage backends (parse-on-read)

With `QUERY_BACKEND=duckdb` the service reads raw and decoded parquet straight from object storage. The backend is inferred from `ParquetBucket` — no separate switch:

| Value | Backend |
|---|---|
| `my-bucket` | S3 (DuckDB httpfs + materializer S3 client) |
| `/data/pipeline` or `file:///data/pipeline` | Local filesystem |

Paths must be absolute. The materializer's filesystem store publishes atomically (temp file + fsync + rename), so din's compactor never reads a torn watermark and DuckDB never globs a partial file.

### Clone layout (required until cloudevent is released)

This branch builds against a local cloudevent checkout via a `replace` directive — clone `cloudevent` as a sibling directory and check out `feat/parquet-sort-zstd-bloom`.

### Single-node quickstart

Run against the same root din writes to:

```bash
PARQUET_BUCKET=/data/pipeline \
QUERY_BACKEND=duckdb \
MATERIALIZER_ENABLED=true \
go run ./cmd/dq
```

**Honest caveat (current state):** the server still requires a reachable ClickHouse at startup even in duckdb mode (segments backend, eventrepo, gRPC fetch) — decoupling is a tracked work item. Until then, the fastest way to see the full parse-on-read path run locally with zero infra is the test suite, which exercises raw bundles → materializer → DuckDB → real GraphQL execution end to end:

```bash
go test ./tests/ -v -run 'TestPipelineEndToEnd|TestDISParity'
go test ./tests/ -v -perf -run 'TestQueryPerformance|TestMaterializerPerformance'
```

Other limitations in single-node mode: segment detection and the eventrepo fetch path (ClickHouse index + S3 presign) keep their backing services; the duckdb signal/event/raw query surfaces work fully from local parquet.
