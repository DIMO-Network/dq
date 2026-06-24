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

## Storage backend (parse-on-read)

The service reads from a DuckLake catalog: decoded signals/events in `lake.signals`/`lake.events` and raw cloudevents in `lake.raw_events`, written by din and the materializer. DuckLake is the only backend — there is no query-backend switch.

Configure the catalog with `DUCKLAKE_CATALOG_DSN` (a Postgres DSN in prod for concurrent writers, or a local catalog-file path for single-node/tests) and `DUCKLAKE_DATA_PATH` (where parquet data files live — an `s3://` prefix in prod, a local directory in tests). `PARQUET_BUCKET` is the bucket the fetch path presigns/downloads externalized cloudevent payloads from.

### Clone layout (required until cloudevent is released)

This branch builds against a local cloudevent checkout via a `replace` directive — clone `cloudevent` as a sibling directory and check out `feat/parquet-sort-zstd-bloom`.

### Single-node quickstart

Run against a local DuckLake catalog file; the materializer decodes din's raw_events into it:

```bash
DUCKLAKE_CATALOG_DSN=/data/catalog.ducklake \
DUCKLAKE_DATA_PATH=/data/lake \
PARQUET_BUCKET=/data/pipeline \
MATERIALIZER_ENABLED=true \
go run ./cmd/dq
```

**Local dev:** the whole query surface — signals, events, raw, segments, and the eventrepo fetch path — is served from DuckLake (the catalog plus S3, or entirely from a local catalog + parquet). The fastest way to see the full parse-on-read path run with zero infra is the test suite, which exercises raw bundles → materializer → DuckDB → real GraphQL execution end to end:

```bash
go test ./tests/ -run 'TestDuckLake_MaterializeFromRawEvents|TestDISParity'
```

No external query engine is required.
