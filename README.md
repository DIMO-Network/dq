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

The queries `indexes` and `latestIndex` have been removed. If a client does not want to incur the cost of loading the referenced documents, then it should not request the fields `data` or `dataBase64`.
