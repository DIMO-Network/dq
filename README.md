# dq (DIMO Query)

This is the result of a merger of two services: [Telemetry](https://github.com/DIMO-Network/telemetry-api) and [Fetch](https://github.com/DIMO-Network/fetch-api).

## Migrating

### From Telemetry

The primary identifier for all queries is now the `did` string. All `tokenId` parameters should be transformed into DIDs, and `subject` parameters merely need to be relabeled.

Enum values are now all consistently upper snake case, so `ignitionDetection` is now `IGNITION_DETECTION`.

The queries `attestations` and `vinVCLatest` queries have been removed. These can be replicated using queries imported from Fetch with appropriate filter settings. More specifically:

```graphql
# Before
attestations(tokenId: 42)

# After
cloudEvents(
  did: "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
  filter: {type: "dimo.attestation"}
)
```

```graphql
# Before
vinVCLatest(tokenId: 42)

# After
latestCloudEvent(
  did: "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
  filter: {type: "dimo.attestation", dataversion: "vin/v1.0"}
)
```

### From Fetch

The queries `indexes` and `latestIndex` have been removed. If a client does not want to incur the cost of loading the referenced documents, then it should not request the fields `data` or `dataBase64`.
