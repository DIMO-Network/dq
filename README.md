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

### CloudEvent access

There are two independent ways a token can authorize the CloudEvent queries (`cloudEvents`,
`latestCloudEvent`, `availableCloudEventTypes`). A request is permitted if **either** applies:

1. **Full access via `permissions`.** A token holding `privilege:GetRawData`, or the combination of
   `privilege:GetLocationHistory` and `privilege:GetNonLocationHistory`, may read every CloudEvent for
   the subject.
2. **A scoped `cloud_events` grant.** Independently of the `permissions` enum, a token may carry a
   `cloud_events` claim that authorizes only specific event types, from specific sources, optionally
   pinned to specific IDs. `"*"` is the wildcard ("any").

```json
{
  "asset": "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:42",
  "cloud_events": {
    "events": [
      { "event_type": "dimo.attestation", "source": "*", "ids": ["*"] }
    ]
  }
}
```

A grant-scoped request is authorized only when its filter falls entirely within a single grant. Any
dimension the filter leaves unset defaults to the wildcard `"*"`, so a narrow grant requires the caller
to scope their query to match it. For example, the grant above permits
`cloudEvents(subject: ..., filter: {type: "dimo.attestation"})` but rejects an unfiltered
`cloudEvents(subject: ...)` or a request for a different `type`.

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
