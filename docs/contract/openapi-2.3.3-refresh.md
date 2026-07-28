# Verified 2.3.3 OpenAPI refresh

## Provenance

- Source: `https://api.replicant.space/swagger/openapi.json`
- Fetched: `2026-07-28T09:11:06.627Z`
- Replicant Space corpus version: `2.3.3`; API `info.version`: `v1`; OpenAPI: `3.0.3`
- SHA-256: `d6f89cbadc523160d25e26cec8ac9673fda7296512ea408c5dd7c55a13c08c3f`
- Baseline SHA-256: `ca018a938541f23c4838e8fe58f78889d9ca4b9ab81b488112f90589dd83c2f4`

The fetched bytes replace `reference/replicant-space/openapi.json` verbatim.
The corpus has 72 paths, 86 operations, and 159 schemas (previously 70, 84,
and 159).

## Operation and path diff

| Change | Contract decision |
| --- | --- |
| Added `GET /v1/leaderboards/colony_moon` | Supported authenticated safe read; retain the existing `raw::LeaderboardsClient::colony_moon` method. |
| Added `GET /v1/leaderboards/colony_planet` | Supported authenticated safe read; retain the existing `raw::LeaderboardsClient::colony_planet` method. |
| `GET /v1/devices` adds optional `tag` and `untagged` query parameters | Existing typed query and mutual-exclusion validation remain correct. |
| `GET /v1/events/stream` adds optional `cursor` and a `422` response | Existing SSE correction remains explicit because OpenAPI still lacks a 2xx `text/event-stream` response. |

No paths or operations were removed. No operation became deprecated or
administrative. The inventory is now 79 supported, 5 deprecated, and 2
administrative operations; the latter two classes remain absent from the raw
callable surface.

## Schema diff

All changed required-field sets are unchanged. Description-only schema changes
are omitted below; the semantic changes are:

| Schema | Change | Code/policy decision |
| --- | --- | --- |
| `MessageSettingsSchema.subscribed` | Enum ordering changed only. | No code change; string values remain open. |
| `BlueprintSchema.print_time` | `number` → `integer`. | Preserve `f64` for source compatibility; fixture verifies integer input. |
| `EnqueuePrintSchema.quantity` | Optional integer, default `1`, range `1..=50`. | Existing command and serialization test already cover it. |
| Device command/printing/prospect/repair/scan/travel ETA schemas and replicant printing ETA | `number` → `integer`. | Preserve `f64`; integer JSON remains compatible and is fixture-tested. |
| `DeviceStatusSchema.hosting_replicant` | Optional nullable object. | Existing reference deserializer accepts object and fixture exercises it. |
| `LeaderboardEntrySchema.designation` | Optional string added. | Add `raw::leaderboards::LeaderboardEntry::designation` and a fixture. |
| `CatalogueStarSchema.region` | Optional nullable string added. | Existing DTO and fixture cover it. |
| `StarItemSchema.region`, `StarItemSchema.has_hub` | Optional nullable string and optional boolean added. | Existing DTO and fixture cover them. |

Unknown fields remain ignored by Serde; the refreshed DTO fixture includes
unknown fields for both device status and leaderboard entries.

## Commands

```sh
rtk proxy node -e 'const fs=require("fs"),path=require("path"),crypto=require("crypto");(async()=>{const url="https://api.replicant.space/swagger/openapi.json";const response=await fetch(url,{headers:{accept:"application/json"}});if(!response.ok)throw new Error(`HTTP ${response.status}`);const body=Buffer.from(await response.arrayBuffer());JSON.parse(body);const root="/run/media/chats/0c7bd812-03b4-405c-9602-31282b68fd64/replicant-client/reference/replicant-space";const target=path.join(root,"openapi.json");const temporary=path.join(root,".openapi.json.refresh");fs.writeFileSync(temporary,body);fs.renameSync(temporary,target);console.log(JSON.stringify({url,fetched_at:new Date().toISOString(),sha256:crypto.createHash("sha256").update(body).digest("hex"),bytes:body.length},null,2));})().catch(error=>{console.error(error);process.exit(1)})'
rtk python3 scripts/generate_operation_inventory.py
rtk python3 scripts/generate_authority_matrix.py
rtk python3 scripts/contract_policy_check.py
rtk python3 scripts/raw_transport_policy_check.py
rtk cargo test --all-features --test contract_2_3_3
rtk cargo fmt --all -- --check
```

## Verification

- Passed: refreshed fixture integration test, raw transport policy check,
  formatter, and the default/raw/events/all-feature compile matrix.
- Existing unrelated failures remain: the full test suite and Clippy compile
  `examples/explore_survey_route.rs`, which references three removed
  `CurrentSystemSurveyCheck` fields; the contract gate's old-crate-name audit
  rejects five pre-existing historical implementation documents.
