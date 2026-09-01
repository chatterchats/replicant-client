# Replicant Space 2.5.2 Contract Unit Audit

## Verdict summary

| Scope | covered | partial | missing | drift | n/a | total |
|---|---:|---:|---:|---:|---:|---:|
| Total | 247 | 18 | 0 | 46 | 17 | 328 |
| operation | 49 | 7 | 0 | 26 | 7 | 89 |
| schema | 119 | 11 | 0 | 20 | 10 | 160 |
| event | 79 | 0 | 0 | 0 | 0 | 79 |

## Missing and drift findings

| Unit | Verdict | Client symbol | Evidence | Notes |
|---|---|---|---|---|
| `operation:GET:/v1/achievements` | drift | `replicant_client::raw::achievements::AchievementsClient::list` | `src/raw/achievements.rs:88` | Source disagreement: the rendered page adds a category query filter while OpenAPI defines no query parameter (reference/replicant-space-2-5-1/api/accounts/achievements/index.md:60); the client follows the OpenAPI route without that filter. |
| `operation:GET:/v1/devices` | drift | `replicant_client::raw::devices::DevicesClient::list` | `src/raw/devices.rs:913` | The list route decodes DeviceStatus entries, but hosting_replicant is normalized from the contract's nullable object to Option<String> by a lossy deserializer (src/raw/devices.rs:262). |
| `operation:GET:/v1/devices/{device_code}` | drift | `replicant_client::raw::devices::DevicesClient::get` | `src/raw/devices.rs:970` | The retrieve route decodes DeviceStatus, but hosting_replicant is normalized from the contract's nullable object to Option<String> by a lossy deserializer (src/raw/devices.rs:262). |
| `operation:POST:/v1/replicants/{replicant_code}/print` | drift | `replicant_client::raw::replicants::ReplicantsClient::print` | `src/raw/replicants.rs:777` | Source disagreement: OpenAPI declares a 200 PrintResponse, while rendered print docs use 202 for initial queueing and document queue-shaped command responses (reference/replicant-space-2-5-1/api/replicants/print/index.md:26); PrintResponse cannot represent those documented queue fields. |
| `schema:app_schemas_devices_DeviceListItemSchema` | drift | `replicant_client::raw::devices::DeviceStatus::replicant_code` | `src/raw/devices.rs:259` | Source disagreement: OpenAPI names the owner field owner_replicant_code, while the rendered list example uses replicant_code (reference/replicant-space-2-5-1/api/devices/list/index.md:54); the public DTO exposes replicant_code. |
| `schema:app_schemas_devices_DeviceStatusSchema` | drift | `replicant_client::raw::devices::DeviceStatus::hosting_replicant` | `src/raw/devices.rs:262` | The OpenAPI schema types hosting_replicant as a nullable object, but DeviceStatus exposes Option<String> through a lossy reference deserializer. |
| `schema:app_schemas_locations_LocationResponseSchema` | drift | `replicant_client::raw::locations::Location` | `src/raw/locations.rs:80<br>reference/replicant-space-2-5-2/api/locations/index.md:78` | Source disagreement: rendered 2.5.2 location docs require boolean atmosphere at reference/replicant-space-2-5-2/api/locations/index.md:78, while PlanetaryBody exposes Option<String> at src/raw/locations.rs:80. |
| `operation:GET:/v1/replicants/{replicant_code}` | drift | `replicant_client::raw::replicants::ReplicantsClient::get` | `src/raw/replicants.rs:693` | ReplicantStatus contains the contract's nullable travel route as a non-null Vec<JsonObject>, so a JSON null route cannot be decoded (src/raw/status.rs:84). |
| `operation:GET:/v1/replicants/{replicant_code}/stars` | drift | `replicant_client::raw::replicants::ReplicantsClient::stars` | `src/raw/replicants.rs:829` | Source disagreement: OpenAPI gives per_page a default of 20 with no maximum, while the rendered page says 1-50 and default 10 (reference/replicant-space-2-5-1/api/replicants/stars/index.md:21); the client forwards the query without resolving the mismatch. |
| `operation:POST:/v1/replicants/{replicant_code}/mine` | drift | `replicant_client::raw::replicants::ReplicantsClient::mine` | `src/raw/replicants.rs:765` | Source disagreement: OpenAPI defines a 200 MineResponse, while the rendered mining page documents 202 Accepted with only status/resource_type (reference/replicant-space-2-5-1/api/replicants/mine/index.md:25); the client follows the OpenAPI response type. |
| `operation:POST:/v1/replicants/{replicant_code}/teleport` | drift | `replicant_client::raw::replicants::ReplicantsClient::teleport` | `src/raw/replicants.rs:867` | Source disagreement: OpenAPI defines a 200 response, while the rendered teleport page documents 202 Accepted (reference/replicant-space-2-5-1/api/replicants/teleport/index.md:30); the client follows the route and OpenAPI response shape. |
| `operation:POST:/v1/replicants/{replicant_code}/travel` | drift | `replicant_client::raw::replicants::ReplicantsClient::travel` | `src/raw/replicants.rs:913` | Source disagreement: OpenAPI defines route as a nullable RouteLeg array, while rendered normal-travel docs show route as one object (reference/replicant-space-2-5-1/api/replicants/travel/index.md:41); TravelResponse uses a non-null Vec<RouteLeg>. |
| `schema:app_schemas_printing_PrintResponseSchema` | drift | `replicant_client::raw::replicants::PrintResponse` | `src/raw/replicants.rs:298` | Source disagreement: OpenAPI lists timing/device/refund fields, while rendered print docs include queue and queue_length in a command response (reference/replicant-space-2-5-1/api/replicants/print/index.md:60); PrintResponse drops those documented fields. |
| `schema:app_schemas_replicants_ReplicantStatusSchema` | drift | `replicant_client::raw::replicants::ReplicantStatus` | `src/raw/replicants.rs:81` | The status DTO includes all top-level properties, but its referenced travel field uses a non-null route Vec<JsonObject> that rejects the contract's nullable route (src/raw/status.rs:84). |
| `schema:app_schemas_replicants_TravelInfoSchema` | drift | `replicant_client::raw::status::TravelInfo` | `src/raw/status.rs:53` | TravelInfo models the named fields, but its route field is a non-null Vec<JsonObject> and cannot deserialize the nullable route permitted by the contract (src/raw/status.rs:84). |
| `schema:app_schemas_travel_TravelResponseSchema` | drift | `replicant_client::raw::replicants::TravelResponse` | `src/raw/replicants.rs:624` | Source disagreement: OpenAPI defines route as a nullable RouteLeg array, while rendered normal-travel docs show route as one object (reference/replicant-space-2-5-1/api/replicants/travel/index.md:41); the client field is a non-null Vec<RouteLeg>. |
| `operation:DELETE:/v1/devices/{device_code}/simulate/{sim_id}` | drift | `replicant_client::raw::simulations::SimulationsClient::cancel` | `src/raw/simulations.rs:197` | Source disagreement: OpenAPI defines no success response, while the rendered outcomes page documents 200 with status and simulation_id (reference/replicant-space-2-5-1/simulations/outcomes/index.md:23); cancel returns untyped JSON. |
| `operation:GET:/v1/devices/tags/{tag}` | drift | `replicant_client::raw::devices::DevicesClient::list_by_tag` | `src/raw/devices.rs:950` | The route and cursor/limit query are covered, but DeviceStatus normalizes the contract's nullable hosting_replicant object to Option<String> (src/raw/devices.rs:262). |
| `operation:GET:/v1/replicants/{replicant_code}/devices` | drift | `replicant_client::raw::replicants::ReplicantsClient::devices` | `src/raw/replicants.rs:713` | Source disagreement: OpenAPI's DeviceListItem names the owner field owner_replicant_code while the rendered device-list example uses replicant_code (reference/replicant-space-2-5-1/api/devices/list/index.md:54); the client reuses DeviceStatus and exposes replicant_code. |
| `operation:POST:/v1/devices/{device_code}/simulate` | drift | `replicant_client::raw::simulations::SimulationsClient::enter` | `src/raw/simulations.rs:170` | Source disagreement: OpenAPI declares 201 Created while the rendered running page documents 200 OK (reference/replicant-space-2-5-1/simulations/running/index.md:15); the typed request and response fields otherwise match. |
| `schema:app_schemas_devices_AccountDeviceListResponseSchema` | drift | `replicant_client::raw::devices::DeviceListResponse` | `src/raw/devices.rs:302` | The wrapper matches devices and next_cursor, but its referenced DeviceStatus loses the contract's nullable hosting_replicant object through Option<String> (src/raw/devices.rs:262). |
| `schema:app_schemas_devices_DeviceCommandResponseSchema` | drift | `replicant_client::raw::devices::DeviceCommandResponse` | `src/raw/devices.rs:647` | Source disagreement: the OpenAPI response omits ward-specific fields documented in the rendered ward response (reference/replicant-space-2-5-1/system-wards/index.md:46); the public DTO includes warding, activated, deactivated, and evicted_miners. |
| `schema:app_schemas_devices_DeviceListResponseSchema` | drift | `replicant_client::raw::devices::DeviceListResponse` | `src/raw/devices.rs:302` | Source disagreement: OpenAPI's DeviceListItem names the owner field owner_replicant_code while the rendered device-list example uses replicant_code (reference/replicant-space-2-5-1/api/devices/list/index.md:54); the client reuses DeviceStatus and exposes replicant_code. |
| `schema:app_schemas_devices_DeviceTagListResponseSchema` | drift | `replicant_client::raw::devices::DeviceListResponse` | `src/raw/devices.rs:302` | The wrapper matches devices and next_cursor, but its referenced DeviceStatus loses the contract's nullable hosting_replicant object through Option<String> (src/raw/devices.rs:262). |
| `schema:app_schemas_device_commands_RenameSchema` | drift | `replicant_client::raw::devices::DeviceCommand::Rename` | `src/raw/devices.rs:541` | Source disagreement: OpenAPI defines the required name property, while the rendered command example uses new_name (reference/replicant-space-2-5-1/api/devices/command/index.md:107); the client serializes name. |
| `operation:GET:/v1/locations/{designation}` | drift | `replicant_client::raw::locations::LocationsClient::get` | `src/raw/locations.rs:279` | Source disagreement: OpenAPI exposes location and scanned fields, while the rendered page shows code, surveyed, and top-level boolean atmosphere (reference/replicant-space-2-5-1/api/locations/index.md:73). The client aliases code and surveyed but retains atmosphere only as Option<String> inside PlanetaryBody. |
| `operation:GET:/v1/replicants/{replicant_code}/scan/devices` | drift | `replicant_client::raw::replicants::ReplicantsClient::scan_devices` | `src/raw/replicants.rs:805` | Source disagreement: OpenAPI omits a success schema, while rendered scan-device docs define a 200 body containing star, device_count, devices, and next_cursor (reference/replicant-space-2-5-1/api/replicants/scan/devices/index.md:41). The client sends all filters but returns a generic JSON value instead of a typed response. |
| `operation:POST:/v1/locations/{designation}/contribute` | drift | `replicant_client::raw::locations::LocationsClient::contribute` | `src/raw/locations.rs:295` | Source disagreement: OpenAPI declares no success response for this POST, while the rendered megastructure page documents a 200 object with accepted, rejected, progress, and status (reference/replicant-space-2-5-1/api/locations/megastructures/index.md:62). The client sends the required devices body but returns a generic JSON value rather than a typed success shape. |
| `operation:POST:/v1/replicants/{replicant_code}/scan` | drift | `replicant_client::raw::replicants::ReplicantsClient::scan` | `src/raw/replicants.rs:792` | Source disagreement: OpenAPI defines replicants as an array of objects, while rendered scan docs show an object keyed by replicant name (reference/replicant-space-2-5-1/api/replicants/scan/index.md:104). SystemScanResponse follows the OpenAPI array and cannot decode the rendered object shape. |
| `schema:app_schemas_scanning_SystemScanResponseSchema` | drift | `replicant_client::raw::replicants::SystemScanResponse::replicants` | `src/raw/replicants.rs:489` | Source disagreement: OpenAPI defines replicants as an array of objects, while rendered scan docs show an object keyed by replicant name (reference/replicant-space-2-5-1/api/replicants/scan/index.md:104). The public field is Vec<JsonObject>, so it follows OpenAPI and cannot decode the documented object. |
| `operation:GET:/v1/accounts/reputation` | drift | `replicant_client::raw::accounts::AccountsClient::reputation` | `src/raw/accounts.rs:412` | Source disagreement: the rendered curl example uses the singular /v1/account/reputation while the endpoint heading and OpenAPI use /v1/accounts/reputation (reference/replicant-space-2-5-1/api/accounts/reputation/index.md:24); the client implements the plural route. |
| `operation:GET:/v1/inventory` | drift | `replicant_client::raw::inventory::InventoryClient::list` | `src/raw/inventory.rs:91` | Source disagreement: rendered inventory examples encode each location's items as a resource-keyed object, while OpenAPI defines an array of InventoryItem values (reference/replicant-space-2-5-1/api/locations/inventory/index.md:51); the client follows the OpenAPI array shape. |
| `operation:GET:/v1/replicants/{replicant_code}/inventory` | drift | `replicant_client::raw::inventory::InventoryClient::for_replicant` | `src/raw/inventory.rs:109` | SystemInventoryResponse models items and locations as non-null Vec fields, but OpenAPI marks both arrays nullable, so explicit nulls fail deserialization. |
| `schema:app_schemas_inventory_LocationInventorySchema` | drift | `replicant_client::raw::inventory::LocationInventory` | `src/raw/inventory.rs:24` | Source disagreement: OpenAPI defines items as an array of InventoryItem values, while the rendered inventory examples use a resource-keyed object (reference/replicant-space-2-5-1/api/locations/inventory/index.md:51); the public DTO follows OpenAPI. |
| `schema:app_schemas_inventory_SystemInventorySchema` | drift | `replicant_client::raw::inventory::SystemInventoryResponse` | `src/raw/inventory.rs:62` | SystemInventoryResponse has the documented scalar and location fields, but its items and locations Vec fields cannot deserialize the nullable arrays declared by OpenAPI. |
| `operation:GET:/v1/accounts/events` | drift | `replicant_client::raw::accounts::AccountsClient::events` | `src/raw/accounts.rs:394` | Source disagreement: OpenAPI defaults the account-events limit to 50 and defines criteria as an array, while the rendered page says default 20 and shows criteria as an object (reference/replicant-space-2-5-1/concepts/civilisations/index.md:47); the client forwards optional filters and accepts both criteria forms. |
| `operation:GET:/v1/events/stream` | drift | `replicant_client::events::EventsClient::stream` | `src/events.rs:651` | Source disagreement: OpenAPI has no 2xx or text/event-stream response, while the rendered page defines a persistent SSE response (reference/replicant-space-2-5-1/api/events/stream/index.md:25); EventsClient implements the documented cursor and SSE framing. |
| `operation:POST:/v1/locations/{location_code}/events/{designation}` | drift | `replicant_client::raw::location_events::LocationEventsClient::resolve` | `src/raw/location_events.rs:44` | Source disagreement: OpenAPI declares no success response, while the rendered page documents HTTP 200 with a resolved-event object (reference/replicant-space-2-5-1/concepts/civilisations/index.md:95); the client sends the empty-body mutation but exposes that response only as serde_json::Value. |
| `schema:app_schemas_location_events_LocationEventSchema` | drift | `replicant_client::raw::events::LocationEvent` | `src/raw/events.rs:41` | Source disagreement: OpenAPI defines nullable criteria as an array, while the rendered account-event example uses a single object (reference/replicant-space-2-5-1/concepts/civilisations/index.md:69); the client accepts both but normalizes them to Vec<JsonObject>. The DTO has no named category field, leaving that OpenAPI property only in flattened extra. |
| `operation:GET:/v1/leaderboards/megastructure` | drift | `replicant_client::raw::leaderboards::LeaderboardsClient::megastructure` | `src/raw/leaderboards.rs:196` | Source disagreement: the rendered megastructure example returns a leaderboard array rather than the OpenAPI LeaderboardResponse shape (reference/replicant-space-2-5-1/api/locations/megastructures/index.md:89). The method decodes the OpenAPI response. |
| `operation:GET:/v1/leaderboards/simulations` | drift | `replicant_client::raw::leaderboards::LeaderboardsClient::simulations` | `src/raw/leaderboards.rs:220` | Source disagreement: OpenAPI applies its global BearerAuth because this operation has no security override, while the rendered page says leaderboard endpoints require no authentication (reference/replicant-space-2-5-1/simulations/leaderboards/index.md:93). The client sends this request without authentication. |
| `operation:GET:/v1/leaderboards/simulations/{scenario_code}` | drift | `replicant_client::raw::leaderboards::LeaderboardsClient::simulation_scenario` | `src/raw/leaderboards.rs:233` | Source disagreement: OpenAPI applies its global BearerAuth because this operation has no security override, while the rendered page says leaderboard endpoints require no authentication (reference/replicant-space-2-5-1/simulations/leaderboards/index.md:93). The client sends this request without authentication. |
| `schema:app_schemas_leaderboards_LeaderboardEntrySchema` | drift | `replicant_client::raw::leaderboards::LeaderboardEntry` | `src/raw/leaderboards.rs:39` | Source disagreement: the rendered megastructure example uses replicant and devices rather than OpenAPI's replicant_code and contribution_count fields (reference/replicant-space-2-5-1/api/locations/megastructures/index.md:90). The DTO follows the OpenAPI component fields. |
| `schema:app_schemas_leaderboards_LeaderboardResponseSchema` | drift | `replicant_client::raw::leaderboards::LeaderboardResponse` | `src/raw/leaderboards.rs:60` | Source disagreement: the rendered megastructure example returns leaderboard, while OpenAPI defines board and entries (reference/replicant-space-2-5-1/api/locations/megastructures/index.md:89). The DTO follows the OpenAPI response shape. |
| `schema:app_schemas_simulations_SimulationHistoryEntrySchema` | drift | `replicant_client::raw::simulations::SimulationHistoryEntry` | `src/raw/simulations.rs:114` | Source disagreement: OpenAPI leaves lifecycle timestamps and score_seconds non-nullable, while rendered history examples emit null for those fields (reference/replicant-space-2-5-1/simulations/outcomes/index.md:78). The client uses Option fields, matching the rendered examples rather than the OpenAPI nullability. |
| `schema:flask_smorest_error_handler_ErrorSchema` | drift | `replicant_client::ErrorDetails` | `src/error.rs:16` | Source disagreement: OpenAPI defines code, errors, message, and status, while the rendered error envelope at reference/replicant-space-2-5-1/errors/index.md:19 defines a single error string; ErrorDetails normalizes both forms rather than exposing one authoritative schema. |

## Partial findings

| Unit | Verdict | Client symbol | Evidence | Notes |
|---|---|---|---|---|
| `operation:GET:/v1/accounts/me` | partial | `replicant_client::raw::accounts::AccountsClient::me` | `src/raw/accounts.rs:342` | AccountMeResponse omits the OpenAPI and documented message_notify object from the otherwise matching success shape. |
| `operation:PATCH:/v1/accounts/me` | partial | `replicant_client::raw::accounts::AccountsClient::update` | `src/raw/accounts.rs:350` | The PATCH method and tri-state fields are present, but AccountUpdateRequest and AccountUpdateResponse both omit the OpenAPI message_notify object. |
| `schema:app_schemas_accounts_AccountMeResponseSchema` | partial | `replicant_client::raw::accounts::AccountMeResponse` | `src/raw/accounts.rs:150` | AccountMeResponse models the account snapshot fields but omits message_notify. |
| `schema:app_schemas_accounts_AccountUpdateRequestSchema` | partial | `replicant_client::raw::accounts::AccountUpdateRequest` | `src/raw/accounts.rs:188` | AccountUpdateRequest models the tri-state update fields but omits nullable message_notify. |
| `schema:app_schemas_accounts_AccountUpdateResponseSchema` | partial | `replicant_client::raw::accounts::AccountUpdateResponse` | `src/raw/accounts.rs:218` | AccountUpdateResponse models every listed property except message_notify. |
| `operation:PATCH:/v1/replicants/{replicant_code}` | partial | `replicant_client::raw::replicants::ReplicantsClient::update` | `src/raw/replicants.rs:701` | The PATCH body has all documented fields, but Option fields with skip_serializing_if cannot emit explicit null to clear nullable profile values (src/raw/common.rs:29). |
| `schema:app_schemas_replicants_ReplicantUpdateRequestSchema` | partial | `replicant_client::raw::replicants::ReplicantUpdateRequest` | `src/raw/replicants.rs:134` | All request properties are present, but skip_serializing_if on Option fields cannot represent explicit null for nullable description, plan, project, and pronouns (src/raw/common.rs:29). |
| `schema:app_schemas_blueprints_BlueprintSchema` | partial | `replicant_client::raw::blueprints::Blueprint` | `src/raw/blueprints.rs:14` | Blueprint models resources and components as generic JsonObject maps rather than integer-valued maps from the OpenAPI schema. |
| `schema:app_schemas_device_commands_CollectResourcesSchema` | partial | `replicant_client::raw::devices::DeviceCommand::CollectResources` | `src/raw/devices.rs:465` | DeviceCommand::CollectResources requires a resources object but leaves its OpenAPI numeric values as generic JSON values. |
| `schema:app_schemas_device_commands_DepositResourcesSchema` | partial | `replicant_client::raw::devices::DeviceCommand::DepositResources` | `src/raw/devices.rs:485` | DeviceCommand::DepositResources models the optional nullable resources object but leaves its OpenAPI numeric values as generic JSON values. |
| `schema:app_schemas_common_ErrorSchema` | partial | `replicant_client::ErrorDetails::message` | `src/error.rs:137` | The simple wire error string is normalized into message and retained only through the broader sanitized error envelope rather than exposed as a serde error field. |
| `schema:app_schemas_scanning_AsteroidBeltDetailSchema` | partial | `replicant_client::raw::replicants::ScanAsteroidBeltDetail::resources` | `src/raw/replicants.rs:342` | The resources property is exposed as generic JsonObject rather than a map whose values are constrained to strings. |
| `operation:GET:/v1/devices/{device_code}/trades` | partial | `replicant_client::raw::trading::TradingClient::list` | `src/raw/trading.rs:29` | The GET route is present, but it returns opaque serde_json::Value instead of a typed response for the documented trade fields (reference/replicant-space-2-5-1/trading/directory/index.md:76). |
| `operation:GET:/v1/replicants/{replicant_code}/traders` | partial | `replicant_client::raw::trading::TradingClient::visible_to_replicant` | `src/raw/trading.rs:85` | The route is present, but the public client returns opaque serde_json::Value instead of a typed traders response for the documented directory fields (reference/replicant-space-2-5-1/trading/directory/index.md:30). |
| `operation:POST:/v1/devices/{device_code}/trades` | partial | `replicant_client::raw::trading::TradingClient::create` | `src/raw/trading.rs:37` | The public POST accepts an untyped JsonObject and returns opaque serde_json::Value, so it does not represent the documented name/stock/criteria/rewards body shape (reference/replicant-space-2-5-1/trading/trades/index.md:26). |
| `operation:GET:/v1/locations/{location_code}/events` | partial | `replicant_client::raw::location_events::LocationEventsClient::list` | `src/raw/location_events.rs:27` | OpenAPI provides no 2xx response schema and no rendered page documents this route's success body; the client assumes the account-event LocationEventListResponse. |
| `schema:app_schemas_simulations_ScenarioSummarySchema` | partial | `replicant_client::raw::simulations::ScenarioSummary` | `src/raw/simulations.rs:19` | The entry_cost field is exposed as opaque JsonObject, so the public DTO does not model the documented device_type and quantity fields (reference/replicant-space-2-5-1/simulations/scenarios/index.md:37). |
| `schema:app_schemas_simulations_SimulationEnterResponseSchema` | partial | `replicant_client::raw::simulations::SimulationEnterResponse` | `src/raw/simulations.rs:61` | The devices field is exposed as Vec<JsonObject>, so the public DTO does not model the documented device_code and device_type fields (reference/replicant-space-2-5-1/simulations/running/index.md:38). |

## n/a rows

| Unit | Verdict | Client symbol | Evidence | Notes |
|---|---|---|---|---|
| `operation:DELETE:/v1/accounts/webhook` | n/a | `` | `policy/operations.json:120` | The deprecated DELETE webhook operation is intentionally excluded by policy. |
| `operation:GET:/v1/accounts/webhook` | n/a | `` | `policy/operations.json:132` | The deprecated GET webhook operation is intentionally excluded by policy. |
| `operation:POST:/v1/accounts/webhook` | n/a | `` | `policy/operations.json:144` | The deprecated POST webhook operation is intentionally excluded by policy. |
| `schema:app_schemas_accounts_WebhookInfoSchema` | n/a | `` | `policy/operations.json:132` | The 2.5.2 schema adds nullable blocked_until, but the deprecated GET webhook operation is intentionally excluded by policy. |
| `schema:app_schemas_accounts_WebhookRegisterSchema` | n/a | `` | `policy/operations.json:144` | The webhook registration schema is intentionally excluded with the deprecated POST webhook operation. |
| `schema:app_schemas_accounts_WebhookRemovedSchema` | n/a | `` | `policy/operations.json:120` | The webhook removal schema is intentionally excluded with the deprecated DELETE webhook operation. |
| `schema:app_schemas_accounts_WebhookResponseSchema` | n/a | `` | `policy/operations.json:144` | The webhook response schema is intentionally excluded with the deprecated POST webhook operation. |
| `operation:GET:/v1/locations/{designation}/inventory` | n/a | `` | `policy/operations.json:620` | The legacy per-location inventory operation is intentionally excluded as deprecated in favor of account-wide inventory. |
| `schema:app_schemas_inventory_InventoryLookupSchema` | n/a | `` | `policy/operations.json:620` | InventoryLookupSchema is used only by the deprecated per-location inventory operation and is intentionally excluded with it. |
| `operation:GET:/v1/replicants/{replicant_code}/events` | n/a | `` | `policy/operations.json:722` | The per-replicant event log is intentionally excluded as deprecated in favor of the unified account event stream. |
| `operation:POST:/v1/admin/message` | n/a | `` | `src/raw/mod.rs:3` | The admin-tagged endpoint is outside the player-facing client because the raw surface explicitly excludes administrative operations. |
| `operation:POST:/v1/admin/story/advance` | n/a | `` | `src/raw/mod.rs:3` | The admin-tagged endpoint is outside the player-facing client because the raw surface explicitly excludes administrative operations. |
| `schema:app_schemas_admin_AdminMessageRequestSchema` | n/a | `` | `src/raw/mod.rs:3` | Reverse tracing finds this schema referenced only by its admin operation, which the raw surface explicitly excludes from the player-facing client. |
| `schema:app_schemas_admin_AdminMessageResponseSchema` | n/a | `` | `src/raw/mod.rs:3` | Reverse tracing finds this schema referenced only by its admin operation, which the raw surface explicitly excludes from the player-facing client. |
| `schema:app_schemas_admin_StoryAdvanceRequestSchema` | n/a | `` | `src/raw/mod.rs:3` | Reverse tracing finds this schema referenced only by its admin operation, which the raw surface explicitly excludes from the player-facing client. |
| `schema:app_schemas_admin_StoryAdvanceResponseSchema` | n/a | `` | `src/raw/mod.rs:3` | Reverse tracing finds this schema referenced only by its admin operation, which the raw surface explicitly excludes from the player-facing client. |
| `schema:flask_smorest_pagination_PaginationMetadataSchema` | n/a | `` | `policy/schema-field-coverage.json:12390` | The generated pagination metadata schema is explicitly excluded as unreferenced by the current player-facing operation set. |

## Source disagreements

| Unit | Evidence | Notes |
|---|---|---|
| `operation:GET:/v1/achievements` | `src/raw/achievements.rs:88` | Source disagreement: the rendered page adds a category query filter while OpenAPI defines no query parameter (reference/replicant-space-2-5-1/api/accounts/achievements/index.md:60); the client follows the OpenAPI route without that filter. |
| `operation:POST:/v1/replicants/{replicant_code}/print` | `src/raw/replicants.rs:777` | Source disagreement: OpenAPI declares a 200 PrintResponse, while rendered print docs use 202 for initial queueing and document queue-shaped command responses (reference/replicant-space-2-5-1/api/replicants/print/index.md:26); PrintResponse cannot represent those documented queue fields. |
| `schema:app_schemas_devices_DeviceListItemSchema` | `src/raw/devices.rs:259` | Source disagreement: OpenAPI names the owner field owner_replicant_code, while the rendered list example uses replicant_code (reference/replicant-space-2-5-1/api/devices/list/index.md:54); the public DTO exposes replicant_code. |
| `schema:app_schemas_locations_LocationResponseSchema` | `src/raw/locations.rs:80<br>reference/replicant-space-2-5-2/api/locations/index.md:78` | Source disagreement: rendered 2.5.2 location docs require boolean atmosphere at reference/replicant-space-2-5-2/api/locations/index.md:78, while PlanetaryBody exposes Option<String> at src/raw/locations.rs:80. |
| `operation:GET:/v1/replicants/{replicant_code}/stars` | `src/raw/replicants.rs:829` | Source disagreement: OpenAPI gives per_page a default of 20 with no maximum, while the rendered page says 1-50 and default 10 (reference/replicant-space-2-5-1/api/replicants/stars/index.md:21); the client forwards the query without resolving the mismatch. |
| `operation:POST:/v1/replicants/{replicant_code}/mine` | `src/raw/replicants.rs:765` | Source disagreement: OpenAPI defines a 200 MineResponse, while the rendered mining page documents 202 Accepted with only status/resource_type (reference/replicant-space-2-5-1/api/replicants/mine/index.md:25); the client follows the OpenAPI response type. |
| `operation:POST:/v1/replicants/{replicant_code}/teleport` | `src/raw/replicants.rs:867` | Source disagreement: OpenAPI defines a 200 response, while the rendered teleport page documents 202 Accepted (reference/replicant-space-2-5-1/api/replicants/teleport/index.md:30); the client follows the route and OpenAPI response shape. |
| `operation:POST:/v1/replicants/{replicant_code}/travel` | `src/raw/replicants.rs:913` | Source disagreement: OpenAPI defines route as a nullable RouteLeg array, while rendered normal-travel docs show route as one object (reference/replicant-space-2-5-1/api/replicants/travel/index.md:41); TravelResponse uses a non-null Vec<RouteLeg>. |
| `schema:app_schemas_printing_PrintResponseSchema` | `src/raw/replicants.rs:298` | Source disagreement: OpenAPI lists timing/device/refund fields, while rendered print docs include queue and queue_length in a command response (reference/replicant-space-2-5-1/api/replicants/print/index.md:60); PrintResponse drops those documented fields. |
| `schema:app_schemas_travel_TravelResponseSchema` | `src/raw/replicants.rs:624` | Source disagreement: OpenAPI defines route as a nullable RouteLeg array, while rendered normal-travel docs show route as one object (reference/replicant-space-2-5-1/api/replicants/travel/index.md:41); the client field is a non-null Vec<RouteLeg>. |
| `operation:DELETE:/v1/devices/{device_code}/simulate/{sim_id}` | `src/raw/simulations.rs:197` | Source disagreement: OpenAPI defines no success response, while the rendered outcomes page documents 200 with status and simulation_id (reference/replicant-space-2-5-1/simulations/outcomes/index.md:23); cancel returns untyped JSON. |
| `operation:GET:/v1/replicants/{replicant_code}/devices` | `src/raw/replicants.rs:713` | Source disagreement: OpenAPI's DeviceListItem names the owner field owner_replicant_code while the rendered device-list example uses replicant_code (reference/replicant-space-2-5-1/api/devices/list/index.md:54); the client reuses DeviceStatus and exposes replicant_code. |
| `operation:POST:/v1/devices/{device_code}/simulate` | `src/raw/simulations.rs:170` | Source disagreement: OpenAPI declares 201 Created while the rendered running page documents 200 OK (reference/replicant-space-2-5-1/simulations/running/index.md:15); the typed request and response fields otherwise match. |
| `schema:app_schemas_devices_DeviceCommandResponseSchema` | `src/raw/devices.rs:647` | Source disagreement: the OpenAPI response omits ward-specific fields documented in the rendered ward response (reference/replicant-space-2-5-1/system-wards/index.md:46); the public DTO includes warding, activated, deactivated, and evicted_miners. |
| `schema:app_schemas_devices_DeviceListResponseSchema` | `src/raw/devices.rs:302` | Source disagreement: OpenAPI's DeviceListItem names the owner field owner_replicant_code while the rendered device-list example uses replicant_code (reference/replicant-space-2-5-1/api/devices/list/index.md:54); the client reuses DeviceStatus and exposes replicant_code. |
| `schema:app_schemas_device_commands_RenameSchema` | `src/raw/devices.rs:541` | Source disagreement: OpenAPI defines the required name property, while the rendered command example uses new_name (reference/replicant-space-2-5-1/api/devices/command/index.md:107); the client serializes name. |
| `operation:GET:/v1/locations/{designation}` | `src/raw/locations.rs:279` | Source disagreement: OpenAPI exposes location and scanned fields, while the rendered page shows code, surveyed, and top-level boolean atmosphere (reference/replicant-space-2-5-1/api/locations/index.md:73). The client aliases code and surveyed but retains atmosphere only as Option<String> inside PlanetaryBody. |
| `operation:GET:/v1/replicants/{replicant_code}/scan/devices` | `src/raw/replicants.rs:805` | Source disagreement: OpenAPI omits a success schema, while rendered scan-device docs define a 200 body containing star, device_count, devices, and next_cursor (reference/replicant-space-2-5-1/api/replicants/scan/devices/index.md:41). The client sends all filters but returns a generic JSON value instead of a typed response. |
| `operation:POST:/v1/locations/{designation}/contribute` | `src/raw/locations.rs:295` | Source disagreement: OpenAPI declares no success response for this POST, while the rendered megastructure page documents a 200 object with accepted, rejected, progress, and status (reference/replicant-space-2-5-1/api/locations/megastructures/index.md:62). The client sends the required devices body but returns a generic JSON value rather than a typed success shape. |
| `operation:POST:/v1/replicants/{replicant_code}/scan` | `src/raw/replicants.rs:792` | Source disagreement: OpenAPI defines replicants as an array of objects, while rendered scan docs show an object keyed by replicant name (reference/replicant-space-2-5-1/api/replicants/scan/index.md:104). SystemScanResponse follows the OpenAPI array and cannot decode the rendered object shape. |
| `schema:app_schemas_scanning_SystemScanResponseSchema` | `src/raw/replicants.rs:489` | Source disagreement: OpenAPI defines replicants as an array of objects, while rendered scan docs show an object keyed by replicant name (reference/replicant-space-2-5-1/api/replicants/scan/index.md:104). The public field is Vec<JsonObject>, so it follows OpenAPI and cannot decode the documented object. |
| `operation:GET:/v1/accounts/reputation` | `src/raw/accounts.rs:412` | Source disagreement: the rendered curl example uses the singular /v1/account/reputation while the endpoint heading and OpenAPI use /v1/accounts/reputation (reference/replicant-space-2-5-1/api/accounts/reputation/index.md:24); the client implements the plural route. |
| `operation:GET:/v1/inventory` | `src/raw/inventory.rs:91` | Source disagreement: rendered inventory examples encode each location's items as a resource-keyed object, while OpenAPI defines an array of InventoryItem values (reference/replicant-space-2-5-1/api/locations/inventory/index.md:51); the client follows the OpenAPI array shape. |
| `schema:app_schemas_inventory_LocationInventorySchema` | `src/raw/inventory.rs:24` | Source disagreement: OpenAPI defines items as an array of InventoryItem values, while the rendered inventory examples use a resource-keyed object (reference/replicant-space-2-5-1/api/locations/inventory/index.md:51); the public DTO follows OpenAPI. |
| `operation:GET:/v1/accounts/events` | `src/raw/accounts.rs:394` | Source disagreement: OpenAPI defaults the account-events limit to 50 and defines criteria as an array, while the rendered page says default 20 and shows criteria as an object (reference/replicant-space-2-5-1/concepts/civilisations/index.md:47); the client forwards optional filters and accepts both criteria forms. |
| `operation:GET:/v1/events/stream` | `src/events.rs:651` | Source disagreement: OpenAPI has no 2xx or text/event-stream response, while the rendered page defines a persistent SSE response (reference/replicant-space-2-5-1/api/events/stream/index.md:25); EventsClient implements the documented cursor and SSE framing. |
| `operation:POST:/v1/locations/{location_code}/events/{designation}` | `src/raw/location_events.rs:44` | Source disagreement: OpenAPI declares no success response, while the rendered page documents HTTP 200 with a resolved-event object (reference/replicant-space-2-5-1/concepts/civilisations/index.md:95); the client sends the empty-body mutation but exposes that response only as serde_json::Value. |
| `schema:app_schemas_location_events_LocationEventSchema` | `src/raw/events.rs:41` | Source disagreement: OpenAPI defines nullable criteria as an array, while the rendered account-event example uses a single object (reference/replicant-space-2-5-1/concepts/civilisations/index.md:69); the client accepts both but normalizes them to Vec<JsonObject>. The DTO has no named category field, leaving that OpenAPI property only in flattened extra. |
| `operation:GET:/v1/leaderboards/megastructure` | `src/raw/leaderboards.rs:196` | Source disagreement: the rendered megastructure example returns a leaderboard array rather than the OpenAPI LeaderboardResponse shape (reference/replicant-space-2-5-1/api/locations/megastructures/index.md:89). The method decodes the OpenAPI response. |
| `operation:GET:/v1/leaderboards/simulations` | `src/raw/leaderboards.rs:220` | Source disagreement: OpenAPI applies its global BearerAuth because this operation has no security override, while the rendered page says leaderboard endpoints require no authentication (reference/replicant-space-2-5-1/simulations/leaderboards/index.md:93). The client sends this request without authentication. |
| `operation:GET:/v1/leaderboards/simulations/{scenario_code}` | `src/raw/leaderboards.rs:233` | Source disagreement: OpenAPI applies its global BearerAuth because this operation has no security override, while the rendered page says leaderboard endpoints require no authentication (reference/replicant-space-2-5-1/simulations/leaderboards/index.md:93). The client sends this request without authentication. |
| `schema:app_schemas_leaderboards_LeaderboardEntrySchema` | `src/raw/leaderboards.rs:39` | Source disagreement: the rendered megastructure example uses replicant and devices rather than OpenAPI's replicant_code and contribution_count fields (reference/replicant-space-2-5-1/api/locations/megastructures/index.md:90). The DTO follows the OpenAPI component fields. |
| `schema:app_schemas_leaderboards_LeaderboardResponseSchema` | `src/raw/leaderboards.rs:60` | Source disagreement: the rendered megastructure example returns leaderboard, while OpenAPI defines board and entries (reference/replicant-space-2-5-1/api/locations/megastructures/index.md:89). The DTO follows the OpenAPI response shape. |
| `schema:app_schemas_simulations_SimulationHistoryEntrySchema` | `src/raw/simulations.rs:114` | Source disagreement: OpenAPI leaves lifecycle timestamps and score_seconds non-nullable, while rendered history examples emit null for those fields (reference/replicant-space-2-5-1/simulations/outcomes/index.md:78). The client uses Option fields, matching the rendered examples rather than the OpenAPI nullability. |
| `schema:flask_smorest_error_handler_ErrorSchema` | `src/error.rs:16` | Source disagreement: OpenAPI defines code, errors, message, and status, while the rendered error envelope at reference/replicant-space-2-5-1/errors/index.md:19 defines a single error string; ErrorDetails normalizes both forms rather than exposing one authoritative schema. |

## Source snapshot

- Snapshot: `reference/replicant-space-2-5-2/`
- OpenAPI SHA-256: `df5f74046e95678f54161b930af6d8b1abbe4b07b1718e485b5a4d46f6757639`
- Counts: 75 paths, 89 operations, 160 schemas, 79 catalogue events, 87 rendered pages, 328 worklist units.

## Methodology

OpenAPI is authoritative, followed by the 87-page rendered markdown mirror, then the v2.5.2 changelog. Source disagreements remain `drift` findings. `covered` requires the complete public transport or representation; `partial` records an incomplete symbol; `missing` records no public implementation; `n/a` is reserved for a concrete player-facing exclusion.

## Fixed slices

| # | Slice | Units |
|---:|---|---:|
| 1 | `01-changelog-2.5.2` | 20 |
| 2 | `02-accounts-achievements` | 33 |
| 3 | `03-replicants-travel-mining-printing` | 33 |
| 4 | `04-devices` | 36 |
| 5 | `05-device-commands-blueprints` | 26 |
| 6 | `06-locations-stars-scanning` | 26 |
| 7 | `07-inventory-trades-species` | 21 |
| 8 | `08-events-messages-location-events` | 18 |
| 9 | `09-leaderboards-simulations` | 27 |
| 10 | `10-admin-feedback-health-tutorials` | 14 |
| 11 | `11-catalogue-ami-through-mining` | 36 |
| 12 | `12-catalogue-print-through-ward` | 38 |

## Calibration findings

- `event:device.stowed`: **covered**, `src/events.rs:514` — GameEvent::device_stowed decodes device.stowed into DeviceStowedPayload with forward-compatible payload fields.
- `event:hub.maintained`: **covered**, `src/events.rs:532` — GameEvent::hub_maintained decodes hub.maintained into HubMaintainedPayload with forward-compatible payload fields.
- `event:hub.warning`: **covered**, `src/events.rs:533` — GameEvent::hub_warning decodes hub.warning into HubWarningPayload with forward-compatible payload fields.
- `event:multiplayer.replicant_entered`: **covered**, `src/events.rs:539` — GameEvent::multiplayer_replicant_entered decodes multiplayer.replicant_entered into MultiplayerReplicantPresencePayload with forward-compatible payload fields.
- `event:multiplayer.replicant_left`: **covered**, `src/events.rs:540` — GameEvent::multiplayer_replicant_left decodes multiplayer.replicant_left into MultiplayerReplicantPresencePayload with forward-compatible payload fields.
- `schema:app_schemas_locations_LocationResponseSchema`: **drift**, `['src/raw/locations.rs:80', 'reference/replicant-space-2-5-2/api/locations/index.md:78']` — Source disagreement: rendered 2.5.2 location docs require boolean atmosphere at reference/replicant-space-2-5-2/api/locations/index.md:78, while PlanetaryBody exposes Option<String> at src/raw/locations.rs:80.

## Changelog delta adjudication

The v2.5.2 changelog documents the event-catalogue field additions (`reference/replicant-space-2-5-2/changelog/index.md:26`) and separately lists reactive AMI Mining Controller re-evaluation, account-wipe and webhook behavior, compacted-device capacity, both event payload fields, notification deduplication, and BobNet chatter changes (`reference/replicant-space-2-5-2/changelog/index.md:30-37`). The wire-visible webhook and event payload deltas are adjudicated in their schema and event rows; the remaining server-behavior changes introduce no operation, schema, or catalogue-event unit and are therefore excluded from the generated worklist.

## Artifacts

- [`audit/2.5.2/doc-pages.jsonl`](audit/2.5.2/doc-pages.jsonl)
- [`audit/2.5.2/worklist.jsonl`](audit/2.5.2/worklist.jsonl)
- [`audit/2.5.2/merged.jsonl`](audit/2.5.2/merged.jsonl)
- [`audit/2.5.2/results/01-changelog-2.5.2.jsonl`](audit/2.5.2/results/01-changelog-2.5.2.jsonl)
- [`audit/2.5.2/results/02-accounts-achievements.jsonl`](audit/2.5.2/results/02-accounts-achievements.jsonl)
- [`audit/2.5.2/results/03-replicants-travel-mining-printing.jsonl`](audit/2.5.2/results/03-replicants-travel-mining-printing.jsonl)
- [`audit/2.5.2/results/04-devices.jsonl`](audit/2.5.2/results/04-devices.jsonl)
- [`audit/2.5.2/results/05-device-commands-blueprints.jsonl`](audit/2.5.2/results/05-device-commands-blueprints.jsonl)
- [`audit/2.5.2/results/06-locations-stars-scanning.jsonl`](audit/2.5.2/results/06-locations-stars-scanning.jsonl)
- [`audit/2.5.2/results/07-inventory-trades-species.jsonl`](audit/2.5.2/results/07-inventory-trades-species.jsonl)
- [`audit/2.5.2/results/08-events-messages-location-events.jsonl`](audit/2.5.2/results/08-events-messages-location-events.jsonl)
- [`audit/2.5.2/results/09-leaderboards-simulations.jsonl`](audit/2.5.2/results/09-leaderboards-simulations.jsonl)
- [`audit/2.5.2/results/10-admin-feedback-health-tutorials.jsonl`](audit/2.5.2/results/10-admin-feedback-health-tutorials.jsonl)
- [`audit/2.5.2/results/11-catalogue-ami-through-mining.jsonl`](audit/2.5.2/results/11-catalogue-ami-through-mining.jsonl)
- [`audit/2.5.2/results/12-catalogue-print-through-ward.jsonl`](audit/2.5.2/results/12-catalogue-print-through-ward.jsonl)

## Appendix: covered rows

| Unit | Client symbol | Evidence |
|---|---|---|
| `operation:GET:/v1/accounts/achievements` | `replicant_client::raw::accounts::AccountsClient::achievements` | `src/raw/accounts.rs:381` |
| `operation:GET:/v1/achievements/{achievement_key}` | `replicant_client::raw::achievements::AchievementsClient::get` | `src/raw/achievements.rs:100` |
| `operation:POST:/v1/devices/{device_code}` | `replicant_client::raw::devices::DevicesClient::command` | `src/raw/devices.rs:1004` |
| `schema:app_schemas_achievements_AchievementSchema` | `replicant_client::raw::accounts::AccountAchievement` | `src/raw/accounts.rs:250` |
| `schema:app_schemas_device_commands_TravelSchema` | `replicant_client::raw::devices::DeviceCommand::Travel` | `src/raw/devices.rs:619` |
| `schema:app_schemas_printing_PrintRequestSchema` | `replicant_client::raw::replicants::PrintRequest` | `src/raw/replicants.rs:279` |
| `schema:app_schemas_stars_CatalogueStarSchema` | `replicant_client::raw::galaxy::CatalogueStar` | `src/raw/galaxy.rs:80` |
| `schema:app_schemas_stars_StarItemSchema` | `replicant_client::raw::galaxy::StarItem` | `src/raw/galaxy.rs:18` |
| `event:device.stowed` | `replicant_client::events::GameEvent::device_stowed` | `src/events.rs:514` |
| `event:hub.maintained` | `replicant_client::events::GameEvent::hub_maintained` | `src/events.rs:532` |
| `event:hub.warning` | `replicant_client::events::GameEvent::hub_warning` | `src/events.rs:533` |
| `event:multiplayer.replicant_entered` | `replicant_client::events::GameEvent::multiplayer_replicant_entered` | `src/events.rs:539` |
| `event:multiplayer.replicant_left` | `replicant_client::events::GameEvent::multiplayer_replicant_left` | `src/events.rs:540` |
| `operation:DELETE:/v1/accounts/me` | `replicant_client::raw::accounts::AccountsClient::request_destructive_wipe` | `src/raw/accounts.rs:367` |
| `operation:GET:/v1/accounts/simulations` | `replicant_client::raw::accounts::AccountsClient::simulations` | `src/raw/accounts.rs:426` |
| `operation:GET:/v1/accounts/verify/{token}` | `replicant_client::raw::accounts::AccountsClient::verify` | `src/raw/accounts.rs:331` |
| `operation:POST:/v1/accounts` | `replicant_client::raw::accounts::AccountsClient::register` | `src/raw/accounts.rs:297` |
| `operation:POST:/v1/accounts/recover` | `replicant_client::raw::accounts::AccountsClient::recover` | `src/raw/accounts.rs:313` |
| `schema:app_schemas_accounts_AccountInfoSchema` | `replicant_client::raw::accounts::AccountInfo` | `src/raw/accounts.rs:33` |
| `schema:app_schemas_accounts_AccountResponseSchema` | `replicant_client::raw::accounts::RegisterResponse` | `src/raw/accounts.rs:50` |
| `schema:app_schemas_accounts_AccountWipeRequestedSchema` | `replicant_client::raw::accounts::AccountWipeRequestedResponse` | `src/raw/accounts.rs:242` |
| `schema:app_schemas_accounts_EventSettingsSchema` | `replicant_client::raw::accounts::EventSettings` | `src/raw/accounts.rs:100` |
| `schema:app_schemas_accounts_MessageSettingsSchema` | `replicant_client::raw::accounts::MessageSettings` | `src/raw/accounts.rs:112` |
| `schema:app_schemas_accounts_RecoverRequestSchema` | `replicant_client::raw::accounts::RecoverRequest` | `src/raw/accounts.rs:61` |
| `schema:app_schemas_accounts_RecoverResponseSchema` | `replicant_client::raw::accounts::RecoverResponse` | `src/raw/accounts.rs:69` |
| `schema:app_schemas_accounts_RegisterRequestSchema` | `replicant_client::raw::accounts::RegisterRequest` | `src/raw/accounts.rs:19` |
| `schema:app_schemas_accounts_ReplicantBriefSchema` | `replicant_client::raw::accounts::ReplicantBrief` | `src/raw/accounts.rs:77` |
| `schema:app_schemas_accounts_ReplicantSummarySchema` | `replicant_client::raw::accounts::AccountReplicantSummary` | `src/raw/accounts.rs:124` |
| `schema:app_schemas_accounts_VerificationResponseSchema` | `replicant_client::raw::accounts::VerificationResponse` | `src/raw/accounts.rs:87` |
| `schema:app_schemas_achievements_AchievementListResponseSchema` | `replicant_client::raw::accounts::AccountAchievementListResponse` | `src/raw/accounts.rs:268` |
| `schema:app_schemas_achievements_public_AchievementDetailResponseSchema` | `replicant_client::raw::achievements::AchievementDetailResponse` | `src/raw/achievements.rs:58` |
| `schema:app_schemas_achievements_public_AchievementIndexResponseSchema` | `replicant_client::raw::achievements::AchievementIndexResponse` | `src/raw/achievements.rs:39` |
| `schema:app_schemas_achievements_public_AchievementPlayerSchema` | `replicant_client::raw::achievements::AchievementPlayer` | `src/raw/achievements.rs:48` |
| `schema:app_schemas_achievements_public_AchievementSummarySchema` | `replicant_client::raw::achievements::AchievementSummary` | `src/raw/achievements.rs:19` |
| `operation:DELETE:/v1/replicants/{replicant_code}/mine` | `replicant_client::raw::replicants::ReplicantsClient::stop_mining` | `src/raw/replicants.rs:754` |
| `operation:DELETE:/v1/replicants/{replicant_code}/travel` | `replicant_client::raw::replicants::ReplicantsClient::cancel_travel` | `src/raw/replicants.rs:899` |
| `operation:GET:/v1/replicants` | `replicant_client::raw::replicants::ReplicantsClient::list` | `src/raw/replicants.rs:674` |
| `operation:GET:/v1/replicants/{replicant_code}/stars/{star_designation}` | `replicant_client::raw::replicants::ReplicantsClient::star` | `src/raw/replicants.rs:851` |
| `operation:POST:/v1/replicants/{replicant_code}/message` | `replicant_client::raw::replicants::ReplicantsClient::message` | `src/raw/replicants.rs:737` |
| `operation:POST:/v1/replicants/{replicant_code}/transfer` | `replicant_client::raw::replicants::ReplicantsClient::transfer` | `src/raw/replicants.rs:882` |
| `schema:app_schemas_mining_ReplicantMineRequestSchema` | `replicant_client::raw::replicants::MineRequest` | `src/raw/replicants.rs:230` |
| `schema:app_schemas_mining_ReplicantMineResponseSchema` | `replicant_client::raw::replicants::MineResponse` | `src/raw/replicants.rs:250` |
| `schema:app_schemas_replicants_MiningInfoSchema` | `replicant_client::raw::status::MiningInfo` | `src/raw/status.rs:10` |
| `schema:app_schemas_replicants_PrintingInfoSchema` | `replicant_client::raw::status::PrintingInfo` | `src/raw/status.rs:34` |
| `schema:app_schemas_replicants_ReplicantMessageRequestSchema` | `replicant_client::raw::replicants::ReplicantMessageRequest` | `src/raw/replicants.rs:197` |
| `schema:app_schemas_replicants_ReplicantMessageResponseSchema` | `replicant_client::raw::replicants::ReplicantMessageResponse` | `src/raw/replicants.rs:207` |
| `schema:app_schemas_replicants_ReplicantSearchItemSchema` | `replicant_client::raw::replicants::ReplicantSearchItem` | `src/raw/replicants.rs:42` |
| `schema:app_schemas_replicants_ReplicantSearchResponseSchema` | `replicant_client::raw::replicants::ReplicantListResponse` | `src/raw/replicants.rs:56` |
| `schema:app_schemas_replicants_ReplicantUpdateResponseSchema` | `replicant_client::raw::replicants::ReplicantUpdateResponse` | `src/raw/replicants.rs:161` |
| `schema:app_schemas_replicants_TeleportInfoSchema` | `replicant_client::raw::replicants::TeleportInfo` | `src/raw/replicants.rs:67` |
| `schema:app_schemas_replicants_TeleportRequestSchema` | `replicant_client::raw::replicants::TeleportRequest` | `src/raw/replicants.rs:528` |
| `schema:app_schemas_replicants_TeleportResponseSchema` | `replicant_client::raw::replicants::TeleportResponse` | `src/raw/replicants.rs:538` |
| `schema:app_schemas_replicants_TransferRequestSchema` | `replicant_client::raw::replicants::TransferRequest` | `src/raw/replicants.rs:559` |
| `schema:app_schemas_replicants_TransferResponseSchema` | `replicant_client::raw::replicants::TransferResponse` | `src/raw/replicants.rs:567` |
| `schema:app_schemas_travel_RouteLegSchema` | `replicant_client::raw::replicants::RouteLeg` | `src/raw/replicants.rs:598` |
| `schema:app_schemas_travel_TravelRequestSchema` | `replicant_client::raw::replicants::TravelRequest` | `src/raw/replicants.rs:580` |
| `operation:DELETE:/v1/devices/{device_code}/permissions` | `replicant_client::raw::devices::DevicesClient::revoke_permission` | `src/raw/devices.rs:1103` |
| `operation:GET:/v1/devices/{device_code}/audit` | `replicant_client::raw::devices::DevicesClient::audit` | `src/raw/devices.rs:1018` |
| `operation:GET:/v1/devices/{device_code}/channels` | `replicant_client::raw::bobnet::BobnetClient::channels` | `src/raw/bobnet.rs:95` |
| `operation:GET:/v1/devices/{device_code}/logs` | `replicant_client::raw::devices::DevicesClient::logs` | `src/raw/devices.rs:1041` |
| `operation:GET:/v1/devices/{device_code}/messages` | `replicant_client::raw::bobnet::BobnetClient::messages` | `src/raw/bobnet.rs:106` |
| `operation:GET:/v1/devices/{device_code}/network` | `replicant_client::raw::devices::DevicesClient::network` | `src/raw/devices.rs:1062` |
| `operation:GET:/v1/devices/{device_code}/permissions` | `replicant_client::raw::devices::DevicesClient::list_permissions` | `src/raw/devices.rs:1071` |
| `operation:GET:/v1/devices/{device_code}/simulate` | `replicant_client::raw::simulations::SimulationsClient::scenarios` | `src/raw/simulations.rs:159` |
| `operation:GET:/v1/devices/{device_code}/simulate/active` | `replicant_client::raw::simulations::SimulationsClient::active` | `src/raw/simulations.rs:182` |
| `operation:PATCH:/v1/devices/{device_code}` | `replicant_client::raw::devices::DevicesClient::configure` | `src/raw/devices.rs:978` |
| `operation:POST:/v1/devices/{device_code}/permissions` | `replicant_client::raw::devices::DevicesClient::grant_permission` | `src/raw/devices.rs:1087` |
| `operation:POST:/v1/devices/{device_code}/retrieve` | `replicant_client::raw::devices::DevicesClient::retrieve` | `src/raw/devices.rs:991` |
| `schema:app_schemas_devices_BobnetMessageItemSchema` | `replicant_client::raw::bobnet::BobnetMessageItem` | `src/raw/bobnet.rs:38` |
| `schema:app_schemas_devices_CargoItemSchema` | `replicant_client::raw::devices::CargoItem` | `src/raw/devices.rs:108` |
| `schema:app_schemas_devices_ChannelItemSchema` | `replicant_client::raw::bobnet::ChannelItem` | `src/raw/bobnet.rs:19` |
| `schema:app_schemas_devices_DeviceChannelsResponseSchema` | `replicant_client::raw::bobnet::DeviceChannelsResponse` | `src/raw/bobnet.rs:29` |
| `schema:app_schemas_devices_DeviceConfigurationRequestSchema` | `replicant_client::raw::devices::DeviceConfigurationRequest` | `src/raw/devices.rs:347` |
| `schema:app_schemas_devices_DeviceConfigurationResponseSchema` | `replicant_client::raw::devices::DeviceConfigurationResponse` | `src/raw/devices.rs:393` |
| `schema:app_schemas_devices_DeviceConfigurationSchema` | `replicant_client::raw::devices::DeviceConfiguration` | `src/raw/devices.rs:358` |
| `schema:app_schemas_devices_DeviceMessagesResponseSchema` | `replicant_client::raw::bobnet::DeviceMessagesResponse` | `src/raw/bobnet.rs:71` |
| `schema:app_schemas_devices_DeviceNetworkSchema` | `replicant_client::raw::devices::DeviceNetwork` | `src/raw/devices.rs:891` |
| `schema:app_schemas_devices_MiningInfoSchema` | `replicant_client::raw::status::MiningInfo` | `src/raw/status.rs:10` |
| `schema:app_schemas_devices_NetworkConnectionSchema` | `replicant_client::raw::devices::NetworkConnection` | `src/raw/devices.rs:879` |
| `schema:app_schemas_devices_PrintingInfoSchema` | `replicant_client::raw::status::PrintingInfo` | `src/raw/status.rs:34` |
| `schema:app_schemas_devices_ProspectInfoSchema` | `replicant_client::raw::devices::ProspectInfo` | `src/raw/devices.rs:118` |
| `schema:app_schemas_devices_RepairInfoSchema` | `replicant_client::raw::devices::RepairInfo` | `src/raw/devices.rs:137` |
| `schema:app_schemas_devices_ScanInfoSchema` | `replicant_client::raw::devices::ScanInfo` | `src/raw/devices.rs:152` |
| `schema:app_schemas_devices_TravelInfoSchema` | `replicant_client::raw::status::TravelInfo` | `src/raw/status.rs:53` |
| `operation:GET:/v1/blueprints` | `replicant_client::raw::blueprints::BlueprintsClient::list` | `src/raw/blueprints.rs:70` |
| `schema:app_schemas_blueprints_BlueprintListSchema` | `replicant_client::raw::blueprints::BlueprintListResponse` | `src/raw/blueprints.rs:52` |
| `schema:app_schemas_device_commands_AdoptSchema` | `replicant_client::raw::devices::DeviceCommand::Adopt` | `src/raw/devices.rs:449` |
| `schema:app_schemas_device_commands_AttachSchema` | `replicant_client::raw::devices::DeviceCommand::Attach` | `src/raw/devices.rs:451` |
| `schema:app_schemas_device_commands_ChangeOwnerSchema` | `replicant_client::raw::devices::DeviceCommand::ChangeOwner` | `src/raw/devices.rs:456` |
| `schema:app_schemas_device_commands_ConfigureSchema` | `replicant_client::raw::devices::DeviceCommand::Configure` | `src/raw/devices.rs:472` |
| `schema:app_schemas_device_commands_DequeuePrintSchema` | `replicant_client::raw::devices::DeviceCommand::DequeuePrint` | `src/raw/devices.rs:491` |
| `schema:app_schemas_device_commands_DetachSchema` | `replicant_client::raw::devices::DeviceCommand::Detach` | `src/raw/devices.rs:497` |
| `schema:app_schemas_device_commands_EnqueuePrintSchema` | `replicant_client::raw::devices::DeviceCommand::EnqueuePrint` | `src/raw/devices.rs:501` |
| `schema:app_schemas_device_commands_MessageSchema` | `replicant_client::raw::devices::DeviceCommand::Message` | `src/raw/devices.rs:524` |
| `schema:app_schemas_device_commands_NoParamSchema` | `replicant_client::raw::devices::DeviceCommand` | `src/raw/devices.rs:443` |
| `schema:app_schemas_device_commands_ProspectSchema` | `replicant_client::raw::devices::DeviceCommand::Prospect` | `src/raw/devices.rs:531` |
| `schema:app_schemas_device_commands_ReleaseSchema` | `replicant_client::raw::devices::DeviceCommand::Release` | `src/raw/devices.rs:539` |
| `schema:app_schemas_device_commands_RepairSchema` | `replicant_client::raw::devices::DeviceCommand::Repair` | `src/raw/devices.rs:548` |
| `schema:app_schemas_device_commands_ReplicateSchema` | `replicant_client::raw::devices::DeviceCommand::Replicate` | `src/raw/devices.rs:557` |
| `schema:app_schemas_device_commands_RetargetSchema` | `replicant_client::raw::devices::DeviceCommand::Retarget` | `src/raw/devices.rs:566` |
| `schema:app_schemas_device_commands_SetDirectiveSchema` | `replicant_client::raw::devices::DeviceCommand::SetDirective` | `src/raw/devices.rs:575` |
| `schema:app_schemas_device_commands_SetWelcomeMessageSchema` | `replicant_client::raw::devices::DeviceCommand::SetWelcomeMessage` | `src/raw/devices.rs:588` |
| `schema:app_schemas_device_commands_StartMiningSchema` | `replicant_client::raw::devices::DeviceCommand::StartMining` | `src/raw/devices.rs:594` |
| `schema:app_schemas_device_commands_StellarCensusSchema` | `replicant_client::raw::devices::DeviceCommand::StellarCensus` | `src/raw/devices.rs:602` |
| `schema:app_schemas_device_commands_StowSchema` | `replicant_client::raw::devices::DeviceCommand::Stow` | `src/raw/devices.rs:611` |
| `schema:app_schemas_device_commands_TriangulateSchema` | `replicant_client::raw::devices::DeviceCommand::Triangulate` | `src/raw/devices.rs:630` |
| `operation:GET:/v1/locations` | `replicant_client::raw::locations::LocationsClient::system_map` | `src/raw/locations.rs:271` |
| `operation:GET:/v1/locations/{star_designation}/stars` | `replicant_client::raw::galaxy::GalaxyClient::stars_near` | `src/raw/galaxy.rs:129` |
| `operation:GET:/v1/stars` | `replicant_client::raw::galaxy::GalaxyClient::catalogue` | `src/raw/galaxy.rs:151` |
| `schema:app_schemas_common_PositionSchema` | `replicant_client::raw::common::Position` | `src/raw/common.rs:20` |
| `schema:app_schemas_locations_LocationContributionRequestSchema` | `replicant_client::raw::locations::LocationContributionRequest` | `src/raw/locations.rs:253` |
| `schema:app_schemas_locations_LocationCountsSchema` | `replicant_client::raw::locations::LocationCounts` | `src/raw/locations.rs:22` |
| `schema:app_schemas_locations_LocationSystemMapSchema` | `replicant_client::raw::locations::LocationSystemMap` | `src/raw/locations.rs:39` |
| `schema:app_schemas_scanning_ActiveLocationEventSummarySchema` | `replicant_client::raw::replicants::ScanLocationEvent` | `src/raw/replicants.rs:316` |
| `schema:app_schemas_scanning_AsteroidBeltSchema` | `replicant_client::raw::replicants::ScanAsteroidBelt` | `src/raw/replicants.rs:348` |
| `schema:app_schemas_scanning_HabitableZoneSchema` | `replicant_client::raw::replicants::ScanHabitableZone` | `src/raw/replicants.rs:434` |
| `schema:app_schemas_scanning_InventoryItemSchema` | `replicant_client::raw::replicants::ScanInventoryItem` | `src/raw/replicants.rs:359` |
| `schema:app_schemas_scanning_PlanetSummarySchema` | `replicant_client::raw::replicants::ScanPlanetSummary` | `src/raw/replicants.rs:369` |
| `schema:app_schemas_scanning_ShopSummarySchema` | `replicant_client::raw::replicants::ScanShopSummary` | `src/raw/replicants.rs:411` |
| `schema:app_schemas_scanning_ShopTradeSchema` | `replicant_client::raw::replicants::ScanShopTrade` | `src/raw/replicants.rs:395` |
| `schema:app_schemas_scanning_StarDetailSchema` | `replicant_client::raw::replicants::ScanStarDetail` | `src/raw/replicants.rs:444` |
| `schema:app_schemas_stars_CatalogueResponseSchema` | `replicant_client::raw::galaxy::CatalogueResponse` | `src/raw/galaxy.rs:106` |
| `schema:app_schemas_stars_StarDetailResponseSchema` | `replicant_client::raw::replicants::StarDetailResponse` | `src/raw/replicants.rs:519` |
| `schema:app_schemas_stars_StarListResponseSchema` | `replicant_client::raw::galaxy::StarListResponse` | `src/raw/galaxy.rs:50` |
| `operation:DELETE:/v1/devices/{device_code}/trades/{trade_code}` | `replicant_client::raw::trading::TradingClient::delete` | `src/raw/trading.rs:49` |
| `operation:GET:/v1/replicants/{replicant_code}/reputation` | `replicant_client::raw::reputation::ReputationClient::for_replicant` | `src/raw/reputation.rs:75` |
| `operation:GET:/v1/species` | `replicant_client::raw::species::SpeciesClient::list` | `src/raw/species.rs:55` |
| `operation:POST:/v1/devices/{device_code}/trades/{trade_code}` | `replicant_client::raw::trading::TradingClient::fulfill` | `src/raw/trading.rs:67` |
| `schema:app_schemas_inventory_AccountInventoryResponseSchema` | `replicant_client::raw::inventory::AccountInventoryResponse` | `src/raw/inventory.rs:49` |
| `schema:app_schemas_inventory_InventoryItemSchema` | `replicant_client::raw::inventory::InventoryItem` | `src/raw/inventory.rs:14` |
| `schema:app_schemas_species_AccountReputationEntrySchema` | `replicant_client::raw::reputation::AccountReputationEntry` | `src/raw/reputation.rs:20` |
| `schema:app_schemas_species_AccountReputationResponseSchema` | `replicant_client::raw::reputation::AccountReputationResponse` | `src/raw/reputation.rs:36` |
| `schema:app_schemas_species_ReplicantReputationEntrySchema` | `replicant_client::raw::reputation::ReplicantReputationEntry` | `src/raw/reputation.rs:45` |
| `schema:app_schemas_species_ReplicantReputationResponseSchema` | `replicant_client::raw::reputation::ReplicantReputationResponse` | `src/raw/reputation.rs:57` |
| `schema:app_schemas_species_SpeciesListResponseSchema` | `replicant_client::raw::species::SpeciesListResponse` | `src/raw/species.rs:37` |
| `schema:app_schemas_species_SpeciesSchema` | `replicant_client::raw::species::Species` | `src/raw/species.rs:12` |
| `operation:GET:/v1/events` | `replicant_client::events::EventsClient::list` | `src/events.rs:625` |
| `operation:GET:/v1/messages` | `replicant_client::raw::messages::MessagesClient::list` | `src/raw/messages.rs:96` |
| `operation:POST:/v1/messages/read` | `replicant_client::raw::messages::MessagesClient::mark_read` | `src/raw/messages.rs:118` |
| `schema:app_schemas_events_EventListResponseSchema` | `replicant_client::raw::devices::DeviceLogsResponse` | `src/raw/devices.rs:868` |
| `schema:app_schemas_events_EventSchema` | `replicant_client::raw::devices::DeviceLogEvent` | `src/raw/devices.rs:848` |
| `schema:app_schemas_events_GameEventSchema` | `replicant_client::events::GameEvent` | `src/events.rs:409` |
| `schema:app_schemas_events_GameEventsResponseSchema` | `replicant_client::events::EventLogResponse` | `src/events.rs:605` |
| `schema:app_schemas_location_events_LocationEventListResponseSchema` | `replicant_client::raw::events::LocationEventListResponse` | `src/raw/events.rs:81` |
| `schema:app_schemas_messages_MessageListResponseSchema` | `replicant_client::raw::messages::MessageListResponse` | `src/raw/messages.rs:55` |
| `schema:app_schemas_messages_MessageSchema` | `replicant_client::raw::messages::Message` | `src/raw/messages.rs:20` |
| `schema:app_schemas_messages_MessagesReadRequestSchema` | `replicant_client::raw::messages::MessagesReadRequest` | `src/raw/messages.rs:67` |
| `schema:app_schemas_messages_MessagesReadResponseSchema` | `replicant_client::raw::messages::MessagesReadResponse` | `src/raw/messages.rs:79` |
| `operation:GET:/v1/leaderboards` | `replicant_client::raw::leaderboards::LeaderboardsClient::index` | `src/raw/leaderboards.rs:136` |
| `operation:GET:/v1/leaderboards/colony_moon` | `replicant_client::raw::leaderboards::LeaderboardsClient::colony_moon` | `src/raw/leaderboards.rs:148` |
| `operation:GET:/v1/leaderboards/colony_planet` | `replicant_client::raw::leaderboards::LeaderboardsClient::colony_planet` | `src/raw/leaderboards.rs:160` |
| `operation:GET:/v1/leaderboards/distance` | `replicant_client::raw::leaderboards::LeaderboardsClient::distance` | `src/raw/leaderboards.rs:172` |
| `operation:GET:/v1/leaderboards/fleet` | `replicant_client::raw::leaderboards::LeaderboardsClient::fleet` | `src/raw/leaderboards.rs:184` |
| `operation:GET:/v1/leaderboards/reputation` | `replicant_client::raw::leaderboards::LeaderboardsClient::reputation` | `src/raw/leaderboards.rs:208` |
| `operation:GET:/v1/leaderboards/trades` | `replicant_client::raw::leaderboards::LeaderboardsClient::trades` | `src/raw/leaderboards.rs:247` |
| `operation:GET:/v1/leaderboards/xp` | `replicant_client::raw::leaderboards::LeaderboardsClient::xp` | `src/raw/leaderboards.rs:259` |
| `schema:app_schemas_leaderboards_LeaderboardBoardSchema` | `replicant_client::raw::leaderboards::LeaderboardBoard` | `src/raw/leaderboards.rs:16` |
| `schema:app_schemas_leaderboards_LeaderboardIndexResponseSchema` | `replicant_client::raw::leaderboards::LeaderboardIndexResponse` | `src/raw/leaderboards.rs:30` |
| `schema:app_schemas_leaderboards_SimLeaderboardEntrySchema` | `replicant_client::raw::leaderboards::SimLeaderboardEntry` | `src/raw/leaderboards.rs:94` |
| `schema:app_schemas_leaderboards_SimLeaderboardIndexResponseSchema` | `replicant_client::raw::leaderboards::SimLeaderboardIndexResponse` | `src/raw/leaderboards.rs:85` |
| `schema:app_schemas_leaderboards_SimLeaderboardResponseSchema` | `replicant_client::raw::leaderboards::SimLeaderboardResponse` | `src/raw/leaderboards.rs:114` |
| `schema:app_schemas_leaderboards_SimLeaderboardScenarioSchema` | `replicant_client::raw::leaderboards::SimLeaderboardScenario` | `src/raw/leaderboards.rs:71` |
| `schema:app_schemas_simulations_ScenarioListResponseSchema` | `replicant_client::raw::simulations::ScenarioListResponse` | `src/raw/simulations.rs:43` |
| `schema:app_schemas_simulations_SimulationActiveResponseSchema` | `replicant_client::raw::simulations::SimulationActiveResponse` | `src/raw/simulations.rs:104` |
| `schema:app_schemas_simulations_SimulationActiveSummarySchema` | `replicant_client::raw::simulations::SimulationActiveSummary` | `src/raw/simulations.rs:84` |
| `schema:app_schemas_simulations_SimulationEnterSchema` | `replicant_client::raw::simulations::SimulationEnterRequest` | `src/raw/simulations.rs:51` |
| `schema:app_schemas_simulations_SimulationHistoryResponseSchema` | `replicant_client::raw::simulations::SimulationHistoryResponse` | `src/raw/simulations.rs:141` |
| `operation:GET:/v1/health` | `replicant_client::raw::Client::health` | `src/raw/client.rs:574` |
| `operation:GET:/v1/tutorials` | `replicant_client::raw::tutorials::TutorialsClient::list` | `src/raw/tutorials.rs:107` |
| `operation:GET:/v1/tutorials/{slug}` | `replicant_client::raw::tutorials::TutorialsClient::get` | `src/raw/tutorials.rs:114` |
| `operation:POST:/v1/feedback` | `replicant_client::raw::feedback::FeedbackClient::submit` | `src/raw/feedback.rs:40` |
| `schema:app_schemas_feedback_FeedbackSubmitRequestSchema` | `replicant_client::raw::feedback::FeedbackSubmitRequest` | `src/raw/feedback.rs:11` |
| `schema:app_schemas_feedback_FeedbackSubmitResponseSchema` | `replicant_client::raw::feedback::FeedbackSubmitResponse` | `src/raw/feedback.rs:23` |
| `event:ami.adopted` | `replicant_client::events::GameEvent::ami_adopted` | `src/events.rs:497` |
| `event:ami.assembled` | `replicant_client::events::GameEvent::ami_assembled` | `src/events.rs:498` |
| `event:ami.launched` | `replicant_client::events::GameEvent::ami_launched` | `src/events.rs:499` |
| `event:ami.released` | `replicant_client::events::GameEvent::ami_released` | `src/events.rs:501` |
| `event:ami.withdrawn` | `replicant_client::events::GameEvent::ami_withdrawn` | `src/events.rs:504` |
| `event:blueprint.unlocked` | `replicant_client::events::GameEvent::blueprint_unlocked` | `src/events.rs:505` |
| `event:bobnet.new` | `replicant_client::events::GameEvent::bobnet_new` | `src/events.rs:506` |
| `event:device.attached` | `replicant_client::events::GameEvent::device_attached` | `src/events.rs:507` |
| `event:device.changed_owner` | `replicant_client::events::GameEvent::device_changed_owner` | `src/events.rs:508` |
| `event:device.compacted` | `replicant_client::events::GameEvent::device_compacted` | `src/events.rs:509` |
| `event:device.compacting` | `replicant_client::events::GameEvent::device_compacting` | `src/events.rs:510` |
| `event:device.decommissioned` | `replicant_client::events::GameEvent::device_decommissioned` | `src/events.rs:511` |
| `event:device.deployed` | `replicant_client::events::GameEvent::device_deployed` | `src/events.rs:512` |
| `event:device.detached` | `replicant_client::events::GameEvent::device_detached` | `src/events.rs:513` |
| `event:device.unfurled` | `replicant_client::events::GameEvent::device_unfurled` | `src/events.rs:515` |
| `event:device.unfurling` | `replicant_client::events::GameEvent::device_unfurling` | `src/events.rs:516` |
| `event:directive.cleared` | `replicant_client::events::GameEvent::directive_cleared` | `src/events.rs:517` |
| `event:directive.completed` | `replicant_client::events::GameEvent::directive_completed` | `src/events.rs:518` |
| `event:directive.paused` | `replicant_client::events::GameEvent::directive_paused` | `src/events.rs:519` |
| `event:directive.resumed` | `replicant_client::events::GameEvent::directive_resumed` | `src/events.rs:520` |
| `event:directive.set` | `replicant_client::events::GameEvent::directive_set` | `src/events.rs:521` |
| `event:diversion.activated` | `replicant_client::events::GameEvent::diversion_activated` | `src/events.rs:522` |
| `event:diversion.deactivated` | `replicant_client::events::GameEvent::diversion_deactivated` | `src/events.rs:523` |
| `event:diversion.diverted` | `replicant_client::events::GameEvent::diversion_diverted` | `src/events.rs:524` |
| `event:diversion.impacted` | `replicant_client::events::GameEvent::diversion_impacted` | `src/events.rs:525` |
| `event:diversion.partial` | `replicant_client::events::GameEvent::diversion_partial` | `src/events.rs:526` |
| `event:event.completed` | `replicant_client::events::GameEvent::event_completed` | `src/events.rs:527` |
| `event:event.discovered` | `replicant_client::events::GameEvent::event_discovered` | `src/events.rs:528` |
| `event:experience.gained` | `replicant_client::events::GameEvent::experience_gained` | `src/events.rs:529` |
| `event:hub.activated` | `replicant_client::events::GameEvent::hub_activated` | `src/events.rs:530` |
| `event:hub.destroyed` | `replicant_client::events::GameEvent::hub_destroyed` | `src/events.rs:531` |
| `event:megastructure.contributed` | `replicant_client::events::GameEvent::megastructure_contributed` | `src/events.rs:534` |
| `event:message.new` | `replicant_client::events::GameEvent::message_new` | `src/events.rs:535` |
| `event:mining.retargeted` | `replicant_client::events::GameEvent::mining_retargeted` | `src/events.rs:536` |
| `event:mining.started` | `replicant_client::events::GameEvent::mining_started` | `src/events.rs:537` |
| `event:mining.stopped` | `replicant_client::events::GameEvent::mining_stopped` | `src/events.rs:538` |
| `event:print.completed` | `replicant_client::events::GameEvent::print_completed` | `src/events.rs:203` |
| `event:print.started` | `replicant_client::events::GameEvent::print_started` | `src/events.rs:542` |
| `event:prospect.completed` | `replicant_client::events::GameEvent::prospect_completed` | `src/events.rs:543` |
| `event:relay.activated` | `replicant_client::events::GameEvent::relay_activated` | `src/events.rs:544` |
| `event:replicant.transferred` | `replicant_client::events::GameEvent::replicant_transferred` | `src/events.rs:545` |
| `event:salvage.depleted` | `replicant_client::events::GameEvent::salvage_depleted` | `src/events.rs:546` |
| `event:salvage.discovered` | `replicant_client::events::GameEvent::salvage_discovered` | `src/events.rs:547` |
| `event:scan.completed` | `replicant_client::events::GameEvent::scan_completed` | `src/events.rs:548` |
| `event:scan.started` | `replicant_client::events::GameEvent::scan_started` | `src/events.rs:549` |
| `event:search.completed` | `replicant_client::events::GameEvent::search_completed` | `src/events.rs:550` |
| `event:search.started` | `replicant_client::events::GameEvent::search_started` | `src/events.rs:551` |
| `event:simulation.abandoned` | `replicant_client::events::GameEvent::simulation_abandoned` | `src/events.rs:552` |
| `event:simulation.completed` | `replicant_client::events::GameEvent::simulation_completed` | `src/events.rs:553` |
| `event:simulation.expired` | `replicant_client::events::GameEvent::simulation_expired` | `src/events.rs:554` |
| `event:simulation.started` | `replicant_client::events::GameEvent::simulation_started` | `src/events.rs:555` |
| `event:site.depleted` | `replicant_client::events::GameEvent::site_depleted` | `src/events.rs:556` |
| `event:story.awakened` | `replicant_client::events::GameEvent::story_awakened` | `src/events.rs:557` |
| `event:story.hint` | `replicant_client::events::GameEvent::story_hint` | `src/events.rs:558` |
| `event:system.body_renamed` | `replicant_client::events::GameEvent::system_body_renamed` | `src/events.rs:559` |
| `event:system.devices_halted` | `replicant_client::events::GameEvent::system_devices_halted` | `src/events.rs:560` |
| `event:system.entry_point_set` | `replicant_client::events::GameEvent::system_entry_point_set` | `src/events.rs:561` |
| `event:system.object_detected` | `replicant_client::events::GameEvent::system_object_detected` | `src/events.rs:390` |
| `event:teleport.completed` | `replicant_client::events::GameEvent::teleport_completed` | `src/events.rs:563` |
| `event:teleport.failed` | `replicant_client::events::GameEvent::teleport_failed` | `src/events.rs:564` |
| `event:teleport.started` | `replicant_client::events::GameEvent::teleport_started` | `src/events.rs:565` |
| `event:trade.completed` | `replicant_client::events::GameEvent::trade_completed` | `src/events.rs:566` |
| `event:trade.created` | `replicant_client::events::GameEvent::trade_created` | `src/events.rs:567` |
| `event:trade.deleted` | `replicant_client::events::GameEvent::trade_deleted` | `src/events.rs:568` |
| `event:transport.collected` | `replicant_client::events::GameEvent::transport_collected` | `src/events.rs:569` |
| `event:transport.delivered` | `replicant_client::events::GameEvent::transport_delivered` | `src/events.rs:570` |
| `event:travel.arrived` | `replicant_client::events::GameEvent::travel_arrived` | `src/events.rs:571` |
| `event:travel.cancelled` | `replicant_client::events::GameEvent::travel_cancelled` | `src/events.rs:572` |
| `event:travel.departed` | `replicant_client::events::GameEvent::travel_departed` | `src/events.rs:573` |
| `event:triangulation.complete` | `replicant_client::events::GameEvent::triangulation_complete` | `src/events.rs:574` |
| `event:triangulation.failed` | `replicant_client::events::GameEvent::triangulation_failed` | `src/events.rs:575` |
| `event:triangulation.started` | `replicant_client::events::GameEvent::triangulation_started` | `src/events.rs:576` |
| `event:ward.activated` | `replicant_client::events::GameEvent::ward_activated` | `src/events.rs:577` |
| `event:ward.deactivated` | `replicant_client::events::GameEvent::ward_deactivated` | `src/events.rs:578` |
