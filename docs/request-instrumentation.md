# Request instrumentation

This note describes the request-timing contract for a browser request that crosses the
nginx sidecar and `replicantd`. It is deliberately a correlation guide, not a source
of inferred timings: use a measured field when present and report `null` otherwise.

## One ID, end to end

The canonical header is `X-Replicant-Request-Id`.

1. The browser starts one logical fetch. It does not select the cross-layer
   correlation ID: nginx **replaces every incoming value** with its `$request_id`
   (including an attacker-supplied value).
2. nginx forwards that generated value to `replicantd` and returns the same value in
   the response header (`always`, including error responses). Its access log uses
   the same `$request_id`.
3. The daemon sanitizes the received value (length and allowed characters). If it is
   absent or invalid, it creates a bounded local ID. It logs and echoes the resulting
   ID. Thus the daemon ID is normally the nginx ID, while malformed direct requests
   can be correlated only within the daemon.
4. The browser reads the response ID and uses it for the structured
   `frontend.daemon_http` event. A missing/unreadable header is `null`; it is not
   guessed from timestamps or URL.

Never log authorization, request/response bodies, or a full query payload. Log a
normalized route and safe bounded dimensions instead.

## Timing boundaries and fields

`frontend.daemon_http` is one structured record with separate fields (all durations
are milliseconds unless suffixed `_s`; unavailable values are `null`):

- `browser_fetch_start_ms`, `browser_request_start_ms`,
  `browser_response_start_ms`, and `browser_response_end_ms` are Resource Timing
  milestones relative to the wrapper's monotonic start.
- `browser_queue_ms` is the standardized `requestStart - fetchStart` pre-request
  interval. It includes browser/socket scheduling plus any measured service-worker,
  DNS, connection, and TLS work; Resource Timing does not expose the narrower
  DevTools-only "stalled" interval.
- `browser_request_ms` is `responseStart - requestStart`;
  `browser_network_ms` is `responseEnd - fetchStart`; and
  `browser_transfer_ms` is `responseEnd - responseStart`.
- `browser_body_parse_ms` is wrapper completion minus Resource Timing
  `responseEnd`, separating decode/JSON parsing from transfer. The legacy `body_ms`
  remains headers-to-wrapper-completion and therefore contains both transfer and
  parsing.
- `browser_dns_ms`, `browser_connect_ms`, `connection_reused`, `transfer_bytes`,
  `encoded_bytes`, and `decoded_bytes` are populated only when the browser exposes
  enough Resource Timing data.
- `proxy_connect_ms` and `proxy_header_ms` are nginx
  `$upstream_connect_time` and `$upstream_header_time`, converted from seconds.
  `proxy_response_ms` remains `null` in the browser because nginx learns the
  finalized `$upstream_response_time` after ordinary headers have been sent; use
  the correlated nginx access record. `proxy_request_ms` is reserved for a proxy
  that can expose a finalized request duration and is otherwise `null`.
- `daemon_handler_ms` is daemon middleware/handler elapsed time, including response
  construction and ordinary JSON serialization, converted from the daemon response
  header. Streaming body transmission is outside this interval.
- `path` is the exact request path with query and fragment removed. `route` is a
  low-cardinality template such as `/api/workflows/:id`; daemon logs use axum's
  matched template such as `/api/workflows/{id}`.
- Page/domain/scene events separately measure projection and readiness. They are
  ordered in the same frontend session but are not falsely folded into HTTP
  latency.

Telemetry delivery queueing is separate from `browser_queue_ms`. A
`frontend.daemon_http` event is emitted after the API fetch completes, then may wait
in the bounded telemetry transport queue before upload.

### Classification decision table

| Observation                                                                      | Classification               | Decision rule                                                                                                  |
| -------------------------------------------------------------------------------- | ---------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `browser_queue_ms` is large while proxy/daemon values are small                  | browser pre-request stall    | Check DNS/connect fields first; the remainder is browser/service-worker/socket scheduling, not server latency. |
| `proxy_connect_ms` is large                                                      | nginx connect stall          | Upstream connection establishment dominates; daemon handler timing may be absent.                              |
| `proxy_header_ms` and/or `daemon_handler_ms` is large before response bytes      | upstream-header/daemon stall | Use daemon handler when present; otherwise nginx header time. These overlap in boundary and must not be added. |
| `browser_transfer_ms` is large, with small header/handler values and large sizes | transfer stall               | Network delivery/body size dominates after headers.                                                            |
| `browser_body_parse_ms` is large                                                 | parsing stall                | Main-thread decode/parse dominates after transfer.                                                             |
| page/scene readiness is late while HTTP fields are small                         | page/scene readiness stall   | Attribute to rendering, scheduling, or dependent work; readiness is a separate boundary.                       |
| measured fields are missing                                                      | unknown                      | Keep `null`; never derive a value by subtracting unrelated clocks.                                             |

For a ten-second request, compare measured intervals with the same request ID. A
10,000 ms browser request with a 9,700 ms daemon handler is a daemon stall; with a
100 ms handler and 9,500 ms browser transfer it is downstream transfer; with a
9,000 ms pre-request interval it is browser-side scheduling/connection work. Use
the DNS/connect subfields to identify measured contributors. If a field is
unavailable, classify only what another measured field proves.

## Four representative structured record sets

Each set shows the browser event, nginx access record, and daemon record joined by the
same ID. Values are illustrative; `null` means unavailable, not zero.

### 1. Browser queue stall

```json
{"event":"frontend.daemon_http","proxy_request_id":"r-q7","path":"/api/overview","route":"/api/overview","elapsed_ms":10000,"browser_fetch_start_ms":1,"browser_request_start_ms":9001,"browser_response_start_ms":9121,"browser_response_end_ms":9980,"browser_queue_ms":9000,"browser_request_ms":120,"browser_network_ms":9979,"browser_transfer_ms":859,"browser_body_parse_ms":20,"proxy_connect_ms":2,"proxy_header_ms":120,"proxy_response_ms":null,"daemon_handler_ms":110,"transfer_bytes":18420,"decoded_bytes":18420}
{"request_id":"r-q7","method":"GET","uri":"/api/overview","status":200,"request_time_s":"0.979","upstream_connect_time_s":"0.002","upstream_header_time_s":"0.120","upstream_response_time_s":"0.125","request_bytes":87,"response_bytes":18420}
{"event":"daemon.http","request_id":"r-q7","method":"GET","path":"/api/overview","route":"/api/overview","status":200,"elapsed_ms":110,"handler_ms":110}
```

### 2. Daemon handler stall

```json
{"event":"frontend.daemon_http","proxy_request_id":"r-d2","path":"/api/galaxy-scene","route":"/api/galaxy-scene","elapsed_ms":10020,"browser_fetch_start_ms":1,"browser_request_start_ms":5,"browser_response_start_ms":9975,"browser_response_end_ms":10005,"browser_queue_ms":4,"browser_request_ms":9970,"browser_network_ms":10004,"browser_transfer_ms":30,"browser_body_parse_ms":15,"proxy_connect_ms":1,"proxy_header_ms":9970,"proxy_response_ms":null,"daemon_handler_ms":9960,"transfer_bytes":4200,"decoded_bytes":4200}
{"request_id":"r-d2","method":"GET","uri":"/api/galaxy-scene","status":200,"request_time_s":"9.981","upstream_connect_time_s":"0.001","upstream_header_time_s":"9.970","upstream_response_time_s":"9.980","request_bytes":90,"response_bytes":4200}
{"event":"daemon.http_slow","request_id":"r-d2","method":"GET","path":"/api/galaxy-scene","route":"/api/galaxy-scene","status":200,"elapsed_ms":9960,"handler_ms":9960}
```

### 3. Large transfer

```json
{"event":"frontend.daemon_http","proxy_request_id":"r-x9","path":"/api/snapshot","route":"/api/snapshot","elapsed_ms":10044,"browser_fetch_start_ms":1,"browser_request_start_ms":4,"browser_response_start_ms":94,"browser_response_end_ms":9794,"browser_queue_ms":3,"browser_request_ms":90,"browser_network_ms":9793,"browser_transfer_ms":9700,"browser_body_parse_ms":250,"proxy_connect_ms":2,"proxy_header_ms":90,"proxy_response_ms":null,"daemon_handler_ms":80,"transfer_bytes":52428800,"decoded_bytes":104857600}
{"request_id":"r-x9","method":"GET","uri":"/api/snapshot","status":200,"request_time_s":"9.791","upstream_connect_time_s":"0.002","upstream_header_time_s":"0.090","upstream_response_time_s":"0.300","request_bytes":85,"response_bytes":52428800}
{"event":"daemon.http","request_id":"r-x9","method":"GET","path":"/api/snapshot","route":"/api/snapshot","status":200,"elapsed_ms":80,"handler_ms":80}
```

### 4. Healthy request

```json
{"event":"frontend.daemon_http","proxy_request_id":"r-h1","path":"/api/health","route":"/api/health","elapsed_ms":13,"browser_fetch_start_ms":1,"browser_request_start_ms":3,"browser_response_start_ms":7,"browser_response_end_ms":12,"browser_queue_ms":2,"browser_request_ms":4,"browser_network_ms":11,"browser_transfer_ms":5,"browser_body_parse_ms":1,"proxy_connect_ms":1,"proxy_header_ms":4,"proxy_response_ms":null,"daemon_handler_ms":2,"transfer_bytes":42,"decoded_bytes":42}
{"request_id":"r-h1","method":"GET","uri":"/api/health","status":200,"request_time_s":"0.006","upstream_connect_time_s":"0.001","upstream_header_time_s":"0.004","upstream_response_time_s":"0.005","request_bytes":82,"response_bytes":42}
{"event":"daemon.http","request_id":"r-h1","method":"GET","path":"/api/health","route":"/api/health","status":200,"elapsed_ms":2,"handler_ms":2}
```

## Browser and proxy limitations

Resource Timing is browser-controlled and may omit or coarsen entries, especially
cross-origin or non-exposed resources. It describes the browser's view: DNS,
connection reuse, queueing, response, and transfer can be zero or unavailable when a
connection is reused; zero does not prove no work occurred. Cache hits may avoid a
network transaction entirely, so nginx and daemon records may not exist. Cache
validation and service workers can likewise produce browser records with no matching
proxy request.
Browsers do not expose a direct connection-reuse flag. `connection_reused` is a
best-effort signal only when non-zero connection timestamps are present and the
connect interval is zero; cached/opaque all-zero entries remain `null`. Resource
Timing also has a bounded browser buffer. If the exact-URL/start-window entry has
already been evicted, every Resource Timing-derived field remains `null`.

The response ID and timing headers must be CORS-exposed for a cross-origin deployment;
otherwise JavaScript sees neither even if the network panel shows them. Same-origin
requests do not need that exposure. Authorization and custom headers can also cause
preflight requests; correlate the API request, not an `OPTIONS` record, and do not
merge their durations.

nginx's `$upstream_response_time` is finalized in the access log after the response
finishes. A response header emitted before completion cannot reliably contain that
finalized value (and streaming responses make this especially clear). Therefore
`X-Replicant-Upstream-Response-Time` is authoritative in nginx access logs when nginx
cannot expose the finalized value in the response; a browser `null` is expected and
must remain `null`.

Finally, daemon handler time and nginx upstream header/response time have overlapping
boundaries. They are diagnostic cross-checks, not additive stages. Join by the
canonical request ID, then use the decision table and only the intervals actually
measured.
