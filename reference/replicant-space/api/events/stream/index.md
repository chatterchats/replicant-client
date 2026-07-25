---
title: "Event stream"
source_url: "https://replicant.space/docs/api/events/stream/"
crawled_at: "2026-07-24T18:16:34.001486+00:00"
---

API · Events

# Event stream

Subscribe to the realtime Server-Sent Events (SSE) stream of everything happening across your account. Events are fired automatically - no polling required.

## Endpoint

`GET /v1/events/stream`

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | string · optional | ID to resume from. Events after this ID will be delivered. You can also pass this as the `Last-Event-ID` header. |

## How it works

The endpoint returns a *text/event-stream* response that stays open.
Each event is framed as a standard SSE message with *id*, *event*, and *data* fields.
The *event* field is the dotted event name (e.g. *mining.started*), so you can attach listeners for specific types.
The *data* field is a JSON object with the same envelope as the [event logs](../logs/index.md) endpoint.

When there is no activity, the server sends periodic *: keepalive* comments to hold the connection open. These are ignored by SSE clients automatically.

## Resuming

Store the *id* of the last event you received. When you reconnect, pass it as the *cursor* query parameter or *Last-Event-ID* header. The stream will replay any events you missed, then continue with new ones in real time.

Events are retained in a capped stream (most recent ~10,000 events). If your cursor is older than the retention window, the stream starts from the earliest available event.

## Muted events

Your account's mute patterns are applied to the stream automatically. Events matching your mute list are never delivered.
AMI digest events are never muted. See [event logs](../logs/index.md) for details on configuring mute patterns.

## Example

GET /v1/events/stream

```
$ curl -N "https://api.replicant.space/v1/events/stream" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Accept: text/event-stream"
```

response SSE stream

```
id: 1752681600000-0
event: mining.started
data: {"version":1,"category":"mining","event":"mining.started","device_code":"2AC61214","payload":{"resource_type":"structural","site":"SOL-BELT-1-SITE-3"},"created_at":"2026-07-16T10:00:00Z"}

id: 1752681620000-0
event: travel.departed
data: {"version":1,"category":"travel","event":"travel.departed","device_code":"7FE981A0","payload":{"travel_type":"cruise","origin":"SOL-4","destination":"SOL-BELT-1"},"created_at":"2026-07-16T10:03:20Z"}

: keepalive
```

## Resuming a stream

GET /v1/events/stream

```
# resume from the last event you received
$ curl -N "https://api.replicant.space/v1/events/stream?cursor=1752681600000-0" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Accept: text/event-stream"
```

## Node.js example

Track the last event ID and reconnect automatically if the connection drops, resuming from where we left off.

node stream.js

```
const https = require("https");

const API_KEY = process.env.REPLICANT_API_KEY;
const BASE    = "https://api.replicant.space";

let lastId = null;

function connect() {
  const url = lastId
    ? `${BASE}/v1/events/stream?cursor=${lastId}`
    : `${BASE}/v1/events/stream`;

  const req = https.get(url, {
    headers: { "Authorization": `Bearer ${API_KEY}` },
  }, (res) => {
    console.log("connected", res.statusCode);

    res.on("data", (chunk) => {
      const text = chunk.toString();
      if (text.startsWith(": keepalive")) return;

      const idMatch   = text.match(/^id: (.+)$/m);
      const nameMatch = text.match(/^event: (.+)$/m);
      const dataMatch = text.match(/^data: (.+)$/m);

      if (idMatch) lastId = idMatch[1];
      if (dataMatch) {
        const event = JSON.parse(dataMatch[1]);
        console.log(nameMatch?.[1], event.device_code, event.payload);
      }
    });

    res.on("end", () => {
      console.log("disconnected, reconnecting in 3s...");
      setTimeout(connect, 3000);
    });
  });

  req.on("error", (err) => {
    console.error("error", err.message);
    setTimeout(connect, 5000);
  });
}

connect();
```

## Browser example

In the browser, use `fetch()` with a readable stream.

JavaScript

```
async function streamEvents(apiKey, onEvent) {
  const res = await fetch("https://api.replicant.space/v1/events/stream", {
    headers: { "Authorization": `Bearer ${apiKey}` },
  });

  const reader = res.body.getReader();
  const decoder = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    for (const line of decoder.decode(value).split("\n")) {
      if (line.startsWith("data: ")) {
        onEvent(JSON.parse(line.slice(6)));
      }
    }
  }
}

streamEvents(apiKey, (event) => {
  console.log(event.event, event.device_code, event.payload);
});
```
