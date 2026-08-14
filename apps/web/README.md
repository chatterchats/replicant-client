# Replicant web application

The Vite development server proxies `/api` and `/ws` to `replicantd` on
`127.0.0.1:8080`.

```sh
# Terminal 1, from the repository root
RS_API_TOKEN=... cargo run -p replicant-server --bin replicantd

# Terminal 2
cd apps/web
npm install
npm run dev
```

Run all frontend checks with `npm run check` or `make web-check` from the
repository root.

## Provenance

The theme colors, application-shell layout, and command-palette interaction
were adapted from the approved `replicant.react` source supplied by its author
and used with the permission provided by the user. Backend behavior was not
copied: this application uses the typed `replicantd` HTTP/WebSocket protocol.
