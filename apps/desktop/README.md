# Replicant desktop application

This Tauri 2 shell presents the existing React application and supervises the
existing `replicantd` executable. The daemon remains a separate process and the
owner of durable workflows; hiding, closing, reopening, or updating the window
does not intrinsically stop automation.

## Development

Install the existing web and desktop dependencies once:

```sh
npm --prefix apps/web install
npm --prefix apps/desktop install
```

Provide the daemon token to Rust, not frontend JavaScript. Either export
`RS_API_TOKEN`, export `RS_API_TOKEN_FILE`, or place the token in a file named
`api-token` under Tauri's per-user application config directory for
`space.replicant.desktop`. Restrict that file to the current user. The desktop
process passes only the file path to its sidecar.

Run the desktop development shell:

```sh
RS_API_TOKEN_FILE=/safe/path/replicant-token make desktop-dev
```

The preparation script builds `replicantd`, copies it to Tauri's required
target-triple sidecar name, and starts Vite through Tauri. The web application
continues to proxy `/api` and `/ws` to `127.0.0.1:8080` in development.

These deployment paths remain independent of Tauri:

```sh
# daemon only
RS_API_TOKEN_FILE=/safe/path/replicant-token cargo run -p replicant-server --bin replicantd

# CLI against that daemon
cargo run -p replicant-cli -- daemon

# web development against that daemon
npm --prefix apps/web run dev
```

## Desktop lifecycle and settings

At startup the shell probes `http://127.0.0.1:8080/api/health`. It reuses a
compatible running daemon or starts its bundled sidecar on that fixed loopback
address. The React client's existing snapshot and capped-backoff reconnect logic
recovers when the daemon restarts.

Close-to-tray is enabled by default and can be toggled from the tray menu. The
choice is stored as non-secret `desktop.json` in the application config
directory. “Quit (leave automation running)” is the safe default: it exits only
the presentation shell. “Quit and stop managed automation” stops only a sidecar
started by this desktop process; it never kills a separately launched daemon.

The sidecar uses `client.sqlite` and `runtime.sqlite` in Tauri's per-user local
application data directory unless `REPLICANT_DB` or `REPLICANT_RUNTIME_DB` is
already configured. No API token, database contents, or shell/process command is
exposed through the frontend capability set.

## Validation and release packaging

The lightweight packaging smoke checks do not require a display server:

```sh
make desktop-check
make ci
```

Build signed or unsigned native packages on each target operating system after
installing Tauri's documented platform prerequisites:

```sh
npm --prefix apps/desktop install
make desktop-build
```

`make desktop-build` builds the release daemon sidecar, builds the React assets
with their loopback HTTP/WebSocket endpoint, and invokes `tauri build`. Native
installers are emitted beneath `target/release/bundle`. Configure the standard
Tauri platform signing/notarization environment in release CI; never add keys,
certificates, tokens, `.env` files, or built sidecars to the repository.
