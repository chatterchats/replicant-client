# Docker deployment

The production Compose stack runs a non-root `replicantd` container and a
non-root nginx container serving the React application. Only the web port is
published. The daemon reaches Replicant Space over outbound HTTPS/SSE; no
inbound webhook or SSE port is required.

## Prerequisites and first start

Install Docker Engine with the Compose plugin. Clone the repository, then:

```sh
cp .env.example .env
# Edit .env and replace the RS_API_TOKEN placeholder.
mkdir -p "$HOME/.local/share/replicant"
# If your user/group IDs are not 1000, set REPLICANT_UID and REPLICANT_GID
# in .env to the output of: id -u; id -g
make docker-build
docker compose up -d
docker compose ps
docker compose logs -f
```

Open `http://localhost:8080`. Set `REPLICANT_WEB_PORT` in `.env` to publish a
different host port. Stop the stack without deleting data with:

```sh
docker compose down
```

`replicantd` binds `0.0.0.0:8080` only inside the Compose network. Its port is
not published by the default configuration. Native runs retain the
`127.0.0.1:8080` default.

Both services have container health checks. The daemon checks
`/api/health`; the web container checks `/healthz` and waits for a healthy
daemon before starting. The proxy preserves same-origin `/api` requests and
WebSocket upgrades at `/ws`, with a one-day idle timeout for live connections.

`replicantd` writes each tracing event to both stdout/stderr and a persistent
application log at `/var/lib/replicant/logs/replicantd.log`. Because
`/var/lib/replicant` is the same bind-mounted data root as the SQLite
databases, the host can read the file directly at
`${REPLICANT_DATA_DIR:-$HOME/.local/share/replicant}/logs/replicantd.log`
without using Docker commands. Set `REPLICANT_LOG_DIR` to override the daemon
log directory for native/custom deployments.

Compose also keeps stdout/stderr through Docker's `local` logging driver with
10 MiB / 3-file rotation, so `docker compose logs` remains useful without
becoming the only copy of runtime diagnostics. `RUST_LOG` controls both sinks.
The supplied default keeps normal application events at `info`, enables
Director decision tracing at `debug`, and suppresses noisy raw-HTTP internals
to `warn`. HTTP request timing is emitted by `replicant_server` at `debug`
(with failures promoted to `warn`/`error`). For deeper investigation,
temporarily use the debug profile shown in `.env.example`.

## Secrets

The simple setup reads `RS_API_TOKEN` from the ignored `.env` file. For a
mounted secret instead, keep the token in a host file and run:

```sh
RS_API_TOKEN_FILE_HOST=/absolute/path/to/rs_api_token \
  docker compose -f compose.yaml -f compose.secret.yaml up -d
```

The overlay mounts the file read-only at `/run/secrets/rs_api_token` and sets
`RS_API_TOKEN_FILE`. A non-empty `RS_API_TOKEN` normally takes precedence over
`RS_API_TOKEN_FILE`; the overlay clears it so the file is used. Credentials
are never copied into either image or sent to the browser.

## Durable state and persistence check

The host directory `${HOME}/.local/share/replicant` is mounted at
`/var/lib/replicant` and holds:

- `replicant-client.sqlite` plus SQLite WAL files: managed current projections,
  applied event cursor, and managed operations;
- `replicant-history.sqlite` plus SQLite WAL files: long-lived account events and
  raw AMI telemetry (30-day raw digest retention);
- `replicant-runtime.sqlite` plus SQLite WAL files: workflows, checkpoints,
  claims, triggers, schedules, and activity;
- `logs/replicantd.log`: persistent structured daemon/runtime/workflow logs.

Override the host path with `REPLICANT_DATA_DIR`. It must exist and be writable
by `REPLICANT_UID:REPLICANT_GID`; these default to `1000:1000`. Prove the
directory is reused by two newly created containers without a live account:

```sh
make docker-persistence-smoke
```

The first one-shot container writes `/var/lib/replicant/.persistence-smoke`;
the second is a fresh container and verifies the marker. Because this is a bind
mount, `docker compose down` does not remove the host data directory.

The current profile and application configuration are environment-based. The
ignored host `.env` file therefore survives container recreation separately
from the data directory. There are no other durable application paths today.

Deployments created before the host-directory default used the Docker volume
`replicant-data`. Migrate it while the daemon is stopped:

```sh
docker compose down
mkdir -p "$HOME/.local/share/replicant"
docker run --rm \
  -v replicant-data:/from:ro \
  -v "$HOME/.local/share/replicant:/to" \
  alpine:3.22 \
  sh -c "cp -a /from/. /to/ && chown -R $(id -u):$(id -g) /to"
docker compose up -d
```

Keep the old volume until the migrated workflows and managed state have been
verified.

## Backup and restore

Stop the daemon for a consistent backup, then archive the host data directory:

```sh
docker compose stop replicantd
mkdir -p backups
tar -C "${REPLICANT_DATA_DIR:-$HOME/.local/share/replicant}" \
  -czf backups/replicant-data.tgz .
docker compose start replicantd
```

Keep all three SQLite databases in the same dated backup. Restore into a new
directory so the failed data remains recoverable:

```sh
docker compose down
mkdir -p "$HOME/.local/share/replicant-restored"
tar -C "$HOME/.local/share/replicant-restored" \
  -xzf backups/replicant-data.tgz
REPLICANT_DATA_DIR="$HOME/.local/share/replicant-restored" docker compose up -d
```

After verifying the restored stack, put the selected directory in `.env`.
Never delete the old directory until the restored workflows and managed state
have reconciled successfully.

## Upgrade and rollback

For a safe upgrade, take a backup, pull the desired commit or release, rebuild,
and recreate without removing the volume:

```sh
docker compose stop replicantd
# Back up as above, then update the checked-out source.
make docker-build
docker compose up -d
docker compose ps
docker compose logs -f replicantd
```

SIGTERM from Compose follows the daemon's native graceful shutdown path and
allows 30 seconds for checkpointing. If an image upgrade fails, stop the stack,
return to the previously working source/image, and start it against the same
directory. If a migration prevents rollback, preserve the failed directory and
start the old image against a restored pre-upgrade directory instead.

## Daemon-only and explicit remote access

Run automation without the GUI while keeping the daemon private:

```sh
docker compose up -d replicantd
docker compose logs -f replicantd
```

An operator who deliberately needs CLI/API access from another host can apply
the headless overlay:

```sh
REPLICANTD_PORT=8080 \
  docker compose -f compose.yaml -f compose.headless.yaml up -d replicantd
```

The daemon API has no public-network authentication boundary. Publish it only
behind a firewall, VPN, or authenticated reverse proxy; never expose it
directly to the internet. The CLI remains a native binary and is not included
in the minimal daemon image.

## Validation and architecture

`make docker-build` now performs the release builds on the host first: Cargo
builds `target/release/replicantd`, the web build is staged under
`target/docker/web`, and the Dockerfiles only package those artifacts. The
daemon image runs `ldd` during assembly so an incompatible host-built binary
fails the build immediately instead of producing an image that cannot start.
The runtime image is pinned to Fedora 44; change that base deliberately if the
local build host/ABI changes.

`make docker-check` performs the same local artifact build, resolves Compose
configuration, and packages both production images without requiring an account. With a configured token,
`make docker-smoke` starts the stack, waits for health, checks the static web
health and proxied daemon health, and verifies a WebSocket `101` upgrade.
Ordinary `make ci` does not require Docker.

Because the daemon binary is now built on the host, build the release artifacts
for the same architecture as the target Docker engine. The runtime images use
standard Fedora/Alpine bases and contain no compiler toolchains.
