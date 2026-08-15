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
docker compose build
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

Logs go to stdout/stderr and are available through `docker compose logs`.
Docker logging-driver retention should be configured at the host level if
persistent logs are wanted; no application log is written to the container
layer.

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

The named volume `replicant-data` is mounted at `/var/lib/replicant` and holds:

- `replicant-client.sqlite` plus SQLite WAL files: managed projections, event
  history, and managed operations;
- `replicant-runtime.sqlite` plus SQLite WAL files: workflows, checkpoints,
  claims, triggers, schedules, and activity;
- future intentionally persistent profile configuration or file logs placed
  below the same data root.

Override the volume name with `REPLICANT_DATA_VOLUME`. Prove the volume is
reused by two newly created containers without a live account:

```sh
make docker-persistence-smoke
```

The first one-shot container writes `/var/lib/replicant/.persistence-smoke`;
the second is a fresh container and verifies the marker. Do not use
`docker compose down -v`: that command destroys the databases.

The current profile and application configuration are environment-based. The
ignored host `.env` file therefore survives container recreation separately
from the named volume. There are no other durable application paths today.

## Backup and restore

Stop the daemon for a consistent backup, then archive the whole data volume:

```sh
docker compose stop replicantd
mkdir -p backups
docker run --rm \
  -v replicant-data:/data:ro \
  -v "$PWD/backups:/backup" \
  alpine:3.22 \
  tar -C /data -czf /backup/replicant-data.tgz .
docker compose start replicantd
```

Keep both SQLite databases in the same dated backup. Restore into a new volume
so the failed data remains recoverable:

```sh
docker compose down
docker volume create replicant-data-restored
docker run --rm \
  -v replicant-data-restored:/data \
  -v "$PWD/backups:/backup:ro" \
  alpine:3.22 \
  tar -C /data -xzf /backup/replicant-data.tgz
REPLICANT_DATA_VOLUME=replicant-data-restored docker compose up -d
```

After verifying the restored stack, put the selected volume name in `.env`.
Never delete the old volume until the restored workflows and managed state
have reconciled successfully.

## Upgrade and rollback

For a safe upgrade, take a backup, pull the desired commit or release, rebuild,
and recreate without removing the volume:

```sh
docker compose stop replicantd
# Back up as above, then update the checked-out source.
docker compose build --pull
docker compose up -d
docker compose ps
docker compose logs -f replicantd
```

SIGTERM from Compose follows the daemon's native graceful shutdown path and
allows 30 seconds for checkpointing. If an image upgrade fails, stop the stack,
return to the previously working source/image, and start it against the same
volume. If a migration prevents rollback, preserve the failed volume and start
the old image against a restored pre-upgrade volume instead.

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

`make docker-check` resolves Compose configuration and builds both production
images without requiring an account. With a configured token,
`make docker-smoke` starts the stack, waits for health, checks the static web
health and proxied daemon health, and verifies a WebSocket `101` upgrade.
Ordinary `make ci` does not require Docker.

The Dockerfiles use standard Debian/Alpine multi-stage images and make no
architecture-specific downloads, keeping them suitable for BuildKit/buildx on
`linux/amd64` and `linux/arm64` where the pinned upstream images are available.
