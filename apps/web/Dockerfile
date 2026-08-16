# syntax=docker/dockerfile:1.7

FROM rust:1.94-bookworm AS wasm
ARG WASM_PACK_VERSION=0.13.1
RUN cargo install wasm-pack --version "${WASM_PACK_VERSION}" --locked
WORKDIR /src
COPY crates/galaxy-renderer crates/galaxy-renderer
RUN wasm-pack build crates/galaxy-renderer \
    --target web \
    --out-dir /wasm \
    --release \
    --locked

FROM node:22-bookworm-slim AS builder
WORKDIR /src
COPY apps/web/package.json apps/web/package-lock.json ./
RUN npm ci
COPY apps/web/ ./
COPY --from=wasm /wasm ./src/wasm/galaxy_renderer
RUN npm run build:web

FROM nginxinc/nginx-unprivileged:1.28-alpine
# Rendered at container start so the daemon token is a runtime value: the
# image contains no secret and rotating one needs only a restart.
COPY apps/web/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=builder /src/dist /usr/share/nginx/html
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1
