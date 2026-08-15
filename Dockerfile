# syntax=docker/dockerfile:1.7

FROM rust:1.94-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p replicant-server --bin replicantd

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 replicant \
    && install -d -o replicant -g replicant /var/lib/replicant
COPY --from=builder /src/target/release/replicantd /usr/local/bin/replicantd
USER replicant
ENV REPLICANT_DB=/var/lib/replicant/replicant-client.sqlite \
    REPLICANT_RUNTIME_DB=/var/lib/replicant/replicant-runtime.sqlite \
    REPLICANTD_BIND=127.0.0.1:8080 \
    RUST_LOG=info
VOLUME ["/var/lib/replicant"]
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/api/health >/dev/null || exit 1
ENTRYPOINT ["replicantd"]
