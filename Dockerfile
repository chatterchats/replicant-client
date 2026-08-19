# syntax=docker/dockerfile:1.7

# `make docker-build` compiles this binary on the host first. The container
# build is intentionally packaging-only so Docker never recompiles the Rust
# workspace.
FROM fedora:44
RUN dnf install -y ca-certificates curl shadow-utils \
    && dnf clean all \
    && useradd --create-home --uid 10001 replicant \
    && install -d -o replicant -g replicant /var/lib/replicant
COPY target/release/replicantd /usr/local/bin/replicantd
# Fail image assembly immediately when a host-built binary depends on a shared
# library or libc symbol that is unavailable in the runtime image.
RUN ldd /usr/local/bin/replicantd >/dev/null
USER replicant
ENV REPLICANT_DB=/var/lib/replicant/replicant-client.sqlite \
    REPLICANT_RUNTIME_DB=/var/lib/replicant/replicant-runtime.sqlite \
    REPLICANT_LOG_DIR=/var/lib/replicant/logs \
    REPLICANTD_BIND=127.0.0.1:8080 \
    RUST_LOG=info,replicant_runtime::orchestration=debug,replicant_workflow::supervisor=info,replicant_server=info,replicant_client::raw::http=warn
VOLUME ["/var/lib/replicant"]
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/api/health >/dev/null || exit 1
ENTRYPOINT ["replicantd"]
