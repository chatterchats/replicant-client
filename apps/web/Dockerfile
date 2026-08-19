# syntax=docker/dockerfile:1.7

# `make docker-build` generates the complete static site locally and stages it
# under target/docker/web. Docker only packages the verified build output.
FROM nginxinc/nginx-unprivileged:1.28-alpine
COPY apps/web/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY target/docker/web /usr/share/nginx/html
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1
