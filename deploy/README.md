# Deployment assets

Provisioning files consumed by the Docker Compose stack. Not built or tested by
`make ci`.

```
grafana/
  dashboards/       API, Runtime & Automation, and Empire Overview dashboards
  provisioning/
    datasources/    points Grafana at the telemetry database
    dashboards/     auto-loads the dashboards above
```

These are used only by the optional `observability` Compose profile:

```sh
make observability-up      # starts replicantd + Grafana
make observability-down
```

The dashboards read `replicant-telemetry.sqlite`, which is isolated from
managed and workflow state. The Empire Overview dashboard backfills from
retained applied-event history and reconciles that projection against periodic
authoritative managed-state snapshots.

Full deployment documentation is in [`../docs/docker.md`](../docs/docker.md).
