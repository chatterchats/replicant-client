# replicant-workflow

Durable workflow state, execution, and supervision. This crate owns the
mechanism; `replicant-runtime` supplies the workflow implementations.

## What it provides

- **Workflow instances** with persisted `checkpoint_json` as authoritative
  execution state.
- **Resource claims** so two workflows cannot select, move, or repurpose the
  same device mid-mission.
- **Waits and triggers** — a workflow parks on a condition instead of polling.
- **Parent/child relationships** — `WorkflowContext::create_child` persists
  `parent_id`, and `child_workflows` lets a restarted coordinator rediscover
  work it created before a crash.
- **Supervision** — the supervisor drives ready workflows and records activity.

## Event discipline

Upstream game events arrive through the managed client's SSE pipeline.
Workflows consume its local watcher and durable journal and **must not open
independent upstream event connections**.

Events only *wake* a workflow. The managed durable state predicate remains the
source of truth — never treat event arrival as proof that the state change
landed.

## Checkpoint authority

The workflow database is authoritative for intent-native execution state. Where
a mature legacy executor still requires a mission or plan file, the intent
workflow materializes that file into a workflow-owned temporary directory from
its checkpoint immediately before calling the executor, then writes the result
back into `checkpoint_json`. A missing durable checkpoint deletes any stale
temporary adapter file rather than trusting it after restart.

## Adding a workflow kind

Workflow kinds are registered by `replicant-runtime`, not here. Read
[`../../ARCHITECTURE.md`](../../ARCHITECTURE.md) first — the intent-native and
`compatibility` categories have different visibility in the web and Tauri
operation pickers.

## Tests

```sh
cargo test -p replicant-workflow --all-features
```
