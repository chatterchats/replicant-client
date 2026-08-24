# SQLite migrations

Schema for the managed client's databases. These files are **compiled into the
binary** with `include_str!` from `src/managed/store.rs` — they are not read
from disk at runtime. Adding a file to this directory does nothing until it is
also wired into the store.

## Two databases

| Directory | Database | Default path |
| --- | --- | --- |
| `migrations/` | Operational managed state: account binding, normalized projections, simulation realms, the applied event cursor, reconciliation work, durable operation outcomes. | `~/.local/share/replicant/replicant-client.sqlite` |
| `migrations/history/` | Long-lived account event history, split out of the operational database in migration 0004. | sibling `replicant-history.sqlite` |

The two sequences are numbered independently. `history/0001_initial.sql` is not
a re-run of `0001_initial.sql`.

Telemetry lives in a third database (`replicant-telemetry.sqlite`) whose schema
is owned by the runtime, not by this directory.

## Current sequence

| File | Effect |
| --- | --- |
| `0001_initial.sql` | Fresh schema version 1. Intentionally standalone — a new database is created from this file alone, not by replaying later migrations. |
| `0002_device_relationship_semantics.sql` | Renames the version-1 `hosted_by` owner/operator relationship. |
| `0003_reconciliation_leader.sql` | Reconciliation leader election state. |
| `0004_history_split.sql` | Moves event history to the sibling history database. Completed by the Rust migrator, not by SQL alone — the file cannot do the cross-database move by itself. |
| `0005_message_metadata.sql` | Inbox cursor, unread count, and refresh time for the managed `messages` table. |
| `history/0001_initial.sql` | Fresh history-database schema. |
| `history/0002_indexes.sql` | History query indexes. |

## Adding a migration

1. Write `000N_description.sql`.
2. Add a matching `include_str!` constant in `src/managed/store.rs` and apply it
   in the migrator.
3. **Update `0001_initial.sql`** so a fresh database lands in the same shape as
   a migrated one. The two paths must converge.
4. **Update `policy/persistence-schema.json`** — `scripts/schema_policy_check.py`
   compares it against `0001_initial.sql` and will fail the build otherwise.
5. Add or extend a test covering the migrated shape.

Step 3 is the one that gets missed. Fresh-install and upgrade paths diverging
is the failure mode this layout is designed to catch.

## Reserved tables

Some tables in `0001_initial.sql` are intentionally reserved rather than
partially implemented — notably the generic provenance scaffold. `source_documents`
cannot be dropped safely while populated tables still carry legacy foreign-key
columns referencing it, so that cleanup is deferred until those columns have a
tested forward migration. See `ARCHITECTURE.md` for the reasoning.
