# Scan report projection

`scan.completed` and `ami.survey.digest.report.scans[]` use one pure domain
adapter. It accepts only the documented planet and moon report forms whose
type-keyed body has a matching `designation`; it derives the existing normalized
location knowledge and retains the complete report as sanitized source evidence
in `Location::unknown.event_scan_report`. Other report shapes remain forward
compatible and are reconciled instead of guessed.

The observation is `EventLog` / `EventDelta`, keyed by the event realm, and is
merged without tombstones. The event-journal insert, valid location upserts,
fallback location work, and applied cursor are one SQLite transaction. The
ordered event lane publishes exactly one revision only after that commit. A
duplicate event ID loses the journal insert and produces neither a revision nor
work.

Each scan entry is handled independently. Invalid entries leave their raw event
journaled, retain valid siblings, emit a diagnostic, and enqueue only a known
target for durable REST reconciliation. Fully projectable reports enqueue no
targeted HTTP refresh. The only remaining crash boundary is after commit and
before in-memory publication; restart restores the committed event and location
state, so no reconciliation intent or observation is lost.
