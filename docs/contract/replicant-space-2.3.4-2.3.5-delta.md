# Replicant Space 2.3.4–2.3.5 contract delta

This record preserves the rendered-document changes newer than the checked-in
verified 2.3.3 OpenAPI corpus. The supplied 2.3.5 archive contains rendered
documentation but no OpenAPI document, so it must not be presented as a new
machine-readable baseline.

## 2.3.4 — 2026-07-30

- Fixed retrograde scoring for Star Snooker. This is server behavior and has
  no client wire-shape change.

## 2.3.5 — 2026-08-02

- Autofactory `enqueue_print` accepts optional `flatpack: true` for devices
  whose blueprint includes the open `modular` feature. Flatpacked output is
  compacted for transport and prints slightly faster.
- `print.started` includes the expected completion timestamp in
  `completes_at`.
- `trade.completed` includes role-specific outcomes: buyers receive
  `rewards_received`, while sellers receive `criteria_received` and
  `remaining_stock`. Both outcome objects can include `resources` and device
  codes.
- Planet and moon response objects include `scanned` for both true and false
  cases.
- `ami.mining.digest` has a new report shape for the `gather_salvage`
  directive. The supplied rendered page does not define that nested shape, so
  the client preserves it through an open `extra` map rather than inventing a
  schema.

The remaining announcements are server-side or behavior-only: replication
targets now retain the matrix feature, belt-search assignment was corrected,
shop announcement messages allow 500 characters, event stream history was
increased to 100,000 entries, and long-term event history foundations were
added. They require no new route or typed request field in this client.

## Client implementation

- Raw autofactory commands serialize the optional `flatpack` flag.
- Managed print operations expose `AutofactoryPrintOptions::flatpacked()`.
- The reusable printing scheduler and CLI expose flatpacked mode and reject
  non-modular blueprint requests before submission.
- Typed helpers decode the new print, trade, and AMI mining event fields while
  preserving future fields.
- Managed trade reconciliation schedules every device named by either new
  role-specific outcome as well as the legacy `new_device_codes` field.
- Raw planet and moon detail preserves `scanned` independently of the
  location-level flag.

## Evidence

The supplied Replicant Space 2.3.5 rendered-document archive has SHA-256
`1b8e96d9f94cd1e3e8fb5a56bc7451f8e53a2bb99bceca1d4e6ab1228aafe7a9`
and was generated at `2026-08-03T00:42:38.460301+00:00`. Relevant pages are:

- `autofactories/index.md`
- `api/events/catalogue/index.md`
- `api/events/ami-digests/index.md`
- the archive changelog data

No new operations were introduced by these rendered corrections;
`policy/documented-operation-deltas.json` therefore remains empty.
