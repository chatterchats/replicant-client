# Replicant Space 2.3.2–2.3.3 contract delta

This historical record preserves rendered-document and changelog corrections
that informed the 2.3.3 corpus. The verified 2.3.3 OpenAPI document now
contains the two colony routes; this file remains evidence for corrections
that must not be retroactively edited into an older specification.

## 2.3.2 — 2026-07-25

- Added authenticated `colony_moon` and `colony_planet` leaderboards (now
  present in the verified 2.3.3 OpenAPI corpus).
- Improved new-system scanning performance; no client wire-shape change.

## 2.3.3 — 2026-07-27

- Adopted devices emit individual events while their controller directive is
  inactive.
- `ami.survey.digest.report.scans[]` carries the full results of body scans
  completed since the previous digest.
- Survey controllers coordinate launch and recall while remaining active.
- ETA and print-time fields are emitted as integer seconds.
- `blueprint.unlocked` includes `print_time`.
- Autofactory `enqueue_print` accepts optional `quantity`.
- The event stream schema documents its `cursor` parameter.
- Star catalogue and stellar census entries include `region`; catalogue and
  census examples include `has_hub`.
- `GET /v1/devices` accepts `tag` and mutually exclusive `untagged` filters.
- Device status can include `hosting_replicant` for a vessel containing a
  replicant matrix.

## Evidence

The supplied 2.3.3 rendered-document archive has SHA-256
`e271b1a32602dd9ec80da6b9d64f392efc58a574f8da7584de0ae9205acabc73`.
Relevant pages include:

- `api/locations/star-catalogue/index.md`
- `api/replicants/stars/index.md`
- `api/devices/list/index.md`
- `api/events/catalogue/index.md`
- `api/events/ami-digests/index.md`
- `api/events/stream/index.md`
- `autofactories/index.md`

The two colony leaderboard endpoint names are established by the supplied
2.3.2 changelog announcement; the rendered archive contains no general
leaderboards page.
