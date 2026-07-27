# Riker Assessment Clues — 2026-07-26

This note records player-supplied BobNet assessment feedback for the Colony Survey event. It is **behavioral evidence for an example ranking heuristic**, not an API schema and not proof of the server's exact hidden scoring weights.

## Observed reports

### `creme-34` — score 83

Riker praised:

- K-class host star, described as quiet and dependable;
- breathable atmosphere;
- surface gravity close to Earth;
- a rich nearby asteroid belt with strong early-industry potential.

Riker also noted an established ecosystem and warned that it would trigger ethical review.

### `FredNerk-1` — score 85

Riker praised:

- nitrogen/oxygen breathable atmosphere;
- Earth-normal gravity;
- low axial tilt and stable seasons.

Riker noted:

- an M-dwarf host star;
- near-tidal locking;
- likely settlement concentration in the twilight band.

Despite those cautions, the world scored 85.

## Safe deductions for the candidate example

These observations support the following **ranking** behavior:

1. Breathable atmosphere and Earth-like gravity carry very large positive weight.
2. Low axial tilt is a positive climate-stability factor.
3. K-class stars are a positive signal.
4. M-dwarf status is not an automatic rejection.
5. Near tidal locking is a caution or score penalty, not a hard exclusion.
6. A rich nearby belt is a useful infrastructure bonus, but secondary to basic habitability.
7. Complex or established ecosystems remain eligible below the `Intelligent` threshold, but should receive an ethical-risk penalty.
8. No-life, prebiotic, and microbial worlds may be easier ethically, but the exact server weights remain unknown.

## What must not be inferred

Do not claim that:

- the example predicts Riker's actual score;
- any exact numeric weight is authoritative;
- all K stars are quiet;
- all M dwarfs are unsafe;
- tidal locking always lowers the server score by a fixed amount;
- complex life is disqualifying;
- the event feedback establishes JSON field names or nesting.

The implementation must verify every location field against current authoritative response fixtures or documentation before adding typed fields.
