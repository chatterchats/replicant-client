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

### `FredNerk-1` — score 85, low-tilt near-synchronous world

Riker praised:

- nitrogen/oxygen breathable atmosphere;
- Earth-normal gravity;
- low axial tilt and stable seasons.

Riker noted:

- an M-dwarf host star;
- near-tidal locking;
- likely settlement concentration in the twilight band.

Despite those cautions, the world scored 85.

### `FredNerk-1` — score 85, resource-heavy red-dwarf world

Riker praised:

- a standard atmosphere that was breathable without infrastructure;
- a balanced distance from SOL: far enough for safety but close enough for connectivity;
- a heavy nearby belt containing rare materials, silicates, and volatiles.

Riker described the red-dwarf light as an adaptation issue rather than a disqualifier. His comment about the world's name is not treated as a physical scoring factor.

### `FredNerk-1` — score 79, extremely slow-rotation world

Riker praised:

- a full breathable atmospheric envelope;
- comfortable, Earth-like mean surface temperature;
- a reasonable communications distance from SOL.

Riker warned that the planet barely rotates and would have severe thermal extremes between hemispheres. This world still remained viable, but scored lower than the otherwise similar high-scoring M-dwarf examples.

## Safe deductions for the candidate example

These observations support the following **ranking** behavior:

1. Breathable atmosphere and Earth-like gravity carry very large positive weight.
2. Low axial tilt is a positive climate-stability factor.
3. K-class stars are a positive signal.
4. M-dwarf or red-dwarf status is not an automatic rejection or necessarily a direct score penalty.
5. Near tidal locking is a modest caution, not a hard exclusion.
6. Extremely slow rotation and explicit hemispheric thermal extremes deserve a materially larger penalty than generic potential tidal locking.
7. A rich, heavy, or resource-diverse nearby belt is a strong infrastructure bonus, but remains secondary to basic habitability.
8. Complex or established ecosystems remain eligible below the `Intelligent` threshold, but should receive an ethical-risk penalty.
9. Distance from SOL appears to have a broad preferred middle band rather than a monotonic “farther is always better” rule: strategic separation must be balanced against communications and logistics.
10. No-life, prebiotic, and microbial worlds may be easier ethically, but the exact server weights remain unknown.

## What must not be inferred

Do not claim that:

- the example predicts Riker's actual score;
- any exact numeric weight or distance band is authoritative;
- all K stars are quiet;
- all M dwarfs are unsafe;
- tidal locking always lowers the server score by a fixed amount;
- complex life is disqualifying;
- phrases such as “standard atmosphere,” “full atmospheric envelope,” “heavy belt,” or “barely turns” establish exact API wire values;
- the event feedback establishes JSON field names or nesting.

The implementation must verify every location field against current authoritative response fixtures or documentation before adding typed fields.
