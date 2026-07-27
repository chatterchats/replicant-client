# Riker Colony Survey Candidate Example

The bundled `reference/examples/rikers_colony_candidates.rs` is a required target example for Phase 11.6.01b.

It uses two stages:

1. **Hard eligibility query** using Riker's published requirements.
2. **Explainable local ranking** using clues from his first two BobNet assessments.

The ranking is deliberately labeled a heuristic. It does not claim to reproduce the server's hidden scoring formula.

## Hard eligibility policy

The initial local query requires:

- live-realm planet or moon;
- complete survey/environment and life knowledge;
- breathable atmosphere;
- gravity from `0.8g` through `1.3g`, inclusive;
- mean surface temperature from `10°C` through `25°C`, inclusive;
- no intelligent or spacefaring civilisation;
- known life stage below intelligent, including complex ecosystems.

Complex life is not rejected. Riker awarded 83 points to a world with an established ecosystem, while flagging ethical concerns.

## Weighted ranking clues

The example then reads each candidate's local committed snapshot and assigns transparent heuristic weights for:

- closeness to exactly `1.0g`;
- closeness to a comfortable central temperature;
- magnetic-field protection;
- habitable-zone membership;
- low axial tilt;
- rotation/tidal-locking state;
- host-star spectral class;
- nearby asteroid-belt richness;
- detected life stage and ethical burden;
- distance from SOL.

The observed feedback implies:

- K-class stars are a bonus;
- M dwarfs remain eligible;
- near tidal locking is a caution, not a hard exclusion;
- rich belts improve industrial viability;
- complex ecosystems incur an ethical penalty rather than rejection.

See `reference/riker-assessment-clues-2026-07-26.md`.

## Safety and behavior

- `client.sync().full()` is the only intended remote synchronization step.
- Candidate query, snapshot reads, scoring, and sorting are local-only.
- Unknown values do not pass hard predicates.
- Unknown bonus values contribute neither a bonus nor a penalty.
- The example prints suggestions but never sends BobNet messages.
- Results are sorted by heuristic score, then distance from SOL, and capped at ten.
- The printed score is explicitly a local heuristic, not Riker's expected score.

## Required API exercised

```rust
let handles = client
    .locations()
    .find()
    .in_realm(Realm::Live)
    .planetary_bodies()
    .surveyed()
    .atmosphere_is(Atmosphere::Breathable)
    .without_advanced_civilisation()
    .life_stage_below(LifeStage::Intelligent)
    .gravity_g_between(0.8..=1.3)
    .surface_temp_c_between(10.0..=25.0)
    .collect()
    .await?;

for handle in handles {
    let location = handle.snapshot().await?;
    // Score typed, local-only location facts.
}
```

The final API must expose equivalent typed local access to verified facts for:

- atmosphere;
- gravity;
- surface temperature;
- magnetic field;
- habitable-zone membership;
- life knowledge;
- distance from SOL;
- and, when verified by current response fixtures, axial tilt, rotation/tidal-lock state, host-star spectral type, and nearby-belt abundance.

If the final normalized model uses different equally ergonomic names, update the bundled example and explain the mapping in the evidence report.
