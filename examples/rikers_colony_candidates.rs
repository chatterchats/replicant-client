//! Produces an explainable, local-only Riker colony shortlist.
//!
//! The score is a planning heuristic, not a prediction of an event's hidden
//! server score. It prints suggestions and never sends a BobNet message.

use std::{cmp::Ordering, env};

use replicant_client::{Atmosphere, Client, Knowledge, LifeStage, Location, Realm, SecretString};

#[derive(Debug)]
struct Candidate {
    designation: String,
    heuristic_score: f64,
    distance_from_sol_ly: Option<f64>,
    strengths: Vec<String>,
    cautions: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .authentication_token(SecretString::from(env::var("REPLICANT_TOKEN")?))
        .sqlite("replicant-client.sqlite")
        .start()
        .await?;

    // This is the only remote step. Every query and score below reads the
    // committed local snapshot produced by full synchronization.
    let sync = client.sync().full().await?;
    eprintln!("full sync readiness: {:?}", sync.readiness);

    let worlds = client
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

    let mut candidates = worlds.iter().map(assess).collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .heuristic_score
            .partial_cmp(&left.heuristic_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                right
                    .distance_from_sol_ly
                    .partial_cmp(&left.distance_from_sol_ly)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.designation.cmp(&right.designation))
    });

    for candidate in candidates.into_iter().take(10) {
        println!("Riker, how about {}?", candidate.designation);
        println!(
            "  heuristic score: {:.1} (local planning heuristic)",
            candidate.heuristic_score
        );
        if !candidate.strengths.is_empty() {
            println!("  strengths: {}", candidate.strengths.join("; "));
        }
        if !candidate.cautions.is_empty() {
            println!("  cautions: {}", candidate.cautions.join("; "));
        }
    }
    client.close().await?;
    Ok(())
}

fn assess(world: &Location) -> Candidate {
    let mut score: f64 = 25.0;
    let mut strengths = Vec::new();
    let mut cautions = Vec::new();

    if matches!(
        world.atmosphere(),
        Knowledge::Present(Atmosphere::Breathable)
    ) {
        score += 20.0;
        strengths.push("breathable atmosphere".into());
    }
    if let Knowledge::Present(gravity) = world.gravity_g() {
        score += 15.0 * (1.0 - ((gravity - 1.0).abs() / 0.30)).clamp(0.0, 1.0);
        strengths.push(format!("{gravity:.2}g gravity"));
    }
    if let Knowledge::Present(temp) = world.surface_temp_c() {
        score += 10.0 * (1.0 - ((temp - 18.0).abs() / 15.0)).clamp(0.0, 1.0);
        strengths.push(format!("{temp:.1} C mean temperature"));
    }
    if matches!(world.magnetic_field_present(), Knowledge::Present(true)) {
        score += 6.0;
        strengths.push("magnetic field".into());
    }
    if matches!(world.in_habitable_zone(), Knowledge::Present(true)) {
        score += 5.0;
        strengths.push("habitable-zone orbit".into());
    }
    if let Knowledge::Present(tilt) = world.axial_tilt_deg() {
        score += 5.0 * (1.0 - (tilt.abs() / 45.0)).clamp(0.0, 1.0);
        strengths.push(format!("{tilt:.1} degree axial tilt"));
    }
    if let Knowledge::Present(rotation) = world.rotation_state() {
        match rotation.as_str() {
            "regular" => {
                score += 3.0;
                strengths.push("regular day/night cycle".into());
            }
            "near_synchronous" => {
                score -= 4.0;
                cautions.push("near-tidal locking; review twilight-band settlement".into());
            }
            "synchronous" => {
                score -= 8.0;
                cautions.push("tidally locked".into());
            }
            _ => {}
        }
    }
    if let Knowledge::Present(spectral) = world.star_spectral_type() {
        match spectral
            .chars()
            .next()
            .map(|class| class.to_ascii_uppercase())
        {
            Some('K') => {
                score += 6.0;
                strengths.push(format!("K-class host ({spectral})"));
            }
            Some('G') => {
                score += 4.0;
                strengths.push(format!("G-class host ({spectral})"));
            }
            Some('M') => {
                score += 1.0;
                cautions.push(format!(
                    "M-dwarf host ({spectral}); review stellar activity"
                ));
            }
            _ => {}
        }
    }
    if let Knowledge::Present(richness) = world.nearby_belt_richness() {
        match richness.as_str() {
            "rich" => {
                score += 6.0;
                strengths.push("rich nearby asteroid belt".into());
            }
            "high" => {
                score += 4.0;
                strengths.push("resource-rich nearby belt".into());
            }
            "moderate" => score += 2.0,
            _ => {}
        }
    }
    if matches!(world.life_stage(), Knowledge::Present(LifeStage::Complex)) {
        score -= 5.0;
        cautions.push("complex ecosystem requires ethical review".into());
    }
    let distance_from_sol_ly = match world.distance_from_sol_ly() {
        Knowledge::Present(distance) => {
            score += (distance / 20.0).clamp(0.0, 5.0);
            Some(*distance)
        }
        _ => None,
    };
    Candidate {
        designation: world.id().as_str().into(),
        heuristic_score: score,
        distance_from_sol_ly,
        strengths,
        cautions,
    }
}
