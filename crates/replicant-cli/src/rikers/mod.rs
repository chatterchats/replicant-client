//! Produces an explainable, local-only Riker colony shortlist.
//!
//! The score is a planning heuristic, not a prediction of an event's hidden
//! server score. It prints suggestions and never sends a BobNet message.

use std::{
    cmp::Ordering,
    env,
    io::{self, ErrorKind},
};

use replicant_client::{Client, Knowledge, LifeStage, Location, Realm, SecretString};

const PREFERRED_DISTANCE_INNER_LY: f64 = 15.0;
const PREFERRED_DISTANCE_OUTER_LY: f64 = 35.0;
const KNOWN_REGION_EDGE_LY: f64 = 50.0;

#[derive(Debug)]
struct Candidate {
    designation: String,
    heuristic_score: f64,
    distance_fit_score: f64,
    strengths: Vec<String>,
    cautions: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RotationClass {
    Regular,
    NearSynchronous,
    Slow,
    Synchronous,
    Severe,
    Unknown,
}

#[derive(Debug)]
struct Config {
    database: String,
    limit: usize,
    diagnostics: bool,
}

impl Config {
    fn from_args_and_env(
        arguments: impl IntoIterator<Item = String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut database =
            env::var("REPLICANT_DB").unwrap_or_else(|_| "replicant-client.sqlite".to_owned());
        let mut limit = env::var("RS_RIKERS_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(10usize);
        let mut diagnostics = true;
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--database" => database = required_argument(&mut arguments, "--database")?,
                "--limit" => {
                    limit = required_argument(&mut arguments, "--limit")?
                        .parse()
                        .map_err(|_| {
                            io::Error::new(ErrorKind::InvalidInput, "--limit must be an integer")
                        })?;
                }
                "--no-diagnostics" => diagnostics = false,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidInput,
                        format!("unexpected argument: {other}"),
                    )
                    .into());
                }
            }
        }
        if limit == 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "--limit must be greater than zero",
            )
            .into());
        }
        Ok(Self {
            database,
            limit,
            diagnostics,
        })
    }
}

fn required_argument(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync + 'static>> {
    arguments.next().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("{option} requires a value"),
        )
        .into()
    })
}

fn print_help() {
    println!(
        "Riker colony candidates\n\n\
Usage:\n  replicant-cli rikers [OPTIONS]\n\n\
Options:\n  --database PATH     Managed SQLite database\n  --limit N           Maximum candidates to print (default: 10)\n  --no-diagnostics    Hide staged local-query counts\n  -h, --help          Show this help\n\n\
This command synchronizes known survey data, scores candidates locally, and\n\
prints suggestions. It never sends a BobNet message or performs gameplay\n\
mutations."
    );
}

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let config = Config::from_args_and_env(arguments)?;
    let token = env::var("RS_API_TOKEN").map_err(|_| {
        io::Error::new(
            ErrorKind::NotFound,
            "RS_API_TOKEN is required; export it before running this command",
        )
    })?;

    eprintln!("database: {}", config.database);

    let client = Client::builder()
        .authentication_token(SecretString::from(token))
        .sqlite(&config.database)
        .start()
        .await?;

    // This is the only remote step. Every query and score below reads the
    // committed local snapshot produced by full synchronization.
    let sync = client.sync().full().await?;
    eprintln!("full sync readiness: {:?}", sync.readiness);

    if config.diagnostics {
        print_location_pipeline_diagnostics(&client).await?;
    }

    let worlds = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .surveyed()
        .breathable_atmosphere()
        .without_advanced_civilisation()
        .life_stage_below(LifeStage::Intelligent)
        //.gravity_g_between(0.6..=1.4)
        //.surface_temp_c_between(8.0..=25.0)
        .collect()
        .await?;

    eprintln!(
        "final hard-filter query matched {} candidate world(s)",
        worlds.len()
    );

    if worlds.is_empty() {
        eprintln!();
        eprintln!("No locally persisted worlds satisfy every hard filter.");
        eprintln!(
            "Use the staged counts above to identify the first predicate that reduces the pool to zero."
        );
        eprintln!(
            "A zero count commonly means the relevant survey value is still unknown in the local database,"
        );
        eprintln!(
            "not necessarily that every explored world definitively failed that requirement."
        );

        client.close().await?;
        return Ok(());
    }

    let mut candidates = worlds.iter().map(assess).collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .heuristic_score
            .partial_cmp(&left.heuristic_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                right
                    .distance_fit_score
                    .partial_cmp(&left.distance_fit_score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.designation.cmp(&right.designation))
    });

    for candidate in candidates.into_iter().take(config.limit) {
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

async fn print_location_pipeline_diagnostics(
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!();
    eprintln!("local location-query diagnostics:");

    let live_locations = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .collect()
        .await?;
    print_stage("all persisted live locations", live_locations.len());

    let planetary_bodies = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .collect()
        .await?;
    print_stage("planetary bodies", planetary_bodies.len());

    let surveyed = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .surveyed()
        .collect()
        .await?;
    print_stage("surveyed planetary bodies", surveyed.len());

    let breathable = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .surveyed()
        .breathable_atmosphere()
        .collect()
        .await?;
    print_stage("with breathable atmosphere", breathable.len());

    let no_advanced_civilisation = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .surveyed()
        .breathable_atmosphere()
        .without_advanced_civilisation()
        .collect()
        .await?;
    print_stage(
        "without an advanced civilisation",
        no_advanced_civilisation.len(),
    );

    let below_intelligent_life = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .surveyed()
        .breathable_atmosphere()
        .without_advanced_civilisation()
        .life_stage_below(LifeStage::Intelligent)
        .collect()
        .await?;
    print_stage(
        "with life below intelligent stage",
        below_intelligent_life.len(),
    );

    let acceptable_gravity = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .surveyed()
        .breathable_atmosphere()
        .without_advanced_civilisation()
        .life_stage_below(LifeStage::Intelligent)
        .gravity_g_between(0.8..=1.3)
        .collect()
        .await?;
    print_stage(
        "with gravity from 0.8g through 1.3g",
        acceptable_gravity.len(),
    );

    let acceptable_temperature = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .surveyed()
        .breathable_atmosphere()
        .without_advanced_civilisation()
        .life_stage_below(LifeStage::Intelligent)
        .gravity_g_between(0.8..=1.3)
        .surface_temp_c_between(10.0..=25.0)
        .collect()
        .await?;
    print_stage(
        "with surface temperature from 10 C through 25 C",
        acceptable_temperature.len(),
    );

    eprintln!();
    eprintln!(
        "These are local snapshot queries. A sharp drop generally identifies either a restrictive predicate"
    );
    eprintln!(
        "or a field that has not yet been normalized and persisted from detailed survey data."
    );

    Ok(())
}

fn print_stage(label: &str, count: usize) {
    eprintln!("  {label:<48} {count:>6}");
}

fn assess(world: &Location) -> Candidate {
    let mut score: f64 = 25.0;
    let mut distance_fit_score = 0.0;
    let mut strengths = Vec::new();
    let mut cautions = Vec::new();

    if matches!(
        world.atmosphere(),
        Knowledge::Present(atmosphere) if atmosphere.is_breathable()
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
        match classify_rotation(rotation) {
            RotationClass::Regular => {
                score += 3.0;
                strengths.push("regular day/night cycle".into());
            }
            RotationClass::NearSynchronous => {
                score -= 2.0;
                cautions.push("near-tidal locking; assess the twilight band".into());
            }
            RotationClass::Slow => {
                score -= 5.0;
                cautions.push("slow rotation may produce large day/night swings".into());
            }
            RotationClass::Synchronous => {
                score -= 8.0;
                cautions.push("tidally locked; settlement geography is constrained".into());
            }
            RotationClass::Severe => {
                score -= 11.0;
                cautions.push(
                    "extremely slow rotation; severe hemispheric thermal extremes likely".into(),
                );
            }
            RotationClass::Unknown => {}
        }
    }
    if let Knowledge::Present(spectral) = world.star_spectral_type() {
        match spectral
            .chars()
            .next()
            .map(|class| class.to_ascii_uppercase())
        {
            Some('K') => {
                score += 5.0;
                strengths.push(format!("K-class host ({spectral})"));
            }
            Some('G') => {
                score += 4.0;
                strengths.push(format!("G-class host ({spectral})"));
            }
            Some('M') => {
                cautions.push(format!(
                    "M-dwarf host ({spectral}); review light spectrum and stellar activity"
                ));
            }
            _ => {}
        }
    }
    if let Knowledge::Present(richness) = world.nearby_belt_richness() {
        let belt_bonus = belt_resource_bonus(richness);
        score += belt_bonus;
        match belt_bonus {
            bonus if bonus >= 7.0 => {
                strengths.push(format!("major nearby resource belt ({richness})"));
            }
            bonus if bonus >= 5.0 => {
                strengths.push(format!("resource-rich nearby belt ({richness})"));
            }
            bonus if bonus > 0.0 => {
                strengths.push(format!("usable nearby belt ({richness})"));
            }
            _ => {}
        }
    }
    if matches!(world.life_stage(), Knowledge::Present(LifeStage::Complex)) {
        score -= 4.0;
        cautions.push("complex ecosystem requires ethical review".into());
    }
    if let Knowledge::Present(distance) = world.distance_from_sol_ly() {
        distance_fit_score = distance_bonus(*distance);
        score += distance_fit_score;
        if (PREFERRED_DISTANCE_INNER_LY..=PREFERRED_DISTANCE_OUTER_LY).contains(distance) {
            strengths.push(format!(
                "{distance:.1} ly from SOL; balanced strategic separation and connectivity"
            ));
        } else if *distance < 10.0 {
            cautions.push(format!(
                "only {distance:.1} ly from SOL; limited strategic separation"
            ));
        } else if *distance > 40.0 {
            cautions.push(format!(
                "{distance:.1} ly from SOL; long-range communications and logistics burden"
            ));
        }
    }
    Candidate {
        designation: world.id().as_str().into(),
        heuristic_score: score,
        distance_fit_score,
        strengths,
        cautions,
    }
}

fn classify_rotation(value: &str) -> RotationClass {
    let normalized = normalize_label(value);

    if normalized.contains("barely")
        || normalized.contains("almost_stopped")
        || normalized.contains("extremely_slow")
        || normalized.contains("very_slow")
    {
        RotationClass::Severe
    } else if normalized.contains("near_synchronous")
        || normalized.contains("near_tidally_locked")
        || normalized.contains("near_tidal")
    {
        RotationClass::NearSynchronous
    } else if normalized == "synchronous"
        || normalized.contains("tidally_locked")
        || normalized.contains("tidal_locked")
    {
        RotationClass::Synchronous
    } else if normalized.contains("slow") {
        RotationClass::Slow
    } else if normalized.contains("regular") || normalized.contains("normal") {
        RotationClass::Regular
    } else {
        RotationClass::Unknown
    }
}

fn belt_resource_bonus(value: &str) -> f64 {
    let normalized = normalize_label(value);

    if normalized.contains("rich") || normalized.contains("heavy") {
        8.0
    } else if normalized.contains("dense") || normalized.contains("high") {
        6.0
    } else if normalized.contains("moderate") {
        3.0
    } else {
        0.0
    }
}

fn distance_bonus(distance_ly: f64) -> f64 {
    if !distance_ly.is_finite() || distance_ly < 0.0 {
        return 0.0;
    }

    if distance_ly < PREFERRED_DISTANCE_INNER_LY {
        6.0 * (distance_ly / PREFERRED_DISTANCE_INNER_LY).clamp(0.0, 1.0)
    } else if distance_ly <= PREFERRED_DISTANCE_OUTER_LY {
        6.0
    } else {
        6.0 * ((KNOWN_REGION_EDGE_LY - distance_ly)
            / (KNOWN_REGION_EDGE_LY - PREFERRED_DISTANCE_OUTER_LY))
            .clamp(0.0, 1.0)
    }
}

fn normalize_label(value: &str) -> String {
    value
        .trim()
        .chars()
        .map(|character| match character {
            ' ' | '-' => '_',
            other => other.to_ascii_lowercase(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_prefers_a_balanced_middle_band() {
        assert!(distance_bonus(20.0) > distance_bonus(5.0));
        assert!(distance_bonus(30.0) > distance_bonus(45.0));
        assert!((distance_bonus(15.0) - distance_bonus(35.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn heavy_or_rich_belts_receive_the_strongest_bonus() {
        assert!(belt_resource_bonus("heavy") > belt_resource_bonus("moderate"));
        assert!(belt_resource_bonus("rich") > belt_resource_bonus("high"));
    }

    #[test]
    fn severe_slow_rotation_is_worse_than_near_tidal_locking() {
        assert_eq!(
            classify_rotation("near-synchronous"),
            RotationClass::NearSynchronous
        );
        assert_eq!(classify_rotation("barely turns"), RotationClass::Severe);
    }
}
