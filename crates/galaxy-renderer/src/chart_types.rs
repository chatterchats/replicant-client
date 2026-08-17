use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Vec3 {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlStar {
    pub designation: String,
    pub color: String,
    #[serde(default)]
    pub spectral_type: String,
    pub current: bool,
    pub exploration: String,
    #[serde(default)]
    pub is_hub: bool,
    #[serde(default)]
    pub is_relay: bool,
    #[serde(default)]
    pub is_megastructure: bool,
    #[serde(default)]
    pub dimmed: bool,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlSignal {
    pub key: String,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TravelRouteLeg {
    pub leg: i32,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub time_seconds: f32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlLink {
    pub from: Vec3,
    pub to: Vec3,
    #[serde(default)]
    pub explored: bool,
    #[serde(default)]
    pub secondary: bool,
    #[serde(default)]
    pub relay: bool,
    #[serde(default)]
    pub relay_coverage_gap: bool,
    #[serde(default)]
    pub travel_route: bool,
    #[serde(default)]
    pub exploration_route: bool,
    #[serde(default)]
    pub travel_started_at: Option<String>,
    #[serde(default)]
    pub travel_ends_at: Option<String>,
    #[serde(default)]
    pub travel_route_leg_index: Option<i32>,
    #[serde(default)]
    pub travel_progress: Option<f32>,
    #[serde(default)]
    pub travel_route_legs: Vec<TravelRouteLeg>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlPulse {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub intensity: f32,
    pub r: f32,
    pub g: f32,
    pub b: f32,
}

pub fn parse_stars(json: &str) -> Vec<GlStar> {
    serde_json::from_str(json).unwrap_or_default()
}

pub fn parse_signals(json: &str) -> Vec<GlSignal> {
    serde_json::from_str(json).unwrap_or_default()
}

pub fn parse_links(json: &str) -> Vec<GlLink> {
    serde_json::from_str(json).unwrap_or_default()
}

pub fn parse_pulses(json: &str) -> Vec<GlPulse> {
    serde_json::from_str(json).unwrap_or_default()
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlInfluenceCenter {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlColoredSphereCenter {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    #[serde(default = "default_influence_red")]
    pub r: f32,
    #[serde(default = "default_influence_green")]
    pub g: f32,
    #[serde(default = "default_influence_blue")]
    pub b: f32,
}

fn default_influence_red() -> f32 {
    0.28
}

fn default_influence_green() -> f32 {
    0.65
}

fn default_influence_blue() -> f32 {
    1.0
}

pub fn parse_influence_centers(json: &str) -> Vec<GlInfluenceCenter> {
    serde_json::from_str(json).unwrap_or_default()
}

pub fn parse_colored_sphere_centers(json: &str) -> Vec<GlColoredSphereCenter> {
    serde_json::from_str(json).unwrap_or_default()
}
