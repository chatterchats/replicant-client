//! Activity status shared by device and replicant responses.

use serde::{Deserialize, Serialize};

use crate::raw::JsonObject;

/// An in-progress mining operation.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct MiningInfo {
    /// Current resource availability at the mining site.
    pub availability: Option<String>,
    /// The asteroid belt being mined.
    pub belt: Option<String>,
    /// Seconds per mining cycle.
    pub cycle_time_seconds: Option<f64>,
    /// Resource density at the site.
    pub density: Option<String>,
    /// Mining cycles completed but not yet collected, when reported.
    pub pending_cycles: Option<i64>,
    /// Quantity mined but not yet collected, when reported.
    pub pending_quantity: Option<f64>,
    /// Total quantity mined so far, when reported.
    pub quantity_mined: Option<i64>,
    /// Resource type being mined.
    pub resource_type: Option<String>,
    /// When mining started, RFC3339.
    pub started_at: Option<String>,
}

/// An in-progress print job.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PrintingInfo {
    /// When the current print completes, RFC3339.
    pub completes_at: Option<String>,
    /// Device type being printed.
    pub device_type: Option<String>,
    /// Estimated seconds remaining.
    pub eta_seconds: Option<f64>,
    /// Completion percentage.
    pub progress_percent: Option<f64>,
    /// When the current print started, RFC3339.
    pub started_at: Option<String>,
    /// Tags to apply to the printed device, when reported.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// In-progress travel.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct TravelInfo {
    /// When this leg arrives, RFC3339.
    pub arrives_at: Option<String>,
    /// When travel departed, RFC3339.
    pub departed_at: Option<String>,
    /// This leg's destination.
    pub destination: Option<String>,
    /// Human-readable destination name, when reported.
    pub destination_name: Option<String>,
    /// Destination type, when reported.
    pub destination_type: Option<String>,
    /// Distance of this leg, in AU, when reported.
    pub distance_au: Option<f64>,
    /// Distance of this leg, in light-years, when reported.
    pub distance_ly: Option<f64>,
    /// Estimated seconds remaining for this leg.
    pub eta_seconds: Option<f64>,
    /// When the final destination is reached, RFC3339, when reported.
    pub final_arrives_at: Option<String>,
    /// The overall route's final destination, when reported.
    pub final_destination: Option<String>,
    /// Human-readable final destination name, when reported.
    pub final_destination_name: Option<String>,
    /// This leg's origin.
    pub origin: Option<String>,
    /// Human-readable origin name, when reported.
    pub origin_name: Option<String>,
    /// Completion percentage of this leg.
    pub progress_percent: Option<f64>,
    /// Remaining route legs, open-shaped.
    #[serde(default)]
    pub route: Vec<JsonObject>,
    /// Estimated seconds remaining for the whole route, when reported.
    pub route_eta_seconds: Option<f64>,
    /// Completion percentage of the whole route, when reported.
    pub route_progress_percent: Option<f64>,
    /// Current travel stage.
    pub stage: Option<String>,
    /// Total route distance, in light-years.
    pub total_distance_ly: Option<f64>,
    /// Total route time, in seconds.
    pub total_time_seconds: Option<f64>,
    /// Travel type, e.g. `"direct"` or `"relay"`.
    pub r#type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_status_accepts_device_and_replicant_fields() {
        let travel: TravelInfo = serde_json::from_value(serde_json::json!({
            "destination_name": "Home",
            "eta_seconds": 5,
            "route_eta_seconds": 9
        }))
        .expect("travel status");
        assert_eq!(travel.eta_seconds, Some(5.0));
        assert_eq!(travel.destination_name.as_deref(), Some("Home"));
        assert_eq!(travel.route_eta_seconds, Some(9.0));
        assert!(travel.route.is_empty());

        let mining: MiningInfo = serde_json::from_value(serde_json::json!({
            "pending_cycles": 2,
            "resource_type": "iron"
        }))
        .expect("mining status");
        assert_eq!(mining.pending_cycles, Some(2));

        let printing: PrintingInfo = serde_json::from_value(serde_json::json!({
            "device_type": "mining_drone"
        }))
        .expect("printing status");
        assert!(printing.tags.is_empty());
    }
}
