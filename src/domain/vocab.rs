use serde::{Deserialize, Deserializer, Serialize, Serializer};

macro_rules! open_value {
    ($name:ident { $($known:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub enum $name { $($known,)+ Unknown(String) }
        impl $name {
            pub fn as_str(&self) -> &str { match self { $(Self::$known => $wire,)+ Self::Unknown(value) => value } }
        }
        impl From<String> for $name {
            fn from(value: String) -> Self { match value.as_str() { $($wire => Self::$known,)+ _ => Self::Unknown(value) } }
        }
        impl From<&str> for $name { fn from(value: &str) -> Self { Self::from(value.to_owned()) } }
        impl Serialize for $name { fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> { serializer.serialize_str(self.as_str()) } }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> { Ok(Self::from(String::deserialize(deserializer)?)) }
        }
    };
}

open_value!(EventName {
    BobnetNew => "bobnet.new",
    DeviceCompacted => "device.compacted", DeviceCompacting => "device.compacting",
    DeviceDecommissioned => "device.decommissioned",
    DeviceUnfurled => "device.unfurled", DeviceUnfurling => "device.unfurling",
    PrintCompleted => "print.completed", PrintStarted => "print.started",
    SimulationCompleted => "simulation.completed", TradeCompleted => "trade.completed",
    TriangulationComplete => "triangulation.complete",
    TriangulationFailed => "triangulation.failed",
    TriangulationStarted => "triangulation.started",
    WardActivated => "ward.activated", WardDeactivated => "ward.deactivated"
});
open_value!(DeviceCommand {
    Activate => "activate", Deactivate => "deactivate", Deploy => "deploy", Stow => "stow",
    Attach => "attach", Compact => "compact", Triangulate => "triangulate", Unfurl => "unfurl"
});
open_value!(DeviceFeature {
    Mining => "mining", Printing => "printing", Scanning => "scanning", Travel => "travel"
});
open_value!(DeviceStatus { Active => "active", Deactivated => "deactivated", Idle => "idle", Offline => "offline" });
// AMI controller wire values are confirmed against
// `reference/replicant-space-2-5-1/api/replicants/events/index.md` (`ami_mining_controller`);
// the survey/transport/fleet siblings follow the same `ami_<kind>_controller`
// naming convention documented in `reference/replicant-space-2-5-1/ami/index.md`.
open_value!(DeviceType {
    MiningController => "ami_mining_controller",
    SurveyController => "ami_survey_controller",
    TradeController => "ami_trade_controller",
    TransportController => "ami_transport_controller",
    AtmoProcessor => "atmo_processor",
    Autofactory => "autofactory",
    CargoFreighter => "cargo_freighter",
    CargoLifter => "cargo_lifter",
    CargoVessel => "cargo_vessel",
    CasimirArray => "casimir_array",
    CommSatellite => "comm_satellite",
    ComputeCore => "compute_core",
    DefenceGrid => "defence_grid",
    ElectrodynamicTether => "electrodynamic_tether",
    EmptyReplicantMatrix => "empty_replicant_matrix",
    ExoticMatterInjector => "exotic_matter_injector",
    ExoticParticleTrap => "exotic_particle_trap",
    FiltrationArray => "filtration_array",
    FleetTender => "fleet_tender",
    FtlBeacon => "ftl_beacon",
    FtlRelay => "ftl_relay",
    FusionBarge => "fusion_barge",
    GalacticObservatory => "galactic_observatory",
    GravityLens => "gravity_lens",
    HabModule => "hab_module",
    HeavenVessel => "heaven_vessel",
    HullPlate => "hull_plate",
    HydroponicBay => "hydroponic_bay",
    MaintenanceDrone => "maintenance_drone",
    MassDriver => "mass_driver",
    MatrixContainer => "matrix_container",
    MeshRelay => "mesh_relay",
    MiningDrone => "mining_drone",
    MobileFleet => "mobile_fleet",
    NegativeEnergyConduit => "negative_energy_conduit",
    NutrientSynthesizer => "nutrient_synthesizer",
    OrbitalDefencePlatform => "orbital_defence_platform",
    OrbitalFarm => "orbital_farm",
    PointDefenceArray => "point_defence_array",
    PowerCellArray => "power_cell_array",
    Propulsor => "propulsor",
    RacingVessel => "racing_vessel",
    RadiationShroud => "radiation_shroud",
    SeismicMonitor => "seismic_monitor",
    SensorArray => "sensor_array",
    ShieldGenerator => "shield_generator",
    SignalBooster => "signal_booster",
    SolarCollector => "solar_collector",
    StructuralFabricator => "structural_fabricator",
    SurgeCarrier => "surge_carrier",
    SurgePlate => "surge_plate",
    SurgePlatform => "surge_platform",
    SurveyDrone => "survey_drone",
    SystemHub => "system_hub",
    ThermalLance => "thermal_lance",
    TidalCompensator => "tidal_compensator",
    TransportDrone => "transport_drone",
    TransportHauler => "transport_hauler",
    FleetController => "ami_fleet_controller",
    ReplicantInterface => "replicant_interface",
    FtlSlingshot => "ftl_slingshot",
    SystemWard => "system_ward",
});
// Directive wire values from `reference/replicant-space-2-5-1/ami/*-controller/index.md`.
open_value!(DeviceDirective {
    GatherResources => "gather_resources", GatherEvenly => "gather_evenly",
    MaintainRatios => "maintain_ratios", DepleteSmallest => "deplete_smallest",
    GatherSalvage => "gather_salvage", SurveySystem => "survey_system",
    BeltSearch => "belt_search", Delivery => "delivery", Shuttle => "shuttle",
    Ferry => "ferry", Consolidate => "consolidate"
});
open_value!(ReplicantStatus { Active => "active", Offline => "offline", Traveling => "traveling" });
open_value!(SpeciesKind { Human => "human" });
open_value!(LocationType { Planet => "planet", Moon => "moon", Belt => "belt", Station => "station" });
open_value!(Atmosphere {
    Breathable => "breathable",
    Standard => "standard",
    Thin => "thin",
    Dense => "dense",
    Crushing => "crushing",
    None => "none"
});

impl Atmosphere {
    /// Returns whether this atmosphere supports unassisted human breathing.
    ///
    /// The live API currently reports surveyed breathable worlds as
    /// `standard`; `breathable` remains accepted for compatibility with the
    /// earlier modeled vocabulary and future semantic responses.
    #[must_use]
    pub fn is_breathable(&self) -> bool {
        matches!(self, Self::Breathable | Self::Standard)
    }
}
open_value!(LifeStage {
    Prebiotic => "prebiotic", Microbial => "microbial", Complex => "complex",
    Intelligent => "intelligent", Spacefaring => "spacefaring"
});

impl LifeStage {
    /// Canonical documented life-stage rank. Future values deliberately have
    /// no order until the contract defines one.
    #[must_use]
    pub fn canonical_rank(&self) -> Option<u8> {
        match self {
            Self::Prebiotic => Some(0),
            Self::Microbial => Some(1),
            Self::Complex => Some(2),
            Self::Intelligent => Some(3),
            Self::Spacefaring => Some(4),
            Self::Unknown(_) => None,
        }
    }
}
open_value!(TradeStatus { Open => "open", Completed => "completed", Cancelled => "cancelled" });
open_value!(EventCategory { Account => "account", Device => "device", Replicant => "replicant" });

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_blueprint_device_type_is_known() {
        let document: serde_json::Value =
            serde_json::from_str(include_str!("../../blueprints.json")).expect("blueprints JSON");
        for blueprint in document["blueprints"].as_array().expect("blueprints array") {
            let wire = blueprint["device_type"]
                .as_str()
                .expect("blueprint device_type");
            let device_type = DeviceType::from(wire);
            assert!(
                !matches!(device_type, DeviceType::Unknown(_)),
                "unknown blueprint device type: {wire}"
            );
            assert_eq!(device_type.as_str(), wire);
        }
    }
}
