use serde::{Deserialize, Deserializer, Serialize, Serializer};

macro_rules! open_value {
    ($name:ident { $($known:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Debug, Eq, Hash, PartialEq)]
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
    TriangulationStarted => "triangulation.started"
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
// `reference/replicant-space/api/replicants/events/index.md` (`ami_mining_controller`);
// the survey/transport/fleet siblings follow the same `ami_<kind>_controller`
// naming convention documented in `reference/replicant-space/ami/index.md`.
open_value!(DeviceType {
    MiningDrone => "mining_drone",
    MiningController => "ami_mining_controller",
    SurveyController => "ami_survey_controller",
    TransportController => "ami_transport_controller",
    FleetController => "ami_fleet_controller",
    ReplicantInterface => "replicant_interface",
    FtlRelay => "ftl_relay"
});
// Directive wire values from `reference/replicant-space/ami/*-controller/index.md`.
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
