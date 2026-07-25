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
    BobnetNew => "bobnet.new", DeviceDecommissioned => "device.decommissioned",
    TradeCompleted => "trade.completed", SimulationCompleted => "simulation.completed"
});
open_value!(DeviceCommand {
    Activate => "activate", Deactivate => "deactivate", Deploy => "deploy", Stow => "stow",
    Attach => "attach", Compact => "compact", Unfurl => "unfurl"
});
open_value!(DeviceFeature {
    Mining => "mining", Printing => "printing", Scanning => "scanning", Travel => "travel"
});
open_value!(DeviceStatus { Active => "active", Deactivated => "deactivated", Offline => "offline" });
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
open_value!(TradeStatus { Open => "open", Completed => "completed", Cancelled => "cancelled" });
open_value!(EventCategory { Account => "account", Device => "device", Replicant => "replicant" });
