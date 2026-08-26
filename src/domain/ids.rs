use core::fmt;
use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(
            Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }
        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

string_id!(AccountId);
string_id!(DeviceId);
string_id!(ReplicantId);
string_id!(LocationId);
string_id!(StarId);
string_id!(EventId);
string_id!(OperationId);
string_id!(TradeId);
string_id!(BlueprintId);
string_id!(AchievementId);
string_id!(SpeciesId);
string_id!(ResourceSiteId);
string_id!(LocationEventId);
string_id!(IncomingObjectId);
string_id!(MessageId);

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct SimulationId(i64);

impl SimulationId {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for SimulationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Realm {
    #[default]
    Live,
    Simulation(SimulationId),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct WorldKey<I> {
    pub realm: Realm,
    pub id: I,
}

impl<I> WorldKey<I> {
    pub fn live(id: I) -> Self {
        Self {
            realm: Realm::Live,
            id,
        }
    }
    pub fn in_realm(realm: Realm, id: I) -> Self {
        Self { realm, id }
    }
}

pub type DeviceKey = WorldKey<DeviceId>;
pub type ReplicantKey = WorldKey<ReplicantId>;
pub type LocationKey = WorldKey<LocationId>;
pub type StarKey = WorldKey<StarId>;
pub type TradeKey = WorldKey<TradeId>;
pub type ResourceSiteKey = WorldKey<ResourceSiteId>;
pub type LocationEventKey = WorldKey<LocationEventId>;
pub type IncomingObjectKey = WorldKey<IncomingObjectId>;
