//! Forward-compatible "known-or-unknown" string vocabularies.
//!
//! Replicant Space can add new event names, command names, and status values
//! without a contract version bump (see guide section 29). Fields drawn from
//! those open vocabularies are modeled as an enum with one variant per
//! currently documented value plus an `Unknown(String)` fallback, so a
//! server-added value deserializes safely and round-trips back out unchanged
//! rather than causing a decode failure.

macro_rules! open_vocab {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $text:expr),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $name {
            $(#[doc = concat!("The `", $text, "` wire value.")] $variant,)+
            /// A value not recognized by this build of the client. The original wire text is preserved.
            Unknown(String),
        }

        impl $name {
            /// Returns the wire representation of this value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $text,)+
                    Self::Unknown(text) => text.as_str(),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl From<String> for $name {
            fn from(text: String) -> Self {
                match text.as_str() {
                    $($text => Self::$variant,)+
                    _ => Self::Unknown(text),
                }
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.as_str().to_owned()
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Ok(Self::from(String::deserialize(deserializer)?))
            }
        }
    };
}

open_vocab! {
    /// The dotted `event` name on an account event envelope (event log or SSE).
    ///
    /// See `reference/replicant-space-2-5-2/api/events/catalogue/index.md` for the
    /// full documented catalogue. New event names are added without a
    /// contract version bump, so unrecognized names decode to `Unknown`
    /// rather than failing.
    EventName {
        AmiAdopted => "ami.adopted",
        AmiAssembled => "ami.assembled",
        AmiLaunched => "ami.launched",
        AmiReleased => "ami.released",
        AmiWithdrawn => "ami.withdrawn",
        AmiMiningDigest => "ami.mining.digest",
        AmiSurveyDigest => "ami.survey.digest",
        AmiTransportDigest => "ami.transport.digest",
        BlueprintUnlocked => "blueprint.unlocked",
        BobnetNew => "bobnet.new",
        DeviceAttached => "device.attached",
        DeviceChangedOwner => "device.changed_owner",
        DeviceCompacted => "device.compacted",
        DeviceCompacting => "device.compacting",
        DeviceDecommissioned => "device.decommissioned",
        DeviceDeployed => "device.deployed",
        DeviceDetached => "device.detached",
        DeviceStowed => "device.stowed",
        DeviceUnfurled => "device.unfurled",
        DeviceUnfurling => "device.unfurling",
        DirectiveCleared => "directive.cleared",
        DirectiveCompleted => "directive.completed",
        DirectivePaused => "directive.paused",
        DirectiveResumed => "directive.resumed",
        DirectiveSet => "directive.set",
        DiversionActivated => "diversion.activated",
        DiversionDeactivated => "diversion.deactivated",
        DiversionDiverted => "diversion.diverted",
        DiversionImpacted => "diversion.impacted",
        DiversionPartial => "diversion.partial",
        EventCompleted => "event.completed",
        EventDiscovered => "event.discovered",
        ExperienceGained => "experience.gained",
        HubActivated => "hub.activated",
        HubDestroyed => "hub.destroyed",
        HubMaintained => "hub.maintained",
        HubWarning => "hub.warning",
        MegastructureContributed => "megastructure.contributed",
        MessageNew => "message.new",
        MultiplayerReplicantEntered => "multiplayer.replicant_entered",
        MultiplayerReplicantLeft => "multiplayer.replicant_left",
        MiningRelocated => "mining.relocated",
        MiningRetargeted => "mining.retargeted",
        MiningStarted => "mining.started",
        MiningStopped => "mining.stopped",
        PrintStarted => "print.started",
        PrintCompleted => "print.completed",
        ProspectCompleted => "prospect.completed",
        RelayActivated => "relay.activated",
        ReplicantTransferred => "replicant.transferred",
        SalvageDepleted => "salvage.depleted",
        SalvageDiscovered => "salvage.discovered",
        ScanCompleted => "scan.completed",
        ScanStarted => "scan.started",
        SearchCompleted => "search.completed",
        SearchStarted => "search.started",
        SimulationAbandoned => "simulation.abandoned",
        SimulationCompleted => "simulation.completed",
        SimulationExpired => "simulation.expired",
        SimulationStarted => "simulation.started",
        SiteDepleted => "site.depleted",
        StoryAwakened => "story.awakened",
        StoryHint => "story.hint",
        SystemBodyRenamed => "system.body_renamed",
        SystemDevicesHalted => "system.devices_halted",
        SystemEntryPointSet => "system.entry_point_set",
        SystemObjectDetected => "system.object_detected",
        TeleportCompleted => "teleport.completed",
        TeleportFailed => "teleport.failed",
        TeleportStarted => "teleport.started",
        TradeCompleted => "trade.completed",
        TradeCreated => "trade.created",
        TradeDeleted => "trade.deleted",
        TriangulationComplete => "triangulation.complete",
        TriangulationFailed => "triangulation.failed",
        TriangulationStarted => "triangulation.started",
        TransportCollected => "transport.collected",
        TransportDelivered => "transport.delivered",
        TravelArrived => "travel.arrived",
        TravelCancelled => "travel.cancelled",
        TravelDeparted => "travel.departed",
        WardActivated => "ward.activated",
        WardDeactivated => "ward.deactivated",
    }
}

#[cfg(test)]
mod tests {
    use super::EventName;

    #[test]
    fn known_value_round_trips_through_its_variant() {
        let name = EventName::from("mining.started".to_owned());
        assert_eq!(name, EventName::MiningStarted);
        assert_eq!(name.as_str(), "mining.started");
    }

    #[test]
    fn unknown_value_round_trips_through_the_original_text() {
        let name = EventName::from("megastructure.upgraded".to_owned());
        assert_eq!(
            name,
            EventName::Unknown("megastructure.upgraded".to_owned())
        );
        assert_eq!(name.as_str(), "megastructure.upgraded");

        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, "\"megastructure.upgraded\"");
        let round_tripped: EventName = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, name);
    }
}
