use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DevicePredicate {
    pub realm: Option<Realm>,
    pub device_type: Option<DeviceType>,
    pub status: Option<DeviceStatus>,
    pub access: Option<AccessScope>,
    pub location: Option<LocationId>,
    pub feature: Option<DeviceFeature>,
    pub command: Option<DeviceCommand>,
}

impl DevicePredicate {
    pub fn in_realm(mut self, realm: Realm) -> Self {
        self.realm = Some(realm);
        self
    }
    pub fn of_type(mut self, device_type: DeviceType) -> Self {
        self.device_type = Some(device_type);
        self
    }
    pub fn with_status(mut self, status: DeviceStatus) -> Self {
        self.status = Some(status);
        self
    }
    pub fn with_access(mut self, access: AccessScope) -> Self {
        self.access = Some(access);
        self
    }
    pub fn at(mut self, location: impl Into<LocationId>) -> Self {
        self.location = Some(location.into());
        self
    }
    pub fn with_feature(mut self, feature: DeviceFeature) -> Self {
        self.feature = Some(feature);
        self
    }
    pub fn with_command(mut self, command: DeviceCommand) -> Self {
        self.command = Some(command);
        self
    }
    pub fn matches(&self, device: &Device) -> bool {
        self.realm
            .as_ref()
            .is_none_or(|value| value == &device.key.realm)
            && self
                .device_type
                .as_ref()
                .is_none_or(|value| device.device_type.as_ref() == Some(value))
            && self
                .status
                .as_ref()
                .is_none_or(|value| device.status.as_ref() == Some(value))
            && self
                .access
                .as_ref()
                .is_none_or(|value| value == &device.access)
            && self.location.as_ref().is_none_or(|value| {
                device
                    .location
                    .as_ref()
                    .is_some_and(|location| &location.id == value)
            })
            && self
                .feature
                .as_ref()
                .is_none_or(|value| device.features.contains(value))
            && self
                .command
                .as_ref()
                .is_none_or(|value| device.available_commands.contains(value))
    }
}
