//! Finite application actions.

use std::io;

use replicant_client::{Client, Operation, OperationStatus, raw};

use crate::ActionResult;

/// Inputs for [`clear_tags`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClearTagsAction {
    /// Prefix selecting tags to remove.
    pub tag_prefix: String,
    /// When true, report matching tags without submitting mutations.
    pub dry_run: bool,
}

impl ClearTagsAction {
    /// Creates a mutating clear-tags action for a prefix.
    #[must_use]
    pub fn new(tag_prefix: impl Into<String>) -> Self {
        Self {
            tag_prefix: tag_prefix.into(),
            dry_run: false,
        }
    }
}

/// Clear-tags outcome for one matching device.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClearedDeviceTags {
    /// Device code.
    pub device: String,
    /// Matching tags selected for removal.
    pub tags: Vec<String>,
    /// Whether a managed mutation was submitted.
    pub changed: bool,
}

/// Typed result of a clear-tags action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClearTagsActionResult {
    /// Total owned devices scanned.
    pub scanned_devices: usize,
    /// Per-device outcomes for devices carrying matching tags.
    pub devices: Vec<ClearedDeviceTags>,
}

impl ClearTagsActionResult {
    /// Number of matching tags found.
    #[must_use]
    pub fn removed_tags(&self) -> usize {
        self.devices.iter().map(|device| device.tags.len()).sum()
    }

    /// Number of devices changed through managed operations.
    #[must_use]
    pub fn changed_devices(&self) -> usize {
        self.devices.iter().filter(|device| device.changed).count()
    }
}

/// Removes matching tags from every owned device through managed operations.
pub async fn clear_tags(
    client: &Client,
    action: &ClearTagsAction,
) -> ActionResult<ClearTagsActionResult> {
    if action.tag_prefix.is_empty() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "tag_prefix must not be empty").into(),
        );
    }

    let handles = client.devices().refresh_many().collect().await?;
    let scanned_devices = handles.len();
    let mut devices = Vec::new();

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let tags = matching_tags(&snapshot.tags, &action.tag_prefix);
        if tags.is_empty() {
            continue;
        }

        if !action.dry_run {
            let operation = handle
                .configure(raw::devices::DeviceConfiguration {
                    remove_tags: Some(tags.clone()),
                    ..Default::default()
                })
                .await?;
            ensure_operation_accepted(&operation).await?;
        }
        devices.push(ClearedDeviceTags {
            device: handle.id().as_str().to_owned(),
            tags,
            changed: !action.dry_run,
        });
    }

    Ok(ClearTagsActionResult {
        scanned_devices,
        devices,
    })
}

fn matching_tags(tags: &[String], prefix: &str) -> Vec<String> {
    tags.iter()
        .filter(|tag| tag.starts_with(prefix))
        .cloned()
        .collect()
}

async fn ensure_operation_accepted(operation: &Operation) -> ActionResult<()> {
    let outcome = operation.outcome().await?;
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(io::Error::other(format!(
            "operation {} ended as {:?}: {:?}",
            operation.id().as_str(),
            outcome.status,
            outcome.response
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_only_prefixed_tags_and_summarizes_result() {
        let tags = vec!["keep".into(), "evt-one".into(), "evt-two".into()];
        assert_eq!(matching_tags(&tags, "evt-"), ["evt-one", "evt-two"]);

        let result = ClearTagsActionResult {
            scanned_devices: 2,
            devices: vec![ClearedDeviceTags {
                device: "DEV-1".into(),
                tags: matching_tags(&tags, "evt-"),
                changed: true,
            }],
        };
        assert_eq!(result.removed_tags(), 2);
        assert_eq!(result.changed_devices(), 1);
    }
}
