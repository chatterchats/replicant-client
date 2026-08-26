//! Shared helpers reused by every raw domain module: an open-JSON alias for
//! untyped or forward-compatible fields, a 3D catalogue position, a
//! tri-state PATCH field, and small path/query encoding helpers for building
//! request paths by hand (the raw transport takes a single path string with
//! no built-in query builder).

use std::fmt::Write as _;

use serde::{Deserialize, Serialize, Serializer};

/// An open JSON object, used for fields the contract declares as untyped
/// (`{}`), and for opaque server-declared-but-unschema'd request/response
/// bodies (for example device permission grants).
pub type JsonObject = serde_json::Map<String, serde_json::Value>;

/// A 3D galactic coordinate, shared by star, scan, and replicant status
/// responses.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Position {
    /// X coordinate.
    pub x: f64,
    /// Y coordinate.
    pub y: f64,
    /// Z coordinate.
    pub z: f64,
}

/// A tri-state PATCH field, for account/replicant update requests where
/// omitting a key ("leave unchanged"), sending `null` ("clear it"), and
/// sending a value ("set it") are three distinct, observable server
/// behaviors that a plain `Option<T>` cannot express (an `Option` cannot
/// tell `#[serde(skip_serializing_if = "Option::is_none")]` to distinguish
/// "was `None`" from "was absent").
///
/// Defaults to [`UpdateField::Omitted`], so building a request that only
/// touches a couple of fields does not require naming every field.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum UpdateField<T> {
    /// Do not include this field in the request; the server leaves it
    /// unchanged.
    #[default]
    Omitted,
    /// Include this field as JSON `null`; the server clears it.
    Null,
    /// Include this field with a value; the server sets it.
    Value(T),
}

impl<T> UpdateField<T> {
    /// Returns `true` if this field should be omitted from the request body.
    #[must_use]
    pub fn is_omitted(&self) -> bool {
        matches!(self, Self::Omitted)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for UpdateField<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Option::<T>::deserialize(deserializer)?.map_or(Self::Null, Self::Value))
    }
}

impl<T: Serialize> Serialize for UpdateField<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Omitted | Self::Null => serializer.serialize_none(),
            Self::Value(value) => serializer.serialize_some(value),
        }
    }
}

/// Percent-encodes a single path segment (a device code, replicant code,
/// location designation, and similar identifiers are interpolated into
/// request paths as plain strings now that the raw layer no longer validates
/// them with newtypes).
pub(crate) fn encode_path_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

/// Builds a `path?k=v&k=v` query string from optional parameters, percent-
/// encoding each value and skipping parameters that are `None`. Returns the
/// bare path when every parameter is `None`.
pub(crate) fn with_query(path: &str, params: &[(&str, Option<String>)]) -> String {
    let mut pairs = params.iter().filter_map(|(key, value)| {
        value
            .as_ref()
            .map(|value| format!("{key}={}", encode_query_value(value)))
    });
    match pairs.next() {
        None => path.to_string(),
        Some(first) => {
            let mut out = format!("{path}?{first}");
            for pair in pairs {
                out.push('&');
                out.push_str(&pair);
            }
            out
        }
    }
}

fn encode_query_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{encode_path_segment, with_query};

    #[test]
    fn encode_path_segment_escapes_reserved_bytes() {
        assert_eq!(encode_path_segment("abc-123_XYZ.~"), "abc-123_XYZ.~");
        assert_eq!(encode_path_segment("a/b c"), "a%2Fb%20c");
    }

    #[test]
    fn with_query_skips_none_and_joins_present() {
        assert_eq!(with_query("v1/x", &[("a", None), ("b", None)]), "v1/x");
        assert_eq!(
            with_query(
                "v1/x",
                &[
                    ("a", Some("1".to_string())),
                    ("b", None),
                    ("c", Some("y z".to_string()))
                ]
            ),
            "v1/x?a=1&c=y%20z"
        );
    }
}
