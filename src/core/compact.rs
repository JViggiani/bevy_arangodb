//! Compact binary encoding for large values stored inside JSON documents.
//!
//! Backend documents (Arango / Postgres JSON) are still JSON, but naively
//! serializing large arrays as JSON number trees can OOM or blow document size
//! limits. This module postcard-encodes, zstd-compresses, and base64-wraps any
//! serde value into a small JSON envelope that still round-trips through
//! [`serde_json::Value`].
//!
//! ## Automatic path (preferred)
//!
//! [`PersistenceSession`](crate::core::session::PersistenceSession) serializers
//! call [`to_persist_value`] / [`from_persist_value`] with
//! [`crate::PersistencePluginConfig::compact_threshold_bytes`]: postcard size is
//! probed first; large values store the compact envelope, small values stay as
//! normal JSON. Types need no `serde(with)` annotation.
//!
//! ## Manual path
//!
//! Force compact always with `#[serde(with = "bevy_persistence_database::compact")]`
//! or [`CompactJson`].

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::Value;

/// Envelope version written into JSON documents.
pub const ENCODING: &str = "postcard+zstd-v1";

/// Default zstd compression level (speed / ratio trade-off for persistence).
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Default postcard-size threshold above which session serializers use the
/// compact envelope instead of naive JSON (`256 KiB`).
pub const DEFAULT_COMPACT_THRESHOLD_BYTES: usize = 256 * 1024;

#[derive(Serialize, Deserialize)]
struct CompactEnvelope {
    encoding: String,
    /// Base64(zstd(postcard(T))).
    payload: String,
}

/// Errors from compact encode / decode helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactError(pub String);

impl std::fmt::Display for CompactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CompactError {}

/// True when `value` is (or contains) a compact envelope (`encoding` + `payload`).
///
/// Extra document fields (keys, BPD metadata) are ignored.
pub fn is_compact_envelope(value: &Value) -> bool {
    value.get("encoding").and_then(|v| v.as_str()) == Some(ENCODING)
        && value.get("payload").and_then(|v| v.as_str()).is_some()
}

/// Postcard + zstd + base64 encode `value` to a payload string (no envelope).
pub fn encode<T: Serialize>(value: &T) -> Result<String, CompactError> {
    encode_with_level(value, DEFAULT_ZSTD_LEVEL)
}

/// Like [`encode`] with an explicit zstd level.
pub fn encode_with_level<T: Serialize>(value: &T, zstd_level: i32) -> Result<String, CompactError> {
    let raw = postcard::to_allocvec(value)
        .map_err(|e| CompactError(format!("postcard encode: {e}")))?;
    encode_postcard_bytes(&raw, zstd_level)
}

/// zstd + base64 for already-postcarded bytes (avoids a second postcard pass).
pub fn encode_postcard_bytes(raw: &[u8], zstd_level: i32) -> Result<String, CompactError> {
    let compressed = zstd::encode_all(raw, zstd_level)
        .map_err(|e| CompactError(format!("zstd encode: {e}")))?;
    Ok(BASE64.encode(compressed))
}

/// Inverse of [`encode`].
pub fn decode<T: DeserializeOwned>(payload: &str) -> Result<T, CompactError> {
    let compressed = BASE64
        .decode(payload.as_bytes())
        .map_err(|e| CompactError(format!("base64 decode: {e}")))?;
    let raw = zstd::decode_all(compressed.as_slice())
        .map_err(|e| CompactError(format!("zstd decode: {e}")))?;
    postcard::from_bytes(&raw).map_err(|e| CompactError(format!("postcard decode: {e}")))
}

/// Build a JSON [`Value`] for persistence: compact envelope when postcard size
/// exceeds `threshold_bytes`, otherwise naive `serde_json::to_value`.
pub fn to_persist_value<T: Serialize>(
    value: &T,
    threshold_bytes: usize,
) -> Result<Value, CompactError> {
    let raw = postcard::to_allocvec(value)
        .map_err(|e| CompactError(format!("postcard encode: {e}")))?;
    if raw.len() > threshold_bytes {
        let payload = encode_postcard_bytes(&raw, DEFAULT_ZSTD_LEVEL)?;
        Ok(serde_json::json!({
            "encoding": ENCODING,
            "payload": payload,
        }))
    } else {
        serde_json::to_value(value).map_err(|e| CompactError(format!("json encode: {e}")))
    }
}

/// Inverse of [`to_persist_value`]: compact envelope or plain JSON document body.
pub fn from_persist_value<T: DeserializeOwned>(value: Value) -> Result<T, CompactError> {
    if is_compact_envelope(&value) {
        let payload = value
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| CompactError("compact envelope missing payload".into()))?;
        decode(payload)
    } else {
        serde_json::from_value(value).map_err(|e| CompactError(format!("json decode: {e}")))
    }
}

/// Serialize `value` as a versioned compact JSON envelope.
///
/// Intended for `#[serde(with = "bevy_persistence_database::compact")]`.
pub fn serialize<T: Serialize, S: Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    let payload = encode(value).map_err(serde::ser::Error::custom)?;
    CompactEnvelope {
        encoding: ENCODING.to_string(),
        payload,
    }
    .serialize(serializer)
}

/// Deserialize a versioned compact JSON envelope into `T`.
///
/// Intended for `#[serde(with = "bevy_persistence_database::compact")]`.
pub fn deserialize<'de, T: DeserializeOwned, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<T, D::Error> {
    let doc = CompactEnvelope::deserialize(deserializer)?;
    if doc.encoding != ENCODING {
        return Err(serde::de::Error::custom(format!(
            "unsupported compact persist encoding {:?}",
            doc.encoding
        )));
    }
    decode(&doc.payload).map_err(serde::de::Error::custom)
}

/// Newtype that always serde-encodes `T` via the compact envelope.
///
/// Prefer the session threshold path for normal types; use this only when a
/// value must always be compact regardless of size.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompactJson<T>(pub T);

impl<T> CompactJson<T> {
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for CompactJson<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CompactJson<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> From<T> for CompactJson<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T: Serialize> Serialize for CompactJson<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize(&self.0, serializer)
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for CompactJson<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize(deserializer).map(CompactJson)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        name: String,
        values: Vec<f32>,
    }

    // GIVEN an arbitrary serde value with a large array
    // WHEN compact-encoded and decoded
    // THEN the value round-trips exactly
    #[test]
    fn encode_decode_round_trips_arbitrary_struct() {
        let original = Sample {
            name: "grid".into(),
            values: (0..1000).map(|i| i as f32 * 0.01).collect(),
        };
        let payload = encode(&original).expect("encode");
        let restored: Sample = decode(&payload).expect("decode");
        assert_eq!(restored, original);
    }

    // GIVEN an arbitrary serde value
    // WHEN serialized through the JSON envelope
    // THEN the document carries the versioned encoding tag and is smaller than naive JSON
    #[test]
    fn serde_envelope_is_versioned_and_smaller_than_naive_json() {
        let original = Sample {
            name: "big".into(),
            values: vec![0.25; 2000],
        };
        let mut compact = Vec::new();
        let mut ser = serde_json::Serializer::new(&mut compact);
        serialize(&original, &mut ser).expect("compact serialize");
        let naive = serde_json::to_vec(&original).expect("naive json");
        let value: serde_json::Value = serde_json::from_slice(&compact).expect("parse envelope");
        assert_eq!(value["encoding"], ENCODING);
        assert!(
            compact.len() * 4 < naive.len(),
            "compact {} should be much smaller than naive {}",
            compact.len(),
            naive.len()
        );

        let wrapped = CompactJson(original.clone());
        let back: CompactJson<Sample> =
            serde_json::from_value(serde_json::to_value(&wrapped).unwrap()).unwrap();
        assert_eq!(*back, original);
    }

    // GIVEN an envelope with an unknown encoding tag
    // WHEN deserialized
    // THEN an error is returned
    #[test]
    fn rejects_unknown_encoding_tag() {
        let bad = serde_json::json!({
            "encoding": "unknown-v0",
            "payload": "AAAA",
        });
        let err = serde_json::from_value::<CompactJson<Sample>>(bad).unwrap_err();
        assert!(
            err.to_string().contains("unsupported compact persist encoding"),
            "unexpected error: {err}"
        );
    }

    // GIVEN a small value and a high threshold
    // WHEN to_persist_value runs
    // THEN the result is plain JSON (not a compact envelope)
    #[test]
    fn to_persist_value_keeps_small_values_as_json() {
        let original = Sample {
            name: "tiny".into(),
            values: vec![1.0],
        };
        let value = to_persist_value(&original, DEFAULT_COMPACT_THRESHOLD_BYTES).unwrap();
        assert!(!is_compact_envelope(&value));
        assert_eq!(from_persist_value::<Sample>(value).unwrap(), original);
    }

    // GIVEN a large value and a low threshold
    // WHEN to_persist_value runs
    // THEN the result is a compact envelope that round-trips
    #[test]
    fn to_persist_value_compacts_when_over_threshold() {
        let original = Sample {
            name: "big".into(),
            values: vec![0.5; 5000],
        };
        let value = to_persist_value(&original, 64).unwrap();
        assert!(is_compact_envelope(&value));
        // Document metadata may sit alongside the envelope on disk.
        let mut with_meta = value.clone();
        with_meta
            .as_object_mut()
            .unwrap()
            .insert("_key".into(), Value::String("Sample".into()));
        assert!(is_compact_envelope(&with_meta));
        assert_eq!(from_persist_value::<Sample>(with_meta).unwrap(), original);
    }
}
