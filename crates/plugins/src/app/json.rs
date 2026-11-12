use super::ManifestError;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use std::fmt;

// Decode each object before it becomes a map, so duplicates cannot become last-wins.
struct Seed<'a> {
    path: String,
    failure: &'a mut Option<ManifestError>,
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Value;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("JSON value")
    }
    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Value, E> {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .ok_or_else(|| E::custom("invalid number"))
    }
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
        *self.failure = Some(ManifestError::new("null_field", &self.path));
        Err(E::custom("null field"))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = a.next_element_seed(Seed {
            path: format!("{}/{}", self.path, values.len()),
            failure: self.failure,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some(key) = a.next_key::<String>()? {
            let path = format!(
                "{}/{}",
                self.path,
                key.replace('~', "~0").replace('/', "~1")
            );
            if values.contains_key(&key) {
                *self.failure = Some(ManifestError::new("duplicate_key", &path));
                return Err(serde::de::Error::custom("duplicate key"));
            }
            let value = a.next_value_seed(Seed {
                path,
                failure: self.failure,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}
pub(super) fn parse(bytes: &[u8], path: &str) -> Result<Value, ManifestError> {
    if bytes.len() > 1024 * 1024 {
        return Err(ManifestError::new("document_limit", path));
    }
    let mut d = serde_json::Deserializer::from_slice(bytes);
    let mut failure = None;
    let value = Seed {
        path: path.into(),
        failure: &mut failure,
    }
    .deserialize(&mut d)
    .map_err(|_| failure.unwrap_or_else(|| ManifestError::new("invalid_json", path)))?;
    d.end()
        .map_err(|_| ManifestError::new("invalid_json", path))?;
    Ok(value)
}
