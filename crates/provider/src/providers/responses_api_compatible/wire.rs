//! Bounded private JSON values. The byte transport cap alone does not bound a
//! decoded tree containing many small nodes or unknown metadata fields.
use anyhow::Result;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use std::cell::Cell;

struct Budget {
    remaining: Cell<usize>,
    exceeded: Cell<bool>,
}
impl Budget {
    fn charge<E: de::Error>(&self, bytes: usize) -> std::result::Result<(), E> {
        if bytes > self.remaining.get() {
            self.exceeded.set(true);
            return Err(E::custom("Responses JSON retained limit"));
        }
        self.remaining.set(self.remaining.get() - bytes);
        Ok(())
    }
}
struct Seed<'a> {
    budget: &'a Budget,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Value, D::Error> {
        if self.depth > 64 {
            return Err(de::Error::custom("Responses JSON depth limit"));
        }
        self.budget.charge::<D::Error>(64)?;
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("bounded Responses JSON")
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_none<E: de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Value, E> {
        Ok(Value::Number(v.into()))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Value, E> {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .ok_or_else(|| E::custom("invalid JSON number"))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Value, E> {
        self.budget.charge::<E>(v.len().saturating_mul(2))?;
        Ok(Value::String(v.into()))
    }
    fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Value, E> {
        self.budget.charge::<E>(v.len().saturating_mul(2))?;
        Ok(Value::String(v))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(Seed {
            budget: self.budget,
            depth: self.depth + 1,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            self.budget
                .charge::<A::Error>(128usize.saturating_add(key.len().saturating_mul(2)))?;
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate Responses JSON key"));
            }
            let value = map.next_value_seed(Seed {
                budget: self.budget,
                depth: self.depth + 1,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}
pub(super) fn parse(bytes: &[u8], limit: usize) -> Result<(Value, usize)> {
    let budget = Budget {
        remaining: Cell::new(limit),
        exceeded: Cell::new(false),
    };
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let parsed = Seed {
        budget: &budget,
        depth: 0,
    }
    .deserialize(&mut decoder)
    .and_then(|value| {
        decoder.end()?;
        Ok(value)
    });
    match parsed {
        Ok(value) => Ok((value, limit - budget.remaining.get())),
        Err(_) if budget.exceeded.get() => Err(crate::ProviderResponseTooLarge::new(
            "responses_json",
            limit,
            limit.saturating_add(1),
        )
        .into()),
        Err(_) => Err(anyhow::anyhow!("malformed or unsupported Responses JSON")),
    }
}
/// Same conservative node/string charges as the parser, without allocating a
/// serialized copy while checking retained snapshots.
pub(super) fn retained_bytes(value: &Value) -> usize {
    64usize.saturating_add(match value {
        Value::String(v) => v.len().saturating_mul(2),
        Value::Array(v) => v
            .iter()
            .fold(0usize, |n, v| n.saturating_add(retained_bytes(v))),
        Value::Object(v) => v.iter().fold(0usize, |n, (key, value)| {
            n.saturating_add(128)
                .saturating_add(key.len().saturating_mul(2))
                .saturating_add(retained_bytes(value))
        }),
        _ => 0,
    })
}
