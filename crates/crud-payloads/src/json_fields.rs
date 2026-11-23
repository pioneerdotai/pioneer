use anyhow::Result;

/// Extract top-level fields without deserializing unrelated retained output.
/// Ignored values are still checked for JSON syntax; top-level duplicate labels keep the first value.
pub fn first_object_fields(text: &str, fields: &[&str]) -> Result<serde_json::Value> {
    value_selected(text, JsonSelection::Fields(fields))
}

pub(crate) fn shell_storage(text: &str) -> Result<bool> {
    let value = value_selected(text, JsonSelection::Path(&["storage", "kind"]))?;
    Ok(value
        .pointer("/storage/kind")
        .and_then(|kind| kind.as_str())
        == Some("shell"))
}

#[derive(Clone, Copy)]
enum JsonSelection<'a> {
    Fields(&'a [&'a str]),
    Path(&'a [&'a str]),
}

fn value_selected(text: &str, selection: JsonSelection<'_>) -> Result<serde_json::Value> {
    use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
    use serde_json::Value;
    struct PathSeed<'a>(&'a [&'a str]);
    impl<'de> DeserializeSeed<'de> for PathSeed<'_> {
        type Value = Value;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> std::result::Result<Value, D::Error> {
            deserialize_first(d, JsonSelection::Path(self.0))
        }
    }
    fn deserialize_first<'de, D: Deserializer<'de>>(
        d: D,
        selection: JsonSelection<'_>,
    ) -> std::result::Result<Value, D::Error> {
        struct FirstVisitor<'a> {
            selection: JsonSelection<'a>,
        }
        impl<'de> Visitor<'de> for FirstVisitor<'_> {
            type Value = Value;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("JSON value")
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Value, E> {
                Ok(Value::Null)
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Value, E> {
                Ok(v.into())
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Value, E> {
                Ok(v.into())
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Value, E> {
                Ok(v.into())
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Value, E> {
                serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .ok_or_else(|| E::custom("invalid JSON number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Value, E> {
                Ok(v.into())
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Value, E> {
                Ok(v.into())
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Value, A::Error> {
                while seq.next_element::<de::IgnoredAny>()?.is_some() {}
                Ok(Value::Array(Vec::new()))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Value, A::Error> {
                let mut out = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    let v = match self.selection {
                        JsonSelection::Fields(fields)
                            if fields.contains(&key.as_str()) && !out.contains_key(&key) =>
                        {
                            // A selected top-level field retains its complete value.
                            map.next_value::<Value>()?
                        }
                        JsonSelection::Path([field, rest @ ..])
                            if *field == key && !out.contains_key(&key) =>
                        {
                            map.next_value_seed(PathSeed(rest))?
                        }
                        _ => {
                            map.next_value::<de::IgnoredAny>()?;
                            continue;
                        }
                    };
                    out.entry(key).or_insert(v);
                }
                Ok(Value::Object(out))
            }
        }
        d.deserialize_any(FirstVisitor { selection })
    }
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let value = deserialize_first(&mut deserializer, selection)?;
    deserializer.end()?;
    Ok(value)
}
