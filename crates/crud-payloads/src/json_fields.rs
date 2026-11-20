use anyhow::Result;

/// Extract top-level fields without deserializing unrelated retained output.
/// Ignored values are still checked for JSON syntax; top-level duplicate labels keep the first value.
pub fn first_object_fields(text: &str, fields: &[&str]) -> Result<serde_json::Value> {
    value_selected(text, Some(fields))
}

fn value_selected(text: &str, fields: Option<&[&str]>) -> Result<serde_json::Value> {
    use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
    use serde_json::Value;
    struct First(Value);
    impl<'de> Deserialize<'de> for First {
        fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
            deserialize_first(d, None)
        }
    }
    fn deserialize_first<'de, D: Deserializer<'de>>(
        d: D,
        fields: Option<&[&str]>,
    ) -> std::result::Result<First, D::Error> {
        struct FirstVisitor<'a> {
            fields: Option<&'a [&'a str]>,
        }
        impl<'de> Visitor<'de> for FirstVisitor<'_> {
            type Value = First;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("JSON value")
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<First, E> {
                Ok(First(Value::Null))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<First, E> {
                Ok(First(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<First, E> {
                Ok(First(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<First, E> {
                Ok(First(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<First, E> {
                Ok(First(Value::Number(
                    serde_json::Number::from_f64(v)
                        .ok_or_else(|| E::custom("invalid JSON number"))?,
                )))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<First, E> {
                Ok(First(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<First, E> {
                Ok(First(v.into()))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<First, A::Error> {
                if self.fields.is_some() {
                    while seq.next_element::<de::IgnoredAny>()?.is_some() {}
                    return Ok(First(Value::Array(Vec::new())));
                }
                let mut out = Vec::new();
                while let Some(First(v)) = seq.next_element()? {
                    out.push(v);
                }
                Ok(First(out.into()))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<First, A::Error> {
                let mut out = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if self.fields.is_some_and(|fields| {
                        !fields.contains(&key.as_str()) || out.contains_key(&key)
                    }) {
                        map.next_value::<de::IgnoredAny>()?;
                        continue;
                    }
                    // Selected objects are extracted as JSON and then decoded
                    // normally, matching the old json_extract + from_str path.
                    let v = if self.fields.is_some() {
                        map.next_value::<Value>()?
                    } else {
                        let First(v) = map.next_value()?;
                        v
                    };
                    out.entry(key).or_insert(v);
                }
                Ok(First(Value::Object(out)))
            }
        }
        d.deserialize_any(FirstVisitor { fields })
    }
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let value = deserialize_first(&mut deserializer, fields)?;
    deserializer.end()?;
    Ok(value.0)
}
