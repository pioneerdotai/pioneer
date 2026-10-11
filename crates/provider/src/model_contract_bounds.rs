//! A streaming allocation/cardinality gate for the two encoded selected-model
//! records and their envelope, before their existing typed owners decode them.
use anyhow::Result;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

pub(crate) fn validate(encoded: &str) -> Result<()> {
    if encoded.len() > pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES {
        anyhow::bail!("accepted model metadata exceeds byte limit");
    }
    struct Seed<'a> {
        nodes: &'a mut usize,
        depth: usize,
    }
    impl<'de> DeserializeSeed<'de> for Seed<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> std::result::Result<(), D::Error> {
            *self.nodes += 1;
            // Same finite rich-JSON allowance as the native function surface;
            // count keys too, so a wide object cannot evade the node bound.
            if *self.nodes > 4096 || self.depth > 32 {
                return Err(serde::de::Error::custom(
                    "accepted model metadata shape limit",
                ));
            }
            deserializer.deserialize_any(self)
        }
    }
    impl<'de> Visitor<'de> for Seed<'_> {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded model metadata")
        }
        fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> std::result::Result<(), M::Error> {
            while map
                .next_key_seed(Seed {
                    nodes: self.nodes,
                    depth: self.depth + 1,
                })?
                .is_some()
            {
                map.next_value_seed(Seed {
                    nodes: self.nodes,
                    depth: self.depth + 1,
                })?;
            }
            Ok(())
        }
        fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> std::result::Result<(), S::Error> {
            while seq
                .next_element_seed(Seed {
                    nodes: self.nodes,
                    depth: self.depth + 1,
                })?
                .is_some()
            {}
            Ok(())
        }
    }
    let mut nodes = 0;
    let mut deserializer = serde_json::Deserializer::from_str(encoded);
    Seed {
        nodes: &mut nodes,
        depth: 0,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| anyhow::anyhow!("accepted model metadata is invalid or exceeds shape limit"))?;
    deserializer
        .end()
        .map_err(|_| anyhow::anyhow!("accepted model metadata contains trailing data"))
}
