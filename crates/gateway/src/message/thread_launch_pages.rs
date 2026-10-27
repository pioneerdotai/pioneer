//! Pagination of the two execution-bound launch catalogs. Positions and a
//! content fingerprint are sealed; a catalog change requires a fresh traversal.
use super::thread_read_cursor::ThreadReadCursorCodec;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    fingerprint: String,
    field: usize,
    index: usize,
    model: usize,
    offset: usize,
}

pub(super) fn catalog_page(
    source: Value,
    limit: u32,
    cursor: Option<&str>,
    codec: &ThreadReadCursorCodec,
    scope: &str,
) -> Result<Value> {
    ensure!(
        (1..=100).contains(&limit),
        "limit must be between 1 and 100"
    );
    struct Fingerprint(Sha256);
    impl std::io::Write for Fingerprint {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut digest = Fingerprint(Sha256::new());
    serde_json::to_writer(&mut digest, &source)?;
    let fingerprint = hex::encode(digest.0.finalize());
    let mut state = cursor
        .map(|cursor| codec.decode::<Cursor>(cursor, scope))
        .transpose()?
        .unwrap_or(Cursor {
            fingerprint: fingerprint.clone(),
            field: 0,
            index: 0,
            model: 0,
            offset: 0,
        });
    ensure!(
        state.fingerprint == fingerprint,
        "catalog cursor is stale; restart reading"
    );
    let object = source
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("catalog unavailable"))?;
    let mut fields = object
        .iter()
        .filter(|(_, value)| value.is_array())
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let mut page = Value::Object(
        object
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    if value.is_array() {
                        json!([])
                    } else if key == "identity" {
                        Value::Null
                    } else {
                        value.clone()
                    },
                )
            })
            .collect(),
    );
    page["fragments"] = json!([]);
    if object.contains_key("identity") {
        // The identity header itself can be oversized. Keep its stable ID and
        // page that one object using the same fragment contract as entries.
        fields.insert(0, "identity".to_owned());
        page["identity"] = Value::Null;
    }
    let rows_for = |field: &str| -> &[Value] {
        if field == "identity" {
            std::slice::from_ref(&object[field])
        } else {
            object[field].as_array().expect("array field")
        }
    };
    let is_end = |state: &Cursor| {
        let mut field = state.field;
        let mut index = state.index;
        while field < fields.len() && index == rows_for(&fields[field]).len() {
            field += 1;
            index = 0;
        }
        field == fields.len()
    };
    let mut count = 0;
    while state.field < fields.len() {
        let field = &fields[state.field];
        let rows = rows_for(field);
        if state.index == rows.len() {
            state.field += 1;
            state.index = 0;
            state.model = 0;
            continue;
        }
        ensure!(
            state.index < rows.len(),
            "invalid catalog cursor; restart reading"
        );
        if count == limit {
            break;
        }
        let row = &rows[state.index];
        let mut record = if field == "executors" {
            Value::Object(
                row.as_object()
                    .ok_or_else(|| anyhow::anyhow!("invalid executor catalog"))?
                    .iter()
                    .filter(|(key, _)| key.as_str() != "models")
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            )
        } else {
            row.clone()
        };
        let mut next = state.clone();
        next.offset = 0;
        if field == "executors"
            && row["models"]
                .as_array()
                .is_some_and(|models| !models.is_empty())
        {
            let models = row["models"].as_array().expect("executor models");
            ensure!(
                state.model < models.len(),
                "invalid catalog cursor; restart reading"
            );
            let model = models[state.model].clone();
            next.model += 1;
            if next.model == models.len() {
                next.model = 0;
                next.index += 1;
            }
            record["models"] = json!([model]);
        } else {
            if field == "executors" {
                record["models"] = json!([]);
            }
            next.index += 1;
        }
        let mut proposed = page.clone();
        append_record(&mut proposed, field, record.clone());
        if state.offset == 0 && fits(&mut proposed, &next, codec, scope, is_end(&next))? {
            page = proposed;
            state = next;
            count += 1;
            continue;
        }
        if count > 0 {
            break;
        }
        let encoded = serde_json::to_string(&record)?;
        ensure!(
            state.offset < encoded.len() && encoded.is_char_boundary(state.offset),
            "invalid catalog fragment cursor; restart reading"
        );
        let offset = state.offset;
        let mut low = offset;
        let mut high = encoded
            .len()
            .min(offset.saturating_add(pioneer_protocol::TOOL_RESPONSE_MAX_BYTES));
        let mut best = None;
        while low <= high {
            let mid = low + (high - low) / 2;
            let mut end = mid;
            while !encoded.is_char_boundary(end) {
                end -= 1;
            }
            let mut candidate = page.clone();
            candidate["fragments"] = json!([{"collection":field,"index":state.index,"modelIndex":state.model,
                "encoding":"json","version":fingerprint,"offset":offset,"nextOffset":end,"eof":end == encoded.len(),"text":&encoded[offset..end]}]);
            let after = if end == encoded.len() {
                next.clone()
            } else {
                let mut after = state.clone();
                after.offset = end;
                after
            };
            if fits(&mut candidate, &after, codec, scope, is_end(&after))? {
                best = Some((end, candidate, after));
                low = mid + 1;
            } else if mid == 0 {
                break;
            } else {
                high = mid - 1;
            }
        }
        if encoded.len() - offset <= pioneer_protocol::TOOL_RESPONSE_MAX_BYTES {
            let mut candidate = page.clone();
            candidate["fragments"] = json!([{"collection":field,"index":state.index,"modelIndex":state.model,
                "encoding":"json","version":fingerprint,"offset":offset,"nextOffset":encoded.len(),"eof":true,"text":&encoded[offset..]}]);
            if fits(&mut candidate, &next, codec, scope, is_end(&next))? {
                best = Some((encoded.len(), candidate, next.clone()));
            }
        }
        let (end, candidate, after) =
            best.ok_or_else(|| anyhow::anyhow!("catalog metadata exceeds page capacity"))?;
        ensure!(end > offset, "catalog metadata exceeds page capacity");
        page = candidate;
        state = after;
        break;
    }
    // Normalize empty trailing collections so eof describes the actual end.
    while state.field < fields.len() && state.index == rows_for(&fields[state.field]).len() {
        state.field += 1;
        state.index = 0;
        state.model = 0;
    }
    let eof = state.field == fields.len();
    page["nextCursor"] = if eof {
        Value::Null
    } else {
        json!(codec.encode(&state, scope)?)
    };
    page["eof"] = json!(eof);
    ensure!(
        pioneer_tools::thread_read_page_fits(&page),
        "catalog metadata exceeds page capacity"
    );
    Ok(page)
}

fn fits(
    page: &mut Value,
    state: &Cursor,
    codec: &ThreadReadCursorCodec,
    scope: &str,
    eof: bool,
) -> Result<bool> {
    page["nextCursor"] = if eof {
        Value::Null
    } else {
        json!(codec.encode(state, scope)?)
    };
    page["eof"] = json!(eof);
    Ok(pioneer_tools::thread_read_page_fits(page))
}
fn append_record(page: &mut Value, field: &str, record: Value) {
    if field == "identity" {
        page[field] = record;
        return;
    }
    let records = page[field].as_array_mut().expect("page collection");
    if field == "executors" {
        if let Some(last) = records
            .last_mut()
            .filter(|last| last["providerId"] == record["providerId"])
        {
            last["models"]
                .as_array_mut()
                .expect("executor models")
                .extend(
                    record["models"]
                        .as_array()
                        .expect("executor models")
                        .iter()
                        .cloned(),
                );
            return;
        }
    }
    records.push(record);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_identity_profile_and_destination_is_reachable_and_changes_are_explicit() {
        let codec = ThreadReadCursorCodec::new(&[7; 64]).unwrap();
        let source = json!({"identities":(0..140).map(|n| json!({"id":n})).collect::<Vec<_>>(),
            "profiles":(0..300).map(|n| json!({"id":n})).collect::<Vec<_>>(),
            "targetOptions":(0..205).map(|n| json!({"id":n})).collect::<Vec<_>>(),"generationFingerprint":"fixture"});
        let mut cursor = None;
        let mut collected = std::collections::BTreeMap::<String, Vec<Value>>::new();
        let mut first_cursor = None;
        loop {
            let page = catalog_page(
                source.clone(),
                100,
                cursor.as_deref(),
                &codec,
                "execution-a",
            )
            .unwrap();
            let mut count = 0;
            for field in ["identities", "profiles", "targetOptions"] {
                let rows = page[field].as_array().unwrap();
                count += rows.len();
                collected
                    .entry(field.into())
                    .or_default()
                    .extend(rows.iter().cloned());
            }
            assert!(count <= 100);
            if page["eof"] == true {
                break;
            }
            cursor = Some(page["nextCursor"].as_str().unwrap().to_owned());
            if first_cursor.is_none() {
                first_cursor = cursor.clone();
            }
        }
        for field in ["identities", "profiles", "targetOptions"] {
            assert_eq!(json!(collected[field]), source[field]);
        }
        let mut changed = source.clone();
        changed["identities"][0]["id"] = json!("changed");
        assert!(
            catalog_page(changed, 100, first_cursor.as_deref(), &codec, "execution-a")
                .unwrap_err()
                .to_string()
                .contains("stale")
        );
        assert!(catalog_page(source, 100, first_cursor.as_deref(), &codec, "execution-b").is_err());
    }

    #[test]
    fn nested_models_and_oversized_record_reconstruct_exactly_with_working_cursors() {
        let codec = ThreadReadCursorCodec::new(&[8; 64]).unwrap();
        let large = json!({"id":"large","displayName":"🌍\\\"\n".repeat(400_000)});
        let mut models = vec![large.clone()];
        models.extend((0..205).map(|n| json!({"id":format!("model-{n:03}")})));
        let source = json!({"identityId":"identity","identity":{"id":"identity"},"executors":[
            {"providerId":"provider","models":models}, {"providerId":"empty","models":[]} ]});
        let mut cursor = None;
        let mut text = String::new();
        let mut ids = Vec::new();
        let mut empty_seen = false;
        loop {
            let page =
                catalog_page(source.clone(), 100, cursor.as_deref(), &codec, "models").unwrap();
            assert!(
                serde_json::to_vec(&page).unwrap().len()
                    < pioneer_protocol::TOOL_RESPONSE_MAX_BYTES
            );
            for fragment in page["fragments"].as_array().unwrap() {
                assert_eq!(fragment["offset"].as_u64().unwrap() as usize, text.len());
                text.push_str(fragment["text"].as_str().unwrap());
            }
            for executor in page["executors"].as_array().unwrap() {
                if executor["providerId"] == "empty" {
                    empty_seen = true;
                }
                ids.extend(
                    executor["models"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|model| model["id"].as_str().unwrap().to_owned()),
                );
            }
            if page["eof"] == true {
                break;
            }
            cursor = Some(page["nextCursor"].as_str().unwrap().to_owned());
            assert!(cursor.as_ref().unwrap().len() <= 16384);
        }
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap()["models"],
            json!([large])
        );
        assert_eq!(
            ids,
            (0..205)
                .map(|n| format!("model-{n:03}"))
                .collect::<Vec<_>>()
        );
        assert!(empty_seen);
    }
}
