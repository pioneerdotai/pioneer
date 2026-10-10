//! Local frozen-input lifetime boundary. Holds do not convey consumer authority.
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_entity::compaction_frozen_history as history;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) type ReaderCounts = Arc<Mutex<BTreeMap<String, usize>>>;

#[derive(Debug)]
pub struct FrozenReadHold {
    readers: ReaderCounts,
    manifest: String,
}
impl FrozenReadHold {
    pub(crate) fn register(readers: &ReaderCounts, manifest: &str) -> Self {
        *readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(manifest.to_owned())
            .or_default() += 1;
        Self {
            readers: readers.clone(),
            manifest: manifest.to_owned(),
        }
    }
}
impl Drop for FrozenReadHold {
    fn drop(&mut self) {
        let mut readers = self
            .readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = readers.get_mut(&self.manifest) {
            *count -= 1;
            if *count == 0 {
                readers.remove(&self.manifest);
            }
        }
    }
}

/// R1: both streams must be valid, even when the caller reads only one.
pub(crate) fn logical_bounds(h: &history::Model) -> Result<(i64, i64)> {
    let bounds = header_bounds(h)?;
    ensure!(
        h.expired == 0,
        "frozen_input_expired: сохранённый input освобождён; точный replay недоступен"
    );
    Ok(bounds)
}

/// Counter validation also applies to expired physical backing metadata.
pub(crate) fn header_bounds(h: &history::Model) -> Result<(i64, i64)> {
    ensure!(
        matches!(h.expired, 0 | 1)
            && matches!(h.ready, 0 | 1)
            && h.message_count >= 0
            && h.import_count >= 0
            && (0..=h.message_count).contains(&h.next_ordinal)
            && (0..=h.import_count).contains(&h.next_import)
            && (h.ready == 0
                || (h.next_ordinal == h.message_count && h.next_import == h.import_count)),
        "invalid frozen history counters"
    );
    Ok(if h.ready == 1 {
        (h.message_count, h.import_count)
    } else {
        (h.next_ordinal, h.next_import)
    })
}

/// Decoded outside database capacity, then checked against the exact header by
/// the authoritative writer. None is verified no-descriptor; unknown legacy
/// sentinel is never representable in a new prepared root.
#[derive(Clone, Debug)]
pub(crate) struct PreparedFrozenRoot {
    descriptor: Option<FrozenHistoryRef>,
    owner: Option<String>,
}
impl PreparedFrozenRoot {
    pub(crate) fn history(json: &str) -> Result<Self> {
        let value: serde_json::Value = serde_json::from_str(json)?;
        if let Some(messages) = value.as_array() {
            ensure!(
                messages.iter().all(|message| {
                    message.is_object()
                        && message
                            .get("content")
                            .is_some_and(serde_json::Value::is_string)
                        && matches!(
                            message.get("role").and_then(serde_json::Value::as_str),
                            Some("system" | "user" | "assistant" | "tool")
                        )
                }),
                "invalid inline conversation history"
            );
            return Ok(Self {
                descriptor: None,
                owner: None,
            });
        }
        let descriptor: FrozenHistoryRef = serde_json::from_value(value)?;
        ensure!(
            descriptor.format == 1
                && !descriptor.manifest_id.is_empty()
                && descriptor.identity_sha256.len() == 64
                && descriptor
                    .identity_sha256
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit()),
            "invalid frozen root descriptor"
        );
        i64::try_from(descriptor.messages)?;
        Ok(Self {
            descriptor: Some(descriptor),
            owner: None,
        })
    }
    pub(crate) fn cli(json: &str, thread_receipt: bool) -> Result<Self> {
        let value: serde_json::Value = serde_json::from_str(json)?;
        ensure!(value.is_object(), "invalid CLI root JSON");
        let key = if thread_receipt {
            "pioneerContext"
        } else {
            "pioneerContextBasis"
        };
        let Some(basis) = value.get(key).filter(|v| !v.is_null()) else {
            return Ok(Self {
                descriptor: None,
                owner: None,
            });
        };
        ensure!(basis.is_object(), "invalid CLI context basis");
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct DeliveredTurn {
            turn_id: String,
            message_revision: u64,
            message_deleted: bool,
            #[serde(default)]
            thread_id: Option<String>,
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct DeliveredSource {
            source_thread_id: String,
            scope: String,
            id: String,
            version: String,
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct BasisIdentity {
            #[serde(default)]
            delivered_turns: Vec<DeliveredTurn>,
            #[serde(default)]
            delivered_sources: Vec<DeliveredSource>,
            #[serde(default)]
            pending_turn: Option<DeliveredTurn>,
        }
        let typed: BasisIdentity = serde_json::from_value(basis.clone())?;
        for turn in typed
            .delivered_turns
            .iter()
            .chain(typed.pending_turn.iter())
        {
            ensure!(
                !turn.turn_id.is_empty() && turn.thread_id.as_ref().is_none_or(|s| !s.is_empty()),
                "invalid CLI basis turn identity"
            );
            let _ = (turn.message_revision, turn.message_deleted);
        }
        for source in typed.delivered_sources {
            ensure!(
                !source.source_thread_id.is_empty()
                    && !source.scope.is_empty()
                    && !source.id.is_empty()
                    && !source.version.is_empty(),
                "invalid CLI basis source identity"
            );
        }
        let history_key = if thread_receipt {
            "contextHistoryJson"
        } else {
            "historyJson"
        };
        let owner_key = if thread_receipt {
            "contextManifestOwnerThreadId"
        } else {
            "manifestOwnerThreadId"
        };
        if thread_receipt {
            for field in ["nativeThreadId", "acceptedTurnId"] {
                ensure!(
                    basis
                        .get(field)
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|s| !s.is_empty()),
                    "invalid CLI receipt identity"
                );
            }
            ensure!(
                basis
                    .get("acceptedTurnRevision")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                    && basis
                        .get("acceptedTurnDeleted")
                        .and_then(serde_json::Value::as_bool)
                        .is_some(),
                "invalid CLI receipt acceptance"
            );
            if let Some(head) = basis.get("continuationHead").filter(|v| !v.is_null()) {
                let head: DeliveredTurn = serde_json::from_value(head.clone())?;
                ensure!(
                    !head.turn_id.is_empty(),
                    "invalid CLI continuation identity"
                );
            }
            ensure!(
                basis
                    .get("contextOwnerThreadId")
                    .filter(|v| !v.is_null())
                    .is_some()
                    == basis.get(history_key).filter(|v| !v.is_null()).is_some(),
                "incomplete CLI context receipt"
            );
            let version = basis.get("version").and_then(|v| v.as_u64());
            ensure!(
                matches!(version, Some(1..=4)),
                "unsupported CLI context receipt"
            );
        }
        if !thread_receipt {
            ensure!(
                basis
                    .get("executionThreadId")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|s| !s.is_empty())
                    && basis.get("pendingTurn").filter(|v| !v.is_null()).is_some(),
                "incomplete CLI sent basis"
            );
        }
        let Some(history) = basis.get(history_key).filter(|v| !v.is_null()) else {
            ensure!(thread_receipt, "CLI sent basis has no history");
            return Ok(Self {
                descriptor: None,
                owner: None,
            });
        };
        let mut root = Self::history(
            history
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("invalid CLI history JSON"))?,
        )?;
        root.owner = basis
            .get(owner_key)
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        // Old receipts used contextOwnerThreadId as the manifest owner.
        if root.owner.is_none() && thread_receipt {
            root.owner = basis
                .get("contextOwnerThreadId")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
        }
        ensure!(
            root.descriptor.is_none() || root.owner.as_ref().is_some_and(|s| !s.is_empty()),
            "CLI frozen basis owner is missing"
        );
        Ok(root)
    }
    pub(crate) fn locator(&self) -> Option<String> {
        self.descriptor.as_ref().map(|d| d.manifest_id.clone())
    }
    pub(crate) async fn verify<C: ConnectionTrait>(&self, db: &C, workspace: &str) -> Result<()> {
        if let Some(descriptor) = &self.descriptor {
            let h = exact_header(db, workspace, descriptor).await?;
            ensure!(
                self.owner
                    .as_ref()
                    .is_none_or(|owner| owner == &h.owner_thread),
                "frozen root owner mismatch"
            );
        }
        Ok(())
    }
}

pub(crate) async fn exact_header<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    d: &FrozenHistoryRef,
) -> Result<history::Model> {
    ensure!(d.format == 1, "unsupported frozen history format");
    let h = history::Entity::find_by_id(&d.manifest_id)
        .filter(history::Column::WorkspaceId.eq(workspace))
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen input identity is unavailable"))?;
    logical_bounds(&h)?;
    ensure!(
        h.ready == 1
            && h.identity_sha256 == d.identity_sha256
            && h.message_count == i64::try_from(d.messages)?,
        "frozen input identity mismatch or incomplete capture"
    );
    Ok(h)
}

impl crate::CrudStore {
    pub(crate) async fn acquire_frozen_header(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<(history::Model, FrozenReadHold)> {
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let header = exact_header(&tx, workspace, descriptor).await?;
            let hold = FrozenReadHold::register(&self.frozen_readers, &descriptor.manifest_id);
            tx.commit().await?;
            Ok((header, hold))
        })
        .await
    }
    pub async fn compaction_acquire_frozen_history(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<FrozenReadHold> {
        let (header, hold) = self.acquire_frozen_header(workspace, descriptor).await?;
        let mut scan = LayoutScan::new(header, 0, 2)?;
        while !scan.done() {
            scan.step(self).await?;
        }
        Ok(hold)
    }
}

/// One layout metadata page per step; reused by foreground acquire, proof
/// extraction and lifetime maintenance. No DB capacity is retained.
pub(crate) struct LayoutScan {
    h: history::Model,
    bounds: (i64, i64),
    kind: i64,
    end_kind: i64,
    layout_read: bool,
    after: i64,
    next: i64,
}
impl LayoutScan {
    pub(crate) fn new(h: history::Model, start_kind: i64, end_kind: i64) -> Result<Self> {
        ensure!(
            0 <= start_kind && start_kind < end_kind && end_kind <= 2,
            "invalid layout scan kinds"
        );
        let bounds = header_bounds(&h)?;
        Ok(Self {
            h,
            bounds,
            kind: start_kind,
            end_kind,
            layout_read: false,
            after: -1,
            next: 0,
        })
    }
    pub(crate) fn done(&self) -> bool {
        self.kind == self.end_kind
    }
    pub(crate) async fn step(&mut self, store: &crate::CrudStore) -> Result<()> {
        use pioneer_entity::{compaction_frozen_layout as layout, compaction_frozen_span as span};
        use sea_orm::{QueryOrder, QuerySelect};
        if self.done() {
            return Ok(());
        }
        let bound = if self.kind == 0 {
            self.bounds.0
        } else {
            self.bounds.1
        };
        if !self.layout_read {
            let l = layout::Entity::find_by_id((self.h.id.clone(), self.kind))
                .one(&store.connection)
                .await?;
            let active = if let Some(l) = l {
                ensure!(
                    matches!(l.active, 0 | 1)
                        && matches!(l.pending, 0 | 1)
                        && l.failed == 0
                        && (0..=bound).contains(&l.compared)
                        && (0..=bound).contains(&l.copy_next)
                        && l.copy_to.is_none_or(|end| (0..=bound).contains(&end))
                        && (0..=bound).contains(&l.cleanup_to)
                        && (0..=l.cleanup_to).contains(&l.cleanup_next),
                    "invalid frozen layout state"
                );
                l.active == 1
            } else {
                ensure!(
                    self.h.storage_registered == 0,
                    "registered frozen layout missing"
                );
                false
            };
            self.layout_read = active;
            if !active {
                self.advance();
            }
            return Ok(());
        }
        let page = span::Entity::find()
            .filter(span::Column::ManifestId.eq(&self.h.id))
            .filter(span::Column::Kind.eq(self.kind))
            .filter(span::Column::Start.gt(self.after))
            .order_by_asc(span::Column::Start)
            .limit(64)
            .all(&store.connection)
            .await?;
        for s in &page {
            ensure!(
                s.start == self.next
                    && s.end > s.start
                    && s.end <= bound
                    && !s.source_manifest.is_empty(),
                "invalid frozen logical span"
            );
            self.next = s.end;
            self.after = s.start;
        }
        if page.len() < 64 {
            ensure!(self.next == bound, "frozen layout is incomplete");
            self.advance();
        }
        Ok(())
    }
    fn advance(&mut self) {
        self.kind += 1;
        self.layout_read = false;
        self.after = -1;
        self.next = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reader_drop_releases_same_clone_family_without_database_cleanup() {
        let readers: ReaderCounts = Default::default();
        let scoped = readers.clone();
        let first = FrozenReadHold::register(&readers, "m");
        let second = FrozenReadHold::register(&scoped, "m");
        assert_eq!(readers.lock().unwrap().get("m"), Some(&2));
        drop(first);
        assert_eq!(scoped.lock().unwrap().get("m"), Some(&1));
        drop(second);
        assert!(readers.lock().unwrap().is_empty());
    }
    #[test]
    fn root_shapes_distinguish_no_basis_from_malformed_new_basis() {
        assert_eq!(PreparedFrozenRoot::history("[]").unwrap().locator(), None);
        assert_eq!(
            PreparedFrozenRoot::cli("{\"provider\":\"claude\"}", true)
                .unwrap()
                .locator(),
            None
        );
        assert!(PreparedFrozenRoot::history("null").is_err());
        assert!(PreparedFrozenRoot::history("[42]").is_err());
        assert!(PreparedFrozenRoot::cli("{\"pioneerContext\":{\"version\":4}}", true).is_err());
        assert!(PreparedFrozenRoot::cli("{\"pioneerContextBasis\":{}}", false).is_err());
        assert!(PreparedFrozenRoot::cli("{\"pioneerContext\":{\"version\":99}}", true).is_err());
        let descriptor = FrozenHistoryRef {
            format: 1,
            manifest_id: "m".into(),
            messages: 2,
            identity_sha256: "a".repeat(64),
        };
        let sent = serde_json::json!({"pioneerContextBasis":{"executionThreadId":"child","pendingTurn":{"turnId":"turn","messageRevision":1,"messageDeleted":false},"manifestOwnerThreadId":"parent","historyJson":serde_json::to_string(&descriptor).unwrap()}});
        assert_eq!(
            PreparedFrozenRoot::cli(&sent.to_string(), false)
                .unwrap()
                .locator(),
            Some("m".into())
        );
    }
}
