use crate::CrudStore;
use anyhow::Result;
use pioneer_crud_compaction::frozen_storage_reads as reads;
use std::collections::VecDeque;

pub(super) use reads::ExpiryHeader;

pub(super) async fn discard_page(
    store: &CrudStore,
    after: &str,
    upper: &str,
) -> Result<VecDeque<(Option<String>, bool)>> {
    reads::discard_page(&store.connection, after, upper).await
}

pub(super) async fn discard_preflight(store: &CrudStore, id: &str) -> Result<bool> {
    reads::discard_preflight(&store.connection, id).await
}

pub(super) async fn expiry_page(
    store: &CrudStore,
    after: &str,
    upper: &str,
) -> Result<VecDeque<ExpiryHeader>> {
    reads::expiry_page(&store.connection, after, upper).await
}

pub(super) async fn directly_held(store: &CrudStore, id: &str) -> Result<bool> {
    reads::directly_held(&store.connection, id).await
}
