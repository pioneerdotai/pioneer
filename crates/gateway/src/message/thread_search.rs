//! Gateway-owned bounded search snapshots. Only source references survive a
//! request; every page restores content and authorization from Pioneer again.
use super::{
    MessageProcessor, thread_read_cursor::ThreadReadCursorCodec,
    thread_read_tools::ThreadReadAuthority,
};
use crate::thread_episodic::search::{SearchSource, SourceSearchOutput};
use anyhow::{Result, anyhow, ensure};
use pioneer_agent::TurnToolContext;
use pioneer_memory::episodic_search::{
    EpisodicSearchLimitations, EpisodicSearchScope, EpisodicSourceCandidate,
};
use pioneer_protocol::ThreadsSearchParams;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

const SESSION_TTL: Duration = Duration::from_secs(300);
const MAX_SESSIONS: usize = 64;
const MAX_SESSION_BYTES: usize = 4 * 1024 * 1024;
const PAGE_SOURCES: u32 = 20;

#[derive(Default)]
pub(super) struct ThreadSearchSessions {
    entries: BTreeMap<String, SearchSession>,
}
#[derive(Clone)]
struct SearchSession {
    scope: String,
    references: Vec<EpisodicSourceCandidate>,
    limitations: EpisodicSearchLimitations,
    created: Instant,
    bytes: usize,
}
impl ThreadSearchSessions {
    fn cleanup(&mut self, now: Instant) {
        self.entries
            .retain(|_, session| now.duration_since(session.created) < SESSION_TTL);
    }
    fn insert(&mut self, session: SearchSession) -> Result<String> {
        self.cleanup(Instant::now());
        ensure!(
            session.bytes <= MAX_SESSION_BYTES,
            "search selection exceeds session budget"
        );
        while self.entries.len() >= MAX_SESSIONS
            || self.entries.values().map(|s| s.bytes).sum::<usize>() + session.bytes
                > MAX_SESSION_BYTES
        {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, s)| s.created)
                .map(|(id, _)| id.clone());
            if let Some(oldest) = oldest {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
        let id = pioneer_protocol::generate_id(21);
        self.entries.insert(id.clone(), session);
        Ok(id)
    }
    fn get(&mut self, id: &str, scope: &str) -> Result<SearchSession> {
        self.cleanup(Instant::now());
        let session = self
            .entries
            .get(id)
            .ok_or_else(|| anyhow!("search session expired; restart search without cursor"))?;
        ensure!(session.scope == scope, "invalid cursor scope");
        Ok(session.clone())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchCursor {
    session: String,
    position: usize,
}

impl MessageProcessor {
    pub(crate) fn crud_store_for_episodic(&self) -> &pioneer_crud::CrudStore {
        self.crud_store.as_ref()
    }

    pub(crate) async fn episodic_execution_context(
        &self,
        workspace_id: &str,
        turn_id: &str,
    ) -> Result<TurnToolContext> {
        let (thread_id, actual_workspace) = self
            .crud_store
            .get_turn_location(turn_id)
            .await?
            .ok_or_else(|| anyhow!("execution unavailable"))?;
        ensure!(actual_workspace == workspace_id, "execution unavailable");
        Ok(TurnToolContext {
            workspace_id: workspace_id.to_owned(),
            thread_id,
            turn_id: turn_id.to_owned(),
        })
    }

    pub(crate) async fn episodic_execution_scope(&self, context: &TurnToolContext) -> Option<bool> {
        self.thread_read_authority(context)
            .await
            .ok()
            .map(|authority| authority.observe_workspace)
    }

    pub(crate) async fn search_thread_sources(
        &self,
        context: &TurnToolContext,
        query: &str,
        current_thread: Option<&str>,
    ) -> Result<SourceSearchOutput> {
        let authority = self.thread_read_authority(context).await?;
        let scope = EpisodicSearchScope {
            workspace_id: context.workspace_id.clone(),
            thread_id: current_thread.map(str::to_owned),
        };
        let mut output = self
            .thread_episodic_recall_service
            .search_sources(query, scope, |thread| {
                let authority = &authority;
                async move { self.thread_source_readable(authority, &thread).await }
            })
            .await?;
        let authority = self.thread_read_authority(context).await?;
        let mut verified = Vec::new();
        for source in output.sources {
            match self
                .thread_source_readable(&authority, &source.reference.thread_id)
                .await
            {
                Ok(true) => {}
                _ => {
                    output.limitations.coverage_limited = true;
                    continue;
                }
            }
            match self
                .thread_episodic_recall_service
                .validate_search_source(&source.reference)
                .await
            {
                Ok(Some(mut current)) => {
                    current.score = source.score;
                    verified.push(current);
                }
                _ => output.limitations.coverage_limited = true,
            }
        }
        output.sources = verified;
        Ok(output)
    }

    pub(super) async fn thread_search_page(
        &self,
        context: &TurnToolContext,
        authority: &ThreadReadAuthority,
        params: ThreadsSearchParams,
        codec: &ThreadReadCursorCodec,
    ) -> Result<Value> {
        ensure!(
            (1..=PAGE_SOURCES).contains(&params.limit),
            "search limit must be between 1 and 20"
        );
        ensure!(
            !params.query.trim().is_empty() && params.query.len() <= 4096,
            "query must contain 1..4096 bytes"
        );
        let scope = json!([
            authority.scope("search", "", false, params.limit),
            params.query,
            context.thread_id,
            context.turn_id,
            authority.root_thread_id,
            authority.observe_workspace,
            authority.observe_children
        ])
        .to_string();
        let (cursor, session) = if let Some(cursor) = params.cursor.as_deref() {
            let cursor: SearchCursor = codec.decode(cursor, &scope)?;
            let session = self
                .thread_search_sessions
                .lock()
                .map_err(|_| anyhow!("search unavailable"))?
                .get(&cursor.session, &scope)?;
            ensure!(
                cursor.position <= session.references.len(),
                "invalid cursor position"
            );
            (cursor, session)
        } else {
            let output = self
                .search_thread_sources(context, &params.query, None)
                .await?;
            let mut limitations = output.limitations;
            let references = group_selection(output.sources, &mut limitations);
            let bytes = references
                .iter()
                .map(|r| {
                    r.source_id.len()
                        + r.workspace_id.len()
                        + r.thread_id.len()
                        + r.turn_id.len()
                        + r.item_id.len()
                        + r.source_version.len()
                        + 256
                })
                .sum::<usize>()
                + scope.len()
                + 256;
            let session = SearchSession {
                scope,
                references,
                limitations,
                created: Instant::now(),
                bytes,
            };
            let id = self
                .thread_search_sessions
                .lock()
                .map_err(|_| anyhow!("search unavailable"))?
                .insert(session.clone())?;
            (
                SearchCursor {
                    session: id,
                    position: 0,
                },
                session,
            )
        };
        let mut limitations = session.limitations.clone();
        let mut position = cursor.position;
        let mut threads = Vec::<Value>::new();
        let mut emitted = 0;
        // Reauthorize the execution on every page, including the initial page.
        let authority = self.thread_read_authority(context).await?;
        while position < session.references.len() && emitted < params.limit {
            let reference = &session.references[position];
            match self
                .thread_source_readable(&authority, &reference.thread_id)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    position += 1;
                    continue;
                }
                Err(_) => {
                    limitations.coverage_limited = true;
                    position += 1;
                    continue;
                }
            }
            let source = self
                .thread_episodic_recall_service
                .validate_search_source(reference)
                .await;
            let source = match source {
                Ok(Some(source)) => source,
                Ok(None) => {
                    position += 1;
                    continue;
                }
                Err(_) => {
                    limitations.coverage_limited = true;
                    position += 1;
                    continue;
                }
            };
            let Some(thread) = self
                .crud_store
                .get_thread_by_id(&reference.thread_id)
                .await?
            else {
                position += 1;
                continue;
            };
            if thread.workspace_id != context.workspace_id {
                position += 1;
                continue;
            }
            let mut anchor = json!({"turnId":reference.turn_id});
            if !reference.item_id.is_empty() {
                anchor["itemId"] = json!(reference.item_id);
            }
            let fragment = json!({
                "snippet":source.text.chars().take(360).collect::<String>(),
                "source":{"threadId":reference.thread_id,"anchor":anchor},
                "provenance":{"actorRole":source.provenance.source_actor_role,"sourceContext":source.provenance.source_context}
            });
            let mut proposed = threads.clone();
            if let Some(card) = proposed
                .iter_mut()
                .find(|card| card["threadId"] == reference.thread_id)
            {
                card["fragments"]
                    .as_array_mut()
                    .expect("fragments array")
                    .push(fragment);
            } else {
                proposed.push(json!({"threadId":reference.thread_id,"title":thread.name.map(|s| s.chars().take(256).collect::<String>()),"fragments":[fragment]}));
            }
            let encoded = serde_json::to_string(&proposed)?;
            if encoded.len() > 24 * 1024 || pioneer_compaction::text_tokens(&encoded) > 3000 {
                if threads.is_empty() {
                    limitations.selection_limited = true;
                    position += 1;
                    continue;
                }
                break;
            }
            threads = proposed;
            emitted += 1;
            position += 1;
        }
        let eof = position == session.references.len();
        let next = if eof {
            None
        } else {
            Some(codec.encode(
                &SearchCursor {
                    session: cursor.session,
                    position,
                },
                &session.scope,
            )?)
        };
        // End of snapshot is independent of corpus coverage and semantic approximation.
        Ok(json!({"threads":threads,"nextCursor":next,"eof":eof,"limitations":limitations}))
    }
}

fn group_selection(
    sources: Vec<SearchSource>,
    limitations: &mut EpisodicSearchLimitations,
) -> Vec<EpisodicSourceCandidate> {
    let mut groups = Vec::<(String, Vec<EpisodicSourceCandidate>)>::new();
    for source in sources {
        let thread_id = &source.reference.thread_id;
        if let Some((_, refs)) = groups.iter_mut().find(|(id, _)| id == thread_id) {
            if refs.len() < 3 {
                refs.push(source.reference);
            } else {
                limitations.selection_limited = true;
            }
        } else {
            groups.push((thread_id.clone(), vec![source.reference]));
        }
    }
    groups.into_iter().flat_map(|(_, refs)| refs).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expired_and_evicted_sessions_require_a_new_search_and_do_not_change_coverage() {
        let mut sessions = ThreadSearchSessions::default();
        let session = |created| SearchSession {
            scope: "bound-scope".into(),
            references: Vec::new(),
            limitations: EpisodicSearchLimitations {
                coverage_limited: true,
                ..Default::default()
            },
            created,
            bytes: 512,
        };
        let expired = sessions
            .insert(session(Instant::now() - SESSION_TTL))
            .unwrap();
        assert!(
            sessions
                .get(&expired, "bound-scope")
                .err()
                .unwrap()
                .to_string()
                .contains("restart search")
        );
        let evicted = sessions.insert(session(Instant::now())).unwrap();
        for _ in 0..MAX_SESSIONS {
            sessions.insert(session(Instant::now())).unwrap();
        }
        assert!(sessions.get(&evicted, "bound-scope").is_err());
        let id = sessions.insert(session(Instant::now())).unwrap();
        assert!(sessions.get(&id, "another-principal-or-query").is_err());
        assert!(
            sessions
                .get(&id, "bound-scope")
                .unwrap()
                .limitations
                .coverage_limited
        );
        assert!(sessions.entries.len() <= MAX_SESSIONS);
        assert!(sessions.entries.values().map(|s| s.bytes).sum::<usize>() <= MAX_SESSION_BYTES);
    }
}
