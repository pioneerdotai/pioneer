use anyhow::Result;
use pioneer_crud_memory::episodic_search::{
    ThreadEpisodicSearchAccess, ThreadEpisodicSearchEmbedding, ThreadEpisodicSearchQuery, search,
};
use pioneer_postgres_test::with_database;
use sea_orm::ConnectionTrait;

#[tokio::test]
async fn episodic_search_ranks_deduplicates_and_scopes_lexical_and_vector_hits() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        db.execute_unprepared(r#"
            INSERT INTO workspace (id, name) VALUES ('workspace', 'Workspace'), ('outside', 'Other');
            INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
            VALUES ('thread', 'workspace', '', 'chat', 'model', 'provider', 'active'),
                   ('private', 'outside', '', 'chat', 'model', 'provider', 'active');
            INSERT INTO turn (id, thread_id, status)
            VALUES ('turn', 'thread', 'completed'), ('foreign-turn', 'private', 'completed');
            INSERT INTO thread_episodic_embedding_artifacts (
                id, workspace_id, pipeline_identity_hash, input_hash, provider_id, model,
                dimension, normalized, embedding, created_at, last_used_at
            ) VALUES
                ('same', 'workspace', 'pipeline', 'in1', 'provider', 'embedding', 2, true, '[1,0]', now(), now()),
                ('far', 'workspace', 'pipeline', 'in2', 'provider', 'embedding', 2, true, '[0,1]', now(), now()),
                ('foreign', 'outside', 'pipeline', 'in1', 'provider', 'embedding', 2, true, '[1,0]', now(), now());
            INSERT INTO thread_episodic_items (
                id, workspace_id, thread_id, turn_id, item_id, source_actor_role,
                source_runtime_kind, source_context_json, visibility, status, text_hash,
                source_text_hash, token_estimate, search_text, embedding_artifact_id, updated_at
            ) VALUES
              ('best', 'workspace','thread','turn','m1','user','native','"user_visible_thread_item"',
               'user_visible','active','hash1','hash',4,'CAFÉ Straße','same', '2026-11-19T20:00:00Z'),
              ('older', 'workspace','thread','turn','m1','user','native','"user_visible_thread_item"',
               'user_visible','active','oldhash','hash',4,'Cafe older','same', '2026-11-19T19:00:00Z'),
              ('semantic', 'workspace','thread','turn','m2','user','native','"user_visible_thread_item"',
               'user_visible','active','hash2','hash',4,'different word','far', '2026-11-19T20:00:00Z'),
              ('other', 'outside','private','foreign-turn','m3','user','native','"user_visible_thread_item"',
               'user_visible','active','hash3','hash',4,'Cafe foreign','foreign', '2026-11-19T20:00:00Z'),
              ('pending', 'workspace','thread','turn','m4','user','native','"user_visible_thread_item"',
               'user_visible','pending_index','hash4','hash',4,NULL,NULL, '2026-11-19T20:00:00Z');
        "#).await?;
        let mut query = ThreadEpisodicSearchQuery {
            workspace_id: "workspace".into(),
            thread_id: None,
            access: ThreadEpisodicSearchAccess::default(),
            query: "cafe".into(),
            lexical: true,
            embedding: None,
            created_after_unix: None,
            offset: 0,
            limit: 1,
            snippet_chars: 16,
        };
        let lexical = search(db, &query).await?;
        assert!(lexical.incomplete_sources);
        assert_eq!(lexical.rows.len(), 1);
        assert_eq!(lexical.rows[0].item.id, "best");
        assert_eq!(lexical.rows[0].lexical_rank, Some(1));
        assert_eq!(lexical.rows[0].semantic_rank, None);

        query.embedding = Some(ThreadEpisodicSearchEmbedding {
            provider_id: "provider".into(),
            model: "embedding".into(),
            dimension: 2,
            normalized: true,
            vector: vec![1., 0.],
        });
        let hybrid = search(db, &query).await?;
        assert_eq!(hybrid.rows[0].item.id, "best");
        assert_eq!(hybrid.rows[0].semantic_rank, Some(1));
        query.offset = 1;
        let next = search(db, &query).await?;
        assert_eq!(next.rows.len(), 1);
        assert_eq!(next.rows[0].item.id, "semantic");
        assert_eq!(next.rows[0].semantic_rank, Some(2));

        query.offset = 2;
        let empty = search(db, &query).await?;
        assert!(empty.incomplete_sources);
        assert!(empty.rows.is_empty());
        query.offset = 0;
        query.access.principal_id = Some("no-membership".into());
        assert!(search(db, &query).await?.rows.is_empty());
        Ok(())
    }).await
}
