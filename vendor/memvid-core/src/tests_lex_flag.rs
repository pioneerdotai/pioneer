#[cfg(test)]
mod tests {
    use crate::{Memvid, PutOptions, SearchRequest, run_serial_test};
    use std::sync::Mutex;
    use tempfile::NamedTempFile;

    #[test]
    #[cfg(not(target_os = "windows"))] // Windows file locking prevents tempfile cleanup
    fn test_lex_persists_and_search_works() {
        run_serial_test(|| {
            let temp = NamedTempFile::new().unwrap();
            let path = temp.path();

            // Phase 1: create, enable lex, ingest docs with periodic seals
            {
                let mut mem = Memvid::create(path).unwrap();
                mem.enable_lex().unwrap();

                for i in 0..1000 {
                    let content = format!(
                        "Document {i} with searchable content about technology and artificial intelligence systems"
                    );
                    let opts = PutOptions::builder()
                        .uri(format!("mv2://doc/{i}"))
                        .search_text(content.clone())
                        .build();
                    mem.put_bytes_with_options(content.as_bytes(), opts)
                        .unwrap();
                    if (i + 1) % 100 == 0 {
                        mem.commit().unwrap();
                    }
                }
                mem.commit().unwrap();

                // Index is present in TOC
                assert!(
                    mem.toc.segment_catalog.lex_enabled,
                    "lex_enabled should be set in catalog"
                );
                assert!(
                    !mem.toc.segment_catalog.tantivy_segments.is_empty(),
                    "tantivy_segments should not be empty"
                );
            }

            // Phase 2: reopen RO and search
            {
                let mut mem = Memvid::open_read_only(path).unwrap();
                assert!(mem.lex_enabled, "lex_enabled should persist after reopen");
                assert!(
                    mem.toc.segment_catalog.lex_enabled,
                    "catalog.lex_enabled should persist after reopen"
                );
                assert!(
                    !mem.toc.segment_catalog.tantivy_segments.is_empty(),
                    "tantivy_segments should persist after reopen"
                );

                let resp = mem
                    .search(SearchRequest {
                        query: "artificial intelligence".into(),
                        top_k: 10,
                        snippet_chars: 200,
                        uri: None,
                        scope: None,
                        cursor: None,
                        #[cfg(feature = "temporal_track")]
                        temporal: None,
                        as_of_frame: None,
                        as_of_ts: None,
                        no_sketch: false,
                        acl_context: None,
                        acl_enforcement_mode: crate::types::AclEnforcementMode::Audit,
                    })
                    .expect("search must succeed");

                assert!(
                    !resp.hits.is_empty(),
                    "expected some hits for 'artificial intelligence'"
                );
                let first_hit = &resp.hits[0];
                let text_lower = first_hit.text.to_lowercase();
                assert!(
                    text_lower.contains("artificial") || text_lower.contains("intelligence"),
                    "first hit should contain search terms, got: {}",
                    first_hit.text
                );
            }
        });
    }

    /// Regression test for GitHub issue #201:
    /// Lexical index not enabled when Memvid is wrapped in a Mutex.
    /// The wrapper pattern acquires the lock, performs an operation, releases
    /// the lock — mimicking the typical tokio::sync::Mutex usage in async code.
    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_lex_works_through_mutex_wrapper() {
        run_serial_test(|| {
            let temp = NamedTempFile::new().unwrap();
            let path = temp.path();

            // Wrap Memvid in a Mutex, exactly like an async wrapper would
            let wrapper = Mutex::new(Memvid::create(path).unwrap());

            // Step 1: enable_lex while holding the lock, then release
            {
                let mut mem = wrapper.lock().unwrap();
                mem.enable_lex().unwrap();
            }

            // Step 2: commit while holding the lock (separate acquisition)
            {
                let mut mem = wrapper.lock().unwrap();
                mem.commit().unwrap();
            }

            // Step 3: put data while holding the lock
            {
                let mut mem = wrapper.lock().unwrap();
                let opts = PutOptions::builder()
                    .uri("mv2://test/login".to_string())
                    .search_text("user clicked login button on the authentication page".to_string())
                    .build();
                mem.put_bytes_with_options(b"login event data", opts)
                    .unwrap();
            }

            // Step 4: commit while holding the lock
            {
                let mut mem = wrapper.lock().unwrap();
                mem.commit().unwrap();
            }

            // Step 5: search while holding the lock — this was failing in #201
            {
                let mut mem = wrapper.lock().unwrap();
                let resp = mem
                    .search(SearchRequest {
                        query: "login".into(),
                        top_k: 10,
                        snippet_chars: 200,
                        uri: None,
                        scope: None,
                        cursor: None,
                        #[cfg(feature = "temporal_track")]
                        temporal: None,
                        as_of_frame: None,
                        as_of_ts: None,
                        no_sketch: false,
                        acl_context: None,
                        acl_enforcement_mode: crate::types::AclEnforcementMode::Audit,
                    })
                    .expect("search must succeed through mutex wrapper");

                assert!(
                    !resp.hits.is_empty(),
                    "Should find the frame with 'login' in the message"
                );
            }

            // Step 6: search_lex uses the legacy LexIndex, which may not be
            // populated when only Tantivy is active. Verify it doesn't panic.
            {
                let mut mem = wrapper.lock().unwrap();
                let _ = mem.search_lex("login", 10);
                // Result may be Ok (if legacy index was built) or Err (if only Tantivy).
                // The important thing is it doesn't panic.
            }
        });
    }

    /// Regression test for #201: enable_lex, put, commit, search — all in one lock scope.
    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_lex_works_single_scope() {
        run_serial_test(|| {
            let temp = NamedTempFile::new().unwrap();
            let path = temp.path();

            let mut mem = Memvid::create(path).unwrap();
            mem.enable_lex().unwrap();

            let opts = PutOptions::builder()
                .uri("mv2://test/login".to_string())
                .search_text("user clicked login button on the authentication page".to_string())
                .build();
            mem.put_bytes_with_options(b"login event data", opts)
                .unwrap();
            mem.commit().unwrap();

            let resp = mem
                .search(SearchRequest {
                    query: "login".into(),
                    top_k: 10,
                    snippet_chars: 200,
                    uri: None,
                    scope: None,
                    cursor: None,
                    #[cfg(feature = "temporal_track")]
                    temporal: None,
                    as_of_frame: None,
                    as_of_ts: None,
                    no_sketch: false,
                    acl_context: None,
                    acl_enforcement_mode: crate::types::AclEnforcementMode::Audit,
                })
                .expect("search must succeed");

            assert!(
                !resp.hits.is_empty(),
                "Should find the frame with 'login' in the message"
            );
        });
    }
}
