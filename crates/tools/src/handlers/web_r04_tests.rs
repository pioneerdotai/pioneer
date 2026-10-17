use super::*;
use futures_util::stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn decoded_byte_limit_precedes_allocation_and_is_shared_across_stages() {
    let mut remaining = 8;
    let first = read_stream_bounded(
        stream::iter([Ok::<_, ()>(b"12345".as_slice())]),
        &mut remaining,
    )
    .await
    .unwrap();
    assert_eq!(first, b"12345");
    assert_eq!(remaining, 3);
    assert!(matches!(
        read_stream_bounded(
            stream::iter([Ok::<_, ()>(b"1234".as_slice())]),
            &mut remaining
        )
        .await,
        Err(ToolError::ResponseLimitExceeded)
    ));
    // UTF-8 characters do not substitute for byte accounting.
    let mut remaining = 3;
    assert!(matches!(
        read_stream_bounded(stream::iter([Ok::<_, ()>("😀".as_bytes())]), &mut remaining).await,
        Err(ToolError::ResponseLimitExceeded)
    ));
}

#[tokio::test]
async fn compressed_and_missing_length_responses_cannot_bypass_decoded_byte_limit() {
    let gzip: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 237, 193, 1, 13, 0, 0, 0, 194, 160, 218, 143, 111, 14,
        55, 160, 0, 0, 0, 0, 0, 0, 0, 128, 119, 3, 197, 5, 57, 18, 0, 32, 0, 0,
    ];
    for compressed in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = if compressed {
            gzip.to_vec()
        } else {
            vec![b'x'; 8192]
        };
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            socket.read(&mut request).await.unwrap();
            let header = if compressed {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
            } else {
                "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_owned()
            };
            socket.write_all(header.as_bytes()).await.unwrap();
            let _ = socket.write_all(&body).await;
        });
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        assert!(matches!(
            read_search_body(response, &mut 1024).await,
            Err(ToolError::ResponseLimitExceeded)
        ));
        server.await.unwrap();
    }
}

#[test]
fn json_and_html_traversal_have_fixed_depth_and_cardinality_bounds() {
    let mut nested = serde_json::json!({"Text":"result"});
    for _ in 0..33 {
        nested = serde_json::json!({"Topics":[nested]});
    }
    assert!(matches!(
        parse_search_json(&serde_json::to_vec(&nested).unwrap()),
        Err(ToolError::ResponseLimitExceeded)
    ));
    let wide = serde_json::json!({"RelatedTopics": vec![serde_json::json!({}); 4096]});
    assert!(matches!(
        parse_search_json(&serde_json::to_vec(&wide).unwrap()),
        Err(ToolError::ResponseLimitExceeded)
    ));
    assert!(matches!(
        parse_duckduckgo_html_results(&"<div>".repeat(70), 64, 3),
        Err(ToolError::ResponseLimitExceeded)
    ));
    assert!(matches!(
        parse_duckduckgo_html_results(&"<div></div>".repeat(16_384), 64, 3),
        Err(ToolError::ResponseLimitExceeded)
    ));
    assert!(parse_search_json(br#"{"RelatedTopics":[]}"#).is_ok());
}

#[test]
fn html_collection_and_related_topic_traversal_stop_at_result_limit() {
    let html =
        "<div class='result'><a class='result__a' href='https://example.org'>Title</a></div>"
            .repeat(100);
    assert_eq!(
        parse_duckduckgo_html_results(&html, 64, 3).unwrap().len(),
        3
    );
    let topics = vec![serde_json::json!({"FirstURL":"https://example.org", "Text":"Title"}); 100];
    let mut calls = 0;
    assert!(!collect_related_topics(&topics, &mut |_, _, _| {
        calls += 1;
        calls < 3
    }));
    assert_eq!(calls, 3);
}
