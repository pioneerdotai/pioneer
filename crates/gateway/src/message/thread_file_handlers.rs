use pioneer_protocol::{
    INVALID_PARAMS_CODE, INVALID_REQUEST_CODE, JsonRpcErrorResponse, JsonRpcResponse,
    MarkdownBlock, MarkdownDocument, MarkdownInline, MarkdownMarkKind, RequestId,
    ThreadFileViewGrantCreateParams, ThreadFileViewGrantCreateResponse, TurnItem,
    constants::methods,
};

use super::*;
use crate::authorization::{AuthorizationExternalError, AuthorizedThread};
use crate::message::markdown::parse_markdown_document;
use crate::thread_file_delivery::{ThreadFileDeliveryError, prepare_thread_file};
use crate::view_grants::{
    ThreadFileViewGrantScope, ViewGrantDisposition, ViewGrantError, ViewGrantScope,
    ViewGrantSubject,
};

const MAX_SCOPE_ID_BYTES: usize = 128;
const MAX_HREF_BYTES: usize = 4096;

impl MessageProcessor {
    pub(crate) async fn thread_file_view_grant_create(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedThread,
        request_id: RequestId,
        params: ThreadFileViewGrantCreateParams,
    ) {
        let connection_id = request_context.connection_id();
        if !valid_scope_id(params.thread_id.as_str())
            || !valid_scope_id(params.turn_id.as_str())
            || !valid_scope_id(params.item_id.as_str())
            || !valid_href(params.href.as_str())
            || authorization.thread_id() != params.thread_id
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    "thread file view requires bounded thread, turn, item and link identifiers",
                ),
            )
            .await;
            return;
        }

        let Some((workspace_id, _turn)) = (match self
            .crud_store
            .get_turn(params.thread_id.as_str(), params.turn_id.as_str())
            .await
        {
            Ok(turn) => turn,
            Err(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        }) else {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        };
        if workspace_id != authorization.workspace_id() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let item = match self
            .crud_store
            .get_turn_item(params.turn_id.as_str(), params.item_id.as_str())
            .await
        {
            Ok(Some(item)) if item.item_id() == params.item_id => item,
            Ok(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };
        if !turn_item_contains_exact_link(&item, params.href.as_str()) {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let security = match self
            .crud_store
            .get_turn_execution_security_snapshot(params.turn_id.as_str())
            .await
        {
            Ok(Some(record)) => record.snapshot,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };
        let prepared = match prepare_thread_file(security.sandbox.cwd, params.href.clone()).await {
            Ok(prepared) => prepared,
            Err(
                ThreadFileDeliveryError::InvalidReference
                | ThreadFileDeliveryError::OutsideWorkspace
                | ThreadFileDeliveryError::NotFound
                | ThreadFileDeliveryError::NotText,
            ) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(ThreadFileDeliveryError::TooLarge) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        "thread file exceeds the 10 MiB view limit",
                    ),
                )
                .await;
                return;
            }
            Err(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };

        let Some(service) = self.view_grant_service() else {
            self.send_error(
                connection_id,
                AuthorizationExternalError::Unavailable.response(request_id),
            )
            .await;
            return;
        };
        let issued = service.mint(ViewGrantScope {
            gateway_id: request_context.principal().gateway_id.clone(),
            principal_id: request_context.principal().principal_id.clone(),
            auth_session_id: request_context.principal().session_id.clone(),
            disposition: ViewGrantDisposition::Inline,
            subject: ViewGrantSubject::ThreadFile(ThreadFileViewGrantScope {
                workspace_id,
                thread_id: params.thread_id.clone(),
                turn_id: params.turn_id.clone(),
                item_id: params.item_id.clone(),
                canonical_root: prepared.canonical_root,
                canonical_path: prepared.canonical_path,
                file_name: prepared.file_name.clone(),
                content_type: prepared.content_type.clone(),
                size_bytes: prepared.size_bytes,
                line: prepared.line,
                column: prepared.column,
            }),
        });
        let issued = match issued {
            Ok(issued) => issued,
            Err(ViewGrantError::Capacity) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        "view grant capacity is temporarily unavailable",
                    ),
                )
                .await;
                return;
            }
            Err(_) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };

        let response = ThreadFileViewGrantCreateResponse {
            relative_url: issued.secret.into_relative_url(),
            expires_at: issued.expires_at_unix,
            file_name: prepared.file_name,
            content_type: prepared.content_type,
            size_bytes: prepared.size_bytes,
            line: prepared.line,
            column: prepared.column,
        };
        let rpc_response = match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => response,
            Err(_) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        "failed to encode thread file view response",
                    ),
                )
                .await;
                return;
            }
        };
        if let Err(error) = self.send_json(connection_id, &rpc_response).await {
            tracing::warn!(
                connection_id,
                error = %format!("{error:#}"),
                method = methods::THREAD_FILE_VIEW_GRANT_CREATE,
                "failed to send thread file view response"
            );
        }
    }
}

fn valid_scope_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SCOPE_ID_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_href(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_HREF_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn turn_item_contains_exact_link(item: &TurnItem, href: &str) -> bool {
    match item {
        TurnItem::AgentMessage { text, markdown, .. } => {
            markdown
                .as_ref()
                .is_some_and(|document| markdown_contains_exact_link(document, href))
                || markdown_contains_exact_link(&parse_markdown_document(text), href)
        }
        TurnItem::UserMessage { text, .. } => {
            markdown_contains_exact_link(&parse_markdown_document(text), href)
        }
        TurnItem::Reasoning {
            summary, content, ..
        } => summary
            .iter()
            .chain(content)
            .any(|text| markdown_contains_exact_link(&parse_markdown_document(text), href)),
        _ => false,
    }
}

fn markdown_contains_exact_link(document: &MarkdownDocument, href: &str) -> bool {
    document
        .blocks
        .iter()
        .any(|block| markdown_block_contains_exact_link(block, href))
}

fn markdown_block_contains_exact_link(block: &MarkdownBlock, href: &str) -> bool {
    match block {
        MarkdownBlock::Paragraph(inline)
        | MarkdownBlock::Heading {
            content: inline, ..
        } => markdown_inline_contains_exact_link(inline, href),
        MarkdownBlock::List(list) => list.items.iter().any(|item| {
            item.blocks
                .iter()
                .any(|block| markdown_block_contains_exact_link(block, href))
        }),
        MarkdownBlock::Quote { blocks } => blocks
            .iter()
            .any(|block| markdown_block_contains_exact_link(block, href)),
        MarkdownBlock::Code { .. } | MarkdownBlock::Rule => false,
    }
}

fn markdown_inline_contains_exact_link(inline: &MarkdownInline, href: &str) -> bool {
    inline
        .marks
        .iter()
        .any(|mark| matches!(&mark.kind, MarkdownMarkKind::Link { url } if url == href))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_markdown_link_is_required() {
        let item = TurnItem::AgentMessage {
            id: "item-1".to_owned(),
            text: "Open [main.rs](/workspace/main.rs:42:7).".to_owned(),
            phase: Default::default(),
            markdown: None,
            markdown_version: None,
        };
        assert!(turn_item_contains_exact_link(
            &item,
            "/workspace/main.rs:42:7"
        ));
        assert!(!turn_item_contains_exact_link(&item, "/workspace/main.rs"));
        assert!(!turn_item_contains_exact_link(&item, "https://example.com"));
    }

    #[test]
    fn links_in_code_are_not_file_capabilities() {
        let item = TurnItem::UserMessage {
            id: "item-1".to_owned(),
            text: "`/workspace/main.rs`".to_owned(),
            attachments: Vec::new(),
        };
        assert!(!turn_item_contains_exact_link(&item, "/workspace/main.rs"));
    }
}
