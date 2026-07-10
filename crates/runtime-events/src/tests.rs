use std::sync::Arc;
use std::time::Duration;

use pioneer_protocol::{
    AgentDurableEvent, AgentMessagePhase, AgentProgressEvent, ItemCompletedNotification,
    ItemDeltaNotification, ItemDeltaStream, ItemUpdatedNotification, TurnItem,
};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

use crate::{
    ExecutionEventHub, ExecutionEventHubError, ExecutionSnapshotEvent, OrderedEventIngress,
    OrderedIngressClass, OrderedIngressConfig, OrderedIngressEvent, OrderedIngressOffer,
    ProgressCoalescerConfig,
};

fn completed(turn_id: &str) -> AgentDurableEvent {
    AgentDurableEvent::TurnCompleted {
        thread_id: "thread_1".to_owned(),
        turn_id: turn_id.to_owned(),
        recovery: None,
    }
}

fn delta(text: &str) -> AgentProgressEvent {
    AgentProgressEvent::ItemDelta {
        notification: ItemDeltaNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item_id: "item_1".to_owned(),
            delta: text.to_owned(),
            stream: Some(ItemDeltaStream::AgentMessage),
            payload: None,
            markdown: None,
            markdown_version: None,
        },
    }
}

fn test_config() -> ProgressCoalescerConfig {
    ProgressCoalescerConfig {
        flush_interval: Duration::from_secs(60),
        max_pending_keys: 16,
        max_append_bytes_per_key: 128,
        max_snapshot_bytes_per_key: 64,
        max_flush_batch_size: 16,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestIngressEvent {
    Progress { key: &'static str, text: String },
    Durable(&'static str),
}

impl OrderedIngressEvent for TestIngressEvent {
    type Key = &'static str;

    fn ingress_class(&self) -> OrderedIngressClass<Self::Key> {
        match self {
            Self::Progress { key, .. } => OrderedIngressClass::Coalesced(*key),
            Self::Durable(_) => OrderedIngressClass::Durable,
        }
    }

    fn coalesce(&mut self, newer: Self) {
        match (self, newer) {
            (Self::Progress { text, .. }, Self::Progress { text: newer, .. }) => {
                text.push_str(newer.as_str());
            }
            (current, newer) => *current = newer,
        }
    }
}

fn updated(text: &str) -> ExecutionSnapshotEvent {
    ExecutionSnapshotEvent::ItemUpdated {
        notification: ItemUpdatedNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item: TurnItem::AgentMessage {
                id: "item_1".to_owned(),
                text: text.to_owned(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        },
    }
}

#[tokio::test]
async fn durable_lane_backpressures_instead_of_dropping() {
    let hub = Arc::new(ExecutionEventHub::with_capacity(1, 1));
    let mut receiver = hub.take_durable_receiver().await.expect("receiver");
    hub.publish_durable(completed("turn_1"))
        .await
        .expect("first");
    let publisher = {
        let hub = hub.clone();
        tokio::spawn(async move { hub.publish_durable(completed("turn_2")).await })
    };
    sleep(Duration::from_millis(25)).await;
    assert!(!publisher.is_finished());
    assert!(receiver.recv().await.is_some());
    publisher.await.expect("publisher task").expect("second");
}

#[tokio::test]
async fn commit_waiter_completes_only_after_consumer_finishes_event() {
    let hub = Arc::new(ExecutionEventHub::with_capacity(8, 8));
    let mut receiver = hub.take_durable_receiver().await.expect("receiver");
    let publisher = {
        let hub = hub.clone();
        tokio::spawn(async move { hub.publish_durable_and_wait(completed("turn_1")).await })
    };
    let event = receiver.recv().await.expect("event");
    assert!(matches!(event, AgentDurableEvent::TurnCompleted { .. }));
    sleep(Duration::from_millis(25)).await;
    assert!(!publisher.is_finished());
    receiver.acknowledge_last(Ok(()));
    publisher
        .await
        .expect("publisher task")
        .expect("commit ack");
}

#[tokio::test]
async fn requesting_next_event_without_ack_rejects_commit_waiter() {
    let hub = Arc::new(ExecutionEventHub::with_capacity(8, 8));
    let mut receiver = hub.take_durable_receiver().await.expect("receiver");
    let publisher = {
        let hub = hub.clone();
        tokio::spawn(async move { hub.publish_durable_and_wait(completed("turn_1")).await })
    };
    assert!(receiver.recv().await.is_some());
    let next = tokio::spawn(async move { receiver.recv().await });
    assert!(matches!(
        publisher.await.expect("publisher task"),
        Err(ExecutionEventHubError::CommitRejected(message))
            if message.contains("without acknowledging")
    ));
    next.abort();
}

#[tokio::test]
async fn commit_rejection_is_reported_to_publisher() {
    let hub = Arc::new(ExecutionEventHub::with_capacity(8, 8));
    let mut receiver = hub.take_durable_receiver().await.expect("receiver");
    let publisher = {
        let hub = hub.clone();
        tokio::spawn(async move { hub.publish_durable_and_wait(completed("turn_1")).await })
    };
    assert!(receiver.recv().await.is_some());
    receiver.acknowledge_last(Err("storage unavailable".to_owned()));
    assert_eq!(
        publisher.await.expect("publisher task"),
        Err(ExecutionEventHubError::CommitRejected(
            "storage unavailable".to_owned()
        ))
    );
}

#[tokio::test]
async fn item_completion_flushes_coalesced_progress_first() {
    let hub = ExecutionEventHub::with_progress_config(8, 8, test_config());
    let mut live = hub.subscribe_live();
    let mut durable = hub.take_durable_receiver().await.expect("receiver");
    hub.publish_progress(delta("hello "));
    hub.publish_progress(delta("world"));
    hub.publish_durable(AgentDurableEvent::ItemCompleted {
        notification: ItemCompletedNotification {
            workspace_id: "ws_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item: TurnItem::AgentMessage {
                id: "item_1".to_owned(),
                text: "hello world".to_owned(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        },
    })
    .await
    .expect("durable");
    assert!(matches!(
        timeout(Duration::from_secs(1), live.recv()).await,
        Ok(Ok(AgentProgressEvent::ItemDelta { notification })) if notification.delta == "hello world"
    ));
    assert!(matches!(
        durable.recv().await,
        Some(AgentDurableEvent::ItemCompleted { .. })
    ));
}

#[tokio::test]
async fn ordered_ingress_coalesces_ten_thousand_updates_before_terminal_barrier() {
    let (tx, mut rx) = mpsc::channel(1);
    let ingress = OrderedEventIngress::spawn(
        tx,
        OrderedIngressConfig {
            flush_interval: Duration::from_secs(60),
            max_pending_coalesced_keys: 8,
            max_pending_durable_events: 8,
        },
    );
    for _ in 0..10_000 {
        assert!(matches!(
            ingress
                .offer(TestIngressEvent::Progress {
                    key: "item_1",
                    text: "x".to_owned(),
                })
                .await,
            OrderedIngressOffer::Accepted
        ));
    }
    assert!(matches!(
        ingress
            .offer(TestIngressEvent::Durable("turn/completed"))
            .await,
        OrderedIngressOffer::Accepted
    ));

    let Some(TestIngressEvent::Progress { text, .. }) = rx.recv().await else {
        panic!("coalesced progress must precede terminal event");
    };
    assert_eq!(text.len(), 10_000);
    assert_eq!(
        rx.recv().await,
        Some(TestIngressEvent::Durable("turn/completed"))
    );
    ingress.close();
}

#[tokio::test]
async fn ordered_ingress_bounds_progress_keys_but_never_rejects_lifecycle() {
    let (tx, mut rx) = mpsc::channel(1);
    let ingress = OrderedEventIngress::spawn(
        tx,
        OrderedIngressConfig {
            flush_interval: Duration::from_secs(60),
            max_pending_coalesced_keys: 1,
            max_pending_durable_events: 1,
        },
    );
    assert!(matches!(
        ingress
            .offer(TestIngressEvent::Progress {
                key: "item_1",
                text: "one".to_owned(),
            })
            .await,
        OrderedIngressOffer::Accepted
    ));
    assert!(matches!(
        ingress
            .offer(TestIngressEvent::Progress {
                key: "item_2",
                text: "two".to_owned(),
            })
            .await,
        OrderedIngressOffer::CoalescedKeyLimit(_)
    ));
    assert!(matches!(
        ingress
            .offer(TestIngressEvent::Durable("item/completed"))
            .await,
        OrderedIngressOffer::Accepted
    ));
    assert!(matches!(
        rx.recv().await,
        Some(TestIngressEvent::Progress { key: "item_1", .. })
    ));
    assert_eq!(
        rx.recv().await,
        Some(TestIngressEvent::Durable("item/completed"))
    );
    ingress.close();
}

#[tokio::test]
async fn ordered_ingress_backpressures_durable_backlog_at_configured_bound() {
    let (tx, mut rx) = mpsc::channel(1);
    let ingress = OrderedEventIngress::spawn(
        tx,
        OrderedIngressConfig {
            flush_interval: Duration::from_secs(60),
            max_pending_coalesced_keys: 1,
            max_pending_durable_events: 1,
        },
    );
    assert!(matches!(
        ingress.offer(TestIngressEvent::Durable("first")).await,
        OrderedIngressOffer::Accepted
    ));
    tokio::task::yield_now().await;
    assert!(matches!(
        ingress.offer(TestIngressEvent::Durable("second")).await,
        OrderedIngressOffer::Accepted
    ));
    let third = {
        let ingress = ingress.clone();
        tokio::spawn(async move { ingress.offer(TestIngressEvent::Durable("third")).await })
    };
    sleep(Duration::from_millis(25)).await;
    assert!(!third.is_finished());
    assert_eq!(rx.recv().await, Some(TestIngressEvent::Durable("first")));
    assert!(matches!(
        third.await.expect("third publisher task"),
        OrderedIngressOffer::Accepted
    ));
    assert_eq!(rx.recv().await, Some(TestIngressEvent::Durable("second")));
    assert_eq!(rx.recv().await, Some(TestIngressEvent::Durable("third")));
    ingress.close();
}

#[tokio::test]
async fn snapshot_lane_keeps_latest_value_for_each_item() {
    let hub = ExecutionEventHub::new();
    let mut receiver = hub
        .take_snapshot_receiver()
        .await
        .expect("snapshot receiver");
    assert!(hub.publish_snapshot(updated("first")));
    assert!(hub.publish_snapshot(updated("latest")));
    let Some(ExecutionSnapshotEvent::ItemUpdated { notification }) = receiver.recv().await else {
        panic!("snapshot must be delivered");
    };
    let TurnItem::AgentMessage { text, .. } = notification.item else {
        panic!("expected agent message snapshot");
    };
    assert_eq!(text, "latest");
}
