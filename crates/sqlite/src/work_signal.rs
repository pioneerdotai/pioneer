//! Coalesced invalidation for independent background consumers.
//!
//! A subscription acknowledges changes *before* discovery, never after it.
//! Consequently a commit during discovery remains visible to `wait`. This is
//! only a scheduling hint; durable state and domain deadlines remain authoritative.
use std::collections::BTreeMap;
use tokio::sync::watch;

const MAX_KEYS: usize = 256;
const MAX_KEY_BYTES: usize = 1024;

#[derive(Clone, Debug, Default)]
struct Invalidation {
    unknown: u64,
    revision: u64,
    full_revision: u64,
    keys: BTreeMap<String, u64>,
}
impl Invalidation {
    fn advance(&mut self) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("work revision exhausted");
    }
    fn full(&mut self) {
        self.advance();
        self.full_revision = self.revision;
        self.keys.clear();
    }
}

#[derive(Clone, Debug)]
pub struct WorkSignal(watch::Sender<Invalidation>);

impl Default for WorkSignal {
    fn default() -> Self {
        Self(watch::channel(Invalidation::default()).0)
    }
}

impl WorkSignal {
    /// Identity of a scheduling channel, independent of database row identity.
    pub fn same_channel(&self, other: &Self) -> bool {
        self.0.same_channel(&other.0)
    }

    /// Call after the relevant durable transition has released DB capacity.
    pub fn notify(&self) {
        self.0.send_modify(Invalidation::full);
    }

    /// Bounded hints, not work authority. Overflow requires finite rediscovery.
    pub fn notify_keys(&self, keys: &[String]) {
        if keys.is_empty() {
            return;
        }
        self.0.send_modify(|state| {
            state.advance();
            for key in keys {
                if key.len() > MAX_KEY_BYTES {
                    state.full();
                    return;
                }
                state.keys.insert(key.clone(), state.revision);
                if state.keys.len() > MAX_KEYS {
                    state.full();
                    return;
                }
            }
        });
    }

    /// Every independent consumer owns its own subscription.
    pub fn subscribe(&self) -> WorkSubscription {
        WorkSubscription {
            receiver: self.0.subscribe(),
            settled: 0,
            observed: None,
        }
    }

    /// Create before dispatching a relevant mutation. Explicit completion is
    /// required even for a known rollback/no-change; cancellation is unknown.
    pub fn changing(&self) -> WorkChange {
        WorkChange(Some(self.clone()), None)
    }
}

pub struct WorkChange(Option<WorkSignal>, Option<Vec<String>>);

impl WorkChange {
    /// Retain exact domain hints before dispatch. An uncertain outcome still
    /// publishes UNKNOWN and settles the writer; hints never replace authority.
    pub fn with_keys(mut self, keys: &[String]) -> Self {
        self.1 = Some(keys.to_vec());
        self
    }

    /// The operation has finished and released its DB resources. `changed`
    /// distinguishes useful invalidation from ACK/no-change self-wakes.
    pub fn completed(mut self, changed: bool) {
        if let Some(signal) = self.0.take() {
            if changed {
                if let Some(keys) = self.1.take() {
                    signal.notify_keys(&keys);
                } else {
                    signal.notify();
                }
            }
        }
    }
    /// Known exact keys after the commit and release of database resources.
    pub fn completed_keys(mut self, keys: &[String]) {
        if let Some(signal) = self.0.take() {
            signal.notify_keys(keys);
        }
    }
}

impl Drop for WorkChange {
    fn drop(&mut self) {
        if let Some(signal) = self.0.take() {
            signal.0.send_modify(|state| {
                state.unknown = state
                    .unknown
                    .checked_add(1)
                    .expect("work uncertainty epoch exhausted");
                state.full();
            });
        }
    }
}

pub struct WorkSubscription {
    receiver: watch::Receiver<Invalidation>,
    settled: u64,
    observed: Option<u64>,
}

impl WorkSubscription {
    /// Begin a bounded discovery pass. Changes made during that pass remain
    /// unacknowledged until the next pass starts.
    pub async fn begin_pass(
        &mut self,
        database: &crate::SqliteDatabase,
    ) -> Result<(), sea_orm::DbErr> {
        let (unknown, revision) = {
            let state = self.receiver.borrow_and_update();
            (state.unknown, state.revision)
        };
        if unknown != self.settled {
            database.settle_dispatched_writes().await?;
            self.settled = unknown;
        }
        self.observed = Some(revision);
        Ok(())
    }

    /// None requires full discovery (startup, unknown outcome, unscoped change,
    /// or bounded hint overflow). Each subscription has its own revision frontier.
    pub async fn begin_keyed_pass(
        &mut self,
        database: &crate::SqliteDatabase,
    ) -> Result<Option<Vec<String>>, sea_orm::DbErr> {
        let state = self.receiver.borrow_and_update().clone();
        if state.unknown != self.settled {
            database.settle_dispatched_writes().await?;
            self.settled = state.unknown;
        }
        let keys = self
            .observed
            .filter(|seen| state.full_revision <= *seen)
            .map(|seen| {
                state
                    .keys
                    .iter()
                    .filter(|(_, revision)| **revision > seen)
                    .map(|(key, _)| key.clone())
                    .collect()
            });
        self.observed = Some(state.revision);
        Ok(keys)
    }

    pub async fn changed(&mut self) {
        // Closure means the owner disappeared. Stay parked until the worker's
        // cancellation branch wins rather than spinning on a closed channel.
        if self.receiver.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// Wait for invalidation or a domain-owned Unix deadline. Rechecking the
    /// clock while a deadline exists handles clock jumps and process resume
    /// without asking the database to rediscover already known work.
    pub async fn wait(&mut self, deadline_unix: Option<i64>) {
        let Some(deadline) = deadline_unix else {
            return self.changed().await;
        };
        tokio::select! {
            _ = self.changed() => {},
            _ = wait_for_unix_deadline(deadline) => {},
        }
    }
}

/// Clock wait for an existing Notify-based owner. This has no SQL or discovery
/// responsibility; those owners retain their established notification protocol.
pub async fn wait_for_unix_deadline(deadline: i64) {
    if deadline <= 0 {
        return;
    }
    let Some(deadline) =
        std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(deadline as u64))
    else {
        return std::future::pending::<()>().await;
    };
    loop {
        let Ok(remaining) = deadline.duration_since(std::time::SystemTime::now()) else {
            return;
        };
        let pause = remaining.min(std::time::Duration::from_secs(30));
        tokio::time::sleep(pause).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_KEY_BYTES, MAX_KEYS};
    use futures_util::FutureExt;
    use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

    async fn database() -> crate::SqliteDatabase {
        crate::SqliteDatabase::from_single_connection(
            Database::connect("sqlite::memory:").await.unwrap(),
        )
    }

    // Existing heartbeat/recovery tests cover their own Notify protocols. They
    // do not protect independent subscriptions or a watch invalidation retained
    // across discovery, so this matrix belongs at the shared mechanism boundary.
    #[tokio::test]
    async fn keyed_frontiers_are_independent_bounded_and_keep_concurrent_changes() {
        let database = database().await;
        let signal = database.work_signal("test.keyed");
        let mut first = signal.subscribe();
        let mut second = signal.subscribe();
        assert!(first.begin_keyed_pass(&database).await.unwrap().is_none());
        assert!(second.begin_keyed_pass(&database).await.unwrap().is_none());
        signal
            .changing()
            .with_keys(&["a".into(), "b".into()])
            .completed(true);
        assert_eq!(
            first.begin_keyed_pass(&database).await.unwrap(),
            Some(vec!["a".into(), "b".into()])
        );
        // The second subscriber has not acknowledged A/B. A reappears behind
        // the first subscriber's cursor while it processes its finite batch.
        signal.changing().completed_keys(&["a".into(), "c".into()]);
        assert!(first.wait(None).now_or_never().is_some());
        assert_eq!(
            first.begin_keyed_pass(&database).await.unwrap(),
            Some(vec!["a".into(), "c".into()])
        );
        assert_eq!(
            second.begin_keyed_pass(&database).await.unwrap(),
            Some(vec!["a".into(), "b".into(), "c".into()])
        );
        assert_eq!(
            first.begin_keyed_pass(&database).await.unwrap(),
            Some(vec![])
        );
        assert!(first.wait(None).now_or_never().is_none());
        signal.notify_keys(
            &(0..=MAX_KEYS)
                .map(|n| format!("key-{n}"))
                .collect::<Vec<_>>(),
        );
        assert!(first.begin_keyed_pass(&database).await.unwrap().is_none());
        assert!(second.begin_keyed_pass(&database).await.unwrap().is_none());
        signal.notify();
        signal.notify_keys(&["after-full".into()]);
        assert!(first.begin_keyed_pass(&database).await.unwrap().is_none());
        signal
            .changing()
            .with_keys(&["quiet".into()])
            .completed(false);
        assert!(first.wait(None).now_or_never().is_none());
        drop(signal.changing().with_keys(&["cancelled-source".into()]));
        assert!(first.begin_keyed_pass(&database).await.unwrap().is_none());
        signal.notify_keys(&["x".repeat(MAX_KEY_BYTES + 1)]);
        assert!(first.begin_keyed_pass(&database).await.unwrap().is_none());
        database.close().await.unwrap();
    }

    #[tokio::test]
    async fn independent_consumers_retain_bursts_and_discovery_races() {
        let database = database().await;
        let signal = database.work_signal("test.discovery");
        let mut first = signal.subscribe();
        let mut second = database
            .maintenance()
            .work_signal("test.discovery")
            .subscribe();
        first.begin_pass(&database).await.unwrap();
        second.begin_pass(&database).await.unwrap();
        for _ in 0..1000 {
            signal.notify();
        }
        assert!(first.wait(None).now_or_never().is_some());
        assert!(second.wait(None).now_or_never().is_some());
        first.begin_pass(&database).await.unwrap();
        assert!(first.wait(None).now_or_never().is_none());
        let future_deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 3600;
        assert!(first.wait(Some(future_deadline)).now_or_never().is_none());
        // A commit after the empty observation, before waiter registration.
        signal.notify();
        assert!(first.wait(Some(future_deadline)).now_or_never().is_some());
        first.begin_pass(&database).await.unwrap();
        signal.changing().completed(false);
        assert!(first.wait(None).now_or_never().is_none());
        signal.changing().completed(true);
        assert!(first.wait(None).now_or_never().is_some());
        database.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_dispatched_mutation_is_settled_before_empty_discovery() {
        #[derive(Default)]
        struct Routes(std::sync::Mutex<Vec<crate::SqliteWriteClass>>);
        impl crate::SqliteWriteObserver for Routes {
            fn observe(&self, event: crate::SqliteWriteEvent) {
                if let crate::SqliteWriteEvent::Acquired { class, .. } = event {
                    self.0.lock().unwrap().push(class);
                }
            }
        }
        let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let raw = Database::connect(options).await.unwrap();
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        struct Release(std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
        impl Drop for Release {
            fn drop(&mut self) {
                let (lock, condition) = &*self.0;
                *lock.lock().unwrap() = true;
                condition.notify_one();
            }
        }
        let release = Release(gate.clone());
        {
            let mut connection = raw.get_sqlite_connection_pool().acquire().await.unwrap();
            let mut locked = connection.lock_handle().await.unwrap();
            // The native fixture intentionally pauses a dispatched sqlite3_step
            // to exercise cancellation after dispatch, without production hooks.
            let native =
                unsafe { rusqlite::Connection::from_handle(locked.as_raw_handle().as_ptr()) }
                    .unwrap();
            let entered = entered.clone();
            let gate = gate.clone();
            native
                .create_scalar_function(
                    "work_signal_gate",
                    0,
                    rusqlite::functions::FunctionFlags::SQLITE_UTF8,
                    move |_| {
                        entered.notify_one();
                        let (lock, condition) = &*gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = condition.wait(released).unwrap();
                        }
                        Ok(7_i64)
                    },
                )
                .unwrap();
        }
        let routes = std::sync::Arc::new(Routes::default());
        let writer = crate::SqliteWriteExecutor::with_observer(raw.clone(), routes.clone());
        let database = crate::SqliteDatabase::from_executor(raw, writer);
        database
            .execute_unprepared("CREATE TABLE signal_probe(value INTEGER)")
            .await
            .unwrap();
        let signal = database.work_signal("test.cancelled");
        let mut subscription = signal.subscribe();
        subscription.begin_pass(&database).await.unwrap();
        routes.0.lock().unwrap().clear();
        let change = signal.changing();
        let writing = database.clone();
        let task = tokio::spawn(async move {
            writing
                .execute_unprepared("INSERT INTO signal_probe VALUES(work_signal_gate())")
                .await
                .unwrap();
            change.completed(true);
        });
        entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let maintenance = database.maintenance();
        let mut settling = Box::pin(subscription.begin_pass(&maintenance));
        assert!(settling.as_mut().now_or_never().is_none());
        drop(release);
        settling.await.unwrap();
        assert_eq!(
            *routes.0.lock().unwrap(),
            [
                crate::SqliteWriteClass::Interactive,
                crate::SqliteWriteClass::Maintenance
            ]
        );
        let row = database
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM signal_probe".to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<i64>("", "count").unwrap(), 1);
        database.close().await.unwrap();
    }
}
