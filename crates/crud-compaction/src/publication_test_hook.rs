use pioneer_postgres_pools::PostgresDatabase;

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PublicationTestPause {
    ReaderPreflight,
    BeforeWriter,
    ProofSeal,
    CheckpointOriginAcquire,
}

#[cfg(any(test, feature = "test-support"))]
struct RegisteredPublicationTestHook {
    token: std::sync::Arc<()>,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(any(test, feature = "test-support"))]
static PUBLICATION_TEST_HOOKS: std::sync::OnceLock<
    std::sync::Mutex<
        std::collections::BTreeMap<
            (usize, String, PublicationTestPause),
            RegisteredPublicationTestHook,
        >,
    >,
> = std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-support"))]
pub struct PublicationTestHookHandle {
    key: (usize, String, PublicationTestPause),
    token: std::sync::Arc<()>,
    _database: PostgresDatabase,
    reached: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<tokio::sync::oneshot::Sender<()>>,
}

#[cfg(any(test, feature = "test-support"))]
impl PublicationTestHookHandle {
    pub async fn reached(&mut self) {
        let reached = self
            .reached
            .take()
            .expect("publication hook already awaited");
        tokio::time::timeout(std::time::Duration::from_secs(10), reached)
            .await
            .expect("publication hook was not reached before the diagnostic timeout")
            .expect("publication hook participant ended before reaching the pause");
    }

    pub fn release(mut self) {
        self.release
            .take()
            .expect("publication hook already released")
            .send(())
            .expect("publication hook participant ended before release");
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for PublicationTestHookHandle {
    fn drop(&mut self) {
        let mut hooks = PUBLICATION_TEST_HOOKS
            .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if hooks
            .get(&self.key)
            .is_some_and(|hook| std::sync::Arc::ptr_eq(&hook.token, &self.token))
        {
            hooks.remove(&self.key);
        }
        // Dropping the release sender also unblocks a participant that already
        // took this registration and is waiting at the pause.
    }
}
#[cfg(any(test, feature = "test-support"))]
pub fn arm_publication_test_hook(
    db: &PostgresDatabase,
    operation: &str,
    phase: PublicationTestPause,
) -> PublicationTestHookHandle {
    let key = (db.runtime_identity(), operation.to_owned(), phase);
    let token = std::sync::Arc::new(());
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let hook = RegisteredPublicationTestHook {
        token: token.clone(),
        reached: reached_tx,
        release: release_rx,
    };
    let mut hooks = PUBLICATION_TEST_HOOKS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(!hooks.contains_key(&key), "publication hook already armed");
    hooks.insert(key.clone(), hook);
    PublicationTestHookHandle {
        key,
        token,
        _database: db.clone(),
        reached: Some(reached_rx),
        release: Some(release_tx),
    }
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub async fn trigger_publication_test_hook(
    db: &PostgresDatabase,
    operation: &str,
    phase: PublicationTestPause,
) {
    pause_publication_test_hook(db.runtime_identity(), operation, phase).await;
}

#[cfg(any(test, feature = "test-support"))]
pub async fn pause_publication_test_hook(
    runtime_identity: usize,
    operation: &str,
    phase: PublicationTestPause,
) {
    let key = (runtime_identity, operation.to_owned(), phase);
    let hook = PUBLICATION_TEST_HOOKS
        .get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&key);
    if let Some(hook) = hook {
        let _ = hook.reached.send(());
        let _ = hook.release.await;
    }
}
