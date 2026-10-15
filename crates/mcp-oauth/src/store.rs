use async_trait::async_trait;
use fs4::FileExt;
use pioneer_keystore::{SecretFilter, SecretId, SecretKind, SecretMeta, SecretStore};
use rmcp::transport::auth::{
    AuthError, CredentialRefreshGuard, CredentialStore, OAuthClientConfig, StoredCredentials,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
    sync::{Arc, OnceLock, Weak},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

fn storage_error() -> AuthError {
    AuthError::CredentialStoreError("OAuth storage unavailable".into())
}
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Full registration is retained independently of the SDK's token representation.
#[derive(Clone, Serialize, Deserialize)]
pub struct Registration {
    #[serde(default)]
    pub token_endpoint_auth_method: Option<String>,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
    pub application_type: Option<String>,
    #[serde(default)]
    pub registration_request: Option<serde_json::Value>,
    #[serde(default)]
    pub registration_response: Option<rmcp::transport::auth::ClientRegistrationResponse>,
}
impl From<OAuthClientConfig> for Registration {
    fn from(c: OAuthClientConfig) -> Self {
        Self {
            token_endpoint_auth_method: None,
            client_id: c.client_id,
            client_secret: c.client_secret,
            redirect_uri: c.redirect_uri,
            scopes: c.scopes,
            application_type: c.application_type,
            registration_request: None,
            registration_response: None,
        }
    }
}
impl Registration {
    pub(crate) fn config(&self) -> OAuthClientConfig {
        let mut config = OAuthClientConfig::new(&self.client_id, &self.redirect_uri)
            .with_scopes(self.scopes.clone());
        config.client_secret = self.client_secret.clone();
        config.application_type = self.application_type.clone();
        config
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct AuthorizationRecord {
    pub identity: String,
    pub resource: String,
    pub issuer: String,
    pub registration: Registration,
    pub credentials: Option<StoredCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_consent: Option<Box<PendingConsent>>,
}
/// This candidate is never a usable grant. One atomic record retains the prior
/// registration/grant across cancellation, failed rollback and process restart.
#[derive(Clone, Serialize, Deserialize)]
pub struct PendingConsent {
    pub previous: AuthorizationRecord,
    pub candidate: StoredCredentials,
    #[serde(default)]
    pub committed: bool,
}
impl std::fmt::Debug for AuthorizationRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthorizationRecord([redacted])")
    }
}

/// An atomic record in the existing secret store. No Gateway dependency and no
/// separate token cache that could acknowledge an uncommitted rotation.
#[derive(Clone)]
pub struct OAuthPersistence {
    secrets: Arc<dyn SecretStore>,
    lock_dir: Option<PathBuf>,
    io: Arc<IoOwner>,
}
struct TrackedWrite {
    task: tokio::task::JoinHandle<()>,
    completed: tokio::sync::watch::Receiver<bool>,
}
#[derive(Default)]
struct IoOwner {
    gate: std::sync::Mutex<()>,
    writes: std::sync::Mutex<Vec<TrackedWrite>>,
}
struct WriteCompletion(tokio::sync::watch::Sender<bool>);
impl Drop for WriteCompletion {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}
impl OAuthPersistence {
    pub fn new(secrets: Arc<dyn SecretStore>, lock_dir: Option<PathBuf>) -> Self {
        // Production adapters may be recreated independently (RPC cleanup,
        // GC, tests). They share IO ownership for the same runtime home/store.
        static OWNERS: OnceLock<
            std::sync::Mutex<std::collections::HashMap<String, Weak<IoOwner>>>,
        > = OnceLock::new();
        let key = lock_dir
            .as_ref()
            .map(|dir| dir.to_string_lossy().into_owned())
            .unwrap_or_else(|| format!("store:{:p}", Arc::as_ptr(&secrets)));
        let io = {
            let mut owners = OWNERS
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            owners.retain(|_, owner| owner.strong_count() > 0);
            if let Some(owner) = owners.get(&key).and_then(Weak::upgrade) {
                owner
            } else {
                let owner = Arc::new(IoOwner::default());
                owners.insert(key, Arc::downgrade(&owner));
                owner
            }
        };
        Self {
            secrets,
            lock_dir,
            io,
        }
    }
    pub(crate) async fn client_secret(&self, reference: &str) -> Result<Option<String>, AuthError> {
        let store = self.secrets.clone();
        let id = SecretId::mcp_secret(reference).map_err(|_| storage_error())?;
        tokio::task::spawn_blocking(move || store.get_string(&id).map_err(|_| storage_error()))
            .await
            .map_err(|_| storage_error())?
    }
    /// Management/backend reads expose only confirmed ordinary credentials.
    /// The pending envelope remains available for recovery under the file lease.
    pub async fn read(&self, id: &str) -> Result<Option<AuthorizationRecord>, AuthError> {
        let Some(mut record) = self.read_raw(id).await? else {
            return Ok(None);
        };
        if let Some(pending) = record.pending_consent.as_ref() {
            if pending.committed {
                if self.promotion_fenced(id).await? {
                    record.credentials = pending.previous.credentials.clone();
                } else {
                    record.pending_consent = None;
                }
            }
        }
        Ok(Some(record))
    }
    async fn read_raw(&self, id: &str) -> Result<Option<AuthorizationRecord>, AuthError> {
        let store = self.secrets.clone();
        let id = SecretId::mcp_oauth(id).map_err(|_| storage_error())?;
        tokio::task::spawn_blocking(move || {
            store
                .get_string(&id)
                .map_err(|_| storage_error())?
                .map(|s| serde_json::from_str(&s).map_err(|_| storage_error()))
                .transpose()
        })
        .await
        .map_err(|_| storage_error())?
    }
    pub async fn write(&self, id: &str, record: AuthorizationRecord) -> Result<(), AuthError> {
        self.write_checked(id, record, CancellationToken::new())
            .await
    }
    pub(crate) async fn write_checked(
        &self,
        id: &str,
        record: AuthorizationRecord,
        cancellation: CancellationToken,
    ) -> Result<(), AuthError> {
        let lease = Arc::new(RefreshLease {
            _local: None,
            _file: self.file_guard(id, &cancellation).await?,
        });
        self.write_until(id, record, cancellation, None, Some(lease))
            .await
    }
    async fn write_until(
        &self,
        id: &str,
        record: AuthorizationRecord,
        cancellation: CancellationToken,
        deadline: Option<(SystemTime, Arc<dyn crate::OAuthClock>)>,
        lease: Option<Arc<RefreshLease>>,
    ) -> Result<(), AuthError> {
        let value = serde_json::to_string(&record).map_err(|_| storage_error())?;
        let store = self.secrets.clone();
        let id = SecretId::mcp_oauth(id).map_err(|_| storage_error())?;
        let io = self.io.clone();
        self.blocking_write(move || {
            // Ownership crosses the async cancellation boundary: the actual IO
            // retains both refresh and interprocess guards until put returns.
            let _lease = lease;
            let _io = io.gate.lock().map_err(|_| storage_error())?;
            if cancellation.is_cancelled()
                || deadline
                    .as_ref()
                    .is_some_and(|(expiry, clock)| clock.now() >= *expiry)
            {
                return Err(storage_error());
            }
            match store.put_string(
                &id,
                &value,
                SecretMeta::new(SecretKind::McpOAuth, None, now() as i64),
            ) {
                Ok(()) => Ok(()),
                Err(_) => match store.get_string(&id) {
                    Ok(Some(actual)) if actual == value => Ok(()),
                    _ => Err(storage_error()),
                },
            }
        })
        .await
    }
    async fn blocking_write<F>(&self, write: F) -> Result<(), AuthError>
    where
        F: FnOnce() -> Result<(), AuthError> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (completion, completed) = tokio::sync::watch::channel(false);
        let owner = self.io.clone();
        let task = tokio::task::spawn_blocking(move || {
            // The task keeps the IO owner alive even if every async adapter is
            // dropped. Completion also signals on panic; no payload is exposed.
            let _owner = owner;
            let _completion = WriteCompletion(completion);
            let _ = tx.send(write());
        });
        {
            let mut writes = self
                .io
                .writes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            writes.retain(|write| !write.task.is_finished());
            writes.push(TrackedWrite { task, completed });
        }
        rx.await.map_err(|_| storage_error())?
    }
    /// Waiting never takes task ownership out of the registry. Cancelling a
    /// drain cannot detach a put or hide it from a subsequent GC/replacement.
    pub(crate) async fn drain(&self) {
        loop {
            let pending = {
                let mut writes = self
                    .io
                    .writes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                writes.retain(|write| !write.task.is_finished());
                writes
                    .iter()
                    .map(|write| write.completed.clone())
                    .collect::<Vec<_>>()
            };
            if pending.is_empty() {
                return;
            }
            for mut completed in pending {
                while !*completed.borrow() {
                    if completed.changed().await.is_err() {
                        break;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    }
    async fn file_guard(
        &self,
        id: &str,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileGuard>, AuthError> {
        // Coordinate independent persistence instances as well as processes.
        // The shared in-process gate also applies to injected stores without a
        // filesystem lock directory (deterministic regression fixtures).
        static GATES: OnceLock<
            std::sync::Mutex<std::collections::HashMap<String, Weak<Mutex<()>>>>,
        > = OnceLock::new();
        let key = format!(
            "{}:{id}",
            self.lock_dir
                .as_ref()
                .map(|dir| dir.to_string_lossy().into_owned())
                .unwrap_or_else(|| format!("store:{:p}", Arc::as_ptr(&self.secrets)))
        );
        let gate = {
            let mut gates = GATES
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            gates.retain(|_, gate| gate.strong_count() > 0);
            if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(Mutex::new(()));
                gates.insert(key, Arc::downgrade(&gate));
                gate
            }
        };
        let local = tokio::select! { _=cancellation.cancelled()=>return Err(storage_error()), guard=gate.lock_owned()=>guard };
        if let Some(dir) = self.lock_dir.clone() {
            let id = id.to_owned();
            let file = tokio::task::spawn_blocking(move || {
                pioneer_keystore::ensure_private_runtime_dir(&dir).map_err(|_| storage_error())?;
                use sha2::{Digest, Sha256};
                let path = dir.join(format!(
                    "{}.lock",
                    hex::encode(Sha256::digest(id.as_bytes()))
                ));
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(|_| storage_error())?;
                pioneer_keystore::ensure_private_file(&path).map_err(|_| storage_error())?;
                Ok::<_, AuthError>(file)
            })
            .await
            .map_err(|_| storage_error())??;
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                match FileExt::try_lock(&file) {
                    Ok(()) => break,
                    Err(fs4::TryLockError::WouldBlock)
                        if tokio::time::Instant::now() < deadline =>
                    {
                        tokio::select! {_=cancellation.cancelled()=>return Err(storage_error()),_=tokio::time::sleep(std::time::Duration::from_millis(50))=>{}}
                    }
                    Err(_) => return Err(storage_error()),
                }
            }
            Ok(Some(FileGuard {
                file: Some(file),
                _local: local,
            }))
        } else {
            Ok(Some(FileGuard {
                file: None,
                _local: local,
            }))
        }
    }
    #[cfg(feature = "test-support")]
    pub(crate) async fn refresh_lease_available_for_test(&self, id: &str) -> bool {
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            self.file_guard(id, &CancellationToken::new()),
        )
        .await
        .is_ok_and(|result| result.is_ok())
    }
    pub async fn delete(&self, id: &str) -> Result<(), AuthError> {
        let lease = Arc::new(RefreshLease {
            _local: None,
            _file: self.file_guard(id, &CancellationToken::new()).await?,
        });
        self.delete_unlocked(id, lease).await
    }
    async fn promotion_fenced(&self, id: &str) -> Result<bool, AuthError> {
        let store = self.secrets.clone();
        let id = SecretId::mcp_oauth(&format!("{id}::promotion")).map_err(|_| storage_error())?;
        tokio::task::spawn_blocking(move || {
            store
                .get_string(&id)
                .map(|value| value.is_some())
                .map_err(|_| storage_error())
        })
        .await
        .map_err(|_| storage_error())?
    }
    async fn promotion_fence(
        &self,
        id: &str,
        lease: Arc<RefreshLease>,
        install: bool,
    ) -> Result<(), AuthError> {
        let store = self.secrets.clone();
        let id = SecretId::mcp_oauth(&format!("{id}::promotion")).map_err(|_| storage_error())?;
        let io = self.io.clone();
        self.blocking_write(move || {
            let _lease = lease;
            let _io = io.gate.lock().map_err(|_| storage_error())?;
            let result = if install {
                store.put_string(
                    &id,
                    "pending",
                    SecretMeta::new(SecretKind::McpOAuth, None, now() as i64),
                )
            } else {
                store.delete(&id).map(|_| ())
            };
            match result {
                Ok(()) => Ok(()),
                Err(_) => match store.get_string(&id) {
                    Ok(value) if value.is_some() == install => Ok(()),
                    _ => Err(storage_error()),
                },
            }
        })
        .await
    }
    pub(crate) async fn clear_stale_record(
        &self,
        id: &str,
        identity: &str,
    ) -> Result<(), AuthError> {
        let lease = Arc::new(RefreshLease {
            _local: None,
            _file: self.file_guard(id, &CancellationToken::new()).await?,
        });
        if let Some(record) = self.read_raw(id).await? {
            if record.identity != identity {
                self.delete_unlocked(id, lease.clone()).await?;
            } else if let Some(pending) = record.pending_consent {
                if pending.committed && !self.promotion_fenced(id).await? {
                    return Ok(());
                }
                self.write_until(
                    id,
                    pending.previous,
                    CancellationToken::new(),
                    None,
                    Some(lease.clone()),
                )
                .await?;
            }
        }
        self.promotion_fence(id, lease, false).await?;
        Ok(())
    }
    async fn delete_unlocked(&self, id: &str, lease: Arc<RefreshLease>) -> Result<(), AuthError> {
        let store = self.secrets.clone();
        let fence =
            SecretId::mcp_oauth(&format!("{id}::promotion")).map_err(|_| storage_error())?;
        let id = SecretId::mcp_oauth(id).map_err(|_| storage_error())?;
        let io = self.io.clone();
        self.blocking_write(move || {
            let _lease = lease;
            let _io = io.gate.lock().map_err(|_| storage_error())?;
            // Re-establish quarantine before removing an uncertain committed
            // account. If deletion fails, an independent reader/restart must not
            // treat the leftover candidate as confirmed merely because an earlier
            // promotion removed its fence. Err never acknowledges cleanup.
            store
                .put_string(
                    &fence,
                    "pending",
                    SecretMeta::new(SecretKind::McpOAuth, None, now() as i64),
                )
                .map_err(|_| storage_error())?;
            store.delete(&id).map_err(|_| storage_error())?;
            store
                .delete(&fence)
                .map(|_| ())
                .map_err(|_| storage_error())
        })
        .await
    }
    pub async fn ids(&self) -> Result<Vec<String>, AuthError> {
        // Include mutations started by retired callers and independent adapters
        // before enumerating; an uncommitted put must not evade orphan GC.
        self.drain().await;
        let store = self.secrets.clone();
        tokio::task::spawn_blocking(move || {
            store
                .list(SecretFilter::Kind(SecretKind::McpOAuth))
                .map(|rows| {
                    rows.into_iter()
                        .map(|r| r.id.user().trim_end_matches("::promotion").to_owned())
                        .collect()
                })
                .map_err(|_| storage_error())
        })
        .await
        .map_err(|_| storage_error())?
    }
    pub(crate) fn credential_store(
        &self,
        id: String,
        identity: String,
        gate: Arc<Mutex<()>>,
        cancellation: CancellationToken,
    ) -> PersistentCredentials {
        PersistentCredentials {
            persistence: self.clone(),
            id,
            identity,
            gate,
            cancellation,
            deadline: None,
            lease: Arc::new(std::sync::Mutex::new(None)),
            consent_baseline: Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

struct FileGuard {
    file: Option<File>,
    _local: tokio::sync::OwnedMutexGuard<()>,
}
impl Drop for FileGuard {
    fn drop(&mut self) {
        if let Some(file) = &self.file {
            let _ = file.unlock();
        }
    }
}

// A blocking mutation owns a strong reference even if its async caller and
// the SDK CredentialRefreshGuard are dropped. Cleanup obtains the file guard
// only after that mutation actually finishes, then rereads before deleting.
struct RefreshLease {
    _local: Option<tokio::sync::OwnedMutexGuard<()>>,
    _file: Option<FileGuard>,
}

pub(crate) struct PersistentCredentials {
    pub(crate) persistence: OAuthPersistence,
    pub(crate) id: String,
    pub(crate) identity: String,
    pub(crate) gate: Arc<Mutex<()>>,
    pub(crate) cancellation: CancellationToken,
    deadline: Option<(SystemTime, Arc<dyn crate::OAuthClock>)>,
    lease: Arc<std::sync::Mutex<Option<Weak<RefreshLease>>>>,
    consent_baseline: Arc<std::sync::Mutex<Option<Option<StoredCredentials>>>>,
}
impl Clone for PersistentCredentials {
    fn clone(&self) -> Self {
        Self {
            persistence: self.persistence.clone(),
            id: self.id.clone(),
            identity: self.identity.clone(),
            gate: self.gate.clone(),
            cancellation: self.cancellation.clone(),
            deadline: self.deadline.clone(),
            // SDK managers get independent lease ownership. A different manager
            // must not borrow the exchange owner's reentrancy through Weak.
            lease: Arc::new(std::sync::Mutex::new(None)),
            consent_baseline: self.consent_baseline.clone(),
        }
    }
}
impl PersistentCredentials {
    /// Plain SDK access-token reads on an existing session must not observe an
    /// exchanged grant before terminal admission, including scope upgrades.
    pub(crate) fn stage_consent(&self, previous: Option<StoredCredentials>) {
        *self
            .consent_baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(previous);
    }
    pub(crate) fn consent_staged(&self) -> bool {
        self.consent_baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
    pub(crate) fn finish_failed_consent(&self) {
        *self
            .consent_baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
    pub(crate) async fn commit_consent(&self) -> Result<(), AuthError> {
        let mut record = self
            .persistence
            .read_raw(&self.id)
            .await?
            .ok_or_else(storage_error)?;
        let pending = record.pending_consent.as_mut().ok_or_else(storage_error)?;
        let lease = self.active_lease().ok_or_else(storage_error)?;
        // Confirm the durable fence before any promotion. Neither a write error
        // nor unknown readback may expose a candidate to another process.
        self.persistence
            .promotion_fence(&self.id, lease.clone(), true)
            .await?;
        record.credentials = Some(pending.candidate.clone());
        pending.committed = true;
        // Retain the full previous record until fenced outcome is resolved.
        // write_until confirms post-mutation errors by exact value readback.
        self.restore_record(record).await?;
        loop {
            if self
                .persistence
                .promotion_fence(&self.id, lease.clone(), false)
                .await
                .is_ok()
            {
                break;
            }
            match self.persistence.promotion_fenced(&self.id).await {
                Ok(false) => break,
                Ok(true) => return Err(storage_error()),
                Err(_) => {
                    // An uncertain delete is not a Failed commit. The owned
                    // exchange retains its lease and resolves it with backoff.
                    tokio::select! { _=self.cancellation.cancelled()=>return Err(AuthError::AuthorizationRequired), _=tokio::time::sleep(std::time::Duration::from_millis(250))=>{} }
                }
            }
        }
        *self
            .consent_baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        Ok(())
    }
    pub(crate) fn with_deadline(
        &self,
        deadline: SystemTime,
        clock: Arc<dyn crate::OAuthClock>,
    ) -> Self {
        let mut store = self.clone();
        // Explicit transfer for this code-exchange caller only. Normal clone
        // never shares reentrancy with an unrelated live SDK manager.
        *store
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            self.active_lease().as_ref().map(Arc::downgrade);
        store.deadline = Some((deadline, clock));
        store
    }
    fn active_lease(&self) -> Option<Arc<RefreshLease>> {
        self.lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }
    pub(crate) async fn write_record(&self, record: AuthorizationRecord) -> Result<(), AuthError> {
        let existing = self.active_lease();
        let _guard = if existing.is_none() {
            self.acquire_refresh_guard().await?
        } else {
            None
        };
        self.persistence
            .write_until(
                &self.id,
                record,
                self.cancellation.clone(),
                self.deadline.clone(),
                self.active_lease(),
            )
            .await
    }
    /// Rollback of an accepted but cancelled code exchange. The exchange owner
    /// still holds the refresh lease, so no rotation can occur between the
    /// snapshot, actual put completion and restoration.
    pub(crate) async fn restore_record(
        &self,
        record: AuthorizationRecord,
    ) -> Result<(), AuthError> {
        let lease = self.active_lease().ok_or_else(storage_error)?;
        self.persistence
            .write_until(
                &self.id,
                record,
                CancellationToken::new(),
                None,
                Some(lease),
            )
            .await
    }
    pub(crate) async fn record(&self) -> Result<Option<AuthorizationRecord>, AuthError> {
        let record = self
            .persistence
            .read(&self.id)
            .await?
            .filter(|r| r.identity == self.identity);
        let Some(mut record) = record else {
            return Ok(None);
        };
        if let Some(pending) = record.pending_consent.take() {
            if !pending.committed || self.persistence.promotion_fenced(&self.id).await? {
                return Ok(Some(pending.previous));
            }
        }
        Ok(Some(record))
    }
}
#[async_trait]
impl CredentialStore for PersistentCredentials {
    async fn load(&self) -> Result<Option<StoredCredentials>, AuthError> {
        let baseline = self
            .consent_baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(previous) = baseline {
            return Ok(previous);
        }
        // An independent adapter/process has no local baseline. Its plain token
        // read waits the same file lease as refresh, so it cannot expose a pending
        // consent put. SDK refresh already owns this adapter's lease (reentrant).
        let _guard = if self.active_lease().is_none() {
            match self.acquire_refresh_guard().await {
                Ok(guard) => guard,
                Err(error) => {
                    let baseline = self
                        .consent_baseline
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    return baseline.map(Ok).unwrap_or(Err(error));
                }
            }
        } else {
            None
        };
        let record = self.record().await?;
        // Revalidate after the actual asynchronous read. A reader may have
        // begun before stage_consent; neither cached lease nor pre-read snapshot
        // authorizes a newly written candidate.
        let baseline = self
            .consent_baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(baseline.unwrap_or_else(|| record.and_then(|r| r.credentials)))
    }
    async fn save(&self, credentials: StoredCredentials) -> Result<(), AuthError> {
        let _guard = if self.active_lease().is_none() {
            self.acquire_refresh_guard().await?
        } else {
            None
        };
        let mut record = self
            .persistence
            .read(&self.id)
            .await?
            .filter(|r| r.identity == self.identity)
            .ok_or_else(storage_error)?;
        if credentials.client_id != record.registration.client_id
            || credentials.issuer.as_deref() != Some(&record.issuer)
        {
            return Err(storage_error());
        }
        let consent = self
            .consent_baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some();
        if consent {
            let previous = match record.pending_consent.take() {
                Some(pending)
                    if !pending.committed
                        || self.persistence.promotion_fenced(&self.id).await? =>
                {
                    pending.previous
                }
                _ => record.clone(),
            };
            record.credentials = previous.credentials.clone();
            record.pending_consent = Some(Box::new(PendingConsent {
                previous,
                candidate: credentials,
                committed: false,
            }));
        } else {
            if record.pending_consent.is_some() {
                return Err(storage_error());
            }
            record.credentials = Some(credentials);
        }
        self.write_record(record).await
    }
    async fn clear(&self) -> Result<(), AuthError> {
        let _guard = if self.active_lease().is_none() {
            self.acquire_refresh_guard().await?
        } else {
            None
        };
        if let Some(mut record) = self.record().await? {
            record.credentials = None;
            self.write_record(record).await?;
        }
        Ok(())
    }

    async fn acquire_refresh_guard(&self) -> Result<Option<CredentialRefreshGuard>, AuthError> {
        let local = tokio::select! {_=self.cancellation.cancelled()=>return Err(storage_error()),guard=self.gate.clone().lock_owned()=>guard};
        let file = self
            .persistence
            .file_guard(&self.id, &self.cancellation)
            .await?;
        let lease = Arc::new(RefreshLease {
            _local: Some(local),
            _file: file,
        });
        *self
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(&lease));
        Ok(Some(CredentialRefreshGuard::new(lease)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> AuthorizationRecord {
        AuthorizationRecord{identity:"identity".into(),resource:"https://resource.test/mcp".into(),issuer:"https://issuer.test".into(),registration:Registration{token_endpoint_auth_method:None,client_id:"client".into(),client_secret:Some("secret-canary".into()),redirect_uri:"http://127.0.0.1:37643/oauth/mcp/callback".into(),scopes:vec!["read".into()],application_type:Some("native".into()),registration_request:None,registration_response:None},credentials:Some(serde_json::from_value(serde_json::json!({"client_id":"client","token_response":{"access_token":"access-canary","refresh_token":"refresh-canary","token_type":"Bearer","expires_in":3600},"granted_scopes":["read"],"token_received_at":42,"issuer":"https://issuer.test"})).unwrap()),pending_consent:None}
    }
    struct ReadBarrierStore {
        inner: pioneer_keystore::MemorySecretStore,
        armed: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Notify,
        released: std::sync::Mutex<bool>,
        resumed: std::sync::Condvar,
        fail_write: std::sync::atomic::AtomicBool,
        fail_account_delete: std::sync::atomic::AtomicBool,
        mutation_phase: std::sync::atomic::AtomicU8,
        after_mutation: std::sync::atomic::AtomicBool,
        fail_readback: std::sync::atomic::AtomicBool,
        unreadable: std::sync::atomic::AtomicBool,
        delete_after_error: std::sync::atomic::AtomicBool,
        marker_unreadable: std::sync::atomic::AtomicBool,
        marker_deleted: tokio::sync::Notify,
    }
    impl ReadBarrierStore {
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.resumed.notify_all();
        }
    }
    impl SecretStore for ReadBarrierStore {
        fn get_string(&self, id: &SecretId) -> pioneer_keystore::Result<Option<String>> {
            if id.user().ends_with("::promotion")
                && self
                    .marker_unreadable
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(pioneer_keystore::KeystoreError::ReadFailed(
                    "injected fence readback".into(),
                ));
            }
            if id.user() == "installation"
                && self.unreadable.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(pioneer_keystore::KeystoreError::ReadFailed(
                    "injected".into(),
                ));
            }
            if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                let mut released = self.released.lock().unwrap();
                while !*released {
                    released = self
                        .resumed
                        .wait_timeout(released, std::time::Duration::from_secs(3))
                        .unwrap()
                        .0;
                    if !*released {
                        panic!("actual read not released");
                    }
                }
            }
            self.inner.get_string(id)
        }
        fn put_string(
            &self,
            id: &SecretId,
            value: &str,
            meta: SecretMeta,
        ) -> pioneer_keystore::Result<()> {
            if self.fail_write.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(pioneer_keystore::KeystoreError::WriteFailed(
                    "injected".into(),
                ));
            }
            let phase = serde_json::from_str::<serde_json::Value>(value)
                .ok()
                .and_then(|r| r.get("pending_consent").cloned())
                .filter(|r| r.is_object())
                .map(|r| if r["committed"] == true { 2 } else { 1 })
                .unwrap_or(0);
            if phase != 0
                && phase
                    == self
                        .mutation_phase
                        .load(std::sync::atomic::Ordering::SeqCst)
            {
                if self
                    .after_mutation
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    self.inner.put_string(id, value, meta)?;
                }
                self.unreadable.store(
                    self.fail_readback.load(std::sync::atomic::Ordering::SeqCst),
                    std::sync::atomic::Ordering::SeqCst,
                );
                return Err(pioneer_keystore::KeystoreError::WriteFailed(
                    "injected after delegate".into(),
                ));
            }
            self.inner.put_string(id, value, meta)
        }
        fn delete(&self, id: &SecretId) -> pioneer_keystore::Result<bool> {
            if !id.user().ends_with("::promotion")
                && self
                    .fail_account_delete
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(pioneer_keystore::KeystoreError::DeleteFailed(
                    "injected account failure".into(),
                ));
            }
            if id.user().ends_with("::promotion")
                && self
                    .delete_after_error
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                self.inner.delete(id)?;
                self.marker_unreadable
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                self.marker_deleted.notify_one();
                return Err(pioneer_keystore::KeystoreError::DeleteFailed(
                    "injected after deletion".into(),
                ));
            }
            self.inner.delete(id)
        }
        fn exists(&self, id: &SecretId) -> pioneer_keystore::Result<bool> {
            self.inner.exists(id)
        }
        fn list(
            &self,
            filter: SecretFilter,
        ) -> pioneer_keystore::Result<Vec<pioneer_keystore::SecretEntryMeta>> {
            self.inner.list(filter)
        }
    }
    fn read_store() -> Arc<ReadBarrierStore> {
        Arc::new(ReadBarrierStore {
            inner: Default::default(),
            armed: Default::default(),
            entered: Default::default(),
            released: Default::default(),
            resumed: Default::default(),
            fail_write: Default::default(),
            fail_account_delete: Default::default(),
            mutation_phase: Default::default(),
            after_mutation: Default::default(),
            fail_readback: Default::default(),
            unreadable: Default::default(),
            delete_after_error: Default::default(),
            marker_unreadable: Default::default(),
            marker_deleted: Default::default(),
        })
    }

    #[tokio::test]
    async fn failed_clear_reinstates_candidate_quarantine_for_independent_restart_reader() {
        use std::sync::atomic::Ordering::SeqCst;
        let secrets = read_store();
        let persistence = OAuthPersistence::new(secrets.clone(), None);
        let previous = record();
        let mut promoted = previous.clone();
        let mut candidate_json =
            serde_json::to_value(promoted.credentials.clone().unwrap()).unwrap();
        candidate_json["token_response"]["access_token"] = serde_json::json!("retired-candidate");
        let candidate: StoredCredentials = serde_json::from_value(candidate_json).unwrap();
        promoted.credentials = Some(candidate.clone());
        promoted.pending_consent = Some(Box::new(PendingConsent {
            previous: previous.clone(),
            candidate,
            committed: true,
        }));
        persistence.write("installation", promoted).await.unwrap();
        secrets.fail_account_delete.store(true, SeqCst);
        assert!(persistence.delete("installation").await.is_err());
        assert!(
            secrets
                .inner
                .exists(&SecretId::mcp_oauth("installation::promotion").unwrap())
                .unwrap()
        );
        let restarted = OAuthPersistence::new(secrets.clone(), None);
        let visible = restarted.read("installation").await.unwrap().unwrap();
        assert!(visible.pending_consent.is_some());
        assert_eq!(
            serde_json::to_value(visible.credentials).unwrap(),
            serde_json::to_value(previous.credentials).unwrap()
        );
        secrets.fail_account_delete.store(false, SeqCst);
        restarted.delete("installation").await.unwrap();
        assert!(restarted.read("installation").await.unwrap().is_none());
        assert!(
            !secrets
                .inner
                .exists(&SecretId::mcp_oauth("installation::promotion").unwrap())
                .unwrap()
        );
    }
    #[tokio::test]
    async fn uncertain_post_mutation_fence_delete_is_owned_until_readback_resolves() {
        use std::sync::atomic::Ordering::SeqCst;
        let secrets = read_store();
        let persistence = OAuthPersistence::new(secrets.clone(), None);
        let previous = record();
        persistence
            .write("installation", previous.clone())
            .await
            .unwrap();
        let writer = Arc::new(persistence.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        ));
        let lease = writer.acquire_refresh_guard().await.unwrap();
        writer.stage_consent(previous.credentials.clone());
        let mut response = serde_json::to_value(previous.credentials.unwrap()).unwrap();
        response["token_response"]["access_token"] = serde_json::json!("confirmed-candidate");
        writer
            .save(serde_json::from_value(response).unwrap())
            .await
            .unwrap();
        secrets.delete_after_error.store(true, SeqCst);
        let owner = writer.clone();
        let mut commit = tokio::spawn(async move { owner.commit_consent().await });
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            secrets.marker_deleted.notified(),
        )
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "unknown deletion must not report Failed"
        );
        assert!(
            persistence.read("installation").await.is_err(),
            "unknown receipt must not be usable"
        );
        let raw = persistence.read_raw("installation").await.unwrap().unwrap();
        assert!(raw.pending_consent.unwrap().committed);
        secrets.delete_after_error.store(false, SeqCst);
        secrets.marker_unreadable.store(false, SeqCst);
        tokio::time::timeout(std::time::Duration::from_secs(3), commit)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(lease);
        let reader = persistence.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        assert_eq!(
            serde_json::to_value(reader.load().await.unwrap().unwrap()).unwrap()["token_response"]
                ["access_token"],
            "confirmed-candidate"
        );
    }
    #[tokio::test]
    async fn post_mutation_errors_cannot_expose_failed_consent_to_independent_sdk_readers() {
        use std::sync::atomic::Ordering::SeqCst;
        for phase in [1, 2] {
            for after in [false, true] {
                for unknown in [false, true] {
                    let secrets = read_store();
                    let persistence = OAuthPersistence::new(secrets.clone(), None);
                    let previous = record();
                    persistence
                        .write("installation", previous.clone())
                        .await
                        .unwrap();
                    let writer = persistence.credential_store(
                        "installation".into(),
                        "identity".into(),
                        Arc::new(Mutex::new(())),
                        CancellationToken::new(),
                    );
                    let lease = writer.acquire_refresh_guard().await.unwrap();
                    writer.stage_consent(previous.credentials.clone());
                    let mut response =
                        serde_json::to_value(previous.credentials.clone().unwrap()).unwrap();
                    response["token_response"]["access_token"] = serde_json::json!("candidate");
                    secrets.mutation_phase.store(phase, SeqCst);
                    secrets.after_mutation.store(after, SeqCst);
                    secrets.fail_readback.store(unknown, SeqCst);
                    let save = writer.save(serde_json::from_value(response).unwrap()).await;
                    let outcome = if save.is_ok() {
                        writer.commit_consent().await
                    } else {
                        save
                    };
                    let success = after && !unknown;
                    assert_eq!(outcome.is_ok(), success);
                    drop(lease);
                    drop(writer);
                    secrets.unreadable.store(false, SeqCst);
                    secrets.mutation_phase.store(0, SeqCst);
                    let restarted = OAuthPersistence::new(secrets.clone(), None);
                    let reader = restarted.credential_store(
                        "installation".into(),
                        "identity".into(),
                        Arc::new(Mutex::new(())),
                        CancellationToken::new(),
                    );
                    let visible =
                        serde_json::to_value(reader.load().await.unwrap().unwrap()).unwrap();
                    assert_eq!(
                        visible["token_response"]["access_token"],
                        if success {
                            "candidate"
                        } else {
                            "access-canary"
                        }
                    );
                    restarted
                        .clear_stale_record("installation", "identity")
                        .await
                        .unwrap();
                    assert_eq!(
                        serde_json::to_value(reader.load().await.unwrap().unwrap()).unwrap(),
                        visible
                    );
                    if !success {
                        assert_eq!(
                            serde_json::to_value(
                                restarted.read("installation").await.unwrap().unwrap()
                            )
                            .unwrap(),
                            serde_json::to_value(previous).unwrap()
                        );
                    }
                }
            }
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pre_stage_actual_read_cannot_expose_candidate_before_terminal_commit() {
        for outcome in 0..3 {
            let secrets = read_store();
            let directory = std::env::temp_dir().join(format!(
                "pioneer-oauth-read-visibility-{}",
                uuid::Uuid::new_v4()
            ));
            struct DirectoryCleanup(PathBuf);
            impl Drop for DirectoryCleanup {
                fn drop(&mut self) {
                    let _ = std::fs::remove_dir_all(&self.0);
                }
            }
            let _directory_cleanup = DirectoryCleanup(directory.clone());
            let persistence = OAuthPersistence::new(secrets.clone(), Some(directory));
            let previous = record();
            persistence
                .write("installation", previous.clone())
                .await
                .unwrap();
            let writer = Arc::new(persistence.credential_store(
                "installation".into(),
                "identity".into(),
                Arc::new(Mutex::new(())),
                CancellationToken::new(),
            ));
            let lease = writer.acquire_refresh_guard().await.unwrap();
            secrets
                .armed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let shared = writer.clone();
            let read = tokio::spawn(async move { shared.load().await });
            secrets.entered.notified().await;
            let independent = persistence.credential_store(
                "installation".into(),
                "identity".into(),
                Arc::new(Mutex::new(())),
                CancellationToken::new(),
            );
            let independent_read = tokio::spawn(async move { independent.load().await });
            writer.stage_consent(previous.credentials.clone());
            let mut response = serde_json::to_value(previous.credentials.clone().unwrap()).unwrap();
            response["token_response"]["access_token"] = serde_json::json!("not-yet-committed");
            writer
                .save(serde_json::from_value(response).unwrap())
                .await
                .unwrap();
            secrets.release();
            assert_eq!(
                serde_json::to_value(read.await.unwrap().unwrap()).unwrap(),
                serde_json::to_value(previous.credentials.clone()).unwrap()
            );
            assert!(
                !independent_read.is_finished(),
                "independent reader must await terminal lease release"
            );
            if outcome == 0 {
                writer.commit_consent().await.unwrap();
            } else {
                writer.restore_record(previous.clone()).await.unwrap();
            }
            drop(lease);
            let visible = serde_json::to_value(independent_read.await.unwrap().unwrap()).unwrap();
            if outcome == 0 {
                assert_eq!(
                    visible["token_response"]["access_token"],
                    "not-yet-committed"
                );
            } else {
                assert_eq!(visible, serde_json::to_value(previous.credentials).unwrap());
            }
        }
    }
    #[tokio::test]
    async fn failed_rollback_pending_record_survives_adapter_rebind_and_storage_recovery() {
        let secrets = read_store();
        let persistence = OAuthPersistence::new(secrets.clone(), None);
        let previous = record();
        persistence
            .write("installation", previous.clone())
            .await
            .unwrap();
        let writer = persistence.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        let lease = writer.acquire_refresh_guard().await.unwrap();
        writer.stage_consent(previous.credentials.clone());
        let mut response = serde_json::to_value(previous.credentials.clone().unwrap()).unwrap();
        response["token_response"]["access_token"] = serde_json::json!("cancelled-candidate");
        writer
            .save(serde_json::from_value(response).unwrap())
            .await
            .unwrap();
        secrets
            .fail_write
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(writer.restore_record(previous.clone()).await.is_err());
        drop(lease);
        drop(writer);
        drop(persistence);
        let restarted = OAuthPersistence::new(secrets.clone(), None);
        let reader = restarted.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        assert_eq!(
            serde_json::to_value(reader.load().await.unwrap()).unwrap(),
            serde_json::to_value(previous.credentials.clone()).unwrap()
        );
        assert!(
            restarted
                .clear_stale_record("installation", "identity")
                .await
                .is_err()
        );
        assert!(
            restarted
                .read("installation")
                .await
                .unwrap()
                .unwrap()
                .pending_consent
                .is_some()
        );
        secrets
            .fail_write
            .store(false, std::sync::atomic::Ordering::SeqCst);
        restarted
            .clear_stale_record("installation", "identity")
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(restarted.read("installation").await.unwrap().unwrap()).unwrap(),
            serde_json::to_value(previous).unwrap()
        );
    }
    #[tokio::test]
    async fn pending_consent_is_hidden_from_existing_and_independent_sdk_readers() {
        for commit in [false, true] {
            let persistence =
                OAuthPersistence::new(Arc::new(pioneer_keystore::MemorySecretStore::new()), None);
            let previous = record();
            persistence
                .write("installation", previous.clone())
                .await
                .unwrap();
            let cancellation = CancellationToken::new();
            let writer = persistence.credential_store(
                "installation".into(),
                "identity".into(),
                Arc::new(Mutex::new(())),
                cancellation.clone(),
            );
            let reader = persistence.credential_store(
                "installation".into(),
                "identity".into(),
                Arc::new(Mutex::new(())),
                CancellationToken::new(),
            );
            let lease = writer.acquire_refresh_guard().await.unwrap();
            writer.stage_consent(previous.credentials.clone());
            let mut response = serde_json::to_value(previous.credentials.clone().unwrap()).unwrap();
            response["token_response"]["access_token"] = serde_json::json!("pending-new-grant");
            let pending = serde_json::from_value(response).unwrap();
            writer.save(pending).await.unwrap();
            assert_eq!(
                serde_json::to_value(writer.load().await.unwrap()).unwrap(),
                serde_json::to_value(previous.credentials.clone()).unwrap()
            );
            let entered = Arc::new(tokio::sync::Notify::new());
            let ready = entered.clone();
            let mut independent = tokio::spawn(async move {
                ready.notify_one();
                reader.load().await
            });
            entered.notified().await;
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut independent)
                    .await
                    .is_err(),
                "independent plain reader must wait consent admission"
            );
            if commit {
                writer.commit_consent().await.unwrap();
            } else {
                cancellation.cancel();
                writer.restore_record(previous.clone()).await.unwrap();
            }
            drop(lease);
            let visible = serde_json::to_value(independent.await.unwrap().unwrap()).unwrap();
            if commit {
                assert_eq!(
                    visible["token_response"]["access_token"],
                    "pending-new-grant"
                );
            } else {
                assert_eq!(visible, serde_json::to_value(previous.credentials).unwrap());
            }
        }
    }

    #[tokio::test]
    async fn credential_store_resave_preserves_absolute_expiry_and_clear_retains_registration() {
        let persistence =
            OAuthPersistence::new(Arc::new(pioneer_keystore::MemorySecretStore::new()), None);
        persistence.write("installation", record()).await.unwrap();
        let adapter = persistence.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        adapter
            .save(adapter.load().await.unwrap().unwrap())
            .await
            .unwrap();
        assert_eq!(
            adapter.load().await.unwrap().unwrap().token_received_at,
            Some(42)
        );
        adapter.clear().await.unwrap();
        assert!(adapter.load().await.unwrap().is_none());
        let stored = persistence.read("installation").await.unwrap().unwrap();
        assert_eq!(
            stored.registration.client_secret.as_deref(),
            Some("secret-canary")
        );
        assert_eq!(
            stored.registration.application_type.as_deref(),
            Some("native")
        );
        assert!(!format!("{stored:?}").contains("canary"));
    }
    #[tokio::test]
    async fn cancelled_registration_write_cannot_recreate_deleted_installation() {
        let persistence =
            OAuthPersistence::new(Arc::new(pioneer_keystore::MemorySecretStore::new()), None);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(
            persistence
                .write_checked("installation", record(), cancellation)
                .await
                .is_err()
        );
        assert!(persistence.ids().await.unwrap().is_empty());
    }
    #[tokio::test]
    async fn cleanup_waits_for_other_process_rotation_before_deleting_record() {
        let path = std::env::temp_dir().join(format!(
            "pioneer-oauth-delete-test-{}",
            uuid::Uuid::new_v4()
        ));
        let persistence = OAuthPersistence::new(
            Arc::new(pioneer_keystore::MemorySecretStore::new()),
            Some(path.clone()),
        );
        persistence.write("installation", record()).await.unwrap();
        let adapter = persistence.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        let refresh = adapter.acquire_refresh_guard().await.unwrap();
        let cleanup = persistence.clone();
        let deleting = tokio::spawn(async move { cleanup.delete("installation").await });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(!deleting.is_finished());
        adapter
            .save(adapter.load().await.unwrap().unwrap())
            .await
            .unwrap();
        drop(refresh);
        deleting.await.unwrap().unwrap();
        assert!(persistence.read("installation").await.unwrap().is_none());
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn waiting_for_another_process_refresh_guard_observes_cancellation() {
        let path =
            std::env::temp_dir().join(format!("pioneer-oauth-lock-test-{}", uuid::Uuid::new_v4()));
        let persistence = OAuthPersistence::new(
            Arc::new(pioneer_keystore::MemorySecretStore::new()),
            Some(path.clone()),
        );
        let a = persistence.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        let cancellation = CancellationToken::new();
        let b = persistence.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            cancellation.clone(),
        );
        let guard = a.acquire_refresh_guard().await.unwrap();
        let waiting = tokio::spawn(async move { b.acquire_refresh_guard().await });
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        cancellation.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        drop(guard);
        std::fs::remove_dir_all(path).unwrap();
    }
    struct BlockingPutStore {
        inner: pioneer_keystore::MemorySecretStore,
        started: tokio::sync::Notify,
        release: (std::sync::Mutex<bool>, std::sync::Condvar),
    }
    impl SecretStore for BlockingPutStore {
        fn get_string(&self, id: &SecretId) -> pioneer_keystore::Result<Option<String>> {
            self.inner.get_string(id)
        }
        fn put_string(
            &self,
            id: &SecretId,
            value: &str,
            meta: SecretMeta,
        ) -> pioneer_keystore::Result<()> {
            // Barrier is inside the actual mutation, after all outer checks.
            self.started.notify_one();
            let (lock, ready) = &self.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            self.inner.put_string(id, value, meta)
        }
        fn delete(&self, id: &SecretId) -> pioneer_keystore::Result<bool> {
            self.inner.delete(id)
        }
        fn exists(&self, id: &SecretId) -> pioneer_keystore::Result<bool> {
            self.inner.exists(id)
        }
        fn list(
            &self,
            filter: SecretFilter,
        ) -> pioneer_keystore::Result<Vec<pioneer_keystore::SecretEntryMeta>> {
            self.inner.list(filter)
        }
    }
    async fn cancelled_actual_put_then_cleanup(mode: u8) {
        let store = Arc::new(BlockingPutStore {
            inner: pioneer_keystore::MemorySecretStore::new(),
            started: tokio::sync::Notify::new(),
            release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        });
        let original = OAuthPersistence::new(store.clone(), None);
        let independent_cleanup = OAuthPersistence::new(store.clone(), None);
        let credentials = original.credential_store(
            "installation".into(),
            "identity".into(),
            Arc::new(Mutex::new(())),
            CancellationToken::new(),
        );
        let writing = tokio::spawn(async move {
            let _refresh = credentials.acquire_refresh_guard().await.unwrap();
            credentials.write_record(record()).await
        });
        store.started.notified().await;
        writing.abort();
        let _ = writing.await;
        assert!(original.read("installation").await.unwrap().is_none());
        let mut cleanup = tokio::spawn(async move {
            if mode == 1 {
                independent_cleanup
                    .clear_stale_record("installation", "new-identity")
                    .await
            } else if mode == 2 {
                for id in independent_cleanup.ids().await? {
                    independent_cleanup.delete(&id).await?;
                }
                Ok(())
            } else {
                independent_cleanup.delete("installation").await
            }
        });
        // Release the barrier before asserting, so a broken implementation
        // cannot strand a blocking worker when the assertion fails.
        let premature =
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut cleanup).await;
        *store.release.0.lock().unwrap() = true;
        store.release.1.notify_all();
        if premature.is_err() {
            cleanup.await.unwrap().unwrap();
        }
        original.drain().await;
        assert!(
            premature.is_err(),
            "cleanup must wait for the actual cancelled put"
        );
        assert!(
            original.read("installation").await.unwrap().is_none(),
            "retired identity cannot resurrect after cleanup"
        );
    }
    #[tokio::test]
    async fn cancelled_actual_put_is_drained_before_identity_replacement() {
        cancelled_actual_put_then_cleanup(1).await;
    }
    #[tokio::test]
    async fn independent_gc_waits_for_cancelled_actual_put() {
        cancelled_actual_put_then_cleanup(0).await;
    }
    #[tokio::test]
    async fn independent_gc_enumeration_includes_cancelled_uncommitted_put() {
        cancelled_actual_put_then_cleanup(2).await;
    }
}
