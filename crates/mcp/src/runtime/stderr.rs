use crate::redaction::{bounded_text, redact_text};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct StderrTail {
    inner: Arc<Mutex<String>>,
    max_chars: usize,
    secrets: Arc<Vec<String>>,
}

impl StderrTail {
    pub fn new(max_chars: usize, secrets: Vec<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(String::new())),
            max_chars,
            secrets: Arc::new(secrets),
        }
    }

    pub fn spawn_reader<R>(&self, mut reader: R)
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let inner = self.inner.clone();
        let max_chars = self.max_chars;
        let secrets = self.secrets.clone();
        tokio::spawn(async move {
            let mut buffer = [0_u8; 1024];
            loop {
                let read = match reader.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(_) => break,
                };
                let chunk = String::from_utf8_lossy(&buffer[..read]);
                let chunk = redact_text(&chunk, secrets.as_slice());
                let mut guard = inner.lock().await;
                guard.push_str(chunk.as_str());
                *guard = bounded_text(guard.as_str(), max_chars);
            }
        });
    }

    pub async fn snapshot(&self) -> String {
        self.inner.lock().await.clone()
    }
}
