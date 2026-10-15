use super::*;

impl GatewayWsClient {
    pub fn new() -> Self {
        let (command_tx, command_rx) = unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel();

        let command_sender = GatewayWsCommandSender {
            command_tx,
            next_connection_id: Arc::new(AtomicU64::new(0)),
            session_access: Arc::new(Mutex::new(None)),
            connection_generations: Arc::default(),
            #[cfg(test)]
            test_requests: Arc::default(),
        };

        #[cfg(test)]
        let test_event_tx = Arc::new(Mutex::new(Some(event_tx.clone())));
        spawn_worker(command_rx, event_tx);

        Self {
            command_sender,
            event_rx: Arc::new(Mutex::new(event_rx)),
            #[cfg(test)]
            test_event_tx,
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_test_event(&self, event: GatewayWsEvent) {
        self.command_sender
            .connection_generations
            .lock()
            .unwrap()
            .active = Some(super::super::event_connection_id(&event));
        self.test_event_tx
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(event)
            .unwrap();
    }
    #[cfg(test)]
    pub(crate) fn close_test_events(&self) {
        self.test_event_tx.lock().unwrap().take();
    }
    pub fn command_sender(&self) -> GatewayWsCommandSender {
        self.command_sender.clone()
    }

    pub fn drain_events(&self) -> Vec<GatewayWsEvent> {
        let Ok(event_rx) = self.event_rx.lock() else {
            return Vec::new();
        };

        let mut events = Vec::new();

        while let Ok(event) = event_rx.try_recv() {
            events.push(event);
        }

        events
    }

    pub fn recv_event(&self) -> Option<GatewayWsEvent> {
        let Ok(event_rx) = self.event_rx.lock() else {
            return None;
        };
        event_rx.recv().ok()
    }
}

impl Drop for GatewayWsClient {
    fn drop(&mut self) {
        // Stop worker only when the last client handle is being dropped.
        // Intermediate clones are created in UI/background tasks and must not
        // terminate the shared websocket worker.
        if Arc::strong_count(&self.event_rx) == 1 {
            let _ = self.command_sender.shutdown();
        }
    }
}
