use super::*;

impl GatewayWsClient {
    pub fn new() -> Self {
        let (command_tx, command_rx) = work_channel(Boundary::ClientCommand);
        let (event_tx, event_rx) = mpsc::channel();
        let event_tx = super::super::client::GatewayWsEventSender::from(event_tx);

        let command_sender = GatewayWsCommandSender {
            command_tx,
            voice_budget: Arc::default(),
            next_connection_id: Arc::new(AtomicU64::new(0)),
            socket: Arc::default(),
            session_access: Arc::new(Mutex::new(None)),
            connection_generations: Arc::default(),
            #[cfg(test)]
            test_requests: Arc::default(),
            #[cfg(test)]
            test_cancels: Arc::default(),
        };

        #[cfg(test)]
        let test_event_tx = Arc::new(Mutex::new(Some(event_tx.clone())));
        spawn_worker(command_rx, event_tx, command_sender.socket.clone());

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
        match &event {
            GatewayWsEvent::Connected { connection_id, .. } => {
                let previous_owner = {
                    let mut socket = self.command_sender.socket.lock().unwrap();
                    if let Some(previous) = socket.take() {
                        previous.retire();
                    }
                    let (receipt, cancels) = SocketReceipt::new(*connection_id);
                    let previous = self
                        .command_sender
                        .test_cancels
                        .lock()
                        .unwrap()
                        .replace(cancels);
                    *socket = Some(receipt);
                    previous
                };
                drop(previous_owner); // terminal wakes never run under socket/fixture locks
            }
            GatewayWsEvent::Disconnected { .. } => {
                if let Some(socket) = self.command_sender.socket.lock().unwrap().take() {
                    socket.retire();
                }
                let previous_owner = self.command_sender.test_cancels.lock().unwrap().take();
                drop(previous_owner);
            }
            _ => {}
        }
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

    pub fn drain_events(&self) -> Vec<pioneer_observability::work::WorkItem<GatewayWsEvent>> {
        let Ok(event_rx) = self.event_rx.lock() else {
            return Vec::new();
        };

        let mut events = Vec::new();

        while let Ok(event) = event_rx.try_recv() {
            events.push(event);
        }

        events
    }

    pub fn recv_event(&self) -> Option<pioneer_observability::work::WorkItem<GatewayWsEvent>> {
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
