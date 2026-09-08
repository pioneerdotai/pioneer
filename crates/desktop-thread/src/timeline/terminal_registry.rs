//! Live command presentation readers. No shell or PTY is created here.
use gpui_kit::{App, AppContext, Entity};
use std::{
    collections::HashMap,
    io::{self, Cursor, Read},
    rc::Rc,
    sync::{Arc, Condvar, Mutex},
};
use terminal::TerminalView;

#[derive(Default)]
struct ReaderState {
    next: Option<Vec<u8>>,
    closed: bool,
    ending: bool,
}
#[derive(Default)]
struct ReaderShared {
    state: Mutex<ReaderState>,
    changed: Condvar,
}
pub(super) struct TerminalInput {
    shared: Arc<ReaderShared>,
}
pub(super) struct TerminalReader {
    shared: Arc<ReaderShared>,
    current: Cursor<Vec<u8>>,
}
impl TerminalInput {
    pub fn new() -> (Self, TerminalReader) {
        let shared = Arc::new(ReaderShared::default());
        (
            Self {
                shared: shared.clone(),
            },
            TerminalReader {
                shared,
                current: Cursor::new(Vec::new()),
            },
        )
    }
    fn finish(&self) {
        self.shared.state.lock().unwrap().ending = true;
        self.shared.changed.notify_one();
    }
    fn cancel(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.closed = true;
        state.next = None;
        self.shared.changed.notify_one();
    }
    pub fn replace(&self, text: &str) {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed || state.ending {
            return;
        }
        // Cancel a partial control string, reset the emulator, then replay the
        // authoritative output snapshot through the terminal's public reader seam.
        let mut bytes = b"\x18\x1b\\\x1bc\x1b]104\x07\x1b]110\x07\x1b]111\x07\x1b]112\x07".to_vec();
        bytes.extend_from_slice(text.as_bytes());
        state.next = Some(bytes);
        self.shared.changed.notify_one();
    }
}
impl Drop for TerminalInput {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl Read for TerminalReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let mut state = self.shared.state.lock().unwrap();
            if state.closed {
                return Ok(0);
            }
            if self.current.position() < self.current.get_ref().len() as u64 {
                drop(state);
                return self.current.read(output);
            }
            if let Some(bytes) = state.next.take() {
                self.current = Cursor::new(bytes);
                drop(state);
                continue;
            }
            if state.ending {
                return Ok(0);
            }
            drop(self.shared.changed.wait(state).unwrap());
        }
    }
}
/// A row retains its display after EOF. The input is only a cancellation handle
/// once final bytes have drained; it keeps no reader thread alive after EOF.
#[derive(Clone)]
pub(crate) struct TerminalPresentation {
    work_item_id: String,
    session: String,
    text: String,
    cols: usize,
    rows: usize,
    active: bool,
    input: Rc<TerminalInput>,
    pub view: Entity<TerminalView>,
}
#[derive(Default)]
pub(crate) struct TerminalRegistry {
    pub entries: HashMap<String, TerminalPresentation>,
}
impl TerminalRegistry {
    pub fn synchronize(
        &mut self,
        work_item_id: &str,
        session: &str,
        text: &str,
        active: bool,
        previous: Option<&TerminalPresentation>,
        config: terminal::TerminalConfig,
        cx: &mut App,
    ) -> TerminalPresentation {
        let previous = self.entries.get(work_item_id).or(previous);
        let reusable = previous.filter(|owner| {
            owner.work_item_id == work_item_id
                && owner.session == session
                && (owner.active || (!active && owner.text == text))
                && !owner.input.shared.state.lock().unwrap().closed
        });
        let owner = if let Some(previous) = reusable {
            let mut owner = previous.clone();
            if owner.text != text {
                owner.input.replace(text);
                owner.text = text.to_owned();
            }
            if owner.cols != config.cols || owner.rows != config.rows {
                owner.cols = config.cols;
                owner.rows = config.rows;
                owner.view.update(cx, |terminal, cx| {
                    terminal.resize(config.cols, config.rows);
                    cx.notify();
                });
            }
            owner.active = active;
            owner
        } else {
            if let Some(previous) = previous {
                previous.input.cancel();
            }
            let (input, reader) = TerminalInput::new();
            input.replace(text);
            let cols = config.cols;
            let rows = config.rows;
            let view = cx.new(|cx| TerminalView::new(std::io::sink(), reader, config, cx));
            TerminalPresentation {
                work_item_id: work_item_id.to_owned(),
                session: session.to_owned(),
                text: text.to_owned(),
                cols,
                rows,
                active,
                input: Rc::new(input),
                view,
            }
        };
        if active {
            self.entries.insert(work_item_id.to_owned(), owner.clone());
        } else {
            // Retire the active session without cancelling its final snapshot.
            // The row owns the display and cancellation handle through EOF.
            owner.input.finish();
            self.entries.remove(work_item_id);
        }
        owner
    }
    pub fn retain(&mut self, mut live: impl FnMut(&str) -> bool) {
        self.entries.retain(|id, owner| {
            if live(id) {
                true
            } else {
                owner.input.cancel();
                false
            }
        });
    }
    pub fn clear(&mut self) {
        self.retain(|_| false);
    }
}
impl Drop for TerminalRegistry {
    fn drop(&mut self) {
        self.clear();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completed_reader_drains_final_snapshot_then_returns_eof() {
        let (input, mut reader) = TerminalInput::new();
        input.replace("final");
        input.finish();
        let mut bytes = [0; 128];
        let n = reader.read(&mut bytes).unwrap();
        assert!(bytes[..n].ends_with(b"final"));
        assert_eq!(reader.read(&mut bytes).unwrap(), 0);
        input.replace("late");
        assert_eq!(reader.read(&mut bytes).unwrap(), 0);
    }
    #[test]
    fn retired_session_leaves_final_bytes_with_the_row_until_eof_or_row_drop() {
        let (input, mut reader) = TerminalInput::new();
        let active_session = Rc::new(input);
        let row = active_session.clone();
        active_session.replace("final output");
        active_session.finish();
        drop(active_session);
        let mut result = Vec::new();
        reader.read_to_end(&mut result).unwrap();
        assert!(result.ends_with(b"final output"));
        assert!(!row.shared.state.lock().unwrap().closed);
        drop(row);
        assert!(reader.shared.state.lock().unwrap().closed);
        assert_eq!(reader.read(&mut [0; 8]).unwrap(), 0);

        let (input, mut reader) = TerminalInput::new();
        input.replace("unpublished final output");
        input.finish();
        drop(input);
        assert_eq!(reader.read(&mut [0; 8]).unwrap(), 0);
    }
    #[test]
    fn fake_reader_coalesces_revisions_and_close_rejects_pending_bytes() {
        let (input, mut reader) = TerminalInput::new();
        input.replace("old");
        input.replace("new");
        let mut bytes = [0; 64];
        let n = reader.read(&mut bytes).unwrap();
        assert_eq!(
            &bytes[..n],
            b"\x18\x1b\\\x1bc\x1b]104\x07\x1b]110\x07\x1b]111\x07\x1b]112\x07new"
        );
        input.replace("late");
        drop(input);
        assert_eq!(reader.read(&mut bytes).unwrap(), 0);
    }
}

#[cfg(test)]
mod emulator_tests {
    use super::*;
    fn terminal() -> terminal::TerminalState {
        let (tx, _rx) = std::sync::mpsc::channel();
        terminal::TerminalState::new(80, 24, terminal::GpuiEventProxy::new(tx))
    }
    #[test]
    fn replacement_reader_preserves_the_same_final_grid_as_a_fresh_terminal() {
        let (input, mut reader) = TerminalInput::new();
        let mut retained = terminal();
        let mut bytes = [0; 4096];
        for output in [
            "first\r\nsecond",
            "\x1b]4;1;rgb:00/ff/00\x07\x1b[31mgreen",
            "\x1b[31mred\x1b[0m\r\nfinal",
            "short",
        ] {
            input.replace(output);
            let n = reader.read(&mut bytes).unwrap();
            retained.process_bytes(&bytes[..n]);
            let mut fresh = terminal();
            fresh.process_bytes(output.as_bytes());
            let actual = retained.with_term(|term| format!("{:?}", term.grid()));
            let expected = fresh.with_term(|term| format!("{:?}", term.grid()));
            assert_eq!(actual, expected);
            assert_eq!(
                retained.with_term(|term| term.colors()[1]),
                fresh.with_term(|term| term.colors()[1])
            );
        }
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    #[gpui_kit::test]
    fn completed_display_retires_session_reuses_equal_output_and_replaces_late_output(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        // TerminalView's public reader seam includes an OS thread, even for fake bytes.
        cx.background_executor.allow_parking();
        let mut registry = TerminalRegistry::default();
        let mut other_thread = TerminalRegistry::default();
        let config = terminal::TerminalConfig::default;
        let active = cx.update(|cx| {
            registry.synchronize("work", "session", "streaming", true, None, config(), cx)
        });
        let other = cx.update(|cx| {
            other_thread.synchronize("work", "session", "other thread", true, None, config(), cx)
        });
        let input = active.input.shared.clone();
        let completed = cx.update(|cx| {
            registry.synchronize(
                "work",
                "session",
                "final",
                false,
                Some(&active),
                config(),
                cx,
            )
        });
        assert_eq!(active.view.entity_id(), completed.view.entity_id());
        assert!(registry.entries.is_empty());
        assert!(input.state.lock().unwrap().ending);
        assert!(!input.state.lock().unwrap().closed);
        assert!(Rc::ptr_eq(&active.input, &completed.input));
        drop(active);
        let same = cx.update(|cx| {
            registry.synchronize(
                "work",
                "session",
                "final",
                false,
                Some(&completed),
                terminal::TerminalConfig {
                    cols: 90,
                    ..config()
                },
                cx,
            )
        });
        assert_eq!(same.view.entity_id(), completed.view.entity_id());
        assert!(Rc::ptr_eq(&same.input, &completed.input));
        assert_eq!(same.cols, 90);
        let replacement = cx.update(|cx| {
            registry.synchronize(
                "work",
                "session",
                "late final",
                false,
                Some(&same),
                config(),
                cx,
            )
        });
        assert_ne!(replacement.view.entity_id(), same.view.entity_id());
        assert!(input.state.lock().unwrap().closed);
        let repeated = cx.update(|cx| {
            registry.synchronize(
                "work",
                "session",
                "late final",
                false,
                Some(&replacement),
                config(),
                cx,
            )
        });
        assert_eq!(replacement.view.entity_id(), repeated.view.entity_id());
        assert!(registry.entries.is_empty());
        assert_eq!(
            other_thread.entries["work"].view.entity_id(),
            other.view.entity_id()
        );
        assert!(!other.input.shared.state.lock().unwrap().closed);
        let retired = same.view.downgrade();
        drop(same);
        drop(completed);
        let last = replacement.view.downgrade();
        let final_reader = replacement.input.shared.clone();
        drop(replacement);
        drop(repeated);
        cx.run_until_parked();
        assert!(retired.upgrade().is_none());
        assert!(last.upgrade().is_none());
        assert!(final_reader.state.lock().unwrap().closed);
        // Cancelling a registry must also cancel readers held by a published row.
        other_thread.retain(|_| false);
        assert!(other.input.shared.state.lock().unwrap().closed);
    }
    #[gpui_kit::test]
    fn one_entity_and_reader_survive_output_and_size_updates_then_drop(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        // TerminalView's public reader seam includes an OS thread, even for fake bytes.
        cx.background_executor.allow_parking();
        let mut registry = TerminalRegistry::default();
        let first = cx.update(|cx| {
            registry.synchronize(
                "work-a",
                "session",
                "first",
                true,
                None,
                terminal::TerminalConfig::default(),
                cx,
            )
        });
        let reader = registry.entries["work-a"].input.shared.clone();
        let same = cx.update(|cx| {
            registry.synchronize(
                "work-a",
                "session",
                "updated",
                true,
                None,
                terminal::TerminalConfig {
                    cols: 90,
                    ..Default::default()
                },
                cx,
            )
        });
        assert_eq!(first.view.entity_id(), same.view.entity_id());
        assert_eq!(registry.entries.len(), 1);
        assert!(Arc::ptr_eq(
            &reader,
            &registry.entries["work-a"].input.shared
        ));
        let replacement = cx.update(|cx| {
            registry.synchronize(
                "work-a",
                "new-session",
                "new",
                true,
                None,
                terminal::TerminalConfig::default(),
                cx,
            )
        });
        assert_ne!(replacement.view.entity_id(), first.view.entity_id());
        assert!(reader.state.lock().unwrap().closed);
        let weak = replacement.view.downgrade();
        drop(replacement);
        drop(first);
        drop(same);
        let reader = registry.entries["work-a"].input.shared.clone();
        registry.clear();
        assert!(reader.state.lock().unwrap().closed);
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
    }
}
