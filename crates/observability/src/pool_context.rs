//! Fixed SQLite pool attribution, installed only while polling or dropping an operation.
//! No SQL, work payload, or caller identity is retained here.
use std::{cell::Cell, marker::PhantomData, rc::Rc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolContext {
    ReaderInteractive,
    ReaderMaintenance,
    WriterCritical,
    WriterInteractive,
    WriterMaintenance,
}
impl PoolContext {
    pub fn role(self) -> crate::DatabaseRole {
        match self {
            Self::ReaderInteractive | Self::ReaderMaintenance => crate::DatabaseRole::Reader,
            _ => crate::DatabaseRole::Writer,
        }
    }
    pub const fn class(self) -> &'static str {
        match self {
            Self::ReaderInteractive | Self::WriterInteractive => "interactive",
            Self::ReaderMaintenance | Self::WriterMaintenance => "maintenance",
            Self::WriterCritical => "critical",
        }
    }
    pub fn enter(self) -> PoolGuard {
        enter(Some(self))
    }
}
thread_local! {
    static CURRENT: Cell<Option<PoolContext>> = const { Cell::new(None) };
}
pub fn current() -> Option<PoolContext> {
    CURRENT.with(Cell::get)
}
/// Also permits an explicit empty context so unrelated work cannot inherit one.
pub fn enter(context: Option<PoolContext>) -> PoolGuard {
    PoolGuard {
        previous: CURRENT.with(|slot| slot.replace(context)),
        _not_send: PhantomData,
    }
}
pub struct PoolGuard {
    previous: Option<PoolContext>,
    _not_send: PhantomData<Rc<()>>,
}
impl Drop for PoolGuard {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.set(self.previous));
    }
}
