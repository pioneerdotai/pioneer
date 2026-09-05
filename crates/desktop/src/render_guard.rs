#[cfg(debug_assertions)]
use std::cell::Cell;

#[cfg(debug_assertions)]
thread_local! {
    static RENDER_DEPTH: Cell<usize> = const { Cell::new(0) };
}

pub(crate) struct DesktopRenderGuard;

impl DesktopRenderGuard {
    pub(crate) fn enter() -> Self {
        #[cfg(debug_assertions)]
        RENDER_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for DesktopRenderGuard {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        RENDER_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

#[track_caller]
pub(crate) fn assert_not_rendering(operation: &str) {
    #[cfg(not(debug_assertions))]
    let _ = operation;
    #[cfg(debug_assertions)]
    RENDER_DEPTH.with(|depth| {
        assert_eq!(
            depth.get(),
            0,
            "{operation} is not allowed during Desktop rendering"
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_marks_only_its_lexical_render_context() {
        assert_not_rendering("before");
        let result = std::panic::catch_unwind(|| {
            let _guard = DesktopRenderGuard::enter();
            assert_not_rendering("mutation");
        });
        assert_eq!(result.is_err(), cfg!(debug_assertions));
        assert_not_rendering("after");
    }
}
