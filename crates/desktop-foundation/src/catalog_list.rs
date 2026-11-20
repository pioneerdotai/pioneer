use gpui_kit::ListState;

/// Keep measurements and the scroll anchor for unchanged rows when a catalogue
/// inserts, removes or reorders items. Heights are measured by GPUI's list.
pub fn reconcile_catalog_list<T: PartialEq>(state: &ListState, previous: &[T], next: &[T]) {
    let prefix = previous
        .iter()
        .zip(next)
        .take_while(|(a, b)| a == b)
        .count();
    let suffix = previous[prefix..]
        .iter()
        .rev()
        .zip(next[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    if prefix + suffix != previous.len() || prefix + suffix != next.len() {
        state.splice(
            prefix..previous.len() - suffix,
            next.len() - prefix - suffix,
        );
    }
}
