//! Application-owned qualification seams for animated GPUI Kit components.
//!
//! The helpers preserve the exact stock component tree. They observe only
//! Pioneer call-site state and do not depend on modified framework APIs.

/// Preserves the existing loading-state evaluation and compiles only the
/// diagnostic observation out of ordinary builds.
macro_rules! observed_loading {
    ($source_id:expr, $is_loading:expr $(,)?) => {{
        #[cfg(feature = "qualification-diagnostics")]
        {
            let is_loading = $is_loading;
            pioneer_observability::record_qualification_diagnostic!(
                record_loading_animation_source($source_id, is_loading)
            );
            is_loading
        }
        #[cfg(not(feature = "qualification-diagnostics"))]
        {
            $is_loading
        }
    }};
}

/// Builds the exact stock Spinner while compiling source observation out of
/// ordinary builds.
macro_rules! spinner {
    ($source_id:expr, $is_active:expr $(,)?) => {{
        #[cfg(feature = "qualification-diagnostics")]
        {
            let is_active = $is_active;
            pioneer_observability::record_qualification_diagnostic!(
                record_loading_animation_source($source_id, is_active)
            );
        }
        gpui_kit::component::spinner::Spinner::new()
    }};
    ($source_id:expr $(,)?) => {{
        #[cfg(feature = "qualification-diagnostics")]
        {
            pioneer_observability::record_qualification_diagnostic!(
                record_animation_source_observed(
                    $source_id,
                    pioneer_observability::Visibility::NotApplicable,
                )
            );
        }
        gpui_kit::component::spinner::Spinner::new()
    }};
}

pub(crate) use {observed_loading, spinner};
