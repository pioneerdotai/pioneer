use gpui_kit::{
    App, ImageSource, IntoElement, RenderOnce, SharedString, Window, component::avatar::Avatar,
    prelude::FluentBuilder,
};

/// Value adapter that applies authenticated avatar output to a caller-owned
/// stock `Avatar` configuration.
#[derive(IntoElement)]
pub struct AvatarSurface {
    avatar: Avatar,
    source: Option<ImageSource>,
    fallback_name: Option<SharedString>,
}

impl AvatarSurface {
    pub fn new(avatar: Avatar) -> Self {
        Self {
            avatar,
            source: None,
            fallback_name: None,
        }
    }

    pub fn source(mut self, source: impl Into<ImageSource>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn fallback_name(mut self, name: impl Into<SharedString>) -> Self {
        self.fallback_name = Some(name.into());
        self
    }
}

impl RenderOnce for AvatarSurface {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        self.avatar
            .when_some(self.fallback_name, |avatar, name| avatar.name(name))
            .when_some(self.source, |avatar, source| avatar.src(source))
    }
}
