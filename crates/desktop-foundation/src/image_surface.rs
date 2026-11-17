use gpui_kit::{
    base::{Avatar as ImageRoot, AvatarFallback, AvatarImage},
    component::{StyledExt, avatar::Avatar},
    prelude::*,
    *,
};

/// Shared native image surface. Callers supply geometry and a domain fallback.
/// Avatar presentation retains the stock component's initials, border and shape.
#[derive(gpui_kit::IntoElement)]
pub struct ImageSurface {
    source: Option<ImageSource>,
    placeholder: Option<AnyElement>,
    avatar: Option<Avatar>,
    style: StyleRefinement,
}
impl ImageSurface {
    pub fn new() -> Self {
        Self {
            source: None,
            placeholder: None,
            avatar: None,
            style: StyleRefinement::default(),
        }
    }
    pub(crate) fn for_avatar(avatar: Avatar) -> Self {
        Self {
            avatar: Some(avatar),
            ..Self::new()
        }
    }
    pub fn source(mut self, source: impl Into<ImageSource>) -> Self {
        self.source = Some(source.into());
        self
    }
    pub fn placeholder(mut self, placeholder: impl IntoElement) -> Self {
        self.placeholder = Some(placeholder.into_any_element());
        self
    }
}
impl Default for ImageSurface {
    fn default() -> Self {
        Self::new()
    }
}
impl Styled for ImageSurface {
    fn style(&mut self) -> &mut StyleRefinement {
        &mut self.style
    }
}
impl RenderOnce for ImageSurface {
    fn render(self, _: &mut Window, _: &mut App) -> impl IntoElement {
        if let Some(avatar) = self.avatar {
            return avatar
                .when_some(self.source, |avatar, source| {
                    avatar.src(source).bg(transparent_black())
                })
                .refine_style(&self.style)
                .into_any_element();
        }
        let fallback = AvatarFallback::new()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .children(self.placeholder);
        div()
            .size_4()
            .flex_none()
            .child(
                ImageRoot::new()
                    .size_full()
                    .fallback(fallback)
                    .when_some(self.source, |image, source| {
                        image.image(AvatarImage::new(source).size_full())
                    }),
            )
            .refine_style(&self.style)
            .into_any_element()
    }
}
