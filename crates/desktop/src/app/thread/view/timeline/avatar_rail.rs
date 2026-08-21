use super::layout::{
    TIMELINE_AVATAR_SIZE, TIMELINE_AVATAR_STICKY_BOTTOM, TIMELINE_CONTENT_HORIZONTAL_PADDING,
    TimelineAvatarSource, TimelineLayoutIndex,
};
use super::running_indicator::render_running_turn_dino;
use crate::app::root::PioneerDesktop;
use gpui::{prelude::*, *};
use gpui_component::{Sizable as _, avatar::Avatar, theme::ActiveTheme};
use std::{
    path::{Path, PathBuf},
    rc::Rc,
};

#[derive(Clone)]
enum TimelineAvatarVisual {
    Absent,
    HistoricalUser {
        display_name: String,
        cached_path: Option<PathBuf>,
    },
    Agent {
        display_name: String,
        cached_path: Option<PathBuf>,
    },
    RunningAgent {
        is_dark: bool,
        image_id: ElementId,
    },
}

impl PioneerDesktop {
    pub(super) fn render_timeline_avatar_rail(
        &mut self,
        layout: Rc<TimelineLayoutIndex>,
        scroll_handle: gpui_component::VirtualListScrollHandle,
        content_width: Pixels,
        rendered_list_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let historical_revisions = layout
            .grouping()
            .avatar_groups()
            .iter()
            .filter_map(|group| {
                let TimelineAvatarSource::HistoricalUser {
                    author: Some(author),
                } = &group.source
                else {
                    return None;
                };
                let author = self.current_timeline_author_presentation(Some(author));
                author.avatar_revision.as_ref().and_then(|revision| {
                    author
                        .principal_id
                        .map(|principal_id| (principal_id, revision.clone()))
                })
            })
            .collect::<Vec<_>>();
        let requests = self
            .member_avatar_state
            .reconcile_historical_revisions(&historical_revisions);
        self.resolve_member_avatar_requests(requests, cx);

        let visuals = Rc::new(
            layout
                .grouping()
                .avatar_groups()
                .iter()
                .map(|group| match &group.source {
                    TimelineAvatarSource::HistoricalUser { author } => {
                        if let Some(author) =
                            super::timeline_agent_execution_author(author.as_ref())
                        {
                            return self.timeline_agent_avatar_visual(author);
                        }
                        let (display_name, cached_path) =
                            self.timeline_author_avatar_parts(author.as_ref());
                        TimelineAvatarVisual::HistoricalUser {
                            display_name,
                            cached_path,
                        }
                    }
                    TimelineAvatarSource::Agent {
                        shows_running_dino: true,
                        ..
                    } => TimelineAvatarVisual::RunningAgent {
                        is_dark: cx.theme().mode.is_dark(),
                        image_id: ElementId::from((
                            ElementId::from("timeline-avatar-running-dino"),
                            format!("{}:{}", group.first_row_index, group.last_row_index),
                        )),
                    },
                    TimelineAvatarSource::Agent { author, .. } => {
                        let Some(author) = super::timeline_agent_execution_author(author.as_ref())
                        else {
                            return TimelineAvatarVisual::Absent;
                        };
                        self.timeline_agent_avatar_visual(author)
                    }
                })
                .collect::<Vec<_>>(),
        );
        let desktop_entity = cx.entity().clone();

        canvas(
            move |bounds, window, cx| {
                if (bounds.size.width - rendered_list_width).abs() > px(1.) {
                    let desktop_entity = desktop_entity.clone();
                    let measured_width = bounds.size.width;
                    cx.defer(move |cx| {
                        let _ = desktop_entity.update(cx, |view, cx| {
                            if view.update_timeline_layout_width(measured_width) {
                                cx.notify();
                            }
                        });
                    });
                }

                // Rows are centered by GPUI against the current list bounds. Resolve the
                // avatar rail from those same bounds in prepaint so sidebar/window resizing
                // cannot leave the rail one layout frame behind the message content.
                let content_left = px(((bounds.size.width - content_width).as_f32() / 2.).max(0.))
                    + TIMELINE_CONTENT_HORIZONTAL_PADDING;
                let viewport_top = px(0.) - scroll_handle.offset().y;
                let viewport_bottom = viewport_top + bounds.size.height;
                let groups = layout.grouping().avatar_groups();
                let first_group = groups.partition_point(|group| {
                    layout
                        .avatar_group_bounds(group)
                        .is_some_and(|(_, bottom)| bottom <= viewport_top)
                });
                let mut avatars = Vec::new();

                for (group, visual) in groups.iter().zip(visuals.iter()).skip(first_group) {
                    let Some((natural_top, group_bottom)) = layout.avatar_group_bounds(group)
                    else {
                        continue;
                    };
                    if natural_top >= viewport_bottom {
                        break;
                    }

                    let group_top = natural_top - viewport_top;
                    let natural_bottom_top = group_bottom - viewport_top - TIMELINE_AVATAR_SIZE;
                    let sticky_bottom_top =
                        bounds.size.height - TIMELINE_AVATAR_STICKY_BOTTOM - TIMELINE_AVATAR_SIZE;
                    let avatar_top = natural_bottom_top.min(sticky_bottom_top).max(group_top);
                    if avatar_top >= bounds.size.height || avatar_top <= -TIMELINE_AVATAR_SIZE {
                        continue;
                    }

                    let mut avatar = render_timeline_avatar(visual);
                    avatar.prepaint_as_root(
                        bounds.origin + point(content_left, avatar_top),
                        size(
                            AvailableSpace::Definite(TIMELINE_AVATAR_SIZE),
                            AvailableSpace::Definite(TIMELINE_AVATAR_SIZE),
                        ),
                        window,
                        cx,
                    );
                    avatars.push(avatar);
                }

                avatars
            },
            |bounds, mut avatars, window, cx| {
                window.with_content_mask(Some(ContentMask { bounds }), |window| {
                    for avatar in &mut avatars {
                        avatar.paint(window, cx);
                    }
                });
            },
        )
        .absolute()
        .top_0()
        .right_0()
        .bottom_0()
        .left_0()
        .into_any_element()
    }

    fn timeline_author_avatar_parts(
        &self,
        author: Option<&pioneer_protocol::TurnAuthorSnapshot>,
    ) -> (String, Option<PathBuf>) {
        let author = self.current_timeline_author_presentation(author);
        let cached_path = author.principal_id.as_ref().and_then(|principal_id| {
            let revision = author.avatar_revision.as_deref()?;
            self.member_avatar_state
                .presentation_for_revision(principal_id, revision)
                .and_then(|avatar| avatar.cached_image_path.clone())
        });
        (author.display_name, cached_path)
    }

    fn timeline_agent_avatar_visual(
        &self,
        author: &pioneer_protocol::TurnAuthorSnapshot,
    ) -> TimelineAvatarVisual {
        let agent = super::timeline_agent_presentation(Some(author));
        let display_name = agent
            .map(|agent| agent.display_name.as_str())
            .unwrap_or(author.display_name.as_str())
            .trim()
            .to_owned();
        if display_name.is_empty() {
            return TimelineAvatarVisual::Absent;
        }
        let cached_path = agent
            .is_some_and(|agent| {
                agent.identity_source_kind == pioneer_protocol::AgentIdentitySourceKind::NativeAgent
                    && agent.nickname == pioneer_protocol::PIONEER_AGENT_NICKNAME
                    && agent.avatar_revision.as_deref()
                        == Some(pioneer_protocol::PIONEER_AGENT_AVATAR_REVISION)
            })
            .then(|| {
                self.member_avatar_state
                    .agent_cached_image_path()
                    .map(Path::to_path_buf)
            })
            .flatten();
        TimelineAvatarVisual::Agent {
            display_name,
            cached_path,
        }
    }
}

fn render_timeline_avatar(visual: &TimelineAvatarVisual) -> AnyElement {
    match visual {
        TimelineAvatarVisual::Absent => div()
            .w(TIMELINE_AVATAR_SIZE)
            .h(TIMELINE_AVATAR_SIZE)
            .into_any_element(),
        TimelineAvatarVisual::HistoricalUser {
            display_name,
            cached_path: Some(path),
        } => Avatar::new()
            .name(display_name.clone())
            .src(path.clone())
            .with_size(TIMELINE_AVATAR_SIZE)
            .into_any_element(),
        // gpui-component gives custom-size initials a half-size layout box. Use the
        // unconstrained Medium initials layout, then override only the outer box back
        // to the established 32 px timeline size.
        TimelineAvatarVisual::HistoricalUser { display_name, .. } => Avatar::new()
            .name(display_name.clone())
            .size(TIMELINE_AVATAR_SIZE)
            .border_0()
            .into_any_element(),
        TimelineAvatarVisual::Agent {
            display_name: _,
            cached_path: Some(path),
        } => Avatar::new()
            .src(path.clone())
            .with_size(TIMELINE_AVATAR_SIZE)
            .border_0()
            .into_any_element(),
        TimelineAvatarVisual::Agent { display_name, .. } => Avatar::new()
            .name(display_name.clone())
            .size(TIMELINE_AVATAR_SIZE)
            .border_0()
            .into_any_element(),
        TimelineAvatarVisual::RunningAgent { is_dark, image_id } => div()
            .w(TIMELINE_AVATAR_SIZE)
            .h(TIMELINE_AVATAR_SIZE)
            .child(render_running_turn_dino(image_id.clone(), *is_dark))
            .into_any_element(),
    }
}
