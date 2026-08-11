use gpui::{prelude::*, *};
use gpui_component::{
    Colorize, Icon, IconName, Sizable,
    avatar::Avatar,
    combobox::{Combobox, ComboboxState},
    h_flex,
    searchable_list::{SearchableListItem, SearchableVec},
    theme::ActiveTheme,
};
use pioneer_client::composer::state_machine::ComposerMentionCandidate;
use pioneer_protocol::PrincipalId;
use std::path::PathBuf;

#[derive(Clone)]
pub(crate) struct MemberPickerItem {
    candidate: ComposerMentionCandidate,
    avatar_path: Option<PathBuf>,
}

impl MemberPickerItem {
    fn new(candidate: ComposerMentionCandidate, avatar_path: Option<PathBuf>) -> Self {
        Self {
            candidate,
            avatar_path,
        }
    }
}

impl SearchableListItem for MemberPickerItem {
    type Value = ComposerMentionCandidate;

    fn title(&self) -> SharedString {
        format!(
            "{} · @{}",
            self.candidate.display_name, self.candidate.nickname
        )
        .into()
    }

    fn value(&self) -> &Self::Value {
        &self.candidate
    }

    fn render(&self, _: &mut Window, _: &mut App) -> impl IntoElement {
        let avatar = Avatar::new()
            .small()
            .name(self.candidate.display_name.clone())
            .when_some(self.avatar_path.clone(), |this, path| this.src(path));

        h_flex()
            .w_full()
            .min_w_0()
            .items_center()
            .gap_2()
            .py_0p5()
            .child(avatar)
            .child(
                h_flex()
                    .min_w_0()
                    .items_center()
                    .gap_1()
                    .child(
                        div()
                            .min_w_0()
                            .truncate()
                            .text_xs()
                            .child(self.candidate.display_name.clone()),
                    )
                    .when(!self.candidate.nickname.is_empty(), |this| {
                        this.child(
                            div()
                                .min_w_0()
                                .truncate()
                                .text_xs()
                                .opacity(0.6)
                                .child(format!("@{}", self.candidate.nickname)),
                        )
                    }),
            )
    }

    fn matches(&self, query: &str) -> bool {
        let query = query.to_lowercase();
        self.candidate.display_name.to_lowercase().contains(&query)
            || self.candidate.nickname.to_lowercase().contains(&query)
    }
}

pub(crate) type MemberPickerDelegate = SearchableVec<MemberPickerItem>;
pub(crate) type MemberPickerState = Entity<ComboboxState<MemberPickerDelegate>>;

pub(crate) fn new_member_picker_state(
    window: &mut Window,
    cx: &mut Context<ComboboxState<MemberPickerDelegate>>,
) -> ComboboxState<MemberPickerDelegate> {
    ComboboxState::new(
        SearchableVec::new(Vec::<MemberPickerItem>::new()),
        Vec::new(),
        window,
        cx,
    )
    .searchable(true)
}

pub(crate) fn member_picker_items(
    candidates: impl IntoIterator<Item = ComposerMentionCandidate>,
    mut avatar_path: impl FnMut(&PrincipalId) -> Option<PathBuf>,
) -> MemberPickerDelegate {
    SearchableVec::new(
        candidates
            .into_iter()
            .map(|candidate| {
                let path = avatar_path(&candidate.principal_id);
                MemberPickerItem::new(candidate, path)
            })
            .collect::<Vec<_>>(),
    )
}

#[derive(IntoElement)]
pub(crate) struct MemberPicker {
    id: ElementId,
    trigger_id: ElementId,
    state: MemberPickerState,
    icon: Icon,
    inset_trigger: bool,
}

impl MemberPicker {
    pub(crate) fn new(
        id: impl Into<ElementId>,
        trigger_id: impl Into<ElementId>,
        state: &MemberPickerState,
        icon: Icon,
    ) -> Self {
        Self {
            id: id.into(),
            trigger_id: trigger_id.into(),
            state: state.clone(),
            icon,
            inset_trigger: false,
        }
    }

    pub(crate) fn inset_trigger(mut self) -> Self {
        self.inset_trigger = true;
        self
    }
}

impl RenderOnce for MemberPicker {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let hover_background = if cx.theme().mode.is_dark() {
            cx.theme().secondary.lighten(0.1).opacity(0.8)
        } else {
            cx.theme().secondary.darken(0.1).opacity(0.8)
        };
        let active_background = if cx.theme().mode.is_dark() {
            cx.theme().secondary.lighten(0.2).opacity(0.8)
        } else {
            cx.theme().secondary.darken(0.2).opacity(0.8)
        };

        div()
            .id(self.id)
            .size_6()
            .relative()
            .child(
                Combobox::new(&self.state)
                    .appearance(false)
                    .search_placeholder(t!("chat.composer.mention.search").to_string())
                    .placeholder("")
                    .menu_width(px(420.))
                    .check_icon(Icon::new(IconName::Check).opacity(0.0))
                    .with_size(gpui_component::Size::Small)
                    .render_trigger(|_, _, _| div()),
            )
            .child(
                div().absolute().inset_0().bg(cx.theme().background).child(
                    div()
                        .id(self.trigger_id)
                        .size_full()
                        .flex()
                        .items_center()
                        .justify_center()
                        .rounded(cx.theme().radius)
                        .cursor_pointer()
                        .text_color(cx.theme().secondary_foreground)
                        .when(self.inset_trigger, |this| this.ml_0p5())
                        .hover(move |this| this.bg(hover_background))
                        .active(move |this| this.bg(active_background))
                        .child(self.icon),
                ),
            )
    }
}
