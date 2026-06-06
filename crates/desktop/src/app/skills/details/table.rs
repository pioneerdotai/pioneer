use gpui::{prelude::FluentBuilder as _, *};
use gpui_component::{
    Icon, IconName, h_flex,
    table::{Column, TableDelegate, TableState},
    theme::ActiveTheme,
};
pub(crate) use pioneer_client::skills::presentation::{
    SkillDiagnosticsTableCell, SkillDiagnosticsTableRow, SkillDiagnosticsTone,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SkillDiagnosticsTableColumn {
    pub key: &'static str,
    pub title: String,
    pub hint: String,
    pub width: Pixels,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SkillDiagnosticsTableModel {
    pub columns: Vec<SkillDiagnosticsTableColumn>,
    pub rows: Vec<SkillDiagnosticsTableRow>,
}

pub(crate) struct SkillDiagnosticsTableDelegate {
    scope: &'static str,
    model: SkillDiagnosticsTableModel,
    table_columns: Vec<Column>,
}

impl SkillDiagnosticsTableDelegate {
    pub(crate) fn new(scope: &'static str) -> Self {
        Self {
            scope,
            model: SkillDiagnosticsTableModel::default(),
            table_columns: Vec::new(),
        }
    }

    pub(crate) fn set_model(&mut self, model: SkillDiagnosticsTableModel) {
        self.table_columns = model
            .columns
            .iter()
            .map(|column| {
                Column::new(column.key, column.title.clone())
                    .width(column.width)
                    .resizable(false)
                    .movable(false)
                    .selectable(false)
            })
            .collect::<Vec<_>>();
        self.model = model;
    }

    pub(crate) fn model(&self) -> &SkillDiagnosticsTableModel {
        &self.model
    }
}

impl TableDelegate for SkillDiagnosticsTableDelegate {
    fn columns_count(&self, _: &App) -> usize {
        self.table_columns.len()
    }

    fn rows_count(&self, _: &App) -> usize {
        self.model.rows.len()
    }

    fn column(&self, col_ix: usize, _: &App) -> &Column {
        &self.table_columns[col_ix]
    }

    fn render_header(
        &mut self,
        _: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> Stateful<Div> {
        div().id(self.scope)
    }

    fn render_th(
        &mut self,
        col_ix: usize,
        _: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let Some(column) = self.model.columns.get(col_ix) else {
            return div().text_xs().child("-").into_any_element();
        };
        let hint = column.hint.trim().to_owned();

        h_flex()
            .w_full()
            .items_center()
            .gap_1()
            .child(
                div()
                    .text_xs()
                    .line_height(relative(1.))
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(column.title.clone()),
            )
            .when(!hint.is_empty(), |this| {
                this.child(
                    div()
                        .id((gpui::ElementId::from((self.scope, col_ix)), "hint"))
                        .text_color(cx.theme().muted_foreground.opacity(0.8))
                        .child(Icon::new(IconName::Info).size_2p5().mt_px())
                        .tooltip(move |window, tooltip_cx| {
                            gpui_component::tooltip::Tooltip::new(hint.clone())
                                .text_xs()
                                .text_color(tooltip_cx.theme().popover_foreground)
                                .build(window, tooltip_cx)
                        }),
                )
            })
            .into_any_element()
    }

    fn render_tr(
        &mut self,
        row_ix: usize,
        _: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> Stateful<Div> {
        div().id((self.scope, row_ix))
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let Some(row) = self.model.rows.get(row_ix) else {
            return div().into_any_element();
        };
        let Some(cell) = row.cells.get(col_ix) else {
            return div().into_any_element();
        };

        let content = if cell.text.trim().is_empty() {
            "-".to_owned()
        } else {
            cell.text.clone()
        };
        let text_color = match cell.tone {
            SkillDiagnosticsTone::Default => cx.theme().foreground.opacity(0.84),
            SkillDiagnosticsTone::Muted => cx.theme().muted_foreground,
            SkillDiagnosticsTone::Success => cx.theme().success,
            SkillDiagnosticsTone::Warning => cx.theme().warning,
            SkillDiagnosticsTone::Danger => cx.theme().danger,
        };

        let col_marker = match col_ix {
            0 => "c0",
            1 => "c1",
            2 => "c2",
            3 => "c3",
            4 => "c4",
            _ => "cx",
        };

        div()
            .id((gpui::ElementId::from((self.scope, row_ix)), col_marker))
            .h_full()
            .flex()
            .items_center()
            .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                cx.stop_propagation();
            })
            .on_mouse_down(MouseButton::Right, move |_, _, cx| {
                cx.stop_propagation();
            })
            .on_click(move |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                div()
                    .text_xs()
                    .line_height(relative(1.))
                    .text_color(text_color)
                    .overflow_hidden()
                    .whitespace_nowrap()
                    .text_ellipsis()
                    .child(content),
            )
            .into_any_element()
    }
}
