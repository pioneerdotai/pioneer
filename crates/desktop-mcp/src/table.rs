use gpui_kit::component::{
    Icon, IconName, h_flex,
    table::{Column, TableDelegate, TableState},
    theme::ActiveTheme,
};
use gpui_kit::{prelude::FluentBuilder as _, *};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum McpDiagnosticsTone {
    Default,
    Muted,
    Success,
    Warning,
    Danger,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct McpDiagnosticsTableCell {
    pub text: String,
    pub tooltip: Option<String>,
    pub tone: McpDiagnosticsTone,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct McpDiagnosticsTableRow {
    pub cells: Vec<McpDiagnosticsTableCell>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct McpDiagnosticsTableColumn {
    pub key: &'static str,
    pub title: String,
    pub hint: String,
    pub width: Pixels,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct McpDiagnosticsTableModel {
    pub columns: Vec<McpDiagnosticsTableColumn>,
    pub rows: Vec<McpDiagnosticsTableRow>,
    pub keys: Vec<String>,
}

pub(crate) struct McpDiagnosticsTableDelegate {
    scope: String,
    model: McpDiagnosticsTableModel,
    table_columns: Vec<Column>,
}

impl McpDiagnosticsTableDelegate {
    pub(crate) fn new(scope: &'static str) -> Self {
        Self {
            scope: scope.into(),
            model: McpDiagnosticsTableModel::default(),
            table_columns: Vec::new(),
        }
    }

    pub(crate) fn set_scope(&mut self, scope: String) -> bool {
        if self.scope == scope {
            return false;
        }
        self.scope = scope;
        true
    }
    pub(crate) fn set_model(&mut self, model: McpDiagnosticsTableModel) {
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

    pub(crate) fn model(&self) -> &McpDiagnosticsTableModel {
        &self.model
    }
}

impl TableDelegate for McpDiagnosticsTableDelegate {
    fn columns_count(&self, _: &App) -> usize {
        self.table_columns.len()
    }

    fn rows_count(&self, _: &App) -> usize {
        self.model.rows.len()
    }

    fn column(&self, col_ix: usize, _: &App) -> Column {
        self.table_columns[col_ix].clone()
    }

    fn render_header(
        &mut self,
        _: &mut Window,
        _: &mut Context<TableState<Self>>,
    ) -> Stateful<Div> {
        div().id(SharedString::from(self.scope.clone()))
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
                        .id(SharedString::from(format!(
                            "{}:{}:hint",
                            self.scope, column.key
                        )))
                        .text_color(cx.theme().muted_foreground.opacity(0.8))
                        .child(Icon::new(IconName::Info).size_2p5().mt_px())
                        .tooltip(move |window, tooltip_cx| {
                            gpui_kit::component::tooltip::Tooltip::new(hint.clone())
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
        div().id(SharedString::from(format!(
            "{}:{}:row",
            self.scope,
            self.model
                .keys
                .get(row_ix)
                .map(String::as_str)
                .unwrap_or("missing")
        )))
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
            McpDiagnosticsTone::Default => cx.theme().foreground.opacity(0.84),
            McpDiagnosticsTone::Muted => cx.theme().muted_foreground,
            McpDiagnosticsTone::Success => cx.theme().success,
            McpDiagnosticsTone::Warning => cx.theme().warning,
            McpDiagnosticsTone::Danger => cx.theme().danger,
        };

        div()
            .id(SharedString::from(format!(
                "{}:{}:{}:cell",
                self.scope,
                self.model
                    .keys
                    .get(row_ix)
                    .map(String::as_str)
                    .unwrap_or("missing"),
                self.model.columns[col_ix].key
            )))
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
