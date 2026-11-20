//! SQL projections from validated JSON while keeping the stored bytes intact.

use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func};

pub fn queryable_json(value: Expr) -> Expr {
    // PostgreSQL JSON functions decode every escape. Replace escaped NULs in
    // this projection so text outside the requested path cannot fail a read.
    // The exact stored JSON remains available to the task-result repository.
    let value = value.cast_as(Alias::new("text"));
    Expr::case(
        Func::cust(Alias::new("strpos"))
            .args([value.clone(), Expr::val(r"\u0000")])
            .gt(0),
        Func::cust(Alias::new("regexp_replace")).args([
            value.clone(),
            Expr::val(r"(?<!\\)((?:\\\\)*)\\u0000"),
            Expr::val(r"\1\\ufffd"),
            Expr::val("g"),
        ]),
    )
    .finally(value)
    .cast_as(Alias::new("json"))
}

pub fn json_path_text(json: Expr, path: &[&str]) -> Expr {
    json_path("json_extract_path_text", json, path)
}

pub fn json_path_value(json: Expr, path: &[&str]) -> Expr {
    json_path("json_extract_path", json, path)
}

fn json_path(function: &str, json: Expr, path: &[&str]) -> Expr {
    Func::cust(Alias::new(function))
        .args(std::iter::once(json).chain(path.iter().map(|key| Expr::val(*key))))
        .into()
}
