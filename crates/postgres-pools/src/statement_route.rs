//! Route SQL against the managed Pioneer schema before taking pool capacity.

use std::ops::ControlFlow;

use sqlparser::{
    ast::{
        BinaryOperator as Bin, DataType, DescribeAlias, Expr, Ident, InTarget, ObjectName, Query,
        Select, SetExpr, Statement, TableFactor, UnaryOperator, Visit, Visitor,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};

/// PostgreSQL implicitly searches pg_catalog first. Name resolution in the
/// configured app database also includes public (Pioneer and pgvector), and
/// the temporary schema comes last for types and relations.
pub const MANAGED_SEARCH_PATH: &str = "public, pg_temp";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatementRoute {
    Read,
    Write,
}

pub(crate) fn statement_route(sql: &str) -> StatementRoute {
    if let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        && let [statement] = statements.as_slice()
        && statement.visit(&mut ReadVisitor).is_continue()
    {
        StatementRoute::Read
    } else {
        StatementRoute::Write
    }
}

struct ReadVisitor;

impl Visitor for ReadVisitor {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
        // Visit the explained query as well, including any operations that
        // would take effect when ANALYZE executes it.
        allow(
            matches!(statement,
                Statement::Explain {
                    describe_alias: DescribeAlias::Explain,
                    statement,
                    query_plan: false,
                    estimate: false,
                    ..
                } if matches!(statement.as_ref(), Statement::Query(_))
            ) || matches!(statement, Statement::Query(_)),
        )
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        allow(
            query.locks.is_empty()
                && query.for_clause.is_none()
                && query.settings.is_none()
                && query.format_clause.is_none()
                && query.pipe_operators.is_empty()
                && read_set(&query.body),
        )
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        allow(
            select.into.is_none()
                && select.optimizer_hints.is_empty()
                && select.select_modifiers.is_none()
                && select.top.is_none()
                && select.exclude.is_none()
                && select.lateral_views.is_empty()
                && select.prewhere.is_none()
                && select.connect_by.is_empty()
                && select.cluster_by.is_empty()
                && select.distribute_by.is_empty()
                && select.sort_by.is_empty()
                && select.qualify.is_none()
                && select.value_table_mode.is_none(),
        )
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        allow(match expr {
            Expr::Function(function) => read_function(&function.name),
            Expr::BinaryOp { op, .. }
            | Expr::AnyOp { compare_op: op, .. }
            | Expr::AllOp { compare_op: op, .. } => read_binary(op),
            Expr::UnaryOp { op, .. } => matches!(
                op,
                UnaryOperator::Not | UnaryOperator::Plus | UnaryOperator::Minus
            ),
            Expr::Cast { data_type, .. } => read_type(data_type),
            Expr::TypedString(value) => read_type(&value.data_type),
            Expr::In { target, global, .. } => {
                !global && matches!(target, InTarget::List(_) | InTarget::Subquery(_))
            }
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::CompoundFieldAccess { .. }
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::IsDistinctFrom(..)
            | Expr::IsNotDistinctFrom(..)
            | Expr::IsJson { .. }
            | Expr::IsNormalized { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::AtTimeZone { .. }
            | Expr::Extract { .. }
            | Expr::Ceil { .. }
            | Expr::Floor { .. }
            | Expr::Position { .. }
            | Expr::Substring { .. }
            | Expr::Trim { .. }
            | Expr::Overlay { .. }
            | Expr::Collate { .. }
            | Expr::Nested(_)
            | Expr::Value(_)
            | Expr::Case { .. }
            | Expr::Exists { .. }
            | Expr::Subquery(_)
            | Expr::Tuple(_)
            | Expr::Array(_)
            | Expr::Interval(_)
            | Expr::Wildcard(_)
            | Expr::QualifiedWildcard(..)
            | Expr::GroupingSets(_)
            | Expr::Cube(_)
            | Expr::Rollup(_) => true,
            _ => false,
        })
    }

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<()> {
        allow(match table {
            TableFactor::Table {
                name,
                args,
                with_hints,
                version,
                partitions,
                json_path,
                sample,
                index_directives,
                ..
            } => {
                // A function in FROM may be represented as a table with arguments.
                (args.is_none() || read_function(name))
                    && with_hints.is_empty()
                    && version.is_none()
                    && partitions.is_empty()
                    && json_path.is_none()
                    && sample.is_none()
                    && index_directives.is_empty()
            }
            TableFactor::Function { name, .. } => read_function(name),
            TableFactor::Derived { sample: None, .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::UNNEST {
                with_offset: false, ..
            } => true,
            _ => false,
        })
    }
}

fn allow(read: bool) -> ControlFlow<()> {
    if read {
        ControlFlow::Continue(())
    } else {
        ControlFlow::Break(())
    }
}

fn read_set(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(_) | SetExpr::Values(_) | SetExpr::Table(_) | SetExpr::Query(_) => true,
        SetExpr::SetOperation { left, right, .. } => read_set(left) && read_set(right),
        _ => false,
    }
}

fn named(ident: &Ident, expected: &str) -> bool {
    if ident.quote_style.is_some() {
        ident.value == expected
    } else {
        ident.value.eq_ignore_ascii_case(expected)
    }
}

fn callable(name: &ObjectName) -> Option<(Option<&Ident>, &Ident)> {
    match name.0.as_slice() {
        [name] => Some((None, name.as_ident()?)),
        [schema, name] => Some((Some(schema.as_ident()?), name.as_ident()?)),
        _ => None,
    }
}

fn read_function(name: &ObjectName) -> bool {
    let Some((schema, name)) = callable(name) else {
        return false;
    };
    // The public schema is managed by the application migrations. The
    // built-ins below and these particular Pioneer helpers are read-only.
    (schema.is_none_or(|s| named(s, "pg_catalog"))
        && [
            "abs",
            "array_agg",
            "array_length",
            "avg",
            "btrim",
            "cardinality",
            "char_length",
            "character_length",
            "coalesce",
            "concat",
            "concat_ws",
            "convert_to",
            "count",
            "current_database",
            "current_date",
            "current_schema",
            "current_setting",
            "current_timestamp",
            "date_part",
            "date_trunc",
            "dense_rank",
            "encode",
            "floor",
            "generate_series",
            "greatest",
            "json_agg",
            "json_array_elements",
            "json_array_elements_text",
            "json_array_length",
            "json_build_array",
            "json_build_object",
            "json_each",
            "json_each_text",
            "json_extract_path",
            "json_extract_path_text",
            "json_object_keys",
            "json_typeof",
            "jsonb_agg",
            "jsonb_array_elements",
            "jsonb_array_elements_text",
            "jsonb_array_length",
            "jsonb_build_array",
            "jsonb_build_object",
            "jsonb_extract_path",
            "jsonb_extract_path_text",
            "jsonb_typeof",
            "lag",
            "lead",
            "least",
            "left",
            "length",
            "lower",
            "ltrim",
            "max",
            "min",
            "now",
            "nullif",
            "octet_length",
            "pg_backend_pid",
            "pg_blocking_pids",
            "pg_current_snapshot",
            "pg_get_constraintdef",
            "pg_get_functiondef",
            "pg_get_indexdef",
            "pg_visible_in_snapshot",
            "position",
            "rank",
            "regexp_match",
            "regexp_matches",
            "regexp_replace",
            "replace",
            "round",
            "row_number",
            "rtrim",
            "split_part",
            "starts_with",
            "string_agg",
            "strpos",
            "substr",
            "substring",
            "sum",
            "to_char",
            "to_json",
            "to_jsonb",
            "to_regclass",
            "to_regprocedure",
            "to_timestamp",
            "trim",
            "unnest",
            "upper",
        ]
        .iter()
        .any(|candidate| named(name, candidate)))
        || (schema.is_none_or(|s| named(s, "public"))
            && [
                "agent_action_outbox_start_candidate",
                "thread_episodic_search_keys",
                "thread_episodic_search_terms",
            ]
            .iter()
            .any(|candidate| named(name, candidate)))
}

fn read_binary(op: &Bin) -> bool {
    match op {
        Bin::Plus
        | Bin::Minus
        | Bin::Multiply
        | Bin::Divide
        | Bin::Modulo
        | Bin::StringConcat
        | Bin::Gt
        | Bin::Lt
        | Bin::GtEq
        | Bin::LtEq
        | Bin::Eq
        | Bin::NotEq
        | Bin::And
        | Bin::Or
        | Bin::BitwiseOr
        | Bin::BitwiseAnd
        | Bin::PGBitwiseXor
        | Bin::PGBitwiseShiftLeft
        | Bin::PGBitwiseShiftRight
        | Bin::PGExp
        | Bin::PGOverlap
        | Bin::PGRegexMatch
        | Bin::PGRegexIMatch
        | Bin::PGRegexNotMatch
        | Bin::PGRegexNotIMatch
        | Bin::PGLikeMatch
        | Bin::PGILikeMatch
        | Bin::PGNotLikeMatch
        | Bin::PGNotILikeMatch
        | Bin::PGStartsWith
        | Bin::Arrow
        | Bin::LongArrow
        | Bin::HashArrow
        | Bin::HashLongArrow
        | Bin::AtAt
        | Bin::AtArrow
        | Bin::ArrowAt
        | Bin::HashMinus
        | Bin::AtQuestion
        | Bin::Question
        | Bin::QuestionAnd
        | Bin::QuestionPipe
        | Bin::Overlaps
        | Bin::Spaceship
        | Bin::LtDashGt => true,
        // pgvector operators installed by the managed extension in public.
        Bin::Custom(symbol) => matches!(symbol.as_str(), "<=>" | "<->" | "<#>" | "<+>"),
        Bin::PGCustomBinaryOperator(parts) => match parts.as_slice() {
            [schema, op] if schema.eq_ignore_ascii_case("public") || schema == "\"public\"" => {
                matches!(op.as_str(), "<=>" | "<->" | "<#>" | "<+>")
            }
            [schema, op]
                if schema.eq_ignore_ascii_case("pg_catalog") || schema == "\"pg_catalog\"" =>
            {
                matches!(
                    op.as_str(),
                    "=" | "<>" | "<" | ">" | "<=" | ">=" | "+" | "-" | "*" | "/" | "&&"
                )
            }
            _ => false,
        },
        _ => false,
    }
}

fn read_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Custom(name, _) => callable(name).is_some_and(|(schema, name)| {
            (schema.is_none_or(|s| named(s, "public")) && named(name, "vector"))
                || (schema.is_none_or(|s| named(s, "pg_catalog"))
                    && [
                        "bool",
                        "bytea",
                        "float4",
                        "float8",
                        "int2",
                        "int4",
                        "int8",
                        "json",
                        "jsonb",
                        "name",
                        "oid",
                        "pg_snapshot",
                        "regclass",
                        "text",
                        "timestamptz",
                        "uuid",
                        "xid8",
                    ]
                    .iter()
                    .any(|candidate| named(name, candidate)))
        }),
        DataType::Array(array) => match array {
            sqlparser::ast::ArrayElemTypeDef::SquareBracket(element, _) => read_type(element),
            _ => false,
        },
        DataType::Character(_)
        | DataType::Char(_)
        | DataType::CharacterVarying(_)
        | DataType::CharVarying(_)
        | DataType::Varchar(_)
        | DataType::Uuid
        | DataType::Numeric(_)
        | DataType::Decimal(_)
        | DataType::Dec(_)
        | DataType::Float(_)
        | DataType::Int2(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::Int4(_)
        | DataType::Int8(_)
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::Float4
        | DataType::Real
        | DataType::Float8
        | DataType::DoublePrecision
        | DataType::Bool
        | DataType::Boolean
        | DataType::Date
        | DataType::Time(..)
        | DataType::Timestamp(..)
        | DataType::JSON
        | DataType::JSONB
        | DataType::Regclass
        | DataType::Text
        | DataType::Bytea
        | DataType::Bit(_)
        | DataType::BitVarying(_)
        | DataType::VarBit(_) => true,
        _ => false,
    }
}
