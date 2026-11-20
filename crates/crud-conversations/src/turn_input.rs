use anyhow::{Context, Result};
use pioneer_entity::turn_input;
use sea_orm::sea_query::{Alias, Expr, Func};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

/// Read only bounded original text for post-turn processing. The database
/// slices Unicode characters before transferring data from the canonical input.
pub async fn post_turn_user_text<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    max_chars: usize,
) -> Result<(String, bool)> {
    use turn_input::Column as Field;
    let max_chars = max_chars.clamp(1, 32_768);
    let value = Expr::col((turn_input::Entity, Field::Text));
    // SeaQuery does not expose the PostgreSQL text slicing functions.
    let part = Func::cust(Alias::new("left")).args([value.clone(), Expr::val(max_chars as i32)]);
    let length = Func::cust(Alias::new("char_length")).arg(value);
    let rows = turn_input::Entity::find()
        .select_only()
        .column_as(Expr::from(part), "prefix")
        .column_as(Expr::from(length), "char_count")
        .filter(Field::TurnId.eq(turn_id))
        .filter(Field::InputType.eq("text"))
        .order_by_asc(Field::InputIndex)
        .limit(65)
        .into_tuple::<(String, i32)>()
        .all(db)
        .await
        .context("failed to read bounded post-turn user messages")?;
    let mut truncated = rows.len() > 64;
    let mut text = String::new();
    let mut remaining = max_chars;
    for (part, char_count) in rows.into_iter().take(64) {
        if !text.is_empty() && !part.is_empty() {
            if remaining > 0 {
                text.push('\n');
                remaining -= 1;
            } else {
                truncated = true;
            }
        }
        let mut count = 0;
        text.extend(part.chars().take(remaining).inspect(|_| count += 1));
        truncated |= char_count > count as i32;
        remaining -= count;
    }
    Ok((text, truncated))
}
