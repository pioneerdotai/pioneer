use anyhow::{Context, Result};
use pioneer_entity::turn_input;
use pioneer_protocol::{UserInput, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, Func};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

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

const DB_ID_LEN: usize = 21;

#[derive(Clone, Debug)]
pub struct PreparedTurnInput {
    turn_id: String,
    rows: Vec<turn_input::ActiveModel>,
}

pub fn prepare(
    turn_id: &str,
    input: &[UserInput],
    created_at: DateTimeWithTimeZone,
) -> Result<PreparedTurnInput> {
    let mut rows = Vec::with_capacity(input.len());
    for (index, item) in input.iter().enumerate() {
        let (input_type, text) = input_type_and_text(item);
        rows.push(turn_input::ActiveModel {
            insertion_ordinal: Default::default(),
            publication_xid: Default::default(),
            id: Set(generate_id(DB_ID_LEN)),
            turn_id: Set(turn_id.to_owned()),
            input_index: Set(i64::try_from(index).unwrap_or(i64::MAX)),
            input_type: Set(input_type.to_owned()),
            text: Set(text),
            payload: Set(
                serde_json::to_string(item).context("failed to serialize turn input payload")?
            ),
            created_at: Set(created_at),
        });
    }
    Ok(PreparedTurnInput {
        turn_id: turn_id.to_owned(),
        rows,
    })
}

pub async fn replace<C: ConnectionTrait>(db: &C, prepared: PreparedTurnInput) -> Result<()> {
    turn_input::Entity::delete_many()
        .filter(turn_input::Column::TurnId.eq(prepared.turn_id))
        .exec(db)
        .await
        .context("failed to clear turn input rows before projection")?;

    for rows in prepared.rows.chunks(32) {
        turn_input::Entity::insert_many(rows.iter().cloned())
            .exec_without_returning(db)
            .await
            .context("failed to insert projected turn input rows")?;
    }

    Ok(())
}

pub fn input_type_and_text(item: &UserInput) -> (&'static str, Option<String>) {
    match item {
        UserInput::Text { text, .. } => ("text", Some(text.clone())),
        UserInput::Image { .. } => ("image", None),
        UserInput::LocalImage { .. } => ("local_image", None),
        UserInput::File { .. } => ("file", None),
        UserInput::LocalFile { .. } => ("local_file", None),
        UserInput::Audio { .. } => ("audio", None),
        UserInput::LocalAudio { .. } => ("local_audio", None),
        UserInput::Video { .. } => ("video", None),
        UserInput::LocalVideo { .. } => ("local_video", None),
        UserInput::Artifact { .. } => ("artifact", None),
        UserInput::Mention { .. } => ("mention", None),
    }
}
