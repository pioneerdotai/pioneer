//! Unread eligibility is not a read receipt. Existing history is excluded once,
//! at migration commit; no historical viewing date can be recovered reliably.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(DeriveIden)]
enum GatewayIdentity {
    Table,
    UnreadStartedAt,
}
#[derive(DeriveIden)]
enum Thread {
    Table,
    WorkspaceUnreadStartedAt,
}
#[derive(DeriveIden)]
enum UnreadMessagePosition {
    PublishedAt,
    Table,
    ThreadId,
    BlockId,
    TurnId,
    SortKey,
}
#[derive(DeriveIden)]
enum ThreadTimelineBlock {
    Table,
    BlockId,
    ThreadId,
    SortKey,
    MetadataJson,
}
#[derive(DeriveIden)]
enum Turn {
    Table,
    Id,
    ThreadId,
}
#[derive(DeriveIden)]
enum TurnInput {
    Table,
    TurnId,
}
#[derive(DeriveIden)]
enum TurnItem {
    Table,
    TurnId,
    ItemId,
    ItemType,
    Status,
    Payload,
}

fn sqlite_function(name: &str, args: impl IntoIterator<Item = Expr>) -> Expr {
    Func::cust(Alias::new(name)).args(args).into()
}

fn concatenate(left: Expr, right: Expr) -> Expr {
    // SeaQuery exposes SQLite's concatenation operator through Custom.
    left.binary(BinOper::Custom("||"), right)
}

fn historical_completed_answer() -> Expr {
    let completed = Expr::col((TurnItem::Table, TurnItem::Status)).eq("completed");
    let prior_completion = sqlite_function(
        "json_extract",
        [
            Expr::col((
                ThreadTimelineBlock::Table,
                ThreadTimelineBlock::MetadataJson,
            )),
            Expr::val("$.messageCompleted"),
        ],
    )
    .eq(true);
    // These are the frozen historical phase spellings; the serializer writes
    // commentary or omits final_answer. Non-string/missing phases default to
    // final, as in the existing canonical projector.
    let payload = Expr::col((TurnItem::Table, TurnItem::Payload));
    let phase = sqlite_function("json_extract", [payload.clone(), Expr::val("$.phase")]);
    let mut normalized = sqlite_function("lower", [phase]);
    for separator in ["_", "-", " "] {
        normalized = sqlite_function("replace", [normalized, Expr::val(separator), Expr::val("")]);
    }
    let final_phase = Expr::case(
        sqlite_function("json_type", [payload.clone(), Expr::val("$.phase")]).eq("text"),
        normalized.eq("finalanswer"),
    )
    .finally(true);
    let fallback = Expr::case(
        sqlite_function("json_valid", [payload]).eq(true),
        final_phase,
    )
    .finally(false);
    Expr::case(
        Expr::col((ThreadTimelineBlock::Table, ThreadTimelineBlock::BlockId)).is_not_null(),
        completed.clone().or(prior_completion),
    )
    .finally(completed.and(fallback))
    .into()
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(GatewayIdentity::Table)
                    .add_column(
                        ColumnDef::new(GatewayIdentity::UnreadStartedAt)
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Thread::Table)
                    .add_column(
                        ColumnDef::new(Thread::WorkspaceUnreadStartedAt)
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        // One canonical identity per message, never per user. Keep it outside
        // rebuildable projection tables (and the sqlite-zstd turn_item view).
        manager
            .create_table(
                Table::create()
                    .table(UnreadMessagePosition::Table)
                    .col(
                        ColumnDef::new(UnreadMessagePosition::ThreadId)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(UnreadMessagePosition::BlockId)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(UnreadMessagePosition::TurnId)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(UnreadMessagePosition::SortKey)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(UnreadMessagePosition::PublishedAt)
                            .big_integer()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(UnreadMessagePosition::ThreadId)
                            .col(UnreadMessagePosition::BlockId),
                    )
                    .to_owned(),
            )
            .await?;
        let db = manager.get_connection();
        let backend = db.get_database_backend();
        // Existing Gateways activate once here. A fresh database has no
        // Gateway row yet; create_gateway_singleton saves its creation second.
        // No viewing date is inferred and no read receipt is created.
        db.execute_raw(
            backend.build(
                &Query::update()
                    .table(GatewayIdentity::Table)
                    .value(
                        GatewayIdentity::UnreadStartedAt,
                        sqlite_function("strftime", [Expr::val("%s"), Expr::val("now")])
                            .cast_as(Alias::new("INTEGER")),
                    )
                    .to_owned(),
            ),
        )
        .await?;

        // Seed source identities as well as projected positions, so an old
        // completed item absent from a partial projection cannot become new on
        // replay. Existing blocks need metadata only; the missing-block fallback
        // extracts saved phase in SQLite without returning message content.
        for assistant in [false, true] {
            let turn_identity = concatenate(Expr::val("turn:"), Expr::col((Turn::Table, Turn::Id)));
            let identity = if assistant {
                concatenate(
                    concatenate(turn_identity, Expr::val(":assistant:")),
                    Expr::col((TurnItem::Table, TurnItem::ItemId)),
                )
            } else {
                concatenate(turn_identity, Expr::val(":user"))
            };
            let mut source = Query::select();
            source
                .column((Turn::Table, Turn::ThreadId))
                .expr(identity.clone())
                .column((Turn::Table, Turn::Id))
                .expr(
                    Expr::col((ThreadTimelineBlock::Table, ThreadTimelineBlock::SortKey))
                        .if_null(""),
                )
                .expr(Expr::val(0))
                .from(Turn::Table);
            if assistant {
                source
                    .inner_join(
                        TurnItem::Table,
                        Expr::col((TurnItem::Table, TurnItem::TurnId))
                            .equals((Turn::Table, Turn::Id)),
                    )
                    .and_where(Expr::col((TurnItem::Table, TurnItem::ItemType)).eq("agent_message"))
                    // Existing canonical blocks establish historical readability.
                    // For a missing projection, inspect only the saved phase:
                    // completed commentary was never a canonical answer. This
                    // one-time fallback may decompress a sqlite-zstd row; no
                    // payload or HTML is returned to Rust or read by counts.
                    .and_where(historical_completed_answer());
            }
            source.left_join(
                ThreadTimelineBlock::Table,
                Condition::all()
                    .add(
                        Expr::col((ThreadTimelineBlock::Table, ThreadTimelineBlock::BlockId))
                            .eq(identity),
                    )
                    .add(
                        Expr::col((ThreadTimelineBlock::Table, ThreadTimelineBlock::ThreadId))
                            .equals((Turn::Table, Turn::ThreadId)),
                    ),
            );
            if !assistant {
                // Empty Turns are not published prompts. A later first input
                // must be eligible; deleted prompts still retain their block.
                source.cond_where(
                    Condition::any()
                        .add(
                            Expr::col((ThreadTimelineBlock::Table, ThreadTimelineBlock::BlockId))
                                .is_not_null(),
                        )
                        .add(Expr::exists(
                            Query::select()
                                .expr(1)
                                .from(TurnInput::Table)
                                .and_where(
                                    Expr::col((TurnInput::Table, TurnInput::TurnId))
                                        .equals((Turn::Table, Turn::Id)),
                                )
                                .to_owned(),
                        )),
                );
            }
            db.execute_raw(
                backend.build(
                    &Query::insert()
                        .into_table(UnreadMessagePosition::Table)
                        .columns([
                            UnreadMessagePosition::ThreadId,
                            UnreadMessagePosition::BlockId,
                            UnreadMessagePosition::TurnId,
                            UnreadMessagePosition::SortKey,
                            UnreadMessagePosition::PublishedAt,
                        ])
                        .select_from(source.to_owned())
                        .map_err(|e| DbErr::Custom(e.to_string()))?
                        .to_owned(),
                ),
            )
            .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(UnreadMessagePosition::Table).to_owned())
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Thread::Table)
                    .drop_column(Thread::WorkspaceUnreadStartedAt)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(GatewayIdentity::Table)
                    .drop_column(GatewayIdentity::UnreadStartedAt)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
