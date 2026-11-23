//! Publish terminal cleanup work once a relevant write has settled.

use pioneer_entity::turn;
use pioneer_postgres_pools::{PostgresDatabase, WorkChange};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};

pub struct Change(Vec<WorkChange>);

impl Change {
    pub fn events(db: &PostgresDatabase) -> Self {
        Self(
            [
                "maintenance.native",
                "maintenance.receipts",
                "maintenance.zstd",
                "maintenance.portable",
            ]
            .map(|domain| db.work_signal(domain).changing())
            .into(),
        )
    }

    pub fn native(db: &PostgresDatabase) -> Self {
        Self(vec![db.work_signal("maintenance.native").changing()])
    }

    pub fn payload(db: &PostgresDatabase) -> Self {
        Self(
            ["maintenance.zstd", "maintenance.portable"]
                .map(|domain| db.work_signal(domain).changing())
                .into(),
        )
    }

    /// The caller's atomic write already proves a committed completed/failed/
    /// interrupted Turn. Publish directly, before any post-commit reads. A later
    /// resume is harmless: cleanup revalidates current eligibility itself.
    pub fn completed_terminal(self, changed: bool, turns: &[String]) {
        for change in self.0 {
            change.completed_keys(if changed { turns } else { &[] });
        }
    }

    pub async fn complete(self, db: &PostgresDatabase, changed: bool, turns: &[String]) {
        let relevant = if changed && !turns.is_empty() {
            // A bounded primary-key metadata probe after releasing the writer.
            // This also covers late projection/raw arrivals after terminalization.
            // Errors conservatively wake the owner; never turn a committed write
            // into a reported failure merely because this advisory read failed.
            turn::Entity::find()
                .select_only()
                .column(turn::Column::Id)
                .filter(turn::Column::Id.is_in(turns.iter().cloned()))
                .filter(turn::Column::Status.is_in([
                    "completed",
                    "failed",
                    "interrupted",
                    "blocked",
                ]))
                .into_tuple::<String>()
                .all(db)
                .await
                .ok()
        } else {
            Some(Vec::new())
        };
        // Cancellation/error before known completion drops WorkChange and keeps
        // uncertainty and writer settlement, including a late PostgreSQL commit.
        for change in self.0 {
            match &relevant {
                Some(keys) => change.completed_keys(keys),
                None => change.completed(true),
            }
        }
    }
}
