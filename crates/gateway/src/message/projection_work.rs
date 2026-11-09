//! A finite invalidation frontier for the existing projection/finalization owner.
//! Hints never replace durable claims. No database resource is kept here.
use std::collections::VecDeque;

#[derive(Clone)]
pub(super) struct Scope {
    pub turns: Option<Vec<String>>,
    pub replay: bool,
    pub finalization: bool,
}

#[derive(Default)]
pub(super) struct ProjectionWork {
    scopes: VecDeque<Scope>,
    pub cutoff: i64,
    deadline: Option<i64>,
    full_retry: bool,
}

impl ProjectionWork {
    pub fn is_complete(&self) -> bool {
        self.scopes.is_empty()
    }

    pub fn begin(&mut self, keys: Option<Vec<String>>, now: i64) {
        self.cutoff = now;
        // A keyed wake must not erase a lease/retry belonging to another Turn.
        // When that clock matures, rediscover durable state (including changes
        // by the cooperative independent process writer).
        let due = self.deadline.is_some_and(|at| at <= now);
        if due {
            self.deadline = None;
        }
        let full_retry = std::mem::take(&mut self.full_retry);
        let keys = keys.filter(|_| !due && !full_retry);
        let turns = keys.and_then(|keys| {
            keys.into_iter()
                .filter(|key| key != "delivery")
                .map(|key| {
                    key.strip_prefix("turn:")
                        .filter(|id| !id.is_empty())
                        .map(str::to_owned)
                })
                .collect::<Option<Vec<_>>>()
        });
        match turns {
            Some(turns) => {
                for turns in turns.chunks(64) {
                    self.scopes.push_back(Scope {
                        turns: Some(turns.to_vec()),
                        replay: true,
                        finalization: true,
                    });
                }
            }
            None => {
                // A full pass refreshes the global clock; discard stale clocks
                // left behind by a keyed pass which completed their work early.
                self.deadline = None;
                self.scopes.push_back(Scope {
                    turns: None,
                    replay: true,
                    finalization: true,
                });
            }
        }
    }

    pub fn current(&self) -> Option<Scope> {
        self.scopes.front().cloned()
    }

    pub fn advance(&mut self, replay_deadline: Option<i64>, finalization_complete: bool) {
        let Some(mut scope) = self.scopes.pop_front() else {
            return;
        };
        if scope.replay {
            scope.replay = replay_deadline.is_some_and(|at| at <= self.cutoff);
            if let Some(at) = replay_deadline.filter(|_| !scope.replay) {
                self.deadline = Some(self.deadline.map_or(at, |old| old.min(at)));
            }
        }
        scope.finalization &= !finalization_complete;
        if scope.replay || scope.finalization {
            // A hot Turn cannot starve the rest of the captured keyed frontier.
            self.scopes.push_back(scope);
        }
    }

    pub fn retry(&mut self) {
        self.scopes.clear();
        self.deadline = None;
        // Re-enter begin_keyed_pass before discovery, including its settlement
        // fence for uncertain writes. It may itself have failed last time.
        self.full_retry = true;
    }

    pub fn deadline(&self) -> Option<i64> {
        if self.is_complete() {
            self.deadline
        } else {
            Some(0)
        }
    }

    pub async fn wait(&self, changes: &mut pioneer_sqlite::WorkSubscription) {
        // Waiting consumes the watch notification. Between quanta only yield
        // in the caller: changes during this frontier must wake its successor.
        if self.is_complete() {
            changes.wait(self.deadline()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProjectionWork;

    #[test]
    fn keyed_and_delivery_wakes_keep_another_turns_deadline_until_it_is_due() {
        let mut work = ProjectionWork::default();
        work.begin(None, 100);
        assert!(work.current().unwrap().turns.is_none());
        work.advance(Some(200), true);
        assert!(work.is_complete());

        work.begin(Some(vec!["turn:active".into()]), 150);
        assert_eq!(work.current().unwrap().turns.unwrap(), vec!["active"]);
        work.advance(None, true);
        assert_eq!(work.deadline(), Some(200));
        work.begin(Some(vec!["delivery".into()]), 160);
        assert!(
            work.is_complete(),
            "ACKs must not schedule recovery discovery"
        );
        assert_eq!(work.deadline(), Some(200));

        work.begin(Some(vec!["delivery".into()]), 200);
        assert!(work.current().unwrap().turns.is_none());
        work.advance(None, true);
        assert!(work.is_complete());
        assert_eq!(work.deadline(), None, "an exhausted clock must allow idle");
    }

    #[tokio::test]
    async fn a_full_quantum_yields_without_consuming_the_next_pass_wake() {
        use futures_util::FutureExt;

        let database = pioneer_sqlite::SqliteDatabase::from_single_connection(
            sea_orm::Database::connect("sqlite::memory:").await.unwrap(),
        );
        let signal = database.work_signal("projection");
        let mut changes = signal.subscribe();
        let mut work = ProjectionWork::default();
        work.begin(changes.begin_keyed_pass(&database).await.unwrap(), 99);
        work.advance(None, true);
        signal.notify_keys(
            &(0..65)
                .map(|id| format!("turn:{id:02}"))
                .collect::<Vec<_>>(),
        );
        work.begin(changes.begin_keyed_pass(&database).await.unwrap(), 100);
        let first = work.current().unwrap().turns.unwrap();
        assert_eq!(first.len(), 64);
        // A commit arrives behind the captured frontier. It must still wake
        // a new pass after all current scopes reach EOF, without another write.
        signal.notify_keys(&["turn:arrived-during-pass".into()]);
        work.advance(Some(100), false);
        assert_eq!(work.deadline(), Some(0));
        work.wait(&mut changes).await;
        assert_eq!(work.current().unwrap().turns.unwrap(), vec!["64"]);
        work.advance(None, true);
        work.wait(&mut changes).await;
        assert_eq!(work.current().unwrap().turns.unwrap(), first);
        work.advance(None, false);
        work.wait(&mut changes).await;
        let remaining = work.current().unwrap();
        assert!(!remaining.replay && remaining.finalization);
        assert_eq!(work.deadline(), Some(0));
        work.advance(None, true);
        assert!(work.is_complete());
        assert_eq!(work.deadline(), None);
        assert!(
            work.wait(&mut changes).now_or_never().is_some(),
            "completion must not park over an unprocessed source change"
        );
        work.begin(changes.begin_keyed_pass(&database).await.unwrap(), 101);
        assert_eq!(
            work.current().unwrap().turns.unwrap(),
            vec!["arrived-during-pass"]
        );
        work.advance(None, true);
        assert!(work.wait(&mut changes).now_or_never().is_none());
        database.close().await.unwrap();
    }

    #[test]
    fn unknown_hints_and_failed_passes_fall_back_to_full_discovery() {
        for keys in [
            None,
            Some(vec!["turn:".into()]),
            Some(vec!["legacy".into()]),
        ] {
            let mut work = ProjectionWork::default();
            work.begin(keys, 100);
            let scope = work.current().unwrap();
            assert!(scope.turns.is_none() && scope.replay && scope.finalization);
        }
        let mut work = ProjectionWork::default();
        work.begin(Some(vec!["turn:only".into()]), 100);
        work.advance(Some(200), false);
        work.retry();
        assert!(
            work.is_complete(),
            "retry must re-enter invalidation settlement"
        );
        work.begin(Some(vec![]), 101);
        let scope = work.current().unwrap();
        assert!(scope.turns.is_none() && scope.replay && scope.finalization);
        work.advance(None, true);
        assert_eq!(work.deadline(), None);
    }
}
