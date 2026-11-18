//! The scenarios every factum store must pass. Each drives the engine's
//! public API only, as a program would: submit, start, tick, claim, read.
//! A store is conformant when `factum_conformance::suite!` passes on it.
//!
//! Each guarantee factum makes has its home in one scenario here, and no
//! store has tests of its own for these guarantees.

pub mod domain;
pub mod flaky;
pub mod scenarios;

use std::sync::Arc;

use factum::{By, Engine, Error, Instant, ManualClock, Model, Options, Store, Submission, Uuid};

use crate::domain::{OrderList, Payments, Printer};
use crate::flaky::Flaky;

/// A store under test.
pub trait Backend: Sync {
    type S: Store;
    /// A new, empty store.
    fn open(&self) -> Arc<Self::S>;
    /// Another handle on the same data, as a second runner holds it.
    fn reopen(&self, store: &Arc<Self::S>) -> Arc<Self::S>;
}

/// A runner on a flaky handle, and the handle to cue it with.
pub type FlakyRunner<S> = (Engine<Flaky<S>>, Arc<Flaky<S>>);

/// When every scenario starts: 2026-01-01T00:00:00Z.
pub const START: Instant = Instant(1_767_225_600_000);

/// One scenario's world: a store, a clock only the scenario moves, the
/// shop's stand-ins, and engines on handles of the same store.
pub struct World<'a, B: Backend> {
    pub backend: &'a B,
    pub store: Arc<B::S>,
    pub clock: Arc<ManualClock>,
    pub payments: Arc<Payments>,
    pub printer: Arc<Printer>,
    model: Arc<Model>,
}

impl<'a, B: Backend> World<'a, B> {
    pub fn new(backend: &'a B) -> Self {
        Self::with_orders(backend, OrderList::default())
    }

    /// A world whose order list projection is the one given.
    pub fn with_orders(backend: &'a B, orders: OrderList) -> Self {
        let payments = Arc::new(Payments::default());
        let printer = Arc::new(Printer::default());
        let model = Arc::new(domain::shop(payments.clone(), printer.clone(), orders));
        World {
            backend,
            store: backend.open(),
            clock: Arc::new(ManualClock::new(START)),
            payments,
            printer,
            model,
        }
    }

    /// The shop declared again, with another order list projection.
    pub fn shop(&self, orders: OrderList) -> Model {
        domain::shop(self.payments.clone(), self.printer.clone(), orders)
    }

    /// A runner of the shop on its own handle of the store.
    pub fn engine(&self, holder: &str) -> Engine<B::S> {
        Engine::new(
            self.backend.reopen(&self.store),
            self.model.clone(),
            self.clock.clone(),
            holder,
        )
    }

    /// A runner of another model on the same store.
    pub fn engine_with(&self, model: Model, holder: &str) -> Engine<B::S> {
        Engine::new(
            self.backend.reopen(&self.store),
            Arc::new(model),
            self.clock.clone(),
            holder,
        )
    }

    pub fn engine_with_options(&self, options: Options, holder: &str) -> Engine<B::S> {
        self.engine(holder).with_options(options)
    }

    /// A runner whose store crashes or races on cue.
    pub fn flaky(&self, holder: &str) -> FlakyRunner<B::S> {
        let flaky = Arc::new(Flaky::new(self.backend.reopen(&self.store)));
        (
            Engine::new(
                flaky.clone(),
                self.model.clone(),
                self.clock.clone(),
                holder,
            ),
            flaky,
        )
    }

    pub fn advance(&self, millis: i64) {
        self.clock.advance(millis);
    }
}

/// A fresh id.
pub fn id() -> Uuid {
    Uuid::now_v7()
}

/// The person every scenario acts as.
pub fn ann() -> By {
    By::Principal("person:ann".into())
}

/// A submission as Ann, under a fresh key.
pub fn sub(command: &str, params: serde_json::Value) -> Submission {
    Submission::new(command, params, id(), ann())
}

/// The code a refused result was refused with.
pub fn refused<T: std::fmt::Debug>(result: Result<T, Error>) -> String {
    match result {
        Err(Error::Rejected(r)) => r.code,
        other => panic!("expected a rejection, got {other:?}"),
    }
}

/// Runs every scenario against a backend, one `#[tokio::test]` each.
#[macro_export]
macro_rules! suite {
    ($backend:expr) => {
        $crate::suite!(@each $backend;
            commands_record_and_fold,
            unknown_commands_and_bad_params_are_refused,
            a_key_replays_its_first_answer_and_refuses_reuse,
            a_fault_is_no_answer_and_the_key_decides_again,
            a_refusal_that_loses_its_key_answers_what_the_key_holds,
            a_plan_records_nothing,
            a_writer_that_read_an_old_version_decides_again,
            versions_stay_unique_under_two_writers,
            an_ended_stream_refuses_every_command,
            a_stream_ended_by_a_scheduling_command_stays_ended,
            a_scheduled_command_runs_once_when_due,
            a_refused_scheduled_command_is_recorded_not_retried,
            a_recurring_command_runs_once_per_period,
            a_process_runs_to_completion,
            a_refused_step_is_compensated,
            a_step_run_but_not_recorded_is_replayed_not_repeated,
            a_call_whose_answer_was_lost_is_made_again_and_done_once,
            an_at_most_once_call_whose_answer_was_lost_ends_unknown,
            a_call_that_fails_for_now_is_tried_again_then_given_up,
            two_runners_take_each_step_once,
            a_runner_that_dies_loses_its_process_to_another,
            starting_a_process_again,
            a_process_starts_once_on_the_event_it_starts_on,
            a_worker_claims_heartbeats_and_finishes_a_task,
            a_failed_attempt_is_tried_again,
            a_task_whose_lease_ran_out_is_taken_over,
            a_result_whose_lease_ran_out_meanwhile_is_refused,
            a_wake_while_a_runner_holds_the_process_is_not_lost,
            an_at_most_once_task_whose_lease_ran_out_ends_unknown,
            a_heartbeat_never_holds_a_task_past_its_timeout,
            tasks_with_one_ordering_key_run_one_at_a_time_paced,
            a_crash_between_a_tasks_answer_and_its_pace_loses_no_pace,
            a_cancelled_task_ends_its_step_cancelled,
            a_wait_ends_when_its_event_arrives,
            a_wait_for_an_event_already_recorded_ends_at_once,
            a_wait_times_out,
            work_rows_in_the_stored_shape_still_run,
            a_work_row_that_does_not_decode_is_a_fault,
            an_event_that_cannot_be_dispatched_stops_nothing_else,
            a_process_that_faults_stops_no_other,
            projection_rows_follow_the_log_and_rebuild_the_same,
            a_projection_skips_an_event_it_fails_on,
            two_projectors_apply_each_event_once,
            a_projection_that_stops_holds_until_rebuilt,
            a_read_never_serves_rows_another_version_built,
            old_events_are_read_in_their_current_shape,
            snapshots_change_no_answer,
            queries_filter_and_order_by_declared_columns,
            a_change_to_a_projections_tables_rebuilds_it,
            a_projection_that_writes_an_undeclared_table_fails_the_event,
            every_store_refuses_rows_of_an_undeclared_table,
            a_dead_letter_retried_with_fixed_code_is_projected,
            a_stopped_projection_goes_on_once_its_dead_letter_is_retried,
            a_dead_letter_another_version_left_is_not_retried,
            a_sample_holds_a_new_next_to_old_progress,
            runners_in_parallel_do_each_thing_once,
            writers_in_parallel_keep_versions_unique,
        );
    };
    (@each $backend:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $name() {
                $crate::scenarios::$name(&$backend).await;
            }
        )*
    };
}
