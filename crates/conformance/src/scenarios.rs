//! One function per guarantee. Each opens a world on the backend and uses
//! the engine as a program would.

use std::sync::atomic::Ordering;

use factum::{
    Engine, Error, Event, Finished, Op, Options, Outcome, Query, Store, StreamId, Uuid, Value,
    Version,
};
use factum_core::{Work, WorkChange, WorkKind, start_key, step_key};
use factum_store::{AnswerResult, Batch, StoreError, Unit};
use serde_json::json;

use crate::domain::{EXPIRES_MS, LIMIT, OrderList, PICKUP_MS, TALLY};
use crate::flaky::{appends, records_step};
use crate::{Backend, World, ann, id, refused, sub};

fn order(id: Uuid) -> StreamId {
    StreamId::new("order", id)
}

fn kinds(events: &[Event]) -> Vec<&str> {
    events.iter().map(|e| e.kind.as_str()).collect()
}

async fn place<S: Store>(e: &Engine<S>, total: i64) -> Uuid {
    let o = id();
    e.submit(sub("order.place", json!({ "order": o, "total": total })))
        .await
        .unwrap();
    o
}

async fn pay<S: Store>(e: &Engine<S>, o: Uuid) {
    e.submit(sub("order.pay", json!({ "order": o, "charge": "cash" })))
        .await
        .unwrap();
}

async fn status<S: Store>(e: &Engine<S>, o: Uuid) -> Value {
    e.state(&order(o)).await.unwrap()["status"].clone()
}

async fn count<S: Store>(e: &Engine<S>, o: Uuid, kind: &str) -> usize {
    e.history(&order(o))
        .await
        .unwrap()
        .iter()
        .filter(|ev| ev.kind == kind)
        .count()
}

async fn finished<S: Store>(e: &Engine<S>, p: Uuid) -> Option<Finished> {
    e.process(p).await.unwrap().finished
}

async fn checkout<S: Store>(e: &Engine<S>, total: i64) -> (Uuid, Uuid) {
    let o = place(e, total).await;
    let p = id();
    e.start("checkout", p, json!({ "order": o, "total": total }), ann())
        .await
        .unwrap();
    (o, p)
}

/// A paid order, and the fulfil process its payment started.
async fn fulfilling<S: Store>(e: &Engine<S>) -> (Uuid, Uuid) {
    let o = place(e, 100).await;
    pay(e, o).await;
    e.settle().await.unwrap();
    let paid = e
        .history(&order(o))
        .await
        .unwrap()
        .into_iter()
        .find(|ev| ev.kind == "OrderPaid")
        .unwrap();
    (o, start_key(paid.id, "fulfil"))
}

// ---- commands ----

pub async fn commands_record_and_fold<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = id();
    let s = sub("order.place", json!({ "order": o, "total": 500 }));
    let applied = e.submit(s.clone()).await.unwrap();
    assert!(!applied.replayed);
    assert_eq!(kinds(&applied.events), ["OrderPlaced", "CommandScheduled"]);
    let state = e.state(&order(o)).await.unwrap();
    assert_eq!(
        (state["status"].as_str(), state["total"].as_i64()),
        (Some("placed"), Some(500))
    );
    let history = e.history(&order(o)).await.unwrap();
    assert_eq!(
        history.iter().map(|ev| ev.version).collect::<Vec<_>>(),
        [Version(1), Version(2)]
    );
    let meta = &history[0].meta;
    assert_eq!(
        (&meta.by, meta.now, meta.key, meta.command.as_str()),
        (&ann(), w.clock.now_(), Some(s.key), "order.place")
    );
    assert!(history[0].position < history[1].position);
    let past_end = e.store().read_stream(&order(o), Version(5)).await;
    assert!(past_end.unwrap().is_empty(), "nothing after the last event");
}

pub async fn unknown_commands_and_bad_params_are_refused<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    assert_eq!(
        refused(e.submit(sub("order.teleport", json!({}))).await),
        "unknown"
    );
    assert_eq!(
        refused(e.submit(sub("order.place", json!({ "order": "o1" }))).await),
        "invalid_params"
    );
    assert_eq!(
        refused(e.start("teleport", id(), json!({}), ann()).await),
        "unknown"
    );
}

pub async fn a_key_replays_its_first_answer_and_refuses_reuse<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = id();
    let s = sub("order.place", json!({ "order": o, "total": 500 }));
    let first = e.submit(s.clone()).await.unwrap();
    let again = e.engine_again(&w).submit(s.clone()).await.unwrap();
    assert!(again.replayed);
    assert_eq!(again.events, first.events);
    let mut other = s.clone();
    other.params = json!({ "order": o, "total": 600 });
    assert_eq!(refused(e.submit(other).await), "key_reused");
    assert_eq!(e.history(&order(o)).await.unwrap().len(), 2);

    // A refusal is an answer too: the key answers it again, even once the
    // command would now apply.
    let later = id();
    let early = sub("order.pay", json!({ "order": later, "charge": "cash" }));
    assert_eq!(refused(e.submit(early.clone()).await), "not_placed");
    e.submit(sub("order.place", json!({ "order": later, "total": 1 })))
        .await
        .unwrap();
    assert_eq!(refused(e.submit(early).await), "not_placed");
    assert_eq!(status(&e, later).await, "placed");
}

pub async fn a_fault_is_no_answer_and_the_key_decides_again<B: Backend>(b: &B) {
    let w = World::new(b);
    let buggy = w.engine_with(
        factum::Model::new().command::<crate::domain::unreadable::AddUnreadable>(),
        "buggy",
    );
    let c = id();
    let s = sub("counter.add", json!({ "counter": c, "n": 3 }));
    for _ in 0..2 {
        let faulted = buggy.submit(s.clone()).await;
        assert!(matches!(faulted, Err(Error::Fault(_))), "{faulted:?}");
    }
    assert!(w.store.answer(s.key).await.unwrap().is_none());

    let fixed = w.engine("fixed");
    let applied = fixed.submit(s).await.unwrap();
    assert!(!applied.replayed, "the key decided again");
    let state = fixed.state(&StreamId::new("counter", c)).await.unwrap();
    assert_eq!(state["total"], 3);
}

pub async fn a_refusal_that_loses_its_key_answers_what_the_key_holds<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, flaky) = w.flaky("a");
    let other = w.engine("b");
    let o = id();
    let s = sub("order.pay", json!({ "order": o, "charge": "cash" }));
    let rival = s.clone();
    // A refuses the payment, but before it records the refusal another
    // writer places the order and pays it under the same key.
    flaky.race_on(
        |u| {
            u.answer
                .as_ref()
                .is_some_and(|(_, a)| matches!(a.result, AnswerResult::Rejected(_)))
        },
        Box::pin(async move {
            other
                .submit(sub("order.place", json!({ "order": o, "total": 100 })))
                .await
                .unwrap();
            other.submit(rival).await.unwrap();
        }),
    );
    let first = a.submit(s.clone()).await.unwrap();
    let again = w.engine("c").submit(s.clone()).await.unwrap();
    assert!(again.replayed);
    assert_eq!(first.events, again.events);
    assert_eq!(kinds(&first.events), ["OrderPaid"]);
    assert_eq!(count(&a, o, "OrderPaid").await, 1);
}

pub async fn a_plan_records_nothing<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = id();
    let planned = e
        .plan("order.place", json!({ "order": o, "total": 5 }), ann())
        .await
        .unwrap();
    assert_eq!(
        planned.iter().map(|p| p.kind.as_str()).collect::<Vec<_>>(),
        ["OrderPlaced", "CommandScheduled"]
    );
    assert!(e.history(&order(o)).await.unwrap().is_empty());
    assert_eq!(status(&e, o).await, "");
}

pub async fn a_writer_that_read_an_old_version_decides_again<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, flaky) = w.flaky("a");
    let other = w.engine("b");
    let c = id();
    let rival = sub("counter.add", json!({ "counter": c, "n": 10 }));
    flaky.race_on(
        |u| appends(u, "Added"),
        Box::pin(async move {
            other.submit(rival).await.unwrap();
        }),
    );
    a.submit(sub("counter.add", json!({ "counter": c, "n": 1 })))
        .await
        .unwrap();
    let e = w.engine("c");
    assert_eq!(
        e.state(&StreamId::new("counter", c)).await.unwrap()["total"],
        11
    );
    let versions: Vec<_> = e
        .history(&StreamId::new("counter", c))
        .await
        .unwrap()
        .iter()
        .map(|ev| ev.version)
        .collect();
    assert_eq!(versions, [Version(1), Version(2)]);
}

/// The give-up fault of a writer that lost every race, from `result` or
/// from the tick that set it aside.
fn gave_up(error: impl std::fmt::Display) {
    let error = error.to_string();
    assert!(error.contains("gave up after losing 1000 races"), "{error}");
}

/// A writer that never gives up hangs here; this fails it instead.
async fn within_10s<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(10), f)
        .await
        .unwrap_or_else(|_| panic!("{what} kept deciding again past the bound"))
}

pub async fn a_writer_that_loses_every_race_gives_up<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, flaky) = w.flaky("a");

    // A command.
    let c = id();
    flaky.lose_every(|u| appends(u, "Added"));
    let add = sub("counter.add", json!({ "counter": c, "n": 1 }));
    match within_10s("submit", a.submit(add)).await {
        Err(e @ Error::Fault(_)) => gave_up(e),
        other => panic!("expected a give-up fault, got {other:?}"),
    }

    // A process's advance: the tick sets the process aside and goes on.
    let (_, p) = checkout(&a, 500).await;
    flaky.lose_every(|u| appends(u, "StepRecorded") || appends(u, "StepRequested"));
    let tick = within_10s("advance", a.tick()).await.unwrap();
    let fault = tick
        .faults
        .iter()
        .find(|f| f.item == format!("process/{p}"))
        .unwrap_or_else(|| panic!("no fault for process/{p}: {tick:?}"));
    gave_up(&fault.error);

    // A step's answer.
    flaky.lose_every(|_| false);
    let (_, p) = fulfilling(&a).await;
    let task = a.claim_task("pack", "w1", 10_000).await.unwrap().unwrap();
    flaky.lose_every(|u| records_step(u, "pack"));
    let answer = a.finish_task(&task, Ok(json!({ "box": 1 })), "w1");
    match within_10s("answer", answer).await {
        Err(e @ Error::Fault(_)) => gave_up(e),
        other => panic!("expected a give-up fault, got {other:?}"),
    }
    assert_eq!(a.process(p).await.unwrap().progress.outcome("pack"), None);
}

pub async fn versions_stay_unique_under_two_writers<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, bb) = (w.engine("a"), w.engine("b"));
    let c = id();
    for _ in 0..10 {
        let (x, y) = tokio::join!(
            a.submit(sub("counter.add", json!({ "counter": c, "n": 1 }))),
            bb.submit(sub("counter.add", json!({ "counter": c, "n": 1 })))
        );
        x.unwrap();
        y.unwrap();
    }
    let history = a.history(&StreamId::new("counter", c)).await.unwrap();
    assert_eq!(
        history.iter().map(|ev| ev.version.0).collect::<Vec<_>>(),
        (1..=20).collect::<Vec<_>>()
    );
    assert_eq!(
        a.state(&StreamId::new("counter", c)).await.unwrap()["total"],
        20
    );
}

pub async fn an_ended_stream_refuses_every_command<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = place(&e, 5).await;
    pay(&e, o).await;
    e.submit(sub("order.close", json!({ "order": o })))
        .await
        .unwrap();
    assert_eq!(
        refused(e.submit(sub("order.close", json!({ "order": o }))).await),
        "ended"
    );
    assert_eq!(
        refused(
            e.submit(sub("order.cancel", json!({ "order": o, "reason": "x" })))
                .await
        ),
        "ended"
    );
    assert_eq!(status(&e, o).await, "closed");
}

pub async fn a_stream_ended_by_a_scheduling_command_stays_ended<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = place(&e, 5).await;
    pay(&e, o).await;
    e.submit(sub("order.close_and_remind", json!({ "order": o })))
        .await
        .unwrap();
    assert_eq!(
        kinds(&e.history(&order(o)).await.unwrap()).last(),
        Some(&"CommandScheduled")
    );
    assert_eq!(
        refused(
            e.submit(sub("order.cancel", json!({ "order": o, "reason": "x" })))
                .await
        ),
        "ended"
    );
    assert_eq!(status(&e, o).await, "closed");
}

// ---- schedules ----

pub async fn a_scheduled_command_runs_once_when_due<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, bb) = (w.engine("a"), w.engine("b"));
    let o = place(&a, 5).await;
    a.settle().await.unwrap();
    assert_eq!(status(&a, o).await, "placed");
    w.advance(EXPIRES_MS);
    a.settle().await.unwrap();
    bb.settle().await.unwrap();
    a.settle().await.unwrap();
    assert_eq!(status(&a, o).await, "cancelled");
    assert_eq!(count(&a, o, "OrderCancelled").await, 1);
}

pub async fn a_refused_scheduled_command_is_recorded_not_retried<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = place(&e, 5).await;
    pay(&e, o).await;
    w.advance(EXPIRES_MS + 1);
    e.settle().await.unwrap();
    w.advance(EXPIRES_MS);
    e.settle().await.unwrap();
    assert_eq!(status(&e, o).await, "paid");
    let scheduled = e
        .history(&order(o))
        .await
        .unwrap()
        .into_iter()
        .find(|ev| ev.kind == "CommandScheduled")
        .unwrap();
    let ran = e
        .history(&StreamId::new("schedule", scheduled.id))
        .await
        .unwrap();
    assert_eq!(kinds(&ran), ["ScheduleRan"]);
    assert_eq!(ran[0].data["rejection"]["code"], "not_placed");
}

pub async fn a_recurring_command_runs_once_per_period<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, bb) = (w.engine("a"), w.engine("b"));
    let tally = StreamId::new("counter", TALLY);
    a.settle().await.unwrap();
    bb.settle().await.unwrap();
    a.settle().await.unwrap();
    assert_eq!(a.state(&tally).await.unwrap()["total"], 1);
    w.advance(3_599_999);
    bb.settle().await.unwrap();
    assert_eq!(a.state(&tally).await.unwrap()["total"], 1);
    w.advance(1);
    bb.settle().await.unwrap();
    a.settle().await.unwrap();
    assert_eq!(a.state(&tally).await.unwrap()["total"], 2);
}

// ---- processes and actions ----

pub async fn a_process_runs_to_completion<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (o, p) = checkout(&e, 500).await;
    e.settle().await.unwrap();
    let view = e.process(p).await.unwrap();
    assert!(
        matches!(view.finished, Some(Finished::Completed(_))),
        "{view:?}"
    );
    assert_eq!(status(&e, o).await, "paid");
    assert_eq!((w.payments.calls(), w.payments.charges()), (1, 1));
    // The charge was made under the step's key.
    assert!(
        w.payments
            .charged
            .lock()
            .unwrap()
            .contains(&step_key(p, "charge"))
    );
}

pub async fn a_refused_step_is_compensated<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (o, p) = checkout(&e, LIMIT + 1).await;
    e.settle().await.unwrap();
    assert_eq!(
        finished(&e, p).await,
        Some(Finished::Failed("declined".into()))
    );
    assert_eq!(status(&e, o).await, "cancelled");
}

pub async fn a_step_run_but_not_recorded_is_replayed_not_repeated<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, flaky) = w.flaky("a");
    let (o, p) = checkout(&a, 500).await;
    flaky.crash_on(|u| records_step(u, "pay"));
    assert!(a.settle().await.is_err());
    assert_eq!(status(&a, o).await, "paid");

    let bb = w.engine("b");
    bb.settle().await.unwrap();
    assert_eq!(
        finished(&bb, p).await,
        None,
        "a's lease still holds the process"
    );
    w.advance(Options::default().advance_lease_ms);
    bb.settle().await.unwrap();
    assert!(matches!(
        finished(&bb, p).await,
        Some(Finished::Completed(_))
    ));
    assert_eq!(count(&bb, o, "OrderPaid").await, 1);
}

pub async fn a_call_whose_answer_was_lost_is_made_again_and_done_once<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, flaky) = w.flaky("a");
    let (o, p) = checkout(&a, 500).await;
    flaky.crash_on(|u| records_step(u, "charge"));
    assert!(a.settle().await.is_err());
    assert_eq!(w.payments.calls(), 1);

    let bb = w.engine("b");
    bb.settle().await.unwrap();
    assert_eq!(
        w.payments.calls(),
        1,
        "a's delivery holds its lease until the call's timeout"
    );
    w.advance(30_000);
    bb.settle().await.unwrap();
    assert!(matches!(
        finished(&bb, p).await,
        Some(Finished::Completed(_))
    ));
    assert_eq!((w.payments.calls(), w.payments.charges()), (2, 1));
    assert_eq!(status(&bb, o).await, "paid");
}

pub async fn an_at_most_once_call_whose_answer_was_lost_ends_unknown<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, flaky) = w.flaky("a");
    let p = id();
    a.start("receipt", p, json!({ "order": id() }), ann())
        .await
        .unwrap();
    flaky.crash_on(|u| records_step(u, "print"));
    assert!(a.settle().await.is_err());
    w.advance(30_000);
    let bb = w.engine("b");
    bb.settle().await.unwrap();
    let view = bb.process(p).await.unwrap();
    assert_eq!(view.progress.outcome("print"), Some(&Outcome::Unknown));
    assert!(matches!(view.finished, Some(Finished::Stuck(_))));
    assert_eq!(w.printer.printed.load(Ordering::SeqCst), 1);
}

pub async fn a_call_that_fails_for_now_is_tried_again_then_given_up<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    w.payments.busy.store(2, Ordering::SeqCst);
    let (_, p) = checkout(&e, 500).await;
    e.settle().await.unwrap();
    assert_eq!(w.payments.calls(), 1);
    w.advance(999);
    e.settle().await.unwrap();
    assert_eq!(w.payments.calls(), 1, "the first retry waits a second");
    w.advance(1);
    e.settle().await.unwrap();
    assert_eq!(w.payments.calls(), 2);
    w.advance(2_000);
    e.settle().await.unwrap();
    assert_eq!(w.payments.calls(), 3);
    assert!(matches!(
        finished(&e, p).await,
        Some(Finished::Completed(_))
    ));

    w.payments.busy.store(3, Ordering::SeqCst);
    let (o, p) = checkout(&e, 500).await;
    for wait in [0, 1_000, 2_000] {
        w.advance(wait);
        e.settle().await.unwrap();
    }
    assert_eq!(w.payments.calls(), 6);
    assert_eq!(
        e.process(p).await.unwrap().progress.outcome("charge"),
        Some(&Outcome::Failed("busy".into()))
    );
    assert!(matches!(finished(&e, p).await, Some(Finished::Failed(_))));
    assert_eq!(status(&e, o).await, "placed");
}

pub async fn two_runners_take_each_step_once<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, bb) = (w.engine("a"), w.engine("b"));
    let mut runs = Vec::new();
    for _ in 0..5 {
        runs.push(checkout(&a, 100).await);
    }
    for _ in 0..10 {
        let (x, y) = tokio::join!(a.tick(), bb.tick());
        x.unwrap();
        y.unwrap();
    }
    for (o, p) in runs {
        assert!(matches!(
            finished(&a, p).await,
            Some(Finished::Completed(_))
        ));
        assert_eq!(count(&a, o, "OrderPaid").await, 1);
    }
    assert_eq!((w.payments.calls(), w.payments.charges()), (5, 5));
}

pub async fn a_runner_that_dies_loses_its_process_to_another<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, flaky) = w.flaky("a");
    let (_, p) = checkout(&a, 100).await;
    flaky.crash_on(|u| appends(u, "StepRequested"));
    assert!(a.tick().await.is_err());
    let bb = w.engine("b");
    bb.settle().await.unwrap();
    assert!(
        bb.process(p).await.unwrap().pending.is_none(),
        "nobody advanced it while a's lease held"
    );
    w.advance(Options::default().advance_lease_ms);
    bb.settle().await.unwrap();
    assert!(matches!(
        finished(&bb, p).await,
        Some(Finished::Completed(_))
    ));
}

pub async fn starting_a_process_again<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (o, p) = checkout(&e, 100).await;
    let input = json!({ "order": o, "total": 100 });
    e.start("checkout", p, input.clone(), ann()).await.unwrap();
    assert_eq!(
        refused(
            e.start("checkout", p, json!({ "order": o, "total": 1 }), ann())
                .await
        ),
        "key_reused"
    );
    e.settle().await.unwrap();
    assert_eq!(refused(e.start("checkout", p, input, ann()).await), "ended");
    assert_eq!(w.payments.calls(), 1);
}

pub async fn a_process_starts_once_on_the_event_it_starts_on<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, bb) = (w.engine("a"), w.engine("b"));
    let (_, p) = fulfilling(&a).await;
    bb.settle().await.unwrap();
    a.settle().await.unwrap();
    let view = a.process(p).await.unwrap();
    assert_eq!(view.name, "fulfil");
    assert_eq!(view.input["type"], "OrderPaid");
    assert!(a.claim_task("pack", "w1", 10_000).await.unwrap().is_some());
    assert!(
        a.claim_task("pack", "w2", 10_000).await.unwrap().is_none(),
        "one start, one task"
    );
}

// ---- tasks ----

pub async fn a_worker_claims_heartbeats_and_finishes_a_task<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (o, p) = fulfilling(&e).await;
    let task = e.claim_task("pack", "w1", 10_000).await.unwrap().unwrap();
    assert_eq!((task.input["order"].clone(), task.attempt), (json!(o), 1));
    w.advance(9_000);
    e.heartbeat(&task, 10_000).await.unwrap();
    w.advance(9_000);
    e.finish_task(&task, Ok(json!({ "box": 1 })), "w1")
        .await
        .unwrap();
    e.settle().await.unwrap();
    assert_eq!(
        finished(&e, p).await,
        Some(Finished::Completed(json!({ "box": 1 })))
    );
}

pub async fn a_failed_attempt_is_tried_again<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (_, p) = fulfilling(&e).await;
    let first = e.claim_task("pack", "w1", 10_000).await.unwrap().unwrap();
    e.finish_task(&first, Err("jammed".into()), "w1")
        .await
        .unwrap();
    assert!(
        e.claim_task("pack", "w2", 10_000).await.unwrap().is_none(),
        "it waits out its backoff"
    );
    w.advance(1_000);
    let second = e.claim_task("pack", "w2", 10_000).await.unwrap().unwrap();
    assert_eq!(second.attempt, 2);
    e.finish_task(&second, Err("jammed again".into()), "w2")
        .await
        .unwrap();
    e.settle().await.unwrap();
    assert_eq!(
        e.process(p).await.unwrap().progress.outcome("pack"),
        Some(&Outcome::Failed("jammed again".into()))
    );
}

pub async fn a_result_whose_lease_ran_out_meanwhile_is_refused<B: Backend>(b: &B) {
    let w = World::new(b);
    let (e, flaky) = w.flaky("a");
    let (_, p) = fulfilling(&e).await;
    let late = e.claim_task("pack", "w1", 10_000).await.unwrap().unwrap();
    // While w1 reports, its lease runs out and w2 takes the task.
    let (other, clock) = (w.engine("b"), w.clock.clone());
    flaky.race_on(
        move |u| records_step(u, "pack"),
        Box::pin(async move {
            clock.advance(10_000);
            other
                .claim_task("pack", "w2", 10_000)
                .await
                .unwrap()
                .unwrap();
        }),
    );
    assert_eq!(
        refused(e.finish_task(&late, Ok(json!("w1's")), "w1").await),
        "lease_lost"
    );
    let taken = e.claim_task("pack", "w3", 10_000).await.unwrap();
    assert!(taken.is_none(), "w2 holds it");
    assert_eq!(finished(&e, p).await, None);
}

pub async fn a_wake_while_a_runner_holds_the_process_is_not_lost<B: Backend>(b: &B) {
    let w = World::new(b);
    let (a, flaky) = w.flaky("a");
    let p = id();
    a.start("label", p, json!({ "order": id() }), ann())
        .await
        .unwrap();
    a.tick().await.unwrap();
    assert_eq!(
        a.process(p).await.unwrap().pending.map(|(k, _)| k),
        Some("pack".into())
    );
    // a holds the process again and finds it waiting; just before it lets
    // go, a worker finishes the task it waits for.
    let worker = w.engine("worker");
    flaky.race_on(
        |u| {
            u.appends.iter().all(|a| a.events.is_empty())
                && u.work
                    .iter()
                    .any(|c| matches!(c, factum_core::WorkChange::Release { .. }))
        },
        Box::pin(async move {
            let task = worker
                .claim_task("pack", "w", 10_000)
                .await
                .unwrap()
                .unwrap();
            worker
                .finish_task(&task, Ok(json!("packed")), "w")
                .await
                .unwrap();
        }),
    );
    a.settle().await.unwrap();
    assert_eq!(
        finished(&a, p).await,
        Some(Finished::Completed(json!("packed")))
    );
}

pub async fn a_task_whose_lease_ran_out_is_taken_over<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (_, p) = fulfilling(&e).await;
    let first = e.claim_task("pack", "w1", 10_000).await.unwrap().unwrap();
    w.advance(10_000);
    assert_eq!(
        refused(e.heartbeat(&first, 10_000).await),
        "lease_lost",
        "a lease that ran out is not renewed, taken over or not"
    );
    let second = e.claim_task("pack", "w2", 10_000).await.unwrap().unwrap();
    assert_eq!(second.attempt, 2);
    assert_eq!(refused(e.heartbeat(&first, 10_000).await), "lease_lost");
    assert_eq!(
        refused(e.finish_task(&first, Ok(json!("late")), "w1").await),
        "lease_lost"
    );
    e.finish_task(&second, Ok(json!("packed")), "w2")
        .await
        .unwrap();
    e.settle().await.unwrap();
    assert_eq!(
        finished(&e, p).await,
        Some(Finished::Completed(json!("packed")))
    );
}

pub async fn an_at_most_once_task_whose_lease_ran_out_ends_unknown<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let p = id();
    e.start("print", p, json!({ "order": id() }), ann())
        .await
        .unwrap();
    e.settle().await.unwrap();
    e.claim_task("print", "w1", 5_000).await.unwrap().unwrap();
    w.advance(5_000);
    assert!(e.claim_task("print", "w2", 5_000).await.unwrap().is_none());
    e.settle().await.unwrap();
    let view = e.process(p).await.unwrap();
    assert_eq!(view.progress.outcome("print"), Some(&Outcome::Unknown));
    assert_eq!(view.finished, Some(Finished::Stuck("unknown".into())));
}

pub async fn a_heartbeat_never_holds_a_task_past_its_timeout<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    fulfilling(&e).await;
    let task = e.claim_task("pack", "w1", 50_000).await.unwrap().unwrap();
    w.advance(40_000);
    e.heartbeat(&task, 50_000).await.unwrap();
    w.advance(19_999);
    assert!(e.claim_task("pack", "w2", 10_000).await.unwrap().is_none());
    w.advance(1);
    assert!(
        e.claim_task("pack", "w2", 10_000).await.unwrap().is_some(),
        "the type's timeout is a minute"
    );
    assert_eq!(refused(e.heartbeat(&task, 50_000).await), "lease_lost");
}

pub async fn tasks_with_one_ordering_key_run_one_at_a_time_paced<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (o1, o2, o3) = (id(), id(), id());
    for (o, store) in [(o1, "tokyo"), (o2, "tokyo"), (o3, "osaka")] {
        e.start(
            "delivery",
            id(),
            json!({ "order": o, "store": store }),
            ann(),
        )
        .await
        .unwrap();
        e.settle().await.unwrap();
    }
    let first = e.claim_task("deliver", "w", 10_000).await.unwrap().unwrap();
    let other = e.claim_task("deliver", "w", 10_000).await.unwrap().unwrap();
    assert_eq!(
        (first.input["order"].clone(), other.input["order"].clone()),
        (json!(o1), json!(o3))
    );
    assert!(
        e.claim_task("deliver", "w", 10_000)
            .await
            .unwrap()
            .is_none(),
        "tokyo's second waits for its first"
    );
    e.finish_task(&first, Ok(json!("delivered")), "w")
        .await
        .unwrap();
    assert!(
        e.claim_task("deliver", "w", 10_000)
            .await
            .unwrap()
            .is_none(),
        "tokyo is paced"
    );
    w.advance(10_000);
    let second = e.claim_task("deliver", "w", 10_000).await.unwrap().unwrap();
    assert_eq!(second.input["order"], json!(o2));
}

pub async fn a_crash_between_a_tasks_answer_and_its_pace_loses_no_pace<B: Backend>(b: &B) {
    let w = World::new(b);
    let (e, flaky) = w.flaky("a");
    for o in [id(), id()] {
        e.start(
            "delivery",
            id(),
            json!({ "order": o, "store": "tokyo" }),
            ann(),
        )
        .await
        .unwrap();
        e.settle().await.unwrap();
    }
    let first = e.claim_task("deliver", "w", 10_000).await.unwrap().unwrap();
    flaky.crash_on(|u| u.work.iter().any(|c| matches!(c, WorkChange::Pace { .. })));
    assert!(
        e.finish_task(&first, Ok(json!("delivered")), "w")
            .await
            .is_err()
    );
    assert!(
        e.claim_task("deliver", "w", 10_000)
            .await
            .unwrap()
            .is_none(),
        "tokyo's second does not run unpaced"
    );
    e.finish_task(&first, Ok(json!("delivered")), "w")
        .await
        .unwrap();
    w.advance(9_999);
    assert!(
        e.claim_task("deliver", "w", 10_000)
            .await
            .unwrap()
            .is_none(),
        "tokyo is paced"
    );
    w.advance(1);
    assert!(
        e.claim_task("deliver", "w", 10_000)
            .await
            .unwrap()
            .is_some()
    );
}

pub async fn a_cancelled_task_ends_its_step_cancelled<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (_, p) = fulfilling(&e).await;
    e.cancel_task(step_key(p, "pack"), ann()).await.unwrap();
    e.settle().await.unwrap();
    assert_eq!(
        e.process(p).await.unwrap().progress.outcome("pack"),
        Some(&Outcome::Cancelled)
    );
    assert_eq!(
        finished(&e, p).await,
        Some(Finished::Failed("cancelled".into()))
    );
    assert!(e.claim_task("pack", "w", 10_000).await.unwrap().is_none());
}

// ---- waits ----

pub async fn a_wait_ends_when_its_event_arrives<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = place(&e, 5).await;
    pay(&e, o).await;
    let p = id();
    e.start("pickup", p, json!({ "order": o }), ann())
        .await
        .unwrap();
    e.settle().await.unwrap();
    assert_eq!(
        e.process(p).await.unwrap().pending.map(|(k, _)| k),
        Some("closed".into())
    );
    let closed = e
        .submit(sub("order.close", json!({ "order": o })))
        .await
        .unwrap();
    e.settle().await.unwrap();
    assert_eq!(
        finished(&e, p).await,
        Some(Finished::Completed(json!(closed.events[0].id)))
    );
}

pub async fn a_wait_for_an_event_already_recorded_ends_at_once<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = place(&e, 5).await;
    pay(&e, o).await;
    e.submit(sub("order.close", json!({ "order": o })))
        .await
        .unwrap();
    let p = id();
    e.start("pickup", p, json!({ "order": o }), ann())
        .await
        .unwrap();
    e.settle().await.unwrap();
    assert!(matches!(
        finished(&e, p).await,
        Some(Finished::Completed(_))
    ));
}

pub async fn a_wait_times_out<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = place(&e, 5).await;
    let p = id();
    e.start("pickup", p, json!({ "order": o }), ann())
        .await
        .unwrap();
    e.settle().await.unwrap();
    w.advance(PICKUP_MS - 1);
    e.settle().await.unwrap();
    assert_eq!(finished(&e, p).await, None);
    w.advance(1);
    e.settle().await.unwrap();
    assert_eq!(
        e.process(p).await.unwrap().progress.outcome("closed"),
        Some(&Outcome::TimedOut)
    );
    assert_eq!(
        finished(&e, p).await,
        Some(Finished::Failed("not picked up".into()))
    );
}

// ---- work rows as stores keep them ----

/// Puts `row` in place of the row of its kind and id, as it was written.
async fn rewrite<S: Store>(store: &S, row: Work) {
    let unit = Unit {
        work: vec![
            WorkChange::Done {
                kind: row.kind,
                id: row.id,
            },
            WorkChange::Put(row),
        ],
        ..Unit::default()
    };
    store.commit(unit).await.unwrap();
}

/// A checkout whose charge is pending, its row's payload set to `payload`.
async fn charge_stored_as<B: Backend>(w: &World<'_, B>, payload: Value) -> (Engine<B::S>, Uuid) {
    let e = w.engine("a");
    w.payments.busy.store(1, Ordering::SeqCst);
    let (_, p) = checkout(&e, 500).await;
    e.settle().await.unwrap();
    let mut row = w
        .store
        .work(WorkKind::Action, step_key(p, "charge"))
        .await
        .unwrap()
        .expect("the charge is pending");
    row.payload = payload;
    rewrite(&*w.store, Work { lease: None, ..row }).await;
    w.advance(60_000);
    (e, p)
}

/// A pickup whose wait is pending, its row changed by `change`.
async fn wait_stored_as<B: Backend>(
    w: &World<'_, B>,
    change: impl FnOnce(&mut Work),
) -> (Engine<B::S>, Uuid, Uuid) {
    let e = w.engine("a");
    let o = place(&e, 5).await;
    pay(&e, o).await;
    let p = id();
    e.start("pickup", p, json!({ "order": o }), ann())
        .await
        .unwrap();
    e.settle().await.unwrap();
    let mut row = w
        .store
        .work(WorkKind::Await, step_key(p, "closed"))
        .await
        .unwrap()
        .expect("the wait is pending");
    change(&mut row);
    rewrite(&*w.store, Work { lease: None, ..row }).await;
    e.submit(sub("order.close", json!({ "order": o })))
        .await
        .unwrap();
    (e, p, o)
}

pub async fn work_rows_in_the_stored_shape_still_run<B: Backend>(b: &B) {
    let w = World::new(b);
    let (e, p) = charge_stored_as(
        &w,
        json!({ "params": { "amount": 500 }, "at_most_once": false }),
    )
    .await;
    e.settle().await.unwrap();
    assert!(matches!(
        finished(&e, p).await,
        Some(Finished::Completed(_))
    ));
    assert_eq!(w.payments.charges(), 1);

    let w = World::new(b);
    let (e, p, _) = wait_stored_as(&w, |row| row.payload = json!({ "event": "OrderClosed" })).await;
    e.settle().await.unwrap();
    assert!(matches!(
        finished(&e, p).await,
        Some(Finished::Completed(_))
    ));
}

fn fault<T: std::fmt::Debug>(result: Result<T, factum::Error>) {
    assert!(
        matches!(result, Err(factum::Error::Fault(_))),
        "expected a fault, got {result:?}"
    );
}

pub async fn a_work_row_that_does_not_decode_is_a_fault<B: Backend>(b: &B) {
    for payload in [
        json!({ "params": { "amount": 500 } }),
        json!({ "params": { "amount": 500 }, "at_most_once": "no" }),
        json!({ "at_most_once": false }),
    ] {
        let w = World::new(b);
        let (e, _) = charge_stored_as(&w, payload).await;
        fault(e.settle().await);
    }
    let w = World::new(b);
    let (e, _, _) = wait_stored_as(&w, |row| row.payload = json!({ "kind": "OrderClosed" })).await;
    fault(e.settle().await);
    let w = World::new(b);
    let (e, _, _) = wait_stored_as(&w, |row| row.process = None).await;
    fault(e.settle().await);
}

// ---- faults in one item ----

/// A model `build` declares, with the shop's task types.
fn with_tasks(build: impl FnOnce(factum::Model) -> factum::Model) -> factum::Model {
    crate::domain::task_types()
        .into_iter()
        .fold(build(factum::Model::new()), |m, t| m.task(t))
}

fn pending(view: factum::ProcessView) -> Option<String> {
    view.pending.map(|(key, _)| key)
}

pub async fn an_event_that_cannot_be_dispatched_stops_nothing_else<B: Backend>(b: &B) {
    let w = World::new(b);
    // A rolling deploy: the new code records orders in a shape the old
    // runner's upcast refuses.
    let new = w.engine("new");
    let old = w.engine_with(
        with_tasks(|m| {
            m.command::<crate::domain::old::OldPlace>()
                .command::<crate::domain::AddCounter>()
                .process::<crate::domain::FulfilProcess>()
                .process::<crate::domain::LabelProcess>()
        }),
        "old",
    );
    let newer = place(&new, 1).await;
    let o = id();
    old.submit(sub("order.place", json!({ "order": o, "total": 2 })))
        .await
        .unwrap();
    pay(&new, o).await;
    let label = id();
    old.start("label", label, json!({ "order": o }), ann())
        .await
        .unwrap();

    let tick = old.tick().await.unwrap();
    let bad = new.history(&order(newer)).await.unwrap()[0].id;
    assert_eq!(
        tick.faults
            .iter()
            .map(|f| f.item.clone())
            .collect::<Vec<_>>(),
        vec![format!("event {bad}")]
    );
    let history = new.history(&order(o)).await.unwrap();
    let paid = history.iter().find(|e| e.kind == "OrderPaid").unwrap().id;
    let fulfil = old.process(start_key(paid, "fulfil")).await.unwrap();
    assert_eq!(pending(fulfil).as_deref(), Some("pack"), "a later event");
    assert_eq!(
        pending(old.process(label).await.unwrap()).as_deref(),
        Some("pack"),
        "a process advanced in the same tick"
    );

    let dead = old.dead_letters(factum::DISPATCH).await.unwrap();
    assert_eq!(dead.iter().map(|d| d.event).collect::<Vec<_>>(), vec![bad]);
    assert_eq!(old.tick().await.unwrap().faults, Vec::new(), "parked");
    assert!(
        !old.retry_dead_letter(factum::DISPATCH, bad).await.unwrap(),
        "the old code fails again"
    );
    assert!(new.retry_dead_letter(factum::DISPATCH, bad).await.unwrap());
    assert!(new.dead_letters(factum::DISPATCH).await.unwrap().is_empty());
}

pub async fn a_process_that_faults_stops_no_other<B: Backend>(b: &B) {
    let w = World::new(b);
    let buggy = w.engine_with(
        with_tasks(|m| {
            m.command::<crate::domain::unreadable::AddUnreadable>()
                .process::<crate::domain::LabelProcess>()
                .process::<crate::domain::PrintProcess>()
        }),
        "buggy",
    );
    let (label, print) = (id(), id());
    buggy
        .start("label", label, json!({ "order": id() }), ann())
        .await
        .unwrap();
    w.advance(1);
    buggy
        .start("print", print, json!({ "order": id() }), ann())
        .await
        .unwrap();

    let tick = buggy.tick().await.unwrap();
    assert_eq!(
        tick.faults
            .iter()
            .map(|f| f.item.clone())
            .collect::<Vec<_>>(),
        vec![format!("process/{label}")]
    );
    assert_eq!(
        pending(buggy.process(print).await.unwrap()).as_deref(),
        Some("print")
    );
    assert_eq!(pending(buggy.process(label).await.unwrap()), None);

    let fixed = w.engine("fixed");
    w.advance(Options::default().advance_lease_ms);
    fixed.settle().await.unwrap();
    assert_eq!(
        pending(fixed.process(label).await.unwrap()).as_deref(),
        Some("pack"),
        "tried again once its lease ran out"
    );
}

// ---- projections ----

pub async fn projection_rows_follow_the_log_and_rebuild_the_same<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let (o1, o2) = (place(&e, 500).await, place(&e, 700).await);
    pay(&e, o1).await;
    e.settle().await.unwrap();
    let rows = e.rows("orders", "orders").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        e.row("orders", "orders", &o1.to_string()).await.unwrap(),
        Some(json!({ "status": "paid", "total": 500, "currency": "JPY" }))
    );
    assert_eq!(
        e.row("orders", "orders", &o2.to_string())
            .await
            .unwrap()
            .unwrap()["status"],
        "placed"
    );

    let rebuilt = w.engine_with(
        w.shop(OrderList {
            version: 2,
            ..OrderList::default()
        }),
        "b",
    );
    rebuilt.settle().await.unwrap();
    let mut again = rebuilt.rows("orders", "orders").await.unwrap();
    let mut before = rows;
    again.sort_by(|x, y| x.0.cmp(&y.0));
    before.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(again, before);
}

pub async fn a_projection_skips_an_event_it_fails_on<B: Backend>(b: &B) {
    let w = World::with_orders(
        b,
        OrderList {
            fails_on: Some(13),
            ..OrderList::default()
        },
    );
    let e = w.engine("a");
    let bad = place(&e, 13).await;
    let good = place(&e, 14).await;
    e.settle().await.unwrap();
    assert_eq!(e.rows("orders", "orders").await.unwrap().len(), 1);
    assert!(
        e.row("orders", "orders", &good.to_string())
            .await
            .unwrap()
            .is_some()
    );
    let dead = e.dead_letters("orders").await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].event, e.history(&order(bad)).await.unwrap()[0].id);
}

pub async fn two_projectors_apply_each_event_once<B: Backend>(b: &B) {
    let w = World::with_orders(
        b,
        OrderList {
            fails_on: Some(13),
            ..OrderList::default()
        },
    );
    let (a, flaky) = w.flaky("a");
    place(&a, 13).await;
    place(&a, 14).await;
    let other = w.engine("b");
    flaky.race_on_batch(
        "orders",
        Box::pin(async move {
            other.settle().await.unwrap();
        }),
    );
    a.settle().await.unwrap();
    assert_eq!(a.rows("orders", "orders").await.unwrap().len(), 1);
    assert_eq!(
        a.dead_letters("orders").await.unwrap().len(),
        1,
        "the batch read before b caught up applies nothing"
    );
}

pub async fn a_projection_that_stops_holds_until_rebuilt<B: Backend>(b: &B) {
    let stops = OrderList {
        fails_on: Some(13),
        on_failure: factum::OnFailure::Stop,
        ..OrderList::default()
    };
    let w = World::with_orders(b, stops);
    let e = w.engine("a");
    let first = place(&e, 1).await;
    place(&e, 13).await;
    place(&e, 14).await;
    e.settle().await.unwrap();
    let rows = e.rows("orders", "orders").await.unwrap();
    assert_eq!(
        rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
        [first.to_string()],
        "nothing after the dead letter"
    );
    assert_eq!(e.dead_letters("orders").await.unwrap().len(), 1);

    let fixed = w.engine_with(
        w.shop(OrderList {
            version: 2,
            ..OrderList::default()
        }),
        "b",
    );
    fixed.settle().await.unwrap();
    assert_eq!(fixed.rows("orders", "orders").await.unwrap().len(), 3);
    assert!(fixed.dead_letters("orders").await.unwrap().is_empty());
}

pub async fn a_read_never_serves_rows_another_version_built<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o = place(&e, 500).await;
    e.settle().await.unwrap();
    assert_eq!(e.rows("orders", "orders").await.unwrap().len(), 1);

    let v2 = w.engine_with(
        w.shop(OrderList {
            version: 2,
            ..OrderList::default()
        }),
        "b",
    );
    assert!(v2.rows("orders", "orders").await.unwrap().is_empty());
    assert_eq!(
        v2.row("orders", "orders", &o.to_string()).await.unwrap(),
        None
    );
    assert!(
        v2.query("orders", &Query::table("orders"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        e.rows("orders", "orders").await.unwrap().len(),
        1,
        "a read writes nothing: the old version's rows stand until a rebuild"
    );
    v2.settle().await.unwrap();
    assert_eq!(v2.rows("orders", "orders").await.unwrap().len(), 1);
    assert!(e.rows("orders", "orders").await.unwrap().is_empty());
}

// ---- reading old events and snapshots ----

pub async fn old_events_are_read_in_their_current_shape<B: Backend>(b: &B) {
    let w = World::new(b);
    let old = w.engine_with(
        factum::Model::new().command::<crate::domain::old::OldPlace>(),
        "old",
    );
    let o = id();
    let recorded = old
        .submit(sub("order.place", json!({ "order": o, "total": 9 })))
        .await
        .unwrap();
    assert_eq!(
        (
            recorded.events[0].type_version,
            recorded.events[0].data.clone()
        ),
        (1, json!({ "total": 9 }))
    );

    let e = w.engine("new");
    assert_eq!(e.state(&order(o)).await.unwrap()["currency"], "JPY");
    let history = e.history(&order(o)).await.unwrap();
    assert_eq!(
        (history[0].type_version, history[0].data["currency"].clone()),
        (2, json!("JPY"))
    );
    e.settle().await.unwrap();
    assert_eq!(
        e.row("orders", "orders", &o.to_string())
            .await
            .unwrap()
            .unwrap()["currency"],
        "JPY"
    );
}

pub async fn snapshots_change_no_answer<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine_with_options(
        Options {
            snapshot_every: 10,
            ..Options::default()
        },
        "a",
    );
    let c = id();
    let stream = StreamId::new("counter", c);
    for n in 1..=250 {
        e.submit(sub("counter.add", json!({ "counter": c, "n": n })))
            .await
            .unwrap();
    }
    let total = 250 * 251 / 2;
    assert_eq!(e.state(&stream).await.unwrap()["total"], total);
    let kept = e
        .store()
        .snapshot(&stream)
        .await
        .unwrap()
        .expect("a snapshot was kept");
    assert!(kept.version.0 > 200, "kept at {:?}", kept.version);

    let rebuilt = w.engine_with(
        factum::Model::new().command::<crate::domain::AddTo<crate::domain::CountersRebuilt>>(),
        "rebuilt",
    );
    assert_eq!(
        rebuilt.state(&stream).await.unwrap()["total"],
        2 * total,
        "other code folds from the first event, not from the snapshot"
    );
    e.submit(sub("counter.add", json!({ "counter": c, "n": 1 })))
        .await
        .unwrap();
    assert_eq!(
        rebuilt.state(&stream).await.unwrap()["total"],
        2 * (total + 1)
    );
    assert_eq!(e.state(&stream).await.unwrap()["total"], total + 1);
}

// ---- small conveniences ----

trait Now {
    fn now_(&self) -> factum::Instant;
}

impl Now for factum::ManualClock {
    fn now_(&self) -> factum::Instant {
        factum::Clock::now(self)
    }
}

trait Again<B: Backend> {
    fn engine_again(&self, w: &World<'_, B>) -> Engine<B::S>;
}

impl<B: Backend, S: Store> Again<B> for Engine<S> {
    fn engine_again(&self, w: &World<'_, B>) -> Engine<B::S> {
        w.engine("again")
    }
}

// ---- queries ----

pub async fn queries_filter_and_order_by_declared_columns<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    let o1 = place(&e, 500).await;
    let o2 = place(&e, 700).await;
    let o3 = place(&e, 300).await;
    let o4 = place(&e, 900).await;
    pay(&e, o2).await;
    e.settle().await.unwrap();
    let keys = |rows: Vec<(String, Value)>| rows.into_iter().map(|r| r.0).collect::<Vec<_>>();
    let s = |o: Uuid| o.to_string();

    let placed = Query::table("orders")
        .filter("status", Op::Eq, "placed")
        .order_by("total", true);
    assert_eq!(
        keys(e.query("orders", &placed).await.unwrap()),
        [s(o4), s(o1), s(o3)]
    );
    assert_eq!(
        keys(e.query("orders", &placed.clone().limit(2)).await.unwrap()),
        [s(o4), s(o1)]
    );
    let big = Query::table("orders")
        .filter("total", Op::Ge, 500)
        .order_by("total", false);
    assert_eq!(
        keys(e.query("orders", &big).await.unwrap()),
        [s(o1), s(o2), s(o4)]
    );
    let between = Query::table("orders")
        .filter("total", Op::Gt, 300)
        .filter("total", Op::Lt, 900);
    assert_eq!(keys(e.query("orders", &between).await.unwrap()).len(), 2);
    // Numbers sort before text, null before both, as in SQLite.
    let below_text = Query::table("orders").filter("total", Op::Lt, "a");
    assert_eq!(e.query("orders", &below_text).await.unwrap().len(), 4);
    let null = Query::table("orders").filter("total", Op::Ne, Value::Null);
    assert!(
        e.query("orders", &null).await.unwrap().is_empty(),
        "a comparison with null never holds"
    );

    let undeclared = Query::table("orders").filter("customer", Op::Eq, "ann");
    assert_eq!(
        refused(e.query("orders", &undeclared).await),
        "invalid_query"
    );
    assert_eq!(
        refused(e.query("orders", &Query::table("strays")).await),
        "invalid_query"
    );
}

pub async fn a_change_to_a_projections_tables_rebuilds_it<B: Backend>(b: &B) {
    let w = World::with_orders(
        b,
        OrderList {
            fails_on: Some(13),
            ..OrderList::default()
        },
    );
    let e = w.engine("a");
    let bad = place(&e, 13).await;
    e.settle().await.unwrap();
    assert!(
        e.row("orders", "orders", &bad.to_string())
            .await
            .unwrap()
            .is_none()
    );

    let wider = w.engine_with(
        w.shop(OrderList {
            with_customer: true,
            ..OrderList::default()
        }),
        "b",
    );
    wider.settle().await.unwrap();
    assert!(
        wider
            .row("orders", "orders", &bad.to_string())
            .await
            .unwrap()
            .is_some(),
        "same version, other tables: rebuilt"
    );
    assert!(wider.dead_letters("orders").await.unwrap().is_empty());
}

pub async fn a_projection_that_writes_an_undeclared_table_fails_the_event<B: Backend>(b: &B) {
    let w = World::with_orders(
        b,
        OrderList {
            strays_on: Some(7),
            ..OrderList::default()
        },
    );
    let e = w.engine("a");
    place(&e, 7).await;
    place(&e, 8).await;
    e.settle().await.unwrap();
    let dead = e.dead_letters("orders").await.unwrap();
    assert_eq!(dead.len(), 1);
    assert!(
        dead[0].error.contains("does not declare"),
        "{}",
        dead[0].error
    );
    assert_eq!(e.rows("orders", "orders").await.unwrap().len(), 1);
    assert_eq!(refused(e.rows("orders", "strays").await), "invalid_query");
}

pub async fn every_store_refuses_rows_of_an_undeclared_table<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    place(&e, 5).await;
    e.settle().await.unwrap();
    let at = e.store().checkpoint("orders").await.unwrap().unwrap();
    let put = |table: &str| factum::RowOp::Put {
        table: table.into(),
        key: "k".into(),
        row: json!({ "total": 1 }),
    };
    let batch = Batch {
        projection: "orders".into(),
        version: at.version,
        from: at.position,
        to: at.position,
        ops: vec![put("orders"), put("strays")],
        dead: Vec::new(),
        resolved: Vec::new(),
        stop: false,
    };
    assert_eq!(
        e.store().apply_projection(batch).await,
        Err(StoreError::Undeclared {
            projection: "orders".into(),
            table: "strays".into()
        })
    );
    assert_eq!(
        e.row("orders", "orders", "k").await.unwrap(),
        None,
        "all or none"
    );
    assert_eq!(
        e.store().rows("orders", "strays").await.unwrap(),
        Vec::new()
    );
}

// ---- dead letters retried ----

pub async fn a_dead_letter_retried_with_fixed_code_is_projected<B: Backend>(b: &B) {
    let w = World::with_orders(
        b,
        OrderList {
            fails_on: Some(13),
            ..OrderList::default()
        },
    );
    let e = w.engine("a");
    let bad = place(&e, 13).await;
    place(&e, 14).await;
    e.settle().await.unwrap();
    let dead = e.dead_letters("orders").await.unwrap()[0].event;
    assert!(
        !e.retry_dead_letter("orders", dead).await.unwrap(),
        "the same code fails again"
    );
    assert_eq!(e.dead_letters("orders").await.unwrap().len(), 1);

    let fixed = w.engine_with(w.shop(OrderList::default()), "fixed");
    assert!(fixed.retry_dead_letter("orders", dead).await.unwrap());
    assert!(fixed.dead_letters("orders").await.unwrap().is_empty());
    assert_eq!(
        fixed
            .row("orders", "orders", &bad.to_string())
            .await
            .unwrap()
            .unwrap()["total"],
        13
    );
    assert_eq!(
        refused(fixed.retry_dead_letter("orders", dead).await),
        "unknown"
    );
    fixed.settle().await.unwrap();
    assert_eq!(
        fixed.rows("orders", "orders").await.unwrap().len(),
        2,
        "not rebuilt: same version, same tables"
    );
}

pub async fn a_stopped_projection_goes_on_once_its_dead_letter_is_retried<B: Backend>(b: &B) {
    let stops = |fails_on| OrderList {
        fails_on,
        on_failure: factum::OnFailure::Stop,
        ..OrderList::default()
    };
    let w = World::with_orders(b, stops(Some(13)));
    let e = w.engine("a");
    place(&e, 1).await;
    place(&e, 13).await;
    place(&e, 14).await;
    e.settle().await.unwrap();
    assert_eq!(e.rows("orders", "orders").await.unwrap().len(), 1);

    let fixed = w.engine_with(w.shop(stops(None)), "fixed");
    let dead = fixed.dead_letters("orders").await.unwrap()[0].event;
    assert!(fixed.retry_dead_letter("orders", dead).await.unwrap());
    fixed.settle().await.unwrap();
    assert_eq!(fixed.rows("orders", "orders").await.unwrap().len(), 3);
    place(&fixed, 15).await;
    fixed.settle().await.unwrap();
    assert_eq!(fixed.rows("orders", "orders").await.unwrap().len(), 4);
}

pub async fn a_dead_letter_another_version_left_is_not_retried<B: Backend>(b: &B) {
    let w = World::with_orders(
        b,
        OrderList {
            fails_on: Some(13),
            ..OrderList::default()
        },
    );
    let e = w.engine("a");
    let bad = place(&e, 13).await;
    e.settle().await.unwrap();
    let dead = e.dead_letters("orders").await.unwrap()[0].event;

    let v2 = w.engine_with(
        w.shop(OrderList {
            version: 2,
            ..OrderList::default()
        }),
        "b",
    );
    assert!(v2.dead_letters("orders").await.unwrap().is_empty());
    assert_eq!(
        refused(v2.retry_dead_letter("orders", dead).await),
        "unknown"
    );
    assert_eq!(
        e.dead_letters("orders").await.unwrap().len(),
        1,
        "nothing written to the old version"
    );
    assert_eq!(
        e.row("orders", "orders", &bad.to_string()).await.unwrap(),
        None
    );
    v2.settle().await.unwrap();
    assert!(
        v2.row("orders", "orders", &bad.to_string())
            .await
            .unwrap()
            .is_some(),
        "the rebuild projects it with the code running now"
    );
}

// ---- process samples ----

pub async fn a_sample_holds_a_new_next_to_old_progress<B: Backend>(b: &B) {
    let w = World::new(b);
    let e = w.engine("a");
    w.payments.busy.store(1, Ordering::SeqCst);
    let (_, p) = checkout(&e, 100).await;
    e.settle().await.unwrap();
    let sample = e
        .sample(p, "charging, the first call answered busy")
        .await
        .unwrap();
    let kept: factum::Sample =
        serde_json::from_str(&serde_json::to_string(&sample).unwrap()).unwrap();
    assert_eq!(kept, sample);

    assert!(
        w.shop(OrderList::default())
            .check_samples(std::slice::from_ref(&kept))
            .is_empty()
    );
    let renamed = factum::Model::new().process::<crate::domain::CheckoutRenamed>();
    let mismatches = renamed.check_samples(std::slice::from_ref(&kept));
    assert_eq!(mismatches.len(), 1);
    assert!(
        matches!(&mismatches[0].now, Some(factum::Next::Step { key, .. }) if key == "charge-card")
    );
    let gone = factum::Model::new().check_samples(&[kept]);
    assert_eq!(gone[0].now, None);
}

// ---- runners in parallel ----

pub async fn runners_in_parallel_do_each_thing_once<B: Backend>(b: &B) {
    let w = World::new(b);
    let first = w.engine("starter");
    let mut runs = Vec::new();
    for _ in 0..20 {
        runs.push(checkout(&first, 100).await);
    }
    let mut handles = Vec::new();
    for n in 0..4 {
        let e = std::sync::Arc::new(w.engine(&format!("r{n}")));
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                e.tick().await.unwrap();
                tokio::task::yield_now().await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    first.settle().await.unwrap();
    for (o, p) in runs {
        assert!(
            matches!(finished(&first, p).await, Some(Finished::Completed(_))),
            "{p}"
        );
        assert_eq!(count(&first, o, "OrderPaid").await, 1);
    }
    assert_eq!((w.payments.calls(), w.payments.charges()), (20, 20));
}

pub async fn writers_in_parallel_keep_versions_unique<B: Backend>(b: &B) {
    let w = World::new(b);
    let c = id();
    let mut handles = Vec::new();
    for n in 0..4 {
        let e = w.engine(&format!("w{n}"));
        handles.push(tokio::spawn(async move {
            for _ in 0..25 {
                e.submit(sub("counter.add", json!({ "counter": c, "n": 1 })))
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let e = w.engine("reader");
    let history = e.history(&StreamId::new("counter", c)).await.unwrap();
    assert_eq!(
        history.iter().map(|ev| ev.version.0).collect::<Vec<_>>(),
        (1..=100).collect::<Vec<_>>()
    );
    assert_eq!(
        e.state(&StreamId::new("counter", c)).await.unwrap()["total"],
        100
    );
}
