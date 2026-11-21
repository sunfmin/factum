//! What a program declares: aggregates, commands, processes, actions, task
//! types, recurring commands and projections. Each comes typed, for the
//! program to write, and erased (`Dyn*`), for the engine to hold by name.

use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::event::{By, Event, Instant, NewEvent, Rejection, codes};
use crate::process::{Finished, Next, Outcome, Progress};
use crate::work::{SCHEDULED, Work};

/// Rewrites one recorded shape of an event's data into the next.
pub type Upcast = fn(Value) -> Value;

/// A thing the program tracks, folded from its stream.
pub trait Aggregate: Send + Sync + 'static {
    /// The stream prefix: `<NAME>/<uuid>`.
    const NAME: &'static str;
    type State: Default + Serialize + DeserializeOwned;
    /// Its events, as an enum tagged `#[serde(tag = "type", content = "data")]`;
    /// the variant's serde name is the stored type name.
    type Event: Serialize + DeserializeOwned;

    fn evolve(state: &mut Self::State, event: Self::Event);

    /// Whether an event of this type ends the stream.
    fn ending(_kind: &str) -> bool {
        false
    }

    /// For each event type whose shape changed, the rewrites from its first
    /// shape on, in order. A type's current version is one more than its
    /// count of upcasts.
    fn upcasts() -> Vec<(&'static str, Upcast)> {
        Vec::new()
    }

    /// Names the code that folds: a snapshot kept by other code is ignored.
    fn code() -> &'static str {
        "1"
    }
}

/// What a decide asks for: events, and commands to run later.
pub struct Decision<E> {
    pub events: Vec<E>,
    pub scheduled: Vec<Scheduled>,
}

impl<E> Decision<E> {
    pub fn schedule(mut self, at: Instant, command: &str, params: impl Serialize) -> Self {
        let params = serde_json::to_value(params).unwrap_or(Value::Null);
        self.scheduled.push(Scheduled {
            at,
            command: command.to_string(),
            params,
        });
        self
    }
}

impl<E> From<Vec<E>> for Decision<E> {
    fn from(events: Vec<E>) -> Self {
        Decision {
            events,
            scheduled: Vec::new(),
        }
    }
}

/// A command to run once `at` comes.
#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct Scheduled {
    pub at: Instant,
    pub command: String,
    pub params: Value,
}

/// A named request to change one stream.
pub trait Command: Send + Sync + 'static {
    const NAME: &'static str;
    type Aggregate: Aggregate;
    type Params: Serialize + DeserializeOwned;

    /// The id of the stream it changes.
    fn stream(params: &Self::Params) -> Uuid;

    #[allow(clippy::type_complexity)]
    fn decide(
        state: &<Self::Aggregate as Aggregate>::State,
        params: &Self::Params,
        by: &By,
        now: Instant,
    ) -> Result<Decision<<Self::Aggregate as Aggregate>::Event>, Rejection>;
}

/// Where a process starts by itself: on every event of this type recorded
/// on a stream of this aggregate. Its input is that event as
/// [`Recorded`](crate::Recorded).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartsOn {
    pub aggregate: String,
    pub event: String,
}

/// Work across streams, driven by a pure `next`.
pub trait Process: Send + Sync + 'static {
    const NAME: &'static str;
    type Input: Serialize + DeserializeOwned;

    fn starts_on() -> Option<StartsOn> {
        None
    }

    /// What to do next, given the input and every step's outcome so far.
    /// Called again after every outcome, so it must answer the same for the
    /// same arguments.
    fn next(input: &Self::Input, progress: &Progress) -> Next;
}

/// How a failing call or task attempt is tried again. After its last
/// attempt a step ends `Failed` if that attempt answered an error, and
/// `Unknown` if its lease ran out (its runner died, or it outlived its
/// timeout): the work may have been done, and `next` must find out. An
/// answered error is believed, so a system called at least once answers
/// a key it has done with what it answered, not "try later".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// How many attempts in all before the step fails.
    pub attempts: u32,
    pub backoff_ms: i64,
    pub factor: u32,
    pub max_backoff_ms: i64,
    /// Spread each wait by up to this percent, derived from the key, so
    /// what failed together does not come back together.
    pub jitter_percent: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            attempts: 5,
            backoff_ms: 1_000,
            factor: 2,
            max_backoff_ms: 60_000,
            jitter_percent: 0,
        }
    }
}

impl RetryPolicy {
    /// How long to wait after the `failures`-th failure (1 for the first).
    pub fn delay(&self, failures: u32, key: Uuid) -> i64 {
        let mut wait = self.backoff_ms.max(0);
        for _ in 1..failures {
            wait = wait
                .saturating_mul(i64::from(self.factor.max(1)))
                .min(self.max_backoff_ms);
        }
        let wait = wait.min(self.max_backoff_ms.max(0));
        if self.jitter_percent == 0 || wait == 0 {
            return wait;
        }
        let spread = wait * i64::from(self.jitter_percent.min(100)) / 100;
        let seed = Uuid::new_v5(&key, &failures.to_be_bytes()).as_u128();
        let offset = (seed % (2 * spread as u128 + 1)) as i64 - spread;
        (wait + offset).max(0)
    }

    /// On a claim of an action's or a task's row: whether this attempt
    /// runs, or the step ends without it. A row taken over from a lease
    /// that ran out ends `Unknown` when done at most once, and also once
    /// its attempts are spent: the attempt whose lease ran out may have
    /// done the work, so its outcome is not known, only that no answer came.
    pub fn on_claim(&self, row: &Work, at_most_once: bool) -> Attempt {
        let why = if !row.taken_over {
            return Attempt::Run;
        } else if at_most_once {
            "; done at most once, it is not tried again".to_string()
        } else if row.attempts > self.attempts {
            format!(" of {}, the last", self.attempts)
        } else {
            return Attempt::Run;
        };
        let tried = row.attempts.saturating_sub(1);
        Attempt::End(Outcome::Unknown(format!(
            "its lease ran out on attempt {tried}{why}"
        )))
    }

    /// After an attempt failed for now: when the row is due again, or, once
    /// its attempts are spent, `Failed`: the last attempt answered an error.
    pub fn after_failure(
        &self,
        row: &Work,
        now: Instant,
        error: String,
    ) -> Result<Instant, Outcome> {
        if row.attempts >= self.attempts {
            Err(Outcome::Failed(error))
        } else {
            Ok(now.plus(self.delay(row.attempts, row.id)))
        }
    }
}

/// What a claimed action or task comes to before it runs.
#[derive(Clone, Debug, PartialEq)]
pub enum Attempt {
    Run,
    /// Ended without running.
    End(Outcome),
}

/// How an action is called.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionPolicy {
    pub retry: RetryPolicy,
    /// How long one call may take before its delivery is taken over.
    pub timeout_ms: i64,
}

impl Default for ActionPolicy {
    fn default() -> Self {
        ActionPolicy {
            retry: RetryPolicy::default(),
            timeout_ms: 30_000,
        }
    }
}

/// Long work a process queues and a worker pulls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskType {
    pub name: String,
    pub retry: RetryPolicy,
    /// How long one attempt may run, however often it heartbeats.
    pub timeout_ms: Option<i64>,
    /// Never handed to a second worker: an attempt whose lease runs out ends
    /// the step `Unknown`, for `next` to reconcile.
    pub at_most_once: bool,
    /// After a task with an ordering key is done, the next of that key
    /// waits this long.
    pub key_interval_ms: i64,
}

impl TaskType {
    pub fn new(name: impl Into<String>) -> TaskType {
        TaskType {
            name: name.into(),
            retry: RetryPolicy {
                attempts: 1,
                ..RetryPolicy::default()
            },
            timeout_ms: None,
            at_most_once: false,
            key_interval_ms: 0,
        }
    }
}

/// A command run once in every period.
#[derive(Clone, Debug, PartialEq)]
pub struct Recurring {
    pub name: String,
    pub command: String,
    pub params: Value,
    pub every_seconds: i64,
    /// Periods start this long after each multiple of `every_seconds`
    /// since the epoch, in UTC.
    pub offset_seconds: i64,
}

impl Recurring {
    /// The start, in seconds since the epoch, of the period `now` is in.
    pub fn period_start(&self, now: Instant) -> i64 {
        let every = self.every_seconds.max(1);
        let since = now.0.div_euclid(1000) - self.offset_seconds;
        since.div_euclid(every) * every + self.offset_seconds
    }
}

/// What a projection does with an event it fails on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnFailure {
    /// Record it as a dead letter and go on.
    Skip,
    /// Record it and hold there until a new version rebuilds.
    Stop,
}

/// A change to one row of a projection's table.
#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub enum RowOp {
    Put {
        table: String,
        key: String,
        row: Value,
    },
    /// Sets the given fields, keeping the others; creates the row if absent.
    Merge {
        table: String,
        key: String,
        fields: Value,
    },
    Delete {
        table: String,
        key: String,
    },
}

/// Keeps read-model rows from the log. Pure: an event in, row changes out.
pub trait Projection: Send + Sync + 'static {
    fn name(&self) -> &str;
    /// A new version drops the rows and rebuilds them from the whole log;
    /// so does a change to its tables.
    fn version(&self) -> u32;
    /// The tables it writes. A row op on any other table fails the event.
    fn tables(&self) -> Vec<crate::query::Table>;
    fn on_failure(&self) -> OnFailure {
        OnFailure::Skip
    }
    fn project(&self, event: &Event) -> Result<Vec<RowOp>, String>;
}

// ---- erased forms the engine holds ----

pub trait DynAggregate: Send + Sync {
    fn name(&self) -> &str;
    fn code(&self) -> &str;
    fn ending(&self, kind: &str) -> bool;
    fn current_version(&self, kind: &str) -> u32;
    /// `data` recorded at `from` in the current shape.
    fn upcast(&self, kind: &str, from: u32, data: Value) -> Result<Value, String>;
    /// Folds current-shape events onto `state` (the initial state if none).
    fn fold(&self, state: Option<Value>, events: &[(String, Value)]) -> Result<Value, String>;
}

pub trait DynCommand: Send + Sync {
    fn name(&self) -> &str;
    fn aggregate(&self) -> &str;
    fn stream(&self, params: &Value) -> Result<Uuid, Rejection>;
    /// The events the command decides on, or its rejection. The outer error
    /// is a fault: the code could not read its state or write an event, which
    /// is no answer to the command.
    #[allow(clippy::type_complexity)]
    fn decide(
        &self,
        state: &Value,
        params: &Value,
        by: &By,
        now: Instant,
    ) -> Result<Result<Vec<NewEvent>, Rejection>, String>;
}

pub trait DynProcess: Send + Sync {
    fn name(&self) -> &str;
    fn starts_on(&self) -> Option<StartsOn>;
    fn next(&self, input: &Value, progress: &Progress) -> Next;
}

struct Typed<T>(PhantomData<fn() -> T>);

pub fn aggregate_of<A: Aggregate>() -> Box<dyn DynAggregate> {
    Box::new(Typed::<A>(PhantomData))
}

pub fn command_of<C: Command>() -> Box<dyn DynCommand> {
    Box::new(Typed::<C>(PhantomData))
}

pub fn process_of<P: Process>() -> Box<dyn DynProcess> {
    Box::new(Typed::<P>(PhantomData))
}

impl<A: Aggregate> DynAggregate for Typed<A> {
    fn name(&self) -> &str {
        A::NAME
    }
    fn code(&self) -> &str {
        A::code()
    }
    fn ending(&self, kind: &str) -> bool {
        A::ending(kind)
    }
    fn current_version(&self, kind: &str) -> u32 {
        1 + A::upcasts().iter().filter(|(k, _)| *k == kind).count() as u32
    }
    fn upcast(&self, kind: &str, from: u32, mut data: Value) -> Result<Value, String> {
        let steps: Vec<Upcast> = A::upcasts()
            .into_iter()
            .filter(|(k, _)| *k == kind)
            .map(|(_, f)| f)
            .collect();
        let from = from.max(1) as usize;
        if from > steps.len() + 1 {
            return Err(format!(
                "{kind} recorded at version {from}, newer than this code's {}",
                steps.len() + 1
            ));
        }
        for step in &steps[from - 1..] {
            data = step(data);
        }
        Ok(data)
    }
    fn fold(&self, state: Option<Value>, events: &[(String, Value)]) -> Result<Value, String> {
        let mut state: A::State = match state {
            Some(s) => serde_json::from_value(s).map_err(|e| format!("{} state: {e}", A::NAME))?,
            None => A::State::default(),
        };
        for (kind, data) in events {
            if kind == SCHEDULED {
                continue;
            }
            A::evolve(&mut state, decode_event::<A::Event>(kind, data)?);
        }
        serde_json::to_value(state).map_err(|e| e.to_string())
    }
}

impl<C: Command> DynCommand for Typed<C> {
    fn name(&self) -> &str {
        C::NAME
    }
    fn aggregate(&self) -> &str {
        <C::Aggregate as Aggregate>::NAME
    }
    fn stream(&self, params: &Value) -> Result<Uuid, Rejection> {
        Ok(C::stream(&decode_params::<C::Params>(params)?))
    }
    fn decide(
        &self,
        state: &Value,
        params: &Value,
        by: &By,
        now: Instant,
    ) -> Result<Result<Vec<NewEvent>, Rejection>, String> {
        let params = match decode_params::<C::Params>(params) {
            Ok(p) => p,
            Err(r) => return Ok(Err(r)),
        };
        let state =
            serde_json::from_value(state.clone()).map_err(|e| format!("{} state: {e}", C::NAME))?;
        let decision = match C::decide(&state, &params, by, now) {
            Ok(d) => d,
            Err(r) => return Ok(Err(r)),
        };
        let mut events = Vec::with_capacity(decision.events.len() + decision.scheduled.len());
        for event in decision.events {
            events.push(encode_event(&event).map_err(|e| format!("{}: {e}", C::NAME))?);
        }
        for scheduled in decision.scheduled {
            events.push(NewEvent {
                kind: SCHEDULED.to_string(),
                data: json!(scheduled),
            });
        }
        Ok(Ok(events))
    }
}

impl<P: Process> DynProcess for Typed<P> {
    fn name(&self) -> &str {
        P::NAME
    }
    fn starts_on(&self) -> Option<StartsOn> {
        P::starts_on()
    }
    fn next(&self, input: &Value, progress: &Progress) -> Next {
        match serde_json::from_value::<P::Input>(input.clone()) {
            Ok(input) => P::next(&input, progress),
            Err(e) => Next::Finish(Finished::Stuck(format!("{} input: {e}", P::NAME))),
        }
    }
}

fn decode_params<T: DeserializeOwned>(params: &Value) -> Result<T, Rejection> {
    serde_json::from_value(params.clone())
        .map_err(|e| Rejection::new(codes::INVALID_PARAMS, e.to_string()))
}

fn decode_event<E: DeserializeOwned>(kind: &str, data: &Value) -> Result<E, String> {
    let tagged = if data.is_null() {
        json!({ "type": kind })
    } else {
        json!({ "type": kind, "data": data })
    };
    serde_json::from_value(tagged).map_err(|e| format!("{kind}: {e}"))
}

fn encode_event<E: Serialize>(event: &E) -> Result<NewEvent, String> {
    let value = serde_json::to_value(event).map_err(|e| e.to_string())?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or("an event must serialize as {\"type\", \"data\"}")?;
    Ok(NewEvent {
        kind: kind.to_string(),
        data: value.get("data").cloned().unwrap_or(Value::Null),
    })
}
