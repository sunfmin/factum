//! The pure half of factum. Nothing here reads a clock, touches IO or
//! awaits: every function answers from its arguments alone, so the engine
//! can plan without writing, retry without repeating and be tested without
//! a database.

mod event;
mod keys;
mod model;
mod process;
mod query;
mod work;

pub use event::{
    By, Event, Instant, Meta, NewEvent, Position, Recorded, Rejection, StreamId, Version, codes,
};
pub use keys::{fingerprint, recurring_key, result_key, schedule_key, start_key, step_key};
pub use model::{
    ActionPolicy, Aggregate, Attempt, Command, Decision, DynAggregate, DynCommand, DynProcess,
    OnFailure, Process, Projection, Recurring, RetryPolicy, RowOp, Scheduled, StartsOn, TaskType,
    Upcast, aggregate_of, command_of, process_of,
};
pub use process::{
    Advance, Finished, Next, Outcome, PROCESS, ProcessEvent, ProcessState, Progress, Sample, Step,
    advance, check_sample,
};
pub use query::{Filter, Op, Order, Query, Scalar, Table, answer, compare, holds, is_name};
pub use work::{
    Call, RECURRING_RAN, SCHEDULE_RAN, SCHEDULED, Wait, Work, WorkChange, WorkKind, effects,
    recurring_stream, schedule_stream,
};

pub use serde_json::Value;
pub use uuid::Uuid;
