//! factum's engine: the effect half. It reads the clock, talks to the
//! store, calls actions and hands tasks to workers; every decision it acts
//! on is made by `factum-core`'s pure functions.
//!
//! A program declares a [`Model`], opens an [`Engine`] on a store, submits
//! commands, and calls [`Engine::tick`] (or [`Engine::settle`]) to move the
//! work that outlives a command: processes, actions, schedules, recurring
//! commands, waits and projections.

mod clock;
mod engine;
mod model;
mod work;

pub use clock::{Clock, ManualClock, SystemClock};
pub use engine::{Applied, Engine, Error, Options, ProcessView, Submission};
pub use model::{ActionAnswer, BoxFuture, Model};
pub use work::ClaimedTask;

pub use factum_core::{
    ActionPolicy, Aggregate, By, Command, Decision, Event, Finished, Instant, NewEvent, Next,
    OnFailure, Outcome, Process, Progress, Projection, Recurring, Rejection, RetryPolicy, RowOp,
    StartsOn, Step, StreamId, TaskType, Upcast, Uuid, Value, Version, codes,
};
pub use factum_store::{DeadLetter, MemoryStore, Store, StoreError};
