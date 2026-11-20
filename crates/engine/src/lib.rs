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
mod projector;
mod reads;
mod work;

pub use clock::{Clock, ManualClock, SystemClock};
pub use engine::{Applied, DISPATCH, Engine, Error, Fault, Options, ProcessView, Submission, Tick};
pub use model::{ActionAnswer, BoxFuture, Model};
pub use reads::Mismatch;
pub use work::ClaimedTask;

pub use factum_core::{
    ActionPolicy, Aggregate, By, Command, Decision, Event, Finished, Instant, NewEvent, Next,
    OnFailure, Op, Outcome, Process, Progress, Projection, Query, Recorded, Recurring, Rejection,
    RetryPolicy, RowOp, Sample, StartsOn, Step, StreamId, Table, TaskType, Upcast, Uuid, Value,
    Version, codes,
};
pub use factum_store::{DeadLetter, MemoryStore, Store, StoreError};
