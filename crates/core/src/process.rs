//! A process's progress is its own stream; what it does next is a pure
//! function of its input and the outcomes so far.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::event::{Rejection, StreamId};
use crate::model::DynProcess;

/// The aggregate name of every process stream: `process/<id>`.
pub const PROCESS: &str = "process";

/// One thing a process asks for.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    /// Apply a command, keyed by the step.
    Run { command: String, params: Value },
    /// Call another system through the outbox.
    Call {
        action: String,
        params: Value,
        at_most_once: bool,
    },
    /// Queue a task for a worker.
    Queue {
        task_type: String,
        input: Value,
        ordering_key: Option<String>,
    },
    /// Wait for the first event of a type on a stream, for at most a while.
    Await {
        stream: StreamId,
        event: String,
        within_ms: i64,
    },
}

/// How a process ended.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Finished {
    Completed(Value),
    Failed(String),
    /// Something is in a state nobody decided: a person must look.
    Stuck(String),
}

/// What `next` answers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Next {
    Step { key: String, step: Step },
    Finish(Finished),
}

impl Next {
    pub fn run(key: &str, command: &str, params: impl Serialize) -> Next {
        Next::step(
            key,
            Step::Run {
                command: command.into(),
                params: to_value(params),
            },
        )
    }
    pub fn call(key: &str, action: &str, params: impl Serialize) -> Next {
        Next::step(
            key,
            Step::Call {
                action: action.into(),
                params: to_value(params),
                at_most_once: false,
            },
        )
    }
    pub fn call_at_most_once(key: &str, action: &str, params: impl Serialize) -> Next {
        Next::step(
            key,
            Step::Call {
                action: action.into(),
                params: to_value(params),
                at_most_once: true,
            },
        )
    }
    pub fn queue(key: &str, task_type: &str, input: impl Serialize) -> Next {
        Next::step(
            key,
            Step::Queue {
                task_type: task_type.into(),
                input: to_value(input),
                ordering_key: None,
            },
        )
    }
    pub fn queue_ordered(
        key: &str,
        task_type: &str,
        input: impl Serialize,
        ordering_key: &str,
    ) -> Next {
        Next::step(
            key,
            Step::Queue {
                task_type: task_type.into(),
                input: to_value(input),
                ordering_key: Some(ordering_key.into()),
            },
        )
    }
    pub fn await_event(key: &str, stream: StreamId, event: &str, within_ms: i64) -> Next {
        Next::step(
            key,
            Step::Await {
                stream,
                event: event.into(),
                within_ms,
            },
        )
    }
    pub fn complete(value: impl Serialize) -> Next {
        Next::Finish(Finished::Completed(to_value(value)))
    }
    pub fn fail(reason: impl Into<String>) -> Next {
        Next::Finish(Finished::Failed(reason.into()))
    }
    pub fn stuck(reason: impl Into<String>) -> Next {
        Next::Finish(Finished::Stuck(reason.into()))
    }
    fn step(key: &str, step: Step) -> Next {
        Next::Step {
            key: key.into(),
            step,
        }
    }
}

fn to_value(v: impl Serialize) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

/// What became of one step.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Done: a command's recorded events, an action's reply, a task's
    /// result, the awaited event.
    Ok(Value),
    /// The command or the other system said no.
    Rejected(Rejection),
    /// Every attempt failed, or one failed for good.
    Failed(String),
    /// No answer came and the step is not tried again: done at most once,
    /// or its last attempt's lease ran out. It may or may not have
    /// happened; the text says how many attempts were made.
    Unknown(String),
    TimedOut,
    Cancelled,
}

/// Every step's outcome so far.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Progress {
    pub steps: BTreeMap<String, Outcome>,
}

impl Progress {
    pub fn outcome(&self, key: &str) -> Option<&Outcome> {
        self.steps.get(key)
    }
    /// The step's value, if it succeeded.
    pub fn ok(&self, key: &str) -> Option<&Value> {
        match self.steps.get(key) {
            Some(Outcome::Ok(v)) => Some(v),
            _ => None,
        }
    }
}

/// The events of a process stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ProcessEvent {
    #[serde(rename = "ProcessStarted")]
    Started { process: String, input: Value },
    #[serde(rename = "StepRequested")]
    Requested { key: String, step: Step },
    #[serde(rename = "StepRecorded")]
    Recorded { key: String, outcome: Outcome },
    #[serde(rename = "ProcessFinished")]
    Finished { finished: Finished },
}

impl ProcessEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            ProcessEvent::Started { .. } => "ProcessStarted",
            ProcessEvent::Requested { .. } => "StepRequested",
            ProcessEvent::Recorded { .. } => "StepRecorded",
            ProcessEvent::Finished { .. } => "ProcessFinished",
        }
    }

    pub fn data(&self) -> Value {
        serde_json::to_value(self)
            .ok()
            .and_then(|v| v.get("data").cloned())
            .unwrap_or(Value::Null)
    }

    pub fn decode(kind: &str, data: &Value) -> Result<ProcessEvent, String> {
        serde_json::from_value(serde_json::json!({ "type": kind, "data": data }))
            .map_err(|e| format!("{kind}: {e}"))
    }
}

/// A process folded from its stream.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProcessState {
    pub name: String,
    pub input: Value,
    pub requested: BTreeMap<String, Step>,
    pub progress: Progress,
    pub finished: Option<Finished>,
}

impl ProcessState {
    pub fn fold<'a>(
        events: impl IntoIterator<Item = (&'a str, &'a Value)>,
    ) -> Result<ProcessState, String> {
        let mut state = ProcessState::default();
        for (kind, data) in events {
            match ProcessEvent::decode(kind, data)? {
                ProcessEvent::Started { process, input } => {
                    state.name = process;
                    state.input = input;
                }
                ProcessEvent::Requested { key, step } => {
                    state.requested.insert(key, step);
                }
                ProcessEvent::Recorded { key, outcome } => {
                    state.progress.steps.insert(key, outcome);
                }
                ProcessEvent::Finished { finished } => state.finished = Some(finished),
            }
        }
        Ok(state)
    }

    /// The step requested and not yet answered, if any.
    pub fn pending(&self) -> Option<(&String, &Step)> {
        self.requested
            .iter()
            .find(|(k, _)| !self.progress.steps.contains_key(*k))
    }
}

/// What advancing a process comes to.
#[derive(Clone, Debug, PartialEq)]
pub enum Advance {
    /// It has finished.
    Done,
    /// A step it asked for has no outcome yet.
    Wait,
    Take {
        key: String,
        step: Step,
    },
    Finish(Finished),
}

pub fn advance(state: &ProcessState, process: &dyn DynProcess) -> Advance {
    if state.finished.is_some() {
        return Advance::Done;
    }
    if state.pending().is_some() {
        return Advance::Wait;
    }
    match process.next(&state.input, &state.progress) {
        Next::Finish(f) => Advance::Finish(f),
        Next::Step { key, .. } if state.progress.steps.contains_key(&key) => {
            Advance::Finish(Finished::Stuck(format!(
                "next asked for step {key:?} again after its outcome"
            )))
        }
        Next::Step {
            step: Step::Await { within_ms, .. },
            key,
        } if within_ms <= 0 => {
            Advance::Finish(Finished::Stuck(format!("step {key:?} waits for no time")))
        }
        Next::Step { key, step } => Advance::Take { key, step },
    }
}

/// A run of a process as some version of it left it, and what `next`
/// answered for it then. A new version is held to the samples older ones
/// left: its `next` must answer each the same, or the runs those versions
/// left behind would take a step twice or wait forever.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// What the run shows, for the person reading a mismatch.
    pub name: String,
    pub process: String,
    pub input: Value,
    pub progress: Progress,
    pub next: Next,
}

/// What `next` answers a sample now, if that is not what it answered then.
pub fn check_sample(process: &dyn DynProcess, sample: &Sample) -> Option<Next> {
    let now = process.next(&sample.input, &sample.progress);
    (now != sample.next).then_some(now)
}
