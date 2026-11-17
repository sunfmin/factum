//! Work: what outlives the command that asked for it. Each pending piece is
//! a row a store keeps beside the log, and every row is implied by an event
//! committed with it (`effects`), so the rows never say what the log does
//! not.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::event::{Event, Instant, StreamId};
use crate::keys::step_key;
use crate::model::Scheduled;
use crate::process::{PROCESS, ProcessEvent, Step};

/// The reserved type of the event a decide schedules a command with.
pub const SCHEDULED: &str = "CommandScheduled";
/// The one event of a schedule's outcome stream, `schedule/<event id>`.
pub const SCHEDULE_RAN: &str = "ScheduleRan";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkKind {
    /// An unfinished process, due when it can move on. Its id is the
    /// process's; its claim is the one runner's lease.
    Advance,
    /// An outbox delivery. Its id is the step's key.
    Action,
    /// A task for a worker. Its id is the step's key.
    Task,
    /// A scheduled command. Its id is the scheduling event's.
    Schedule,
    /// A wait for an event, due at its deadline. Its name is the stream.
    Await,
}

impl WorkKind {
    pub const ALL: [WorkKind; 5] = [
        WorkKind::Advance,
        WorkKind::Action,
        WorkKind::Task,
        WorkKind::Schedule,
        WorkKind::Await,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            WorkKind::Advance => "advance",
            WorkKind::Action => "action",
            WorkKind::Task => "task",
            WorkKind::Schedule => "schedule",
            WorkKind::Await => "await",
        }
    }

    pub fn parse(s: &str) -> Option<WorkKind> {
        WorkKind::ALL.into_iter().find(|k| k.as_str() == s)
    }
}

/// One pending piece of work.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Work {
    pub kind: WorkKind,
    pub id: Uuid,
    /// The action, task type, command, process or stream it is for.
    pub name: String,
    pub process: Option<Uuid>,
    pub step: Option<String>,
    pub payload: Value,
    pub due: Instant,
    pub ordering_key: Option<String>,
    /// How many times it has been claimed.
    pub attempts: u32,
    pub lease: Option<Uuid>,
    pub holder: Option<String>,
    pub lease_until: Option<Instant>,
    /// When the current claim began.
    pub claimed_at: Option<Instant>,
    /// Set on a claim that took it from a holder whose lease ran out.
    pub taken_over: bool,
}

impl Work {
    pub fn new(kind: WorkKind, id: Uuid, name: impl Into<String>, due: Instant) -> Work {
        Work {
            kind,
            id,
            name: name.into(),
            process: None,
            step: None,
            payload: Value::Null,
            due,
            ordering_key: None,
            attempts: 0,
            lease: None,
            holder: None,
            lease_until: None,
            claimed_at: None,
            taken_over: false,
        }
    }
}

/// A change to the work rows, committed with the events it follows from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkChange {
    /// Adds the row unless one of its kind and id exists.
    Put(Work),
    /// Makes an existing row due at `due`, if that is sooner. A wake during
    /// a claim survives the claim's release.
    Wake {
        kind: WorkKind,
        id: Uuid,
        due: Instant,
    },
    /// Removes the row.
    Done { kind: WorkKind, id: Uuid },
    /// Ends a claim, the row due again at `due` (unless woken meanwhile).
    /// Refused unless `lease` is the row's lease.
    Release {
        kind: WorkKind,
        id: Uuid,
        lease: Uuid,
        due: Instant,
    },
    /// Holds the tasks of `name` with ordering key `key` until `not_before`.
    Pace {
        name: String,
        key: String,
        not_before: Instant,
    },
}

/// The work rows an event implies. The one home of that mapping.
pub fn effects(event: &Event) -> Vec<WorkChange> {
    let now = event.meta.now;
    if event.kind == SCHEDULED {
        return match serde_json::from_value::<Scheduled>(event.data.clone()) {
            Ok(s) => {
                let mut w = Work::new(WorkKind::Schedule, event.id, s.command, s.at);
                w.payload = s.params;
                vec![WorkChange::Put(w)]
            }
            Err(_) => Vec::new(),
        };
    }
    if event.kind == SCHEDULE_RAN && event.stream.aggregate == "schedule" {
        return vec![WorkChange::Done {
            kind: WorkKind::Schedule,
            id: event.stream.id,
        }];
    }
    if event.stream.aggregate != PROCESS {
        return Vec::new();
    }
    let process = event.stream.id;
    let Ok(e) = ProcessEvent::decode(&event.kind, &event.data) else {
        return Vec::new();
    };
    match e {
        ProcessEvent::Started { process: name, .. } => {
            let mut w = Work::new(WorkKind::Advance, process, name, now);
            w.process = Some(process);
            vec![WorkChange::Put(w)]
        }
        ProcessEvent::Requested { key, step } => {
            let id = step_key(process, &key);
            let w = match step {
                Step::Run { .. } => return Vec::new(),
                Step::Call {
                    action,
                    params,
                    at_most_once,
                } => {
                    let mut w = Work::new(WorkKind::Action, id, action, now);
                    w.payload = json!({ "params": params, "at_most_once": at_most_once });
                    w
                }
                Step::Queue {
                    task_type,
                    input,
                    ordering_key,
                } => {
                    let mut w = Work::new(WorkKind::Task, id, task_type, now);
                    w.payload = input;
                    w.ordering_key = ordering_key;
                    w
                }
                Step::Await {
                    stream,
                    event: kind,
                    within_ms,
                } => {
                    let mut w =
                        Work::new(WorkKind::Await, id, stream.to_string(), now.plus(within_ms));
                    w.payload = json!({ "event": kind });
                    w
                }
            };
            vec![WorkChange::Put(Work {
                process: Some(process),
                step: Some(key),
                ..w
            })]
        }
        ProcessEvent::Recorded { key, .. } => {
            let id = step_key(process, &key);
            vec![
                WorkChange::Done {
                    kind: WorkKind::Action,
                    id,
                },
                WorkChange::Done {
                    kind: WorkKind::Task,
                    id,
                },
                WorkChange::Done {
                    kind: WorkKind::Await,
                    id,
                },
                WorkChange::Wake {
                    kind: WorkKind::Advance,
                    id: process,
                    due: now,
                },
            ]
        }
        ProcessEvent::Finished { .. } => vec![WorkChange::Done {
            kind: WorkKind::Advance,
            id: process,
        }],
    }
}

/// The stream a schedule's outcome is recorded on.
pub fn schedule_stream(event: Uuid) -> StreamId {
    StreamId::new("schedule", event)
}
