use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A point in time: milliseconds since the Unix epoch, UTC. The engine reads
/// its clock once per submission and hands the reading down; nothing pure
/// reads a clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Instant(pub i64);

impl Instant {
    /// Later than any time a clock reads: "not due until something wakes it".
    pub const NEVER: Instant = Instant(i64::MAX);

    pub fn plus(self, millis: i64) -> Instant {
        Instant(self.0.saturating_add(millis))
    }
}

/// A stream's version: how many events it holds. A new stream is at 0; its
/// first event is version 1, and versions never skip.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Version(pub u64);

impl Version {
    pub fn next(self) -> Version {
        Version(self.0 + 1)
    }
}

/// Where an event sits in the log's commit order. Opaque to callers: a
/// store chooses what the two numbers mean, and promises only that reading
/// after a position never skips an event committed later at a smaller one.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Position(pub u64, pub u64);

/// `<aggregate>/<uuid>`: the events of one thing.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct StreamId {
    pub aggregate: String,
    pub id: Uuid,
}

impl StreamId {
    pub fn new(aggregate: impl Into<String>, id: Uuid) -> StreamId {
        StreamId {
            aggregate: aggregate.into(),
            id,
        }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.aggregate, self.id)
    }
}

impl TryFrom<String> for StreamId {
    type Error = String;
    fn try_from(s: String) -> Result<Self, String> {
        let (aggregate, id) = s
            .rsplit_once('/')
            .ok_or_else(|| format!("not a stream: {s}"))?;
        let id = Uuid::parse_str(id).map_err(|e| format!("not a stream: {s}: {e}"))?;
        Ok(StreamId::new(aggregate, id))
    }
}

impl From<StreamId> for String {
    fn from(s: StreamId) -> String {
        s.to_string()
    }
}

/// Who applies a command. factum originates the kinds below itself; every
/// other actor is the program's own, recorded as its principal text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum By {
    /// A person, an agent, a system: whatever the program names it.
    Principal(String),
    /// A process's step, by the process's stream.
    Process(StreamId),
    /// A scheduled command, by the id of the event that scheduled it.
    Schedule(Uuid),
    /// A recurring command, by its name.
    Recurring(String),
    /// An action's answer, by the action's name.
    Action(String),
    /// A task's result, by the worker that reported it.
    Worker(String),
}

/// What is recorded beside each event's data.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Meta {
    /// The time the command was decided at.
    pub now: Instant,
    pub by: By,
    /// The idempotency key of the submission that recorded it.
    pub key: Option<Uuid>,
    /// The command that recorded it.
    pub command: String,
}

/// An event a decide returns: its stored type name and its data.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewEvent {
    pub kind: String,
    pub data: Value,
}

/// A recorded fact. Never edited.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// A UUIDv7 the engine gives it.
    pub id: Uuid,
    pub stream: StreamId,
    pub version: Version,
    pub kind: String,
    /// The shape `data` was recorded in. Read code sees the current shape
    /// only: older ones are upcast as they are read.
    pub type_version: u32,
    pub data: Value,
    pub meta: Meta,
    /// Set by the store when it commits; `Position::default()` before.
    pub position: Position,
}

/// A command's "no": a code a caller can act on and a sentence for people.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejection {
    pub code: String,
    pub message: String,
}

impl Rejection {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Rejection {
        Rejection {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

/// The rejection codes factum itself answers with.
pub mod codes {
    /// The stream's last event ends it; it takes no more commands.
    pub const ENDED: &str = "ended";
    /// The key was sent before with another command or other params.
    pub const KEY_REUSED: &str = "key_reused";
    /// No command, process or task type of that name is declared.
    pub const UNKNOWN: &str = "unknown";
    /// The params do not decode into what the command takes.
    pub const INVALID_PARAMS: &str = "invalid_params";
    /// The task's lease ran out or is another worker's.
    pub const LEASE_LOST: &str = "lease_lost";
    /// A query names a table or column its projection does not declare.
    pub const INVALID_QUERY: &str = "invalid_query";
}
