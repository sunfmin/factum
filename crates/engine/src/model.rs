use factum_core::is_name;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use factum_core::{
    ActionPolicy, Aggregate, Command, DynAggregate, DynCommand, DynProcess, Process, Projection,
    Recurring, Rejection, TaskType, Uuid, Value, aggregate_of, command_of, process_of,
};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// What another system answered a call.
#[derive(Clone, Debug, PartialEq)]
pub enum ActionAnswer {
    Done(Value),
    /// The other system's "no": the step's outcome.
    Rejected(Rejection),
    /// Failed for now: tried again by the action's retry policy.
    Passing(String),
    /// Can never be done: the step fails.
    Permanent(String),
}

pub(crate) type ActionFn = Arc<dyn Fn(Uuid, Value) -> BoxFuture<ActionAnswer> + Send + Sync>;

pub(crate) struct ActionDef {
    pub policy: ActionPolicy,
    pub call: ActionFn,
}

/// Everything a program declares, by name.
#[derive(Default)]
pub struct Model {
    pub(crate) aggregates: BTreeMap<String, Box<dyn DynAggregate>>,
    pub(crate) commands: BTreeMap<String, Box<dyn DynCommand>>,
    pub(crate) processes: BTreeMap<String, Box<dyn DynProcess>>,
    pub(crate) actions: BTreeMap<String, ActionDef>,
    pub(crate) tasks: BTreeMap<String, TaskType>,
    pub(crate) recurring: Vec<Recurring>,
    pub(crate) projections: Vec<Box<dyn Projection>>,
}

impl Model {
    pub fn new() -> Model {
        Model::default()
    }

    pub fn aggregate<A: Aggregate>(mut self) -> Model {
        self.aggregates
            .insert(A::NAME.to_string(), aggregate_of::<A>());
        self
    }

    /// Declares the command, and its aggregate if not yet declared.
    pub fn command<C: Command>(mut self) -> Model {
        let aggregate = <C::Aggregate as Aggregate>::NAME;
        if !self.aggregates.contains_key(aggregate) {
            self = self.aggregate::<C::Aggregate>();
        }
        self.commands.insert(C::NAME.to_string(), command_of::<C>());
        self
    }

    pub fn process<P: Process>(mut self) -> Model {
        self.processes
            .insert(P::NAME.to_string(), process_of::<P>());
        self
    }

    /// Declares how the action `name` is called: `call` gets the step's key,
    /// the same on every attempt, and the step's params.
    pub fn action<F, Fut>(mut self, name: &str, policy: ActionPolicy, call: F) -> Model
    where
        F: Fn(Uuid, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ActionAnswer> + Send + 'static,
    {
        let call: ActionFn = Arc::new(move |key, params| Box::pin(call(key, params)));
        self.actions
            .insert(name.to_string(), ActionDef { policy, call });
        self
    }

    pub fn task(mut self, task: TaskType) -> Model {
        self.tasks.insert(task.name.clone(), task);
        self
    }

    pub fn recurring(mut self, recurring: Recurring) -> Model {
        self.recurring.push(recurring);
        self
    }

    /// Declares a projection. Its name, tables and columns must be names (a
    /// letter, then letters, digits or underscores), and each index must name
    /// declared columns; a declaration that breaks this is a programming
    /// error, and panics.
    pub fn projection(mut self, projection: impl Projection) -> Model {
        let name = projection.name().to_string();
        assert!(is_name(&name), "projection name {name:?} is not a name");
        for t in projection.tables() {
            assert!(
                is_name(&t.name),
                "{name}: table name {:?} is not a name",
                t.name
            );
            for c in &t.columns {
                assert!(
                    is_name(c),
                    "{name}.{}: column name {c:?} is not a name",
                    t.name
                );
            }
            for i in t.indexes.iter().flatten() {
                assert!(
                    t.columns.contains(i),
                    "{name}.{}: index on undeclared column {i}",
                    t.name
                );
            }
        }
        self.projections.push(Box::new(projection));
        self
    }
}
