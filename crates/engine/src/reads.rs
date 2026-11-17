//! Reading projections, retrying their dead letters, and sampling processes.

use factum_core::{
    Event, Projection, Query, RowOp, Sample, Table, Uuid, Value, Version, check_sample, codes,
};
use factum_store::Store;

use crate::engine::{Engine, Error, rejected};
use crate::model::Model;

impl<S: Store> Engine<S> {
    /// The row ops `p` makes of `e`, in its current shape; a failure if it
    /// fails or writes a table it did not declare.
    pub(crate) fn project_one(
        &self,
        p: &dyn Projection,
        tables: &[Table],
        e: Event,
    ) -> Result<Vec<RowOp>, String> {
        let ops = p.project(&self.upcast(e).map_err(|e| e.to_string())?)?;
        for op in &ops {
            let (table, _) = factum_store::op_target(op);
            if !tables.iter().any(|t| t.name == table) {
                return Err(format!(
                    "writes table {table:?}, which {} does not declare",
                    p.name()
                ));
            }
        }
        Ok(ops)
    }

    /// Rows of a projection's table that the query asks for. Refused
    /// `invalid_query` if it names a table or column the projection does not
    /// declare.
    pub async fn query(
        &self,
        projection: &str,
        query: &Query,
    ) -> Result<Vec<(String, Value)>, Error> {
        let p = self.projection(projection)?;
        let tables = p.tables();
        let Some(table) = tables.iter().find(|t| t.name == query.table) else {
            return Err(rejected(
                codes::INVALID_QUERY,
                format!("{projection} has no table {}", query.table),
            ));
        };
        if let Some(c) = query
            .columns()
            .find(|c| !table.columns.iter().any(|d| d == c))
        {
            return Err(rejected(
                codes::INVALID_QUERY,
                format!("{} has no column {c}", query.table),
            ));
        }
        Ok(self.store.query(projection, query).await?)
    }

    /// Projects a dead letter's event again with the code running now. If
    /// it projects, its rows are written and the dead letter goes; a
    /// projection that stopped at it goes on past it. Answers whether it
    /// projected; if not, the dead letter stays.
    pub async fn retry_dead_letter(&self, projection: &str, event: Uuid) -> Result<bool, Error> {
        let p = self.projection(projection)?;
        let Some(dead) = self
            .store
            .dead_letters(projection)
            .await?
            .into_iter()
            .find(|d| d.event == event)
        else {
            return Err(rejected(
                codes::UNKNOWN,
                format!("{projection} has no dead letter {event}"),
            ));
        };
        let Some(e) = self
            .store
            .read_stream(&dead.stream, Version(dead.version.0 - 1))
            .await?
            .into_iter()
            .next()
        else {
            return Err(Error::Fault(format!(
                "{} has no version {:?}",
                dead.stream, dead.version
            )));
        };
        match self.project_one(p, &p.tables(), e) {
            Ok(ops) => {
                self.store
                    .resolve_dead_letter(projection, event, ops)
                    .await?;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    fn projection(&self, name: &str) -> Result<&dyn Projection, Error> {
        self.model
            .projections
            .iter()
            .find(|p| p.name() == name)
            .map(|p| p.as_ref())
            .ok_or_else(|| rejected(codes::UNKNOWN, format!("no projection {name}")))
    }

    /// The process as it stands, as a sample: its input, its progress and
    /// what `next` answers for it now. Kept beside the code, it holds later
    /// versions to answering the same.
    pub async fn sample(&self, process: Uuid, name: &str) -> Result<Sample, Error> {
        let (state, _) = self.process_events(process).await?;
        let def = self
            .model
            .processes
            .get(&state.name)
            .ok_or_else(|| rejected(codes::UNKNOWN, format!("no process {process}")))?;
        let next = def.next(&state.input, &state.progress);
        Ok(Sample {
            name: name.into(),
            process: state.name,
            input: state.input,
            progress: state.progress,
            next,
        })
    }
}

/// A sample the model's `next` answers otherwise now.
#[derive(Clone, Debug, PartialEq)]
pub struct Mismatch {
    pub sample: String,
    /// What it answers now; `None` if the process is no longer declared.
    pub now: Option<factum_core::Next>,
}

impl Model {
    /// Holds this model's processes to the samples older versions left.
    pub fn check_samples(&self, samples: &[Sample]) -> Vec<Mismatch> {
        samples
            .iter()
            .filter_map(|s| match self.processes.get(&s.process) {
                None => Some(Mismatch {
                    sample: s.name.clone(),
                    now: None,
                }),
                Some(p) => check_sample(p.as_ref(), s).map(|now| Mismatch {
                    sample: s.name.clone(),
                    now: Some(now),
                }),
            })
            .collect()
    }
}
