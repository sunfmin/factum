//! Sampling processes, and holding a model to its samples.

use factum_core::{Sample, Uuid, check_sample, codes};
use factum_store::Store;

use crate::engine::{Engine, Error, rejected};
use crate::model::Model;

impl<S: Store> Engine<S> {
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
