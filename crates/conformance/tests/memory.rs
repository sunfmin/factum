//! The in-memory store, held to the conformance suite.

use std::sync::Arc;

use factum::MemoryStore;
use factum_conformance::Backend;

struct Memory;

impl Backend for Memory {
    type S = MemoryStore;
    fn open(&self) -> Arc<MemoryStore> {
        Arc::new(MemoryStore::new())
    }
    fn reopen(&self, store: &Arc<MemoryStore>) -> Arc<MemoryStore> {
        store.clone()
    }
}

factum_conformance::suite!(Memory);
