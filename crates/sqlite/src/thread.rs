//! A connection on a thread of its own, and a reply any runtime can await.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use rusqlite::Connection;

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

/// Runs jobs on its connection, one at a time, on its own thread. The
/// thread ends when the worker is dropped.
pub(crate) struct Worker {
    jobs: mpsc::Sender<Job>,
}

impl Worker {
    pub(crate) fn start(mut conn: Connection) -> std::io::Result<Worker> {
        let (jobs, inbox) = mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("factum-sqlite".into())
            .spawn(move || {
                for job in inbox {
                    job(&mut conn);
                }
            })?;
        Ok(Worker { jobs })
    }

    /// Runs `f` on the connection; `None` if the thread has stopped.
    pub(crate) fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> T + Send + 'static,
    ) -> Reply<T> {
        let slot = Arc::new(Mutex::new(Slot {
            value: None,
            waker: None,
            closed: false,
        }));
        let sender = Sender(slot.clone());
        let job: Job = Box::new(move |conn| sender.send(f(conn)));
        if self.jobs.send(job).is_err() {
            slot.lock().unwrap_or_else(|e| e.into_inner()).closed = true;
        }
        Reply(slot)
    }
}

struct Slot<T> {
    value: Option<T>,
    waker: Option<Waker>,
    closed: bool,
}

struct Sender<T>(Arc<Mutex<Slot<T>>>);

impl<T> Sender<T> {
    fn send(self, value: T) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).value = Some(value);
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        slot.closed = true;
        if let Some(w) = slot.waker.take() {
            w.wake();
        }
    }
}

pub(crate) struct Reply<T>(Arc<Mutex<Slot<T>>>);

impl<T> Future for Reply<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(value) = slot.value.take() {
            return Poll::Ready(Some(value));
        }
        if slot.closed {
            return Poll::Ready(None);
        }
        slot.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
