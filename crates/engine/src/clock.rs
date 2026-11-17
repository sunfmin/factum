use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use factum_core::Instant;

/// Where the engine reads `now`, once per submission and once per tick.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        Instant(ms)
    }
}

/// A clock only the program moves: tests advance time by hand.
pub struct ManualClock(AtomicI64);

impl ManualClock {
    pub fn new(at: Instant) -> ManualClock {
        ManualClock(AtomicI64::new(at.0))
    }
    pub fn set(&self, at: Instant) {
        self.0.store(at.0, Ordering::SeqCst);
    }
    pub fn advance(&self, millis: i64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        Instant(self.0.load(Ordering::SeqCst))
    }
}
