//! A seeded simulation of the engine, run by `truefit sim` (`truefit.toml`)
//! through the `sim` binary: from one seed, an order of submissions, ticks
//! of several runners, clock moves and faults — a runner stopping between
//! any two store calls of a tick, a store write failing, a call to another
//! system answering late, twice or never — with factum's promises about
//! the work that outlives a command checked after every step.
//!
//! Everything is chosen from the seed and nothing else: one clock only the
//! simulation moves, one random source, one thread, and futures polled by
//! hand, so a seed reruns step for step.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use factum::{
    ActionAnswer, ActionPolicy, BoxFuture, By, Engine, Error, ManualClock, MemoryStore, Model,
    Outcome, Rejection, Step, Store, StreamId, Submission, Tick, Uuid, Value,
};
use factum_core::{WorkKind, step_key};
use serde_json::json;

use crate::START;
use crate::domain::{
    CancelOrder, Checkout, CheckoutProcess, ExpireOrder, LIMIT, OrderRef, PayOrder, Place,
    PlaceOrder, ReceiptProcess, charge_policy,
};
use crate::flaky::{Call, Flaky};

/// How many steps one seed runs before it settles.
const STEPS: u64 = 200;
const RUNNERS: usize = 3;
const MAX_ORDERS: usize = 6;
/// How many rounds settling may take before what is left counts as lost.
const SETTLE_ROUNDS: u64 = 60;

/// The one random source: splitmix64.
struct Rng(u64);

impl Rng {
    /// A seed is a decimal number, or any other text, hashed (FNV-1a).
    fn from_seed(seed: &str) -> Rng {
        Rng(seed.parse().unwrap_or_else(|_| {
            seed.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3)
            })
        }))
    }

    fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.draw() % n
    }

    fn id(&mut self) -> Uuid {
        Uuid::from_u64_pair(self.draw(), self.draw())
    }
}

/// A call whose answer the simulation holds back.
struct Held {
    label: String,
    answer: ActionAnswer,
    released: bool,
}

/// The other systems the shop calls, stood in for: a payment service that
/// charges once per key, and a printer that prints every time it is asked.
/// Each call answers at once, "try later", or is held back until the
/// simulation lets its answer through, or never.
struct Outside {
    rng: Rng,
    /// Every call answers at once: the simulation is settling.
    calm: bool,
    /// The keys each order was charged under.
    charged: BTreeMap<Uuid, BTreeSet<Uuid>>,
    printed: BTreeMap<Uuid, u32>,
    held: Vec<Arc<Mutex<Held>>>,
}

type Shared = Arc<Mutex<Outside>>;

/// Whether the future polled last waits on a held answer.
static BLOCKED: AtomicBool = AtomicBool::new(false);
/// The step running, for a panic's verdict.
static STEP: AtomicU64 = AtomicU64::new(0);

fn call(outside: &Shared, action: &str, key: Uuid, params: &Value) -> BoxFuture<ActionAnswer> {
    let mut o = outside.lock().unwrap();
    let mode = if o.calm { 0 } else { o.rng.below(10) };
    if mode == 9 {
        return Box::pin(async { ActionAnswer::Passing("busy".into()) });
    }
    let answer = if action == "printer.print" {
        *o.printed.entry(key).or_default() += 1;
        ActionAnswer::Done(json!("ok"))
    } else if params["amount"].as_i64().unwrap_or(0) > LIMIT {
        ActionAnswer::Rejected(Rejection::new("declined", "over the limit"))
    } else {
        let order = params["order"].as_str().and_then(|s| s.parse().ok());
        o.charged
            .entry(order.unwrap_or_default())
            .or_default()
            .insert(key);
        ActionAnswer::Done(json!({ "charge": key }))
    };
    if mode < 7 {
        return Box::pin(async move { answer });
    }
    let held = Arc::new(Mutex::new(Held {
        label: format!("{action} {}", short(key)),
        answer,
        released: false,
    }));
    o.held.push(held.clone());
    Box::pin(std::future::poll_fn(move |_| {
        let h = held.lock().unwrap();
        if h.released {
            Poll::Ready(h.answer.clone())
        } else {
            BLOCKED.store(true, Ordering::SeqCst);
            Poll::Pending
        }
    }))
}

fn short(id: Uuid) -> String {
    id.simple().to_string()[..8].to_string()
}

/// Polls `fut` until it is ready, or waits on a held answer.
fn drive<T>(fut: &mut Pin<Box<dyn Future<Output = T> + Send>>) -> Option<T> {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100_000 {
        BLOCKED.store(false, Ordering::SeqCst);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return Some(v),
            Poll::Pending if BLOCKED.load(Ordering::SeqCst) => return None,
            Poll::Pending => {}
        }
    }
    panic!("a future that neither ends nor waits on a held answer")
}

/// A future that never waits on a held answer, run to its end.
fn now<T: Send>(fut: impl Future<Output = T> + Send) -> T {
    let mut fut: Pin<Box<dyn Future<Output = T> + Send + '_>> = Box::pin(fut);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

type TickFuture = BoxFuture<Result<Tick, Error>>;

struct Runner {
    holder: String,
    engine: Arc<Engine<Flaky<MemoryStore>>>,
    flaky: Arc<Flaky<MemoryStore>>,
    tick: Option<TickFuture>,
    stopped: bool,
}

struct Order {
    id: Uuid,
    checkout: Uuid,
    receipt: Uuid,
}

/// One step's state, as the invariants read it.
struct Seen {
    state: Value,
    broken: Option<&'static str>,
}

struct Sim {
    rng: Rng,
    clock: Arc<ManualClock>,
    store: Arc<MemoryStore>,
    model: Arc<Model>,
    outside: Shared,
    front: Engine<MemoryStore>,
    runners: Vec<Runner>,
    restarts: u32,
    orders: Vec<Order>,
}

impl Sim {
    fn new(seed: &str) -> Sim {
        let mut rng = Rng::from_seed(seed);
        let outside = Arc::new(Mutex::new(Outside {
            rng: Rng(rng.draw()),
            calm: false,
            charged: BTreeMap::new(),
            printed: BTreeMap::new(),
            held: Vec::new(),
        }));
        let (pay, print) = (outside.clone(), outside.clone());
        let model = Arc::new(
            Model::new()
                .command::<PlaceOrder>()
                .command::<PayOrder>()
                .command::<CancelOrder>()
                .command::<ExpireOrder>()
                .process::<CheckoutProcess>()
                .process::<ReceiptProcess>()
                .action("payment.charge", charge_policy(), move |key, params| {
                    call(&pay, "payment.charge", key, &params)
                })
                .action(
                    "printer.print",
                    ActionPolicy::default(),
                    move |key, params| call(&print, "printer.print", key, &params),
                ),
        );
        let clock = Arc::new(ManualClock::new(START));
        let store = Arc::new(MemoryStore::new());
        let front = Engine::new(store.clone(), model.clone(), clock.clone(), "front");
        let mut sim = Sim {
            rng,
            clock,
            store,
            model,
            outside,
            front,
            runners: Vec::new(),
            restarts: 0,
            orders: Vec::new(),
        };
        for _ in 0..RUNNERS {
            let r = sim.runner();
            sim.runners.push(r);
        }
        sim
    }

    fn runner(&mut self) -> Runner {
        self.restarts += 1;
        let holder = format!("runner-{}", self.restarts);
        let flaky = Arc::new(Flaky::new(self.store.clone()));
        let engine = Engine::new(
            flaky.clone(),
            self.model.clone(),
            self.clock.clone(),
            holder.as_str(),
        );
        Runner {
            holder,
            engine: Arc::new(engine),
            flaky,
            tick: None,
            stopped: false,
        }
    }

    /// Runs the seed: every step, then settling, one JSON line each, and
    /// the verdict.
    fn run(mut self) {
        for step in 1..=STEPS {
            STEP.store(step, Ordering::SeqCst);
            let event = self.step();
            if !self.report(step, &event) {
                return;
            }
        }
        self.calm();
        for round in 1..=SETTLE_ROUNDS {
            let step = STEPS + round;
            STEP.store(step, Ordering::SeqCst);
            let event = self.settle_round();
            if !self.report(step, &event) {
                return;
            }
            if self.orders.iter().all(|o| self.finished(o)) {
                println!("{}", json!({ "verdict": "pass", "steps": step }));
                return;
            }
        }
        fail("nothing_lost", STEPS + SETTLE_ROUNDS);
    }

    /// Prints the step and whether every invariant holds after it.
    fn report(&self, step: u64, event: &str) -> bool {
        let seen = self.seen();
        println!(
            "{}",
            json!({ "step": step, "event": event, "state": seen.state })
        );
        match seen.broken {
            Some(invariant) => {
                fail(invariant, step);
                false
            }
            None => true,
        }
    }

    /// One event, chosen from the seed.
    fn step(&mut self) -> String {
        let i = self.rng.below(RUNNERS as u64) as usize;
        match self.rng.below(100) {
            0..10 if self.orders.len() < MAX_ORDERS => self.place(),
            10..20 => self.release(),
            20..32 => {
                let ms = [500, 5_000, 31_000][self.rng.below(3) as usize];
                self.clock.advance(ms);
                format!("clock +{ms}ms")
            }
            32..38 => {
                let calls = self.rng.below(16) as u32;
                self.runners[i].flaky.stop_after(calls);
                format!("{} stops after {calls} store calls", self.runners[i].holder)
            }
            38..44 => {
                self.runners[i].flaky.fail_on(Call::Commit);
                format!("{}'s next store write fails", self.runners[i].holder)
            }
            44..47 => {
                let r = self.runner();
                let old = std::mem::replace(&mut self.runners[i], r);
                format!("{} stops; {} starts", old.holder, self.runners[i].holder)
            }
            _ if self.runners[i].stopped => {
                let r = self.runner();
                let old = std::mem::replace(&mut self.runners[i], r);
                format!("{} restarts as {}", old.holder, self.runners[i].holder)
            }
            _ => self.tick(i),
        }
    }

    fn place(&mut self) -> String {
        let order = Order {
            id: self.rng.id(),
            checkout: self.rng.id(),
            receipt: self.rng.id(),
        };
        let total = 1 + self.rng.below(LIMIT as u64 + 200) as i64;
        let by = By::Principal("person:ann".into());
        let placed = Place {
            order: order.id,
            total,
        };
        let sub = Submission::new("order.place", placed, self.rng.id(), by.clone());
        let checkout = Checkout {
            order: order.id,
            total,
        };
        let receipt = OrderRef { order: order.id };
        now(async {
            self.front.submit(sub).await.expect("an order is placed");
            let f = &self.front;
            f.start("checkout", order.checkout, checkout, by.clone())
                .await
                .expect("checkout starts");
            f.start("receipt", order.receipt, receipt, by)
                .await
                .expect("receipt starts");
        });
        let event = format!("order {} placed, total {total}", self.orders.len());
        self.orders.push(order);
        event
    }

    /// Lets one held answer through, late: after a takeover answered too,
    /// it is the call's second answer.
    fn release(&mut self) -> String {
        let waiting: Vec<_> = {
            let o = self.outside.lock().unwrap();
            o.held
                .iter()
                .filter(|h| !h.lock().unwrap().released)
                .cloned()
                .collect()
        };
        if waiting.is_empty() {
            return "no answer held".into();
        }
        let held = &waiting[self.rng.below(waiting.len() as u64) as usize];
        let label = {
            let mut h = held.lock().unwrap();
            h.released = true;
            h.label.clone()
        };
        let resumed: Vec<String> = (0..RUNNERS).filter_map(|i| self.resume(i)).collect();
        format!("{label} answers late; {}", resumed.join(", "))
    }

    fn tick(&mut self, i: usize) -> String {
        let r = &mut self.runners[i];
        if r.tick.is_none() {
            let engine = r.engine.clone();
            r.tick = Some(Box::pin(async move { engine.tick().await }));
        }
        self.resume(i)
            .unwrap_or_else(|| format!("{} waits", self.runners[i].holder))
    }

    /// Drives runner `i`'s tick on; what came of it, if it ended.
    fn resume(&mut self, i: usize) -> Option<String> {
        let r = &mut self.runners[i];
        let fut = r.tick.as_mut()?;
        let result = drive(fut)?;
        r.tick = None;
        Some(match result {
            Ok(t) => format!("{} ticks: {} done", r.holder, t.done),
            Err(e) => {
                r.stopped = r.flaky.stopped();
                format!("{} ticks: {e}", r.holder)
            }
        })
    }

    /// From here on nothing faults: every held answer comes, every runner
    /// is replaced by a healthy one.
    fn calm(&mut self) {
        self.outside.lock().unwrap().calm = true;
        for h in &self.outside.lock().unwrap().held {
            h.lock().unwrap().released = true;
        }
        for i in 0..RUNNERS {
            self.resume(i);
            self.runners[i] = self.runner();
        }
    }

    /// Past every lease, then a tick of every runner.
    fn settle_round(&mut self) -> String {
        self.clock.advance(61_000);
        let ticks: Vec<String> = (0..RUNNERS).map(|i| self.tick(i)).collect();
        format!("settle: clock +61000ms; {}", ticks.join(", "))
    }

    fn finished(&self, o: &Order) -> bool {
        [o.checkout, o.receipt].iter().all(|p| {
            now(self.front.process(*p))
                .map(|v| v.finished.is_some())
                .unwrap_or(false)
        })
    }

    /// The state the invariants read, and the first invariant broken.
    fn seen(&self) -> Seen {
        let outside = self.outside.lock().unwrap();
        let mut broken = None;
        let mut orders = Vec::new();
        for (n, o) in self.orders.iter().enumerate() {
            let charged = outside.charged.get(&o.id).map_or(0, BTreeSet::len);
            let printed = *outside
                .printed
                .get(&step_key(o.receipt, "print"))
                .unwrap_or(&0);
            let paid = now(self.front.history(&StreamId::new("order", o.id)))
                .map(|h| h.iter().filter(|e| e.kind == "OrderPaid").count())
                .unwrap_or(0);
            let checkout = self.process(o.checkout, "charge");
            let receipt = self.process(o.receipt, "print");
            let checks = [
                ("charged_once", charged <= 1),
                ("paid_once", paid <= 1),
                ("printed_at_most_once", printed <= 1),
                (
                    "unknown_never_done",
                    !matches!(checkout.outcome, Some(Outcome::Ok(_))) || charged == 1,
                ),
                (
                    "unknown_never_done",
                    !matches!(receipt.outcome, Some(Outcome::Ok(_))) || printed == 1,
                ),
                ("nothing_lost", checkout.kept && receipt.kept),
            ];
            if broken.is_none() {
                broken = checks
                    .iter()
                    .find(|(_, holds)| !holds)
                    .map(|(name, _)| *name);
            }
            orders.push(json!({
                "order": n,
                "charged": charged,
                "paid": paid,
                "printed": printed,
                "checkout": checkout.status,
                "receipt": receipt.status,
            }));
        }
        let runners: Vec<Value> = self
            .runners
            .iter()
            .map(|r| {
                let state = match (&r.tick, r.stopped) {
                    (_, true) => "stopped",
                    (Some(_), _) => "waiting",
                    (None, _) => "idle",
                };
                json!({ "holder": r.holder, "state": state })
            })
            .collect();
        let held = outside
            .held
            .iter()
            .filter(|h| !h.lock().unwrap().released)
            .count();
        Seen {
            state: json!({
                "now_ms": self.clock_ms(),
                "runners": runners,
                "held": held,
                "orders": orders,
            }),
            broken,
        }
    }

    fn clock_ms(&self) -> i64 {
        self.front.now().0 - START.0
    }

    /// How one process stands: its step's outcome, a short status, and
    /// whether the store still keeps what will move it on.
    fn process(&self, id: Uuid, step: &str) -> Standing {
        let Ok(view) = now(self.front.process(id)) else {
            return Standing {
                outcome: None,
                status: "missing".into(),
                kept: false,
            };
        };
        let outcome = view.progress.outcome(step).cloned();
        let steps: Vec<String> = view
            .progress
            .steps
            .iter()
            .map(|(k, o)| format!("{k}:{}", outcome_name(o)))
            .collect();
        let mut status = steps.join(" ");
        if let Some(f) = &view.finished {
            let f = serde_json::to_value(f).unwrap_or(Value::Null);
            let end = f.as_object().and_then(|m| m.keys().next().cloned());
            status = format!("{status} -> {}", end.unwrap_or_default());
        }
        let kept = view.finished.is_some()
            || self.row(WorkKind::Advance, id)
            || matches!(&view.pending, Some((k, Step::Call { .. })) if self.row(WorkKind::Action, step_key(id, k)));
        Standing {
            outcome,
            status: status.trim().to_string(),
            kept,
        }
    }

    fn row(&self, kind: WorkKind, id: Uuid) -> bool {
        matches!(now(self.store.work(kind, id)), Ok(Some(_)))
    }
}

struct Standing {
    outcome: Option<Outcome>,
    status: String,
    kept: bool,
}

fn outcome_name(o: &Outcome) -> &'static str {
    match o {
        Outcome::Ok(_) => "ok",
        Outcome::Rejected(_) => "rejected",
        Outcome::Failed(_) => "failed",
        Outcome::Unknown => "unknown",
        Outcome::TimedOut => "timed_out",
        Outcome::Cancelled => "cancelled",
    }
}

fn fail(invariant: &str, step: u64) {
    println!(
        "{}",
        json!({ "verdict": "fail", "invariant": invariant, "step": step })
    );
}

/// Runs one seed; a panic of the system's code is a failed invariant.
pub fn run(seed: &str) {
    std::panic::set_hook(Box::new(|_| {}));
    let sim = Sim::new(seed);
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sim.run())) {
        let msg = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        println!(
            "{}",
            json!({ "verdict": "fail", "invariant": format!("panic: {msg}"), "step": STEP.load(Ordering::SeqCst) })
        );
    }
}
