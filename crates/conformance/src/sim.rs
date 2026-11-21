//! A seeded simulation of the engine, run by `truefit sim` (`truefit.toml`)
//! through the `sim` binary: from one seed, an order of submissions, ticks
//! of several runners, clock moves and faults — a runner stopping between
//! any two store calls of a tick, a store write failing, a call to another
//! system answering late, twice or never — with factum's promises about
//! the work that outlives a command checked after every step.
//!
//! A runner's step is one store write (a commit, or a claim that took a
//! row) or one call to another system: its tick pauses after each, so
//! the runners' ticks interleave write by write. Each step's line is a
//! state of `models/ExactlyOnce.tla`, and a run is checked as its trace
//! (`truefit model --trace`).
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
    ActionAnswer, ActionPolicy, BoxFuture, By, Engine, Error, Instant, ManualClock, MemoryStore,
    Model, Outcome, Rejection, Step, Store, StreamId, Submission, Tick, Uuid, Value,
};
use factum_core::{Finished, Work, WorkKind, step_key};
use serde_json::json;

use crate::START;
use crate::domain::{
    CancelOrder, Checkout, CheckoutProcess, ExpireOrder, LIMIT, OrderRef, PayOrder, Place,
    PlaceOrder, ReceiptProcess, charge_policy,
};
use crate::flaky::{Call, Flaky, short};

/// How many steps one seed runs before it settles.
const STEPS: u64 = 400;
const RUNNERS: usize = 3;
const MAX_ORDERS: usize = 6;
/// How many steps settling may take before what is left counts as lost.
const SETTLE_STEPS: u64 = 3_000;

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
/// The step the future polled last took, a store write or a call, once it
/// took one: empty until then.
static WROTE: Mutex<String> = Mutex::new(String::new());

/// Ends a runner's step: its tick goes on when it is next driven.
fn pause(what: String) -> BoxFuture<()> {
    let mut what = Some(what);
    Box::pin(std::future::poll_fn(move |_| match what.take() {
        Some(w) => {
            *WROTE.lock().unwrap() = w;
            Poll::Pending
        }
        None => Poll::Ready(()),
    }))
}
/// The step running, for a panic's verdict.
static STEP: AtomicU64 = AtomicU64::new(0);

fn call(outside: &Shared, action: &str, key: Uuid, params: &Value) -> BoxFuture<ActionAnswer> {
    let mut o = outside.lock().unwrap();
    let mode = if o.calm { 0 } else { o.rng.below(10) };
    // A key it charged before answers that charge, never "try later": the
    // payment system keeps each key's answer, as an at-least-once call needs.
    let replays = action == "payment.charge" && o.charged.values().any(|ks| ks.contains(&key));
    let label = format!("{action} {}", short(key));
    let busy = mode == 9 && !replays;
    let order: Option<Uuid> = params["order"].as_str().and_then(|s| s.parse().ok());
    let answer = if busy {
        ActionAnswer::Passing("busy".into())
    } else if action == "printer.print" {
        *o.printed.entry(key).or_default() += 1;
        ActionAnswer::Done(json!("ok"))
    } else if action == "payment.find" {
        let charge = o.charged.get(&order.unwrap_or_default());
        let charge = charge.and_then(|keys| keys.first());
        ActionAnswer::Done(charge.map_or(Value::Null, |k| json!({ "charge": k })))
    } else if params["amount"].as_i64().unwrap_or(0) > LIMIT {
        ActionAnswer::Rejected(Rejection::new("declined", "over the limit"))
    } else {
        o.charged
            .entry(order.unwrap_or_default())
            .or_default()
            .insert(key);
        ActionAnswer::Done(json!({ "charge": key }))
    };
    if busy || mode < 7 {
        let what = format!("calls {label}{}", if busy { ": busy" } else { "" });
        return Box::pin(async move {
            pause(what).await;
            answer
        });
    }
    *WROTE.lock().unwrap() = format!("calls {label}: the answer is held");
    let held = Arc::new(Mutex::new(Held {
        label,
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

/// Polls `fut` until it is ready, pauses after a step, or waits on a
/// held answer.
fn drive<T>(fut: &mut Pin<Box<dyn Future<Output = T> + Send>>) -> Option<T> {
    let mut cx = Context::from_waker(Waker::noop());
    WROTE.lock().unwrap().clear();
    for _ in 0..100_000 {
        BLOCKED.store(false, Ordering::SeqCst);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return Some(v),
            Poll::Pending if BLOCKED.load(Ordering::SeqCst) => return None,
            Poll::Pending if !WROTE.lock().unwrap().is_empty() => return None,
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
    total: i64,
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
    /// The runners still to tick in this round of settling.
    round: Vec<usize>,
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
        let (pay, find, print) = (outside.clone(), outside.clone(), outside.clone());
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
                .action("payment.find", charge_policy(), move |key, params| {
                    call(&find, "payment.find", key, &params)
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
            round: Vec::new(),
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
        let flaky = Arc::new(Flaky::new(self.store.clone()).pausing(pause));
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
        for n in 1..=SETTLE_STEPS {
            let step = STEPS + n;
            STEP.store(step, Ordering::SeqCst);
            let event = self.settle_step();
            if !self.report(step, &event) {
                return;
            }
            if self.round.is_empty() && self.orders.iter().all(|o| self.finished(o)) {
                println!("{}", json!({ "verdict": "pass", "steps": step }));
                return;
            }
        }
        fail("nothing_lost", STEPS + SETTLE_STEPS);
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
            total: 1 + self.rng.below(LIMIT as u64 + 200) as i64,
        };
        let total = order.total;
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
    /// it is the call's second answer. Its runner goes on when next ticked.
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
        let mut h = held.lock().unwrap();
        h.released = true;
        format!("{} answers late", h.label)
    }

    fn tick(&mut self, i: usize) -> String {
        let r = &mut self.runners[i];
        if r.tick.is_none() {
            let engine = r.engine.clone();
            r.tick = Some(Box::pin(async move { engine.tick().await }));
        }
        let r = &mut self.runners[i];
        let fut = r.tick.as_mut().expect("a tick to drive");
        let Some(result) = drive(fut) else {
            // A step, or a call whose answer is held, which is a step too.
            let wrote = WROTE.lock().unwrap();
            if wrote.is_empty() {
                return format!("{} waits", r.holder);
            }
            return format!("{} {wrote}", r.holder);
        };
        r.tick = None;
        match result {
            Ok(t) => format!("{} ticks: {} done", r.holder, t.done),
            Err(e) => {
                r.stopped = r.flaky.stopped();
                format!("{} ticks: {e}", r.holder)
            }
        }
    }

    /// From here on nothing faults: every held answer comes, and every
    /// runner stops where it is and is replaced by a healthy one.
    fn calm(&mut self) {
        self.outside.lock().unwrap().calm = true;
        for h in &self.outside.lock().unwrap().held {
            h.lock().unwrap().released = true;
        }
        for i in 0..RUNNERS {
            self.runners[i] = self.runner();
        }
    }

    /// A step of settling: rounds of the clock moving past every lease,
    /// then every runner's tick to its end, a step at a time.
    fn settle_step(&mut self) -> String {
        let Some(&i) = self.round.first() else {
            self.clock.advance(61_000);
            self.round = (0..RUNNERS).collect();
            return "settle: clock +61000ms".into();
        };
        let event = self.tick(i);
        if self.runners[i].tick.is_none() || event.ends_with(" waits") {
            self.round.remove(0);
        }
        event
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
        for o in &self.orders {
            let charged = outside.charged.get(&o.id).map_or(0, BTreeSet::len);
            let printed = *outside
                .printed
                .get(&step_key(o.receipt, "print"))
                .unwrap_or(&0);
            let history =
                now(self.front.history(&StreamId::new("order", o.id))).unwrap_or_default();
            let count = |kind: &str| history.iter().filter(|e| e.kind == kind).count();
            let (paid, cancelled) = (count("OrderPaid"), count("OrderCancelled"));
            let checkout = self.process(o.checkout, "charge", &["charge", "find", "pay", "cancel"]);
            let receipt = self.process(o.receipt, "print", &["print"]);
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
                (
                    "failed_never_done",
                    !matches!(checkout.outcome, Some(Outcome::Failed(_))) || charged == 0,
                ),
                (
                    "failed_never_done",
                    !matches!(receipt.outcome, Some(Outcome::Failed(_))) || printed == 0,
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
                "over": o.total > LIMIT,
                "charged": charged,
                "paid": paid,
                "cancelled": cancelled,
                "printed": printed,
                "checkout": checkout.state,
                "receipt": receipt.state,
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

    /// How one process stands: its step's outcome, its state as the model
    /// has it (each step's outcome, the call it waits on, how it ended, its
    /// rows), and whether the store still keeps what will move it on.
    fn process(&self, id: Uuid, step: &str, keys: &[&str]) -> Standing {
        let Ok(view) = now(self.front.process(id)) else {
            return Standing {
                outcome: None,
                state: Value::Null,
                kept: false,
            };
        };
        let outcome = view.progress.outcome(step).cloned();
        let steps: serde_json::Map<String, Value> = keys
            .iter()
            .map(|k| {
                let o = view.progress.outcome(k).map_or("", outcome_name);
                (k.to_string(), json!(o))
            })
            .collect();
        let pending = match &view.pending {
            Some((k, Step::Call { .. })) => k.clone(),
            _ => String::new(),
        };
        let end = match &view.finished {
            Some(Finished::Completed(_)) => "completed",
            Some(Finished::Failed(_)) => "failed",
            Some(Finished::Stuck(_)) => "stuck",
            None => "",
        };
        let advance = self.work(WorkKind::Advance, id);
        let action = if pending.is_empty() {
            None
        } else {
            self.work(WorkKind::Action, step_key(id, &pending))
        };
        let kept = view.finished.is_some() || advance.is_some() || action.is_some();
        Standing {
            outcome,
            state: json!({
                "steps": steps,
                "pending": pending,
                "end": end,
                "advance": row_state(advance.as_ref()),
                "action": row_state(action.as_ref()),
            }),
            kept,
        }
    }

    fn work(&self, kind: WorkKind, id: Uuid) -> Option<Work> {
        now(self.store.work(kind, id)).ok().flatten()
    }
}

struct Standing {
    outcome: Option<Outcome>,
    state: Value,
    kept: bool,
}

/// A work row as the model has it; one that is not there has every field
/// false or 0.
fn row_state(row: Option<&Work>) -> Value {
    json!({
        "exists": row.is_some(),
        "held": row.is_some_and(|r| r.lease.is_some()),
        "attempts": row.map_or(0, |r| r.attempts),
        "taken_over": row.is_some_and(|r| r.taken_over),
        "waiting": row.is_some_and(|r| r.due == Instant::NEVER),
    })
}

/// A step's outcome in a word; an answer of nothing (`payment.find`
/// finding no charge) is `none`.
fn outcome_name(o: &Outcome) -> &'static str {
    match o {
        Outcome::Ok(Value::Null) => "none",
        Outcome::Ok(_) => "ok",
        Outcome::Rejected(_) => "rejected",
        Outcome::Failed(_) => "failed",
        Outcome::Unknown(_) => "unknown",
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
