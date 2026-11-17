//! A small shop, declared the way a program declares its business: the
//! domain every scenario runs.

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use factum::{
    ActionAnswer, ActionPolicy, Aggregate, By, Command, Decision, Event, Instant, Model, Next,
    OnFailure, Outcome, Process, Progress, Projection, Recurring, Rejection, RetryPolicy, RowOp,
    StartsOn, StreamId, Table, TaskType, Upcast, Uuid, Value,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

// ---- orders ----

#[derive(Default, Serialize, Deserialize)]
pub struct Order {
    pub status: String,
    pub total: i64,
    pub currency: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum OrderEvent {
    OrderPlaced { total: i64, currency: String },
    OrderPaid { charge: Value },
    OrderCancelled { reason: String },
    OrderClosed,
}

pub struct Orders;

fn add_currency(mut data: Value) -> Value {
    data["currency"] = json!("JPY");
    data
}

impl Aggregate for Orders {
    const NAME: &'static str = "order";
    type State = Order;
    type Event = OrderEvent;

    fn evolve(state: &mut Order, event: OrderEvent) {
        match event {
            OrderEvent::OrderPlaced { total, currency } => {
                state.status = "placed".into();
                state.total = total;
                state.currency = currency;
            }
            OrderEvent::OrderPaid { .. } => state.status = "paid".into(),
            OrderEvent::OrderCancelled { .. } => state.status = "cancelled".into(),
            OrderEvent::OrderClosed => state.status = "closed".into(),
        }
    }

    fn ending(kind: &str) -> bool {
        kind == "OrderClosed"
    }

    fn upcasts() -> Vec<(&'static str, Upcast)> {
        vec![("OrderPlaced", add_currency as Upcast)]
    }
}

/// How long a placed order waits to be paid before it expires.
pub const EXPIRES_MS: i64 = 30 * 60 * 1000;

#[derive(Serialize, Deserialize)]
pub struct OrderRef {
    pub order: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct Place {
    pub order: Uuid,
    pub total: i64,
}

pub struct PlaceOrder;

impl Command for PlaceOrder {
    const NAME: &'static str = "order.place";
    type Aggregate = Orders;
    type Params = Place;
    fn stream(p: &Place) -> Uuid {
        p.order
    }
    fn decide(
        s: &Order,
        p: &Place,
        _: &By,
        now: Instant,
    ) -> Result<Decision<OrderEvent>, Rejection> {
        if !s.status.is_empty() {
            return Err(Rejection::new(
                "already_placed",
                "the order is placed already",
            ));
        }
        let placed = OrderEvent::OrderPlaced {
            total: p.total,
            currency: "JPY".into(),
        };
        Ok(Decision::from(vec![placed]).schedule(
            now.plus(EXPIRES_MS),
            "order.expire",
            OrderRef { order: p.order },
        ))
    }
}

#[derive(Serialize, Deserialize)]
pub struct Pay {
    pub order: Uuid,
    pub charge: Value,
}

pub struct PayOrder;

impl Command for PayOrder {
    const NAME: &'static str = "order.pay";
    type Aggregate = Orders;
    type Params = Pay;
    fn stream(p: &Pay) -> Uuid {
        p.order
    }
    fn decide(s: &Order, p: &Pay, _: &By, _: Instant) -> Result<Decision<OrderEvent>, Rejection> {
        placed(s)?;
        Ok(vec![OrderEvent::OrderPaid {
            charge: p.charge.clone(),
        }]
        .into())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Cancel {
    pub order: Uuid,
    pub reason: String,
}

pub struct CancelOrder;

impl Command for CancelOrder {
    const NAME: &'static str = "order.cancel";
    type Aggregate = Orders;
    type Params = Cancel;
    fn stream(p: &Cancel) -> Uuid {
        p.order
    }
    fn decide(
        s: &Order,
        p: &Cancel,
        _: &By,
        _: Instant,
    ) -> Result<Decision<OrderEvent>, Rejection> {
        placed(s)?;
        Ok(vec![OrderEvent::OrderCancelled {
            reason: p.reason.clone(),
        }]
        .into())
    }
}

pub struct ExpireOrder;

impl Command for ExpireOrder {
    const NAME: &'static str = "order.expire";
    type Aggregate = Orders;
    type Params = OrderRef;
    fn stream(p: &OrderRef) -> Uuid {
        p.order
    }
    fn decide(
        s: &Order,
        _: &OrderRef,
        _: &By,
        _: Instant,
    ) -> Result<Decision<OrderEvent>, Rejection> {
        placed(s)?;
        Ok(vec![OrderEvent::OrderCancelled {
            reason: "expired".into(),
        }]
        .into())
    }
}

pub struct CloseOrder;

impl Command for CloseOrder {
    const NAME: &'static str = "order.close";
    type Aggregate = Orders;
    type Params = OrderRef;
    fn stream(p: &OrderRef) -> Uuid {
        p.order
    }
    fn decide(
        s: &Order,
        _: &OrderRef,
        _: &By,
        _: Instant,
    ) -> Result<Decision<OrderEvent>, Rejection> {
        if s.status != "paid" {
            return Err(Rejection::new("not_paid", "only a paid order closes"));
        }
        Ok(vec![OrderEvent::OrderClosed].into())
    }
}

fn placed(s: &Order) -> Result<(), Rejection> {
    if s.status == "placed" {
        Ok(())
    } else {
        Err(Rejection::new(
            "not_placed",
            format!("the order is {:?}", s.status),
        ))
    }
}

/// The order aggregate as older code declared it: `OrderPlaced` had no
/// currency, and was recorded at version 1.
pub mod old {
    use super::*;

    pub struct OldOrders;

    #[derive(Serialize, Deserialize)]
    #[serde(tag = "type", content = "data")]
    pub enum OldOrderEvent {
        OrderPlaced { total: i64 },
    }

    #[derive(Default, Serialize, Deserialize)]
    pub struct OldOrder {
        pub total: i64,
    }

    impl Aggregate for OldOrders {
        const NAME: &'static str = "order";
        type State = OldOrder;
        type Event = OldOrderEvent;
        fn evolve(state: &mut OldOrder, OldOrderEvent::OrderPlaced { total }: OldOrderEvent) {
            state.total = total;
        }
    }

    pub struct OldPlace;

    impl Command for OldPlace {
        const NAME: &'static str = "order.place";
        type Aggregate = OldOrders;
        type Params = Place;
        fn stream(p: &Place) -> Uuid {
            p.order
        }
        fn decide(
            _: &OldOrder,
            p: &Place,
            _: &By,
            _: Instant,
        ) -> Result<Decision<OldOrderEvent>, Rejection> {
            Ok(vec![OldOrderEvent::OrderPlaced { total: p.total }].into())
        }
    }
}

// ---- counters ----

#[derive(Default, Serialize, Deserialize)]
pub struct Counter {
    pub total: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum CounterEvent {
    Added { n: i64 },
}

pub struct Counters;

impl Aggregate for Counters {
    const NAME: &'static str = "counter";
    type State = Counter;
    type Event = CounterEvent;
    fn evolve(state: &mut Counter, CounterEvent::Added { n }: CounterEvent) {
        state.total += n;
    }
}

/// The same counter, folded by other code: it reads no snapshot `Counters` kept.
pub struct CountersRebuilt;

impl Aggregate for CountersRebuilt {
    const NAME: &'static str = "counter";
    type State = Counter;
    type Event = CounterEvent;
    /// Counts every addition twice: new code whose answers differ from the
    /// old code's, so a snapshot the old code kept would show.
    fn evolve(state: &mut Counter, CounterEvent::Added { n }: CounterEvent) {
        state.total += 2 * n;
    }
    fn code() -> &'static str {
        "rebuilt"
    }
}

#[derive(Serialize, Deserialize)]
pub struct Add {
    pub counter: Uuid,
    pub n: i64,
}

pub struct AddTo<A>(std::marker::PhantomData<A>);

impl<A: Aggregate<State = Counter, Event = CounterEvent>> Command for AddTo<A> {
    const NAME: &'static str = "counter.add";
    type Aggregate = A;
    type Params = Add;
    fn stream(p: &Add) -> Uuid {
        p.counter
    }
    fn decide(
        _: &Counter,
        p: &Add,
        _: &By,
        _: Instant,
    ) -> Result<Decision<CounterEvent>, Rejection> {
        if p.n == 0 {
            return Err(Rejection::new("nothing_to_add", "add at least one"));
        }
        Ok(vec![CounterEvent::Added { n: p.n }].into())
    }
}

pub type AddCounter = AddTo<Counters>;

/// A counter whose code has a bug: its state does not read back what it
/// writes, so every decide on it fails before the command is decided.
pub mod unreadable {
    use super::*;

    #[derive(Default, Serialize, Deserialize)]
    pub struct Unreadable {
        #[serde(skip_serializing)]
        pub total: i64,
    }

    pub struct UnreadableCounters;

    impl Aggregate for UnreadableCounters {
        const NAME: &'static str = "counter";
        type State = Unreadable;
        type Event = CounterEvent;
        fn evolve(state: &mut Unreadable, CounterEvent::Added { n }: CounterEvent) {
            state.total += n;
        }
    }

    pub struct AddUnreadable;

    impl Command for AddUnreadable {
        const NAME: &'static str = "counter.add";
        type Aggregate = UnreadableCounters;
        type Params = Add;
        fn stream(p: &Add) -> Uuid {
            p.counter
        }
        fn decide(
            _: &Unreadable,
            p: &Add,
            _: &By,
            _: Instant,
        ) -> Result<Decision<CounterEvent>, Rejection> {
            Ok(vec![CounterEvent::Added { n: p.n }].into())
        }
    }
}

/// The counter the recurring tally adds to.
pub const TALLY: Uuid = Uuid::from_u128(0x7a11);

// ---- processes ----

#[derive(Serialize, Deserialize)]
pub struct Checkout {
    pub order: Uuid,
    pub total: i64,
}

pub struct CheckoutProcess;

impl Process for CheckoutProcess {
    const NAME: &'static str = "checkout";
    type Input = Checkout;
    fn next(i: &Checkout, p: &Progress) -> Next {
        match p.outcome("charge") {
            None => Next::call(
                "charge",
                "payment.charge",
                json!({ "order": i.order, "amount": i.total }),
            ),
            Some(Outcome::Ok(charge)) => match p.outcome("pay") {
                None => Next::run(
                    "pay",
                    "order.pay",
                    json!({ "order": i.order, "charge": charge }),
                ),
                Some(Outcome::Ok(_)) => Next::complete(json!({ "charged": charge })),
                Some(other) => Next::stuck(format!("charged but not paid: {other:?}")),
            },
            Some(Outcome::Rejected(r)) => match p.outcome("cancel") {
                None => Next::run(
                    "cancel",
                    "order.cancel",
                    json!({ "order": i.order, "reason": r.code }),
                ),
                Some(_) => Next::fail(r.code.clone()),
            },
            Some(other) => Next::fail(format!("charge: {other:?}")),
        }
    }
}

pub struct ReceiptProcess;

impl Process for ReceiptProcess {
    const NAME: &'static str = "receipt";
    type Input = OrderRef;
    fn next(i: &OrderRef, p: &Progress) -> Next {
        match p.outcome("print") {
            None => Next::call_at_most_once("print", "printer.print", json!({ "order": i.order })),
            Some(Outcome::Ok(_)) => Next::complete(json!("printed")),
            Some(Outcome::Unknown) => Next::stuck("the printer may or may not have printed"),
            Some(other) => Next::fail(format!("{other:?}")),
        }
    }
}

/// The recorded event a process that starts on one is given.
#[derive(Serialize, Deserialize)]
pub struct Recorded {
    pub id: Uuid,
    pub stream: StreamId,
    #[serde(rename = "type")]
    pub kind: String,
    pub data: Value,
}

pub struct FulfilProcess;

impl Process for FulfilProcess {
    const NAME: &'static str = "fulfil";
    type Input = Recorded;
    fn starts_on() -> Option<StartsOn> {
        Some(StartsOn {
            aggregate: "order".into(),
            event: "OrderPaid".into(),
        })
    }
    fn next(i: &Recorded, p: &Progress) -> Next {
        task_outcome(
            p,
            || Next::queue("pack", "pack", json!({ "order": i.stream.id })),
            "pack",
        )
    }
}

#[derive(Serialize, Deserialize)]
pub struct Print {
    pub order: Uuid,
}

pub struct PrintProcess;

impl Process for PrintProcess {
    const NAME: &'static str = "print";
    type Input = Print;
    fn next(i: &Print, p: &Progress) -> Next {
        task_outcome(
            p,
            || Next::queue("print", "print", json!({ "order": i.order })),
            "print",
        )
    }
}

#[derive(Serialize, Deserialize)]
pub struct Delivery {
    pub order: Uuid,
    pub store: String,
}

pub struct DeliveryProcess;

impl Process for DeliveryProcess {
    const NAME: &'static str = "delivery";
    type Input = Delivery;
    fn next(i: &Delivery, p: &Progress) -> Next {
        task_outcome(
            p,
            || Next::queue_ordered("deliver", "deliver", json!({ "order": i.order }), &i.store),
            "deliver",
        )
    }
}

fn task_outcome(p: &Progress, queue: impl FnOnce() -> Next, key: &str) -> Next {
    match p.outcome(key) {
        None => queue(),
        Some(Outcome::Ok(v)) => Next::complete(v.clone()),
        Some(Outcome::Unknown) => Next::stuck("unknown"),
        Some(Outcome::Cancelled) => Next::fail("cancelled"),
        Some(other) => Next::fail(format!("{other:?}")),
    }
}

/// Counts the order, then has it packed: a command step, then a task.
pub struct LabelProcess;

impl Process for LabelProcess {
    const NAME: &'static str = "label";
    type Input = OrderRef;
    fn next(i: &OrderRef, p: &Progress) -> Next {
        match p.outcome("count") {
            None => Next::run(
                "count",
                "counter.add",
                json!({ "counter": i.order, "n": 1 }),
            ),
            Some(_) => task_outcome(
                p,
                || Next::queue("pack", "pack", json!({ "order": i.order })),
                "pack",
            ),
        }
    }
}

pub struct PickupProcess;

/// How long a paid order waits to be picked up.
pub const PICKUP_MS: i64 = 60 * 60 * 1000;

impl Process for PickupProcess {
    const NAME: &'static str = "pickup";
    type Input = OrderRef;
    fn next(i: &OrderRef, p: &Progress) -> Next {
        match p.outcome("closed") {
            None => Next::await_event(
                "closed",
                StreamId::new("order", i.order),
                "OrderClosed",
                PICKUP_MS,
            ),
            Some(Outcome::Ok(e)) => Next::complete(e["id"].clone()),
            Some(Outcome::TimedOut) => Next::fail("not picked up"),
            Some(other) => Next::stuck(format!("{other:?}")),
        }
    }
}

// ---- other systems, stood in for ----

/// A payment service that charges once per idempotency key.
#[derive(Default)]
pub struct Payments {
    pub calls: AtomicU32,
    pub charged: Mutex<HashSet<Uuid>>,
    /// Answer "try later" this many more times.
    pub busy: AtomicU32,
}

/// Amounts above this are declined.
pub const LIMIT: i64 = 1000;

impl Payments {
    fn charge(&self, key: Uuid, params: &Value) -> ActionAnswer {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.busy.load(Ordering::SeqCst) > 0 {
            self.busy.fetch_sub(1, Ordering::SeqCst);
            return ActionAnswer::Passing("busy".into());
        }
        if params["amount"].as_i64().unwrap_or(0) > LIMIT {
            return ActionAnswer::Rejected(Rejection::new("declined", "over the limit"));
        }
        self.charged.lock().unwrap().insert(key);
        ActionAnswer::Done(json!({ "charge": key }))
    }

    pub fn charges(&self) -> usize {
        self.charged.lock().unwrap().len()
    }

    pub fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

/// A printer that prints every time it is asked, key or no key.
#[derive(Default)]
pub struct Printer {
    pub printed: AtomicU32,
}

// ---- projections ----

/// Orders by id, with their status.
pub struct OrderList {
    pub version: u32,
    /// Fails on orders of this total.
    pub fails_on: Option<i64>,
    pub on_failure: OnFailure,
    /// For orders of this total, also writes a table it does not declare.
    pub strays_on: Option<i64>,
    /// Declares one more column, changing its tables.
    pub with_customer: bool,
}

impl Default for OrderList {
    fn default() -> Self {
        OrderList {
            version: 1,
            fails_on: None,
            on_failure: OnFailure::Skip,
            strays_on: None,
            with_customer: false,
        }
    }
}

impl Projection for OrderList {
    fn name(&self) -> &str {
        "orders"
    }
    fn version(&self) -> u32 {
        self.version
    }
    fn tables(&self) -> Vec<Table> {
        let mut columns = vec!["status", "total", "currency"];
        if self.with_customer {
            columns.push("customer");
        }
        vec![Table::new("orders", &columns).index(&["status", "total"])]
    }
    fn on_failure(&self) -> OnFailure {
        self.on_failure
    }
    fn project(&self, e: &Event) -> Result<Vec<RowOp>, String> {
        if e.stream.aggregate != "order" {
            return Ok(Vec::new());
        }
        let key = e.stream.id.to_string();
        let status = |s: &str| RowOp::Merge {
            table: "orders".into(),
            key: key.clone(),
            fields: json!({ "status": s }),
        };
        Ok(match e.kind.as_str() {
            "OrderPlaced" if Some(e.data["total"].as_i64().unwrap_or(0)) == self.strays_on => {
                vec![RowOp::Put {
                    table: "strays".into(),
                    key,
                    row: json!({}),
                }]
            }
            "OrderPlaced" => {
                if Some(e.data["total"].as_i64().unwrap_or(0)) == self.fails_on {
                    return Err("cannot list this order".into());
                }
                vec![RowOp::Put {
                    table: "orders".into(),
                    key,
                    row: json!({ "status": "placed", "total": e.data["total"], "currency": e.data["currency"] }),
                }]
            }
            "OrderPaid" => vec![status("paid")],
            "OrderCancelled" => vec![status("cancelled")],
            "OrderClosed" => vec![status("closed")],
            _ => Vec::new(),
        })
    }
}

// ---- the model ----

pub fn task_types() -> Vec<TaskType> {
    vec![
        TaskType {
            retry: RetryPolicy {
                attempts: 2,
                backoff_ms: 1_000,
                ..RetryPolicy::default()
            },
            timeout_ms: Some(60_000),
            ..TaskType::new("pack")
        },
        TaskType {
            at_most_once: true,
            ..TaskType::new("print")
        },
        TaskType {
            key_interval_ms: 10_000,
            ..TaskType::new("deliver")
        },
    ]
}

/// The shop, with its stand-ins and the projection as given.
pub fn shop(
    payments: std::sync::Arc<Payments>,
    printer: std::sync::Arc<Printer>,
    orders: OrderList,
) -> Model {
    let mut model = Model::new()
        .command::<PlaceOrder>()
        .command::<PayOrder>()
        .command::<CancelOrder>()
        .command::<ExpireOrder>()
        .command::<CloseOrder>()
        .command::<AddCounter>()
        .process::<CheckoutProcess>()
        .process::<ReceiptProcess>()
        .process::<FulfilProcess>()
        .process::<PrintProcess>()
        .process::<DeliveryProcess>()
        .process::<PickupProcess>()
        .process::<LabelProcess>()
        .action(
            "payment.charge",
            ActionPolicy {
                retry: RetryPolicy {
                    attempts: 3,
                    backoff_ms: 1_000,
                    factor: 2,
                    max_backoff_ms: 60_000,
                    jitter_percent: 0,
                },
                timeout_ms: 30_000,
            },
            move |key, params| {
                let answer = payments.charge(key, &params);
                async move { answer }
            },
        )
        .action("printer.print", ActionPolicy::default(), move |_, _| {
            printer.printed.fetch_add(1, Ordering::SeqCst);
            async { ActionAnswer::Done(json!("ok")) }
        })
        .recurring(Recurring {
            name: "shop.tally".into(),
            command: "counter.add".into(),
            params: json!({ "counter": TALLY, "n": 1 }),
            every_seconds: 3600,
            offset_seconds: 0,
        })
        .projection(orders);
    for t in task_types() {
        model = model.task(t);
    }
    model
}

/// Checkout as a later version declares it: its first step has another key,
/// so the runs the old version left would charge again.
pub struct CheckoutRenamed;

impl Process for CheckoutRenamed {
    const NAME: &'static str = "checkout";
    type Input = Checkout;
    fn next(i: &Checkout, p: &Progress) -> Next {
        match p.outcome("charge-card") {
            None => Next::call(
                "charge-card",
                "payment.charge",
                json!({ "order": i.order, "amount": i.total }),
            ),
            Some(_) => Next::complete(json!("done")),
        }
    }
}
