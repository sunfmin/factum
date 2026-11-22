//! The work that outlives a command: advancing processes, delivering
//! actions, handing tasks to workers, running schedules and waits.

use factum_core::{
    Advance, Attempt, By, Event, Instant, Meta, NewEvent, Outcome, PROCESS, ProcessEvent,
    RECURRING_RAN, Recorded, Rejection, SCHEDULE_RAN, Step, StreamId, Uuid, Value, Version, Work,
    WorkChange, WorkKind, advance, codes, recurring_key, recurring_stream, schedule_key,
    schedule_stream, step_key,
};
use factum_store::{Claim, Store, StoreError, Unit};
use serde_json::json;

use crate::engine::{
    Engine, Error, Fault, Races, Submission, Then, Write, new_event, process_write, rejected,
    set_aside,
};
use crate::model::ActionAnswer;

/// A task a worker holds.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaimedTask {
    pub id: Uuid,
    pub task_type: String,
    pub input: Value,
    /// Present it with every heartbeat and the result.
    pub lease: Uuid,
    /// 1 for the first attempt.
    pub attempt: u32,
    pub process: Option<Uuid>,
    pub step: Option<String>,
}

/// The `Fault::item` a tick names a faulted work row by.
fn item(row: &Work) -> String {
    match row.kind {
        WorkKind::Advance => format!("{PROCESS}/{}", row.id),
        WorkKind::Action => format!("action {} {}", row.name, row.id),
        WorkKind::Task => format!("task {} {}", row.name, row.id),
        WorkKind::Schedule => format!("schedule {}", row.id),
        WorkKind::Await => format!("wait {}", row.id),
    }
}

/// `e` as a process is given it.
pub(crate) fn event_json(e: &Event) -> Value {
    serde_json::to_value(Recorded::from(e)).unwrap_or(Value::Null)
}

impl<S: Store> Engine<S> {
    fn claim(
        &self,
        kind: WorkKind,
        names: Option<Vec<String>>,
        now: Instant,
        lease_ms: i64,
    ) -> Claim {
        Claim {
            kind,
            names,
            now,
            holder: self.holder.clone(),
            lease_ms,
            limit: self.options.batch,
        }
    }

    /// Claims the rows `claim` names and runs each; a row whose run faults
    /// is set aside and the rest go on, a store error ends the loop. How
    /// many ran.
    async fn drain<Fut>(
        &self,
        claim: Claim,
        faults: &mut Vec<Fault>,
        run: impl Fn(Work) -> Fut,
    ) -> Result<usize, Error>
    where
        Fut: Future<Output = Result<(), Error>>,
    {
        let mut count = 0;
        for row in self.store.claim(claim).await? {
            let label = item(&row);
            if set_aside(run(row).await, || label, faults)?.is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    // ---- processes ----

    pub(crate) async fn advance_due(
        &self,
        now: Instant,
        faults: &mut Vec<Fault>,
    ) -> Result<usize, Error> {
        let claim = self.claim(WorkKind::Advance, None, now, self.options.advance_lease_ms);
        self.drain(claim, faults, move |row| async move {
            self.advance_one(&row, now).await
        })
        .await
    }

    /// Moves one process on as far as it goes without waiting, under the
    /// lease of its row.
    async fn advance_one(&self, row: &Work, now: Instant) -> Result<(), Error> {
        let id = row.id;
        let lease = row.lease.expect("a claimed row has a lease");
        let guard = vec![(WorkKind::Advance, id, lease)];
        let stream = StreamId::new(PROCESS, id);
        let by = By::Process(stream.clone());
        let release = |due| WorkChange::Release {
            kind: WorkKind::Advance,
            id,
            lease,
            due,
        };
        let mut races = Races::new();
        loop {
            let (state, version) = self.process_events(id).await?;
            let decision = match self.model.processes.get(&state.name) {
                Some(def) => advance(&state, def.as_ref()),
                None => Advance::Finish(factum_core::Finished::Stuck(format!(
                    "no process {} declared",
                    state.name
                ))),
            };
            let (events, work) = match decision {
                Advance::Wait => (Vec::new(), vec![release(Instant::NEVER)]),
                Advance::Finish(finished) => {
                    (vec![ProcessEvent::Finished { finished }], Vec::new())
                }
                Advance::Take {
                    key,
                    step: Step::Run { command, params },
                } => {
                    let sub = Submission {
                        command,
                        params,
                        key: step_key(id, &key),
                        by: by.clone(),
                    };
                    let recorded = |o: Result<&[Event], &Rejection>| {
                        let outcome = match o {
                            Ok(events) => {
                                Outcome::Ok(Value::Array(events.iter().map(event_json).collect()))
                            }
                            Err(r) => Outcome::Rejected(r.clone()),
                        };
                        let key = key.clone();
                        vec![new_event(ProcessEvent::Recorded { key, outcome })]
                    };
                    let then = Then {
                        write: process_write(id, version, &by, now, guard.clone()),
                        outcome: &recorded,
                    };
                    match self.submit_at(sub, now, Some(&then)).await {
                        Ok(_) | Err(Error::Rejected(_)) => continue,
                        Err(Error::Store(StoreError::Conflict(_))) => {
                            races.lost(format!("advancing {stream}")).await?;
                            continue;
                        }
                        Err(Error::Store(StoreError::LeaseLost)) => return Ok(()),
                        Err(e) => return Err(e),
                    }
                }
                Advance::Take {
                    key,
                    step:
                        Step::Await {
                            stream: on,
                            event,
                            within_ms,
                        },
                } => match self.first_event(&on, &event).await? {
                    Some(found) => {
                        let step = Step::Await {
                            stream: on,
                            event,
                            within_ms,
                        };
                        let recorded = ProcessEvent::Recorded {
                            key: key.clone(),
                            outcome: Outcome::Ok(found),
                        };
                        (
                            vec![ProcessEvent::Requested { key, step }, recorded],
                            Vec::new(),
                        )
                    }
                    None => {
                        let step = Step::Await {
                            stream: on,
                            event,
                            within_ms,
                        };
                        (
                            vec![ProcessEvent::Requested { key, step }],
                            vec![release(Instant::NEVER)],
                        )
                    }
                },
                Advance::Take { key, step } => (
                    vec![ProcessEvent::Requested { key, step }],
                    vec![release(Instant::NEVER)],
                ),
            };
            let waits = work
                .iter()
                .any(|w| matches!(w, WorkChange::Release { .. } | WorkChange::Done { .. }));
            let finishing = events
                .iter()
                .any(|e| matches!(e, ProcessEvent::Finished { .. }));
            match self
                .record(id, version, events, &by, now, work, guard.clone())
                .await
            {
                Ok(()) if waits || finishing => break,
                Ok(()) => continue,
                Err(StoreError::Conflict(_)) => races.lost(format!("advancing {stream}")).await?,
                Err(StoreError::LeaseLost) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
        // A wait just requested may have been answered before its row existed.
        let (state, _) = self.process_events(id).await?;
        if let Some((
            key,
            Step::Await {
                stream: on, event, ..
            },
        )) = state.pending()
            && let Some(found) = self.first_event(on, event).await?
        {
            self.answer_step(
                id,
                key,
                Outcome::Ok(found),
                &By::Process(stream),
                now,
                None,
                &[],
            )
            .await?;
        }
        Ok(())
    }

    async fn first_event(&self, stream: &StreamId, kind: &str) -> Result<Option<Value>, Error> {
        let events = self.store.read_stream(stream, Version(0)).await?;
        match events.into_iter().find(|e| e.kind == kind) {
            Some(e) => Ok(Some(event_json(&self.upcast(e)?))),
            None => Ok(None),
        }
    }

    /// Records a step's outcome, with `work` in the same commit, unless it
    /// has one already. With a lease, only while that lease still holds the
    /// step's row.
    #[allow(clippy::too_many_arguments)]
    async fn answer_step(
        &self,
        process: Uuid,
        key: &str,
        outcome: Outcome,
        by: &By,
        now: Instant,
        guard: Option<(WorkKind, Uuid)>,
        work: &[WorkChange],
    ) -> Result<bool, Error> {
        let mut races = Races::new();
        loop {
            let (state, version) = self.process_events(process).await?;
            if state.progress.steps.contains_key(key) || !state.requested.contains_key(key) {
                return Ok(false);
            }
            let guards = guard
                .map(|(kind, lease)| (kind, step_key(process, key), lease))
                .into_iter()
                .collect();
            let recorded = ProcessEvent::Recorded {
                key: key.to_string(),
                outcome: outcome.clone(),
            };
            match self
                .record(
                    process,
                    version,
                    vec![recorded],
                    by,
                    now,
                    work.to_vec(),
                    guards,
                )
                .await
            {
                Ok(()) => return Ok(true),
                Err(StoreError::Conflict(_)) => {
                    races
                        .lost(format!("answering {key} of {PROCESS}/{process}"))
                        .await?
                }
                Err(StoreError::LeaseLost) => return Ok(false),
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Ends an attempt on a row: lets it go, due again at `Ok(due)`, or
    /// answers its step with `Err(outcome)` and, in the same commit, paces
    /// a task's ordering key: no next task of the key runs before its
    /// interval. Under the row's lease, if it has one. Whether it held.
    async fn end_attempt(
        &self,
        row: &Work,
        end: Result<Instant, Outcome>,
        by: &By,
        now: Instant,
    ) -> Result<bool, Error> {
        let outcome = match end {
            Ok(due) => return self.release(row, due).await,
            Err(outcome) => outcome,
        };
        let (Some(process), Some(step)) = (row.process, row.step.as_deref()) else {
            return Ok(false);
        };
        let mut pace = Vec::new();
        if let Some(key) = &row.ordering_key
            && let Some(tt) = self.model.tasks.get(&row.name)
        {
            // An interval of 0 paces nothing: the key is due at `now`.
            pace.push(WorkChange::Pace {
                name: row.name.clone(),
                key: key.clone(),
                not_before: now.plus(tt.key_interval_ms),
            });
        }
        let guard = row.lease.map(|l| (row.kind, l));
        self.answer_step(process, step, outcome, by, now, guard, &pace)
            .await
    }

    async fn release(&self, row: &Work, due: Instant) -> Result<bool, Error> {
        let Some(lease) = row.lease else {
            return Ok(false);
        };
        let unit = Unit {
            work: vec![WorkChange::Release {
                kind: row.kind,
                id: row.id,
                lease,
                due,
            }],
            ..Unit::default()
        };
        match self.store.commit(unit).await {
            Ok(_) => Ok(true),
            Err(StoreError::LeaseLost) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Answers the waits for the event that just arrived.
    pub(crate) async fn arrived(
        &self,
        e: &Event,
        now: Instant,
        faults: &mut Vec<Fault>,
    ) -> Result<usize, Error> {
        let mut done = 0;
        for row in self
            .store
            .work_named(WorkKind::Await, &e.stream.to_string())
            .await?
        {
            let Some(wait) = set_aside(row.wait().map_err(Error::Fault), || item(&row), faults)?
            else {
                continue;
            };
            if wait.event == e.kind {
                let by = By::Process(StreamId::new(PROCESS, wait.process));
                if self
                    .answer_step(
                        wait.process,
                        &wait.step,
                        Outcome::Ok(event_json(e)),
                        &by,
                        now,
                        None,
                        &[],
                    )
                    .await?
                {
                    done += 1;
                }
            }
        }
        Ok(done)
    }

    pub(crate) async fn expire_waits(
        &self,
        now: Instant,
        faults: &mut Vec<Fault>,
    ) -> Result<usize, Error> {
        let claim = self.claim(WorkKind::Await, None, now, self.options.schedule_lease_ms);
        self.drain(claim, faults, move |row| async move {
            self.expire_wait(&row, now).await
        })
        .await
    }

    async fn expire_wait(&self, row: &Work, now: Instant) -> Result<(), Error> {
        let wait = row.wait().map_err(Error::Fault)?;
        let outcome = match self.first_event(&wait.stream, &wait.event).await? {
            Some(found) => Outcome::Ok(found),
            None => Outcome::TimedOut,
        };
        let by = By::Process(StreamId::new(PROCESS, wait.process));
        self.end_attempt(row, Err(outcome), &by, now).await?;
        Ok(())
    }

    // ---- actions ----

    pub(crate) async fn deliver(
        &self,
        now: Instant,
        faults: &mut Vec<Fault>,
    ) -> Result<usize, Error> {
        let mut count = 0;
        for (name, def) in &self.model.actions {
            let claim = self.claim(
                WorkKind::Action,
                Some(vec![name.clone()]),
                now,
                def.policy.timeout_ms,
            );
            count += self
                .drain(claim, faults, move |row| async move {
                    self.deliver_one(name, def, &row, now).await
                })
                .await?;
        }
        Ok(count)
    }

    async fn deliver_one(
        &self,
        name: &str,
        def: &crate::model::ActionDef,
        row: &Work,
        now: Instant,
    ) -> Result<(), Error> {
        let by = By::Action(name.to_string());
        let call = row.call().map_err(Error::Fault)?;
        let retry = &def.policy.retry;
        let end = match retry.on_claim(row, call.at_most_once) {
            Attempt::End(outcome) => Err(outcome),
            Attempt::Run => {
                // The call is made only under the claim's lease, renewed
                // from the clock's now: one that ran out while earlier calls
                // of the tick took their time may have been taken over, and
                // a call made after the new holder's could land after that
                // one recorded the step.
                let lease = row.lease.expect("a claimed row has a lease");
                let until = self.clock.now().plus(def.policy.timeout_ms);
                if !self
                    .store
                    .renew(WorkKind::Action, row.id, lease, now, until)
                    .await?
                {
                    return Ok(());
                }
                match (def.call)(row.id, call.params).await {
                    ActionAnswer::Done(v) => Err(Outcome::Ok(v)),
                    ActionAnswer::Rejected(r) => Err(Outcome::Rejected(r)),
                    ActionAnswer::Permanent(e) => Err(Outcome::Failed(e)),
                    ActionAnswer::Passing(e) => retry.after_failure(row, now, e),
                }
            }
        };
        self.end_attempt(row, end, &by, now).await?;
        Ok(())
    }

    // ---- schedules and recurring commands ----

    pub(crate) async fn run_due(
        &self,
        now: Instant,
        faults: &mut Vec<Fault>,
    ) -> Result<usize, Error> {
        let claim = self.claim(
            WorkKind::Schedule,
            None,
            now,
            self.options.schedule_lease_ms,
        );
        let mut count = self
            .drain(claim, faults, move |row| async move {
                self.run_scheduled(&row, now).await
            })
            .await?;
        for r in &self.model.recurring {
            let key = recurring_key(&r.name, r.period_start(now));
            let stream = recurring_stream(key);
            if !self
                .store
                .read_stream(&stream, Version(0))
                .await?
                .is_empty()
            {
                continue;
            }
            let sub = Submission {
                command: r.command.clone(),
                params: r.params.clone(),
                key,
                by: By::Recurring(r.name.clone()),
            };
            let ran = self.run_once(sub, stream, RECURRING_RAN, now).await;
            if set_aside(ran, || format!("recurring {}", r.name), faults)?.is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Runs a due scheduled command and records its outcome, a refusal
    /// included.
    async fn run_scheduled(&self, row: &Work, now: Instant) -> Result<(), Error> {
        let sub = Submission {
            command: row.name.clone(),
            params: row.input().clone(),
            key: schedule_key(row.id),
            by: By::Schedule(row.id),
        };
        self.run_once(sub, schedule_stream(row.id), SCHEDULE_RAN, now)
            .await
    }

    /// Submits `sub` once: its outcome, a refusal included, is an event
    /// `ran` that begins `stream`, committed with the command's events. Run
    /// already (`stream` begun by another runner), it does nothing.
    async fn run_once(
        &self,
        sub: Submission,
        stream: StreamId,
        ran: &str,
        now: Instant,
    ) -> Result<(), Error> {
        let outcome = |o: Result<&[Event], &Rejection>| {
            let data = match o {
                Ok(_) => json!({ "ok": true }),
                Err(r) => json!({ "ok": false, "rejection": r }),
            };
            let kind = ran.to_string();
            vec![NewEvent { kind, data }]
        };
        let then = Then {
            write: Write {
                stream,
                expected: Version(0),
                events: Vec::new(),
                meta: Meta {
                    now,
                    by: sub.by.clone(),
                    key: Some(sub.key),
                    command: sub.command.clone(),
                    fingerprint: None,
                },
                work: Vec::new(),
                guards: Vec::new(),
            },
            outcome: &outcome,
        };
        match self.submit_at(sub, now, Some(&then)).await {
            Ok(_) | Err(Error::Rejected(_)) | Err(Error::Store(StoreError::Conflict(_))) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // ---- tasks ----

    /// Hands the oldest due task of the type to `worker`, under a lease of
    /// `lease_ms` (never past the type's timeout).
    pub async fn claim_task(
        &self,
        task_type: &str,
        worker: &str,
        lease_ms: i64,
    ) -> Result<Option<ClaimedTask>, Error> {
        let tt = self.task_type(task_type)?;
        let now = self.clock.now();
        let lease_ms = tt.timeout_ms.map_or(lease_ms, |t| lease_ms.min(t));
        loop {
            let claim = Claim {
                kind: WorkKind::Task,
                names: Some(vec![task_type.to_string()]),
                now,
                holder: worker.to_string(),
                lease_ms,
                limit: 1,
            };
            let Some(row) = self.store.claim(claim).await?.into_iter().next() else {
                return Ok(None);
            };
            if let Attempt::End(outcome) = tt.retry.on_claim(&row, tt.at_most_once) {
                let by = By::Worker(worker.to_string());
                self.end_attempt(&row, Err(outcome), &by, now).await?;
                continue;
            }
            let input = row.input().clone();
            return Ok(Some(ClaimedTask {
                id: row.id,
                task_type: row.name,
                input,
                lease: row.lease.expect("a claimed row has a lease"),
                attempt: row.attempts,
                process: row.process,
                step: row.step,
            }));
        }
    }

    /// Keeps the task for `lease_ms` more, never past its type's timeout.
    pub async fn heartbeat(&self, task: &ClaimedTask, lease_ms: i64) -> Result<(), Error> {
        let tt = self.task_type(&task.task_type)?;
        let now = self.clock.now();
        let row = self.held(task).await?;
        let mut until = now.plus(lease_ms);
        if let (Some(timeout), Some(since)) = (tt.timeout_ms, row.claimed_at) {
            until = until.min(since.plus(timeout));
        }
        if self
            .store
            .renew(WorkKind::Task, task.id, task.lease, now, until)
            .await?
        {
            Ok(())
        } else {
            Err(rejected(codes::LEASE_LOST, "the task's lease ran out"))
        }
    }

    /// Reports the attempt's result. A failure is tried again while the
    /// type's attempts last.
    pub async fn finish_task(
        &self,
        task: &ClaimedTask,
        result: Result<Value, String>,
        worker: &str,
    ) -> Result<(), Error> {
        let tt = self.task_type(&task.task_type)?;
        let now = self.clock.now();
        let row = self.held(task).await?;
        let by = By::Worker(worker.to_string());
        let end = match result {
            Ok(v) => Err(Outcome::Ok(v)),
            Err(e) => tt.retry.after_failure(&row, now, e),
        };
        let retried = end.is_ok();
        if !self.end_attempt(&row, end, &by, now).await? && !retried {
            return Err(rejected(codes::LEASE_LOST, "the task's lease ran out"));
        }
        Ok(())
    }

    /// Ends a queued or running task as cancelled.
    pub async fn cancel_task(&self, id: Uuid, by: By) -> Result<(), Error> {
        let now = self.clock.now();
        let Some(mut row) = self.store.work(WorkKind::Task, id).await? else {
            return Err(rejected(codes::UNKNOWN, format!("no task {id} pending")));
        };
        row.lease = None;
        self.end_attempt(&row, Err(Outcome::Cancelled), &by, now)
            .await?;
        Ok(())
    }

    async fn held(&self, task: &ClaimedTask) -> Result<Work, Error> {
        match self.store.work(WorkKind::Task, task.id).await? {
            Some(row) if row.lease == Some(task.lease) => Ok(row),
            _ => Err(rejected(
                codes::LEASE_LOST,
                "the task is no longer held under this lease",
            )),
        }
    }

    fn task_type(&self, name: &str) -> Result<&factum_core::TaskType, Error> {
        self.model
            .tasks
            .get(name)
            .ok_or_else(|| rejected(codes::UNKNOWN, format!("no task type {name}")))
    }
}
