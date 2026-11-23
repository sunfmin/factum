---------------------------- MODULE ExactlyOnce ----------------------------
(* What factum promises for the work that outlives a command (SPEC §1,     *)
(* rows 5-7, and the work rows), as the simulation's shop runs it          *)
(* (crates/conformance/src/sim.rs): each order starts a checkout, which    *)
(* charges it at least once through the outbox (asking the payment system *)
(* when the charge's outcome is unknown) and pays or cancels it, and a     *)
(* receipt, which prints it at most once.                                  *)
(*                                                                         *)
(* One action for each store write the engine makes as one step (a        *)
(* commit, or a claim that took rows) and for each call to another        *)
(* system: the simulation pauses a runner after each, and prints one       *)
(* record per step, so its runs are traces of this model. Time is left     *)
(* out: any lease may have run out, so a held row may be taken over at any *)
(* step, and a runner may stop between any two steps forever.             *)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    ChargeAttempts,   \* charge_policy(): payment.charge and payment.find
    PrintAttempts     \* ActionPolicy::default(): printer.print

VARIABLES
    orders,   \* each order as the simulation prints it (sim.rs, `seen`)
    calls,    \* the attempts runners hold: claimed, or answered and not ended
    woken     \* advance rows woken while a claim held them (the store's `woken`)

vars == <<orders, calls, woken>>

Procs == {"checkout", "receipt"}
Steps == [checkout |-> {"charge", "find", "pay", "cancel"}, receipt |-> {"print"}]
Action == [charge |-> "payment.charge", find |-> "payment.find", print |-> "printer.print"]
AtMostOnce(s) == s = "print"
Attempts(s) == IF s = "print" THEN PrintAttempts ELSE ChargeAttempts

NoRow == [exists |-> FALSE, held |-> FALSE, attempts |-> 0, taken_over |-> FALSE, waiting |-> FALSE]
NewRow == [NoRow EXCEPT !.exists = TRUE]

NewProc(p) == [steps |-> [s \in Steps[p] |-> ""], pending |-> "", end |-> "",
               advance |-> NewRow, action |-> NoRow]

O == DOMAIN orders
P(o, p) == orders[o][p]
Placed(o) == orders[o].paid = 0 /\ orders[o].cancelled = 0

(* What `next` answers (domain.rs: CheckoutProcess, ReceiptProcess).      *)
Call(s) == [kind |-> "call", step |-> s]
Run(s) == [kind |-> "run", step |-> s]
Finish(e) == [kind |-> "finish", step |-> e]
Pay(st) == CASE st.pay = "" -> Run("pay")
             [] st.pay = "ok" -> Finish("completed")
             [] OTHER -> Finish("stuck")
Decide(p, st) ==
    IF p = "receipt" THEN
        CASE st.print = "" -> Call("print")
          [] st.print = "ok" -> Finish("completed")
          [] st.print = "unknown" -> Finish("stuck")
          [] OTHER -> Finish("failed")
    ELSE CASE st.charge = "" -> Call("charge")
           [] st.charge = "ok" -> Pay(st)
           [] st.charge = "unknown" ->
                CASE st.find = "" -> Call("find")
                  [] st.find = "none" -> Finish("failed")
                  [] st.find = "ok" -> Pay(st)
                  [] OTHER -> Finish("stuck")
           [] st.charge = "rejected" ->
                IF st.cancel = "" THEN Run("cancel") ELSE Finish("failed")
           [] OTHER -> Finish("failed")
Next_(o, p) == Decide(p, P(o, p).steps)

SetProc(o, p, q) == orders' = [orders EXCEPT ![o][p] = q]

(* A claim (store `claim_row`): a new lease, one more attempt, taken over  *)
(* if the row had a lease (one that ran out), and a wake it held dropped.  *)
Claimed(row) == [row EXCEPT !.held = TRUE, !.attempts = @ + 1, !.taken_over = row.held]

(* A release (`release_row`): an advance row waits for a wake (due never) *)
(* unless one came while it was held; an action row is due after its     *)
(* backoff, which time, left out, may have passed at any step.           *)
Released(row, waiting) == [row EXCEPT !.held = FALSE, !.taken_over = FALSE, !.waiting = waiting]
ReleaseAdvance(o, p, q) ==
    /\ SetProc(o, p, [q EXCEPT !.advance = Released(q.advance, <<o, p>> \notin woken)])
    /\ woken' = woken \ {<<o, p>>}

(* A step's outcome recorded (`answer_step`; `effects` of StepRecorded):   *)
(* its call's row done, its process's advance row woken.                  *)
Recorded(o, p, s, outcome) ==
    LET q == P(o, p)
        q2 == [q EXCEPT !.steps[s] = outcome, !.pending = "", !.action = NoRow]
    IN IF q.advance.held
       THEN /\ SetProc(o, p, q2)
            /\ woken' = woken \cup {<<o, p>>}
       ELSE /\ SetProc(o, p, [q2 EXCEPT !.advance.waiting = FALSE])
            /\ UNCHANGED woken

-----------------------------------------------------------------------------
(* The front places an order and starts its two processes (sim `place`).  *)
Place ==
    /\ \E over \in BOOLEAN :
         orders' = Append(orders, [over |-> over, charged |-> 0, paid |-> 0,
                                   cancelled |-> 0, printed |-> 0,
                                   checkout |-> NewProc("checkout"),
                                   receipt |-> NewProc("receipt")])
    /\ UNCHANGED <<calls, woken>>

\* The rows of `S` (process pairs) of the kind `r` ("advance", "action"), claimed.
ClaimRows(S, r) ==
    [o \in O |-> [orders[o] EXCEPT
        !.checkout[r] = IF <<o, "checkout">> \in S THEN Claimed(@) ELSE @,
        !.receipt[r] = IF <<o, "receipt">> \in S THEN Claimed(@) ELSE @]]

(* `advance_due`'s claim: some due advance rows, some of them taken over. *)
ClaimAdvance ==
    \E S \in SUBSET {<<o, p>> \in O \X Procs :
                       P(o, p).advance.exists /\ ~P(o, p).advance.waiting} :
       /\ S # {}
       /\ orders' = ClaimRows(S, "advance")
       /\ woken' = woken \ S
       /\ UNCHANGED calls

(* `deliver`'s claim of one action's rows: each attempt is now a call the *)
(* claiming runner holds under the new lease.                             *)
ClaimAction ==
    \E a \in {"charge", "find", "print"} :
    \E S \in SUBSET {<<o, p>> \in O \X Procs :
                       P(o, p).pending = a /\ P(o, p).action.exists} :
       /\ S # {}
       /\ orders' = ClaimRows(S, "action")
       \* A call whose lease is taken over is not Current again: it neither
       \* calls nor writes (its runner's commits are lost, `Unseen`).
       /\ calls' = {c \in calls : <<c.o, c.p>> \notin S} \cup
            {[o |-> x[1], p |-> x[2], step |-> a, lease |-> P(x[1], x[2]).action.attempts + 1,
              taken_over |-> P(x[1], x[2]).action.held, phase |-> "claimed", answer |-> ""] : x \in S}
       /\ UNCHANGED woken

(* `advance_one`, under the advance row's lease: each commit one action.  *)
Holds(o, p) == P(o, p).advance.held /\ P(o, p).end = ""

\* `Advance::Take` of a call: StepRequested, the outbox row put with it.
Request(o, p) ==
    LET q == P(o, p) IN
    /\ Holds(o, p) /\ q.pending = "" /\ Next_(o, p).kind = "call"
    /\ ReleaseAdvance(o, p, [q EXCEPT !.pending = Next_(o, p).step, !.action = NewRow])
    /\ UNCHANGED calls

\* `Advance::Wait`: a step is pending; the row waits for its outcome.
Wait(o, p) ==
    /\ Holds(o, p) /\ P(o, p).pending # ""
    /\ ReleaseAdvance(o, p, P(o, p))
    /\ UNCHANGED calls

\* `Advance::Finish`: ProcessFinished, the advance row done with it.
Finished(o, p) ==
    LET q == P(o, p) IN
    /\ Holds(o, p) /\ q.pending = "" /\ Next_(o, p).kind = "finish"
    /\ SetProc(o, p, [q EXCEPT !.end = Next_(o, p).step, !.advance = NoRow])
    /\ UNCHANGED <<calls, woken>>

\* `Advance::Take` of a Run step: the command submitted under the step's
\* key, its events and the step's outcome in one commit, under the lease
\* (`submit_at` with the step's `Recorded`; `effects` of StepRecorded wake
\* the advance row the runner holds). A runner whose lease was taken over
\* commits nothing (`Unseen`), so the command is applied at most once.
RunStep(o) ==
    LET q == P(o, "checkout")
        s == Next_(o, "checkout").step
        ok == Placed(o)
    IN
    /\ Holds(o, "checkout") /\ q.pending = ""
    /\ Next_(o, "checkout").kind = "run"
    /\ orders' = [orders EXCEPT ![o].paid = IF ok /\ s = "pay" THEN @ + 1 ELSE @,
                                ![o].cancelled = IF ok /\ s = "cancel" THEN @ + 1 ELSE @,
                                ![o].checkout = [q EXCEPT !.steps[s] = IF ok THEN "ok" ELSE "rejected",
                                                          !.pending = "", !.action = NoRow]]
    /\ woken' = woken \cup {<<o, "checkout">>}
    /\ UNCHANGED calls

(* The order's scheduled `order.expire` runs (`run_scheduled`): a placed  *)
(* order is cancelled, its ScheduleRan committed with the cancel.         *)
Expire(o) ==
    /\ Placed(o)
    /\ orders' = [orders EXCEPT ![o].cancelled = @ + 1]
    /\ UNCHANGED <<calls, woken>>

(* `deliver_one`, for an attempt `c` a runner holds under its lease. Each  *)
(* of its writes is guarded by the lease; one that lost it is `Unseen`.   *)
Current(c) == LET q == P(c.o, c.p) IN
              q.pending = c.step /\ q.action.held /\ q.action.attempts = c.lease
Ended(c, record) ==
    /\ Current(c)
    /\ calls' = calls \ {c}
    /\ record

\* `RetryPolicy::on_claim`: taken over when done at most once, or past the
\* last attempt, the step ends Unknown without a call.
Spent(c) == c.taken_over /\ (AtMostOnce(c.step) \/ c.lease > Attempts(c.step))
EndUnknown(c) ==
    /\ c.phase = "claimed" /\ Spent(c)
    /\ Ended(c, Recorded(c.o, c.p, c.step, "unknown"))

\* The call to the other system (sim `call`), made only under the lease
\* renewed just before it: a runner whose lease was taken over does not
\* call. The payment system keeps one charge per key and never answers
\* "try later" for a key it charged; the printer prints each time it is
\* asked.
CallOutside(c) ==
    LET ord == orders[c.o]
        answer(a) == calls' = (calls \ {c}) \cup {[c EXCEPT !.phase = "answered", !.answer = a]}
    IN
    /\ c.phase = "claimed" /\ ~Spent(c) /\ Current(c)
    /\ UNCHANGED woken
    /\ CASE c.step = "charge" ->
              \/ ord.charged = 0 /\ answer("busy") /\ UNCHANGED orders
              \/ ord.over /\ answer("rejected") /\ UNCHANGED orders
              \/ ~ord.over /\ answer("ok") /\ orders' = [orders EXCEPT ![c.o].charged = 1]
         [] c.step = "find" ->
              \/ answer("busy") /\ UNCHANGED orders
              \/ answer(IF ord.charged > 0 THEN "ok" ELSE "none") /\ UNCHANGED orders
         [] c.step = "print" ->
              \/ answer("busy") /\ UNCHANGED orders
              \/ answer("ok") /\ orders' = [orders EXCEPT ![c.o].printed = @ + 1]

\* `end_attempt` with the call's answer: recorded, or (busy) the row let go
\* to be tried again, or Failed once its attempts are spent.
Answered(c) ==
    LET q == P(c.o, c.p) IN
    /\ c.phase = "answered"
    /\ Ended(c, IF c.answer # "busy" THEN Recorded(c.o, c.p, c.step, c.answer)
                ELSE IF c.lease >= Attempts(c.step) THEN Recorded(c.o, c.p, c.step, "failed")
                ELSE /\ SetProc(c.o, c.p, [q EXCEPT !.action = Released(q.action, FALSE)])
                     /\ UNCHANGED woken)

(* A write this model leaves out (schedule rows, a refused expire's lone  *)
(* ScheduleRan), a lost commit, or a step that writes nothing: a clock    *)
(* move, a fault cued.                                                    *)
Unseen == UNCHANGED vars

-----------------------------------------------------------------------------
Init == /\ orders = <<>>
        /\ calls = {}
        /\ woken = {}

Next == \/ Place \/ ClaimAdvance \/ ClaimAction
        \/ \E o \in O : \/ RunStep(o) \/ Expire(o)
                        \/ \E p \in Procs : Request(o, p) \/ Wait(o, p) \/ Finished(o, p)
        \/ \E c \in calls : EndUnknown(c) \/ CallOutside(c) \/ Answered(c)
        \/ Unseen

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* The promises, by the names the simulation checks them by (sim `seen`). *)

\* No order is charged twice: the payment system keeps one charge per key.
charged_once == \A o \in O : orders[o].charged <= 1
\* No order is paid twice: the pay step's events commit with its outcome.
paid_once == \A o \in O : orders[o].paid <= 1
\* An at-most-once call is made at most once.
printed_at_most_once == \A o \in O : orders[o].printed <= 1
\* No step is recorded done whose call was not made.
unknown_never_done ==
    \A o \in O : /\ P(o, "checkout").steps.charge = "ok" => orders[o].charged = 1
                 /\ P(o, "receipt").steps.print = "ok" => orders[o].printed = 1
\* No step is recorded failed whose call was made (factum#30).
failed_never_done ==
    \A o \in O : /\ P(o, "checkout").steps.charge = "failed" => orders[o].charged = 0
                 /\ P(o, "receipt").steps.print = "failed" => orders[o].printed = 0
\* Every unfinished process keeps a work row.
nothing_lost ==
    \A o \in O, p \in Procs :
        P(o, p).end = "" => P(o, p).advance.exists \/ (P(o, p).pending # "" /\ P(o, p).action.exists)

TypeOK ==
    /\ \A o \in O : orders[o].charged \in Nat /\ orders[o].over \in BOOLEAN
    /\ \A c \in calls : c.phase \in {"claimed", "answered"} /\ c.p \in Procs
    /\ woken \subseteq O \X Procs

(* The bounds TLC checks within (ExactlyOnce.cfg's CONSTRAINT; a trace is  *)
(* not bounded). One order: orders share no state here, and a claim of    *)
(* several orders' rows is, to each order, a claim of its own; two would   *)
(* be some 30,000 squared states. Runners may die without end, each death *)
(* another attempt: one past the last shows a takeover after it.          *)
Bounds == /\ Len(orders) <= 1
          /\ \A o \in O, p \in Procs :
               /\ P(o, p).advance.attempts <= 2
               /\ P(o, p).action.attempts <= Attempts(P(o, p).pending) + 1
=============================================================================
