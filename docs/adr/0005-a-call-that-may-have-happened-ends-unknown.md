# 5. A call that may have happened ends Unknown

## Decision

An action is a call to another system, requested by a step through an
**outbox**: the request and its delivery row commit together.

- Delivered at least once under the step's key, or **at most once**, in which case a call whose answer was lost ends `Unknown` for `next` to reconcile.
- Each action declares its retry schedule (attempts, backoff, factor, cap,
  jitter) and timeout.
- A call whose lease runs out (its runner died, or it outlived its timeout) is taken over and made again only while its attempts last: taken over after the last, it ends `Unknown`, as an at-most-once one does, since that attempt may have done the work; it ends `Failed` only when its last attempt answered an error.
- A call is made only under its claim's lease, renewed from the clock just before it: a runner whose lease was taken over (an earlier call of its tick outlived the timeout) does not make it, so no call lands after another holder recorded the step.

A task:

- Long work a step queues and a **worker** pulls: claim under a lease, heartbeat to keep it (never past the type's timeout), result or failure.
- Declared attempts and backoff; ends as an action does once they are spent (`Unknown` if the last attempt's lease ran out, `Failed` if it answered an error); may be done at most once; may be cancelled; may carry an **ordering key** (one at a time per key, in order, paced).

## Why

factum cannot know whether another system did the work when no answer
came back. `Failed` would let `next` redo work that may be done; `Unknown`
hands that question to the program's own reconciling step.
