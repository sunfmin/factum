# 9. A fault sets one item aside, not the tick

## Decision

**A fault in one item stops nothing else.** A tick runs every loop over every item even when one faults (its code cannot read an event, a state or a row). An event the engine cannot dispatch — start the processes it starts, answer the waits on it — becomes a **dead letter** of the dispatch checkpoint `factum.dispatch`, read and retried as a projection's are.

Any other item — a process to advance, an action, a schedule, a recurring
command, a wait — keeps its row and is tried again once its lease runs
out. The tick reports each item it set aside; only a store error ends it.

## Why

A deploy whose new code records what older runners cannot read would
otherwise stop those runners' every loop, on every tick, until they are
gone.
