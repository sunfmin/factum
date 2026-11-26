# 20. bizkernel moves onto factum in revertable steps

## Decision

bizkernel's own gate and conformance scenarios are the oracle: every step
lands with both green, and is a commit that can be reverted alone.

1. bizkernel's key derivations, stream versions and process step types are
   replaced by `factum-core`'s (the derivations are already byte for byte
   bizkernel's, so its logs keep their keys). Its `Actor` becomes the text
   of `By::Principal` (ADR 0018).
2. bizkernel's store traits are generalized to `factum-store`'s, with
   bizkernel's own types (callers, delegation, modules, policies) taken out
   of the signatures. bizkernel's branches, which every one of its store
   signatures carries today, are rebuilt on top of factum or dropped.
3. `factum-postgres`: the matching part of bizkernel's Postgres store moves
   behind the trait and passes the conformance suite.
4. bizkernel's submission and delivery loops move onto the engine;
   bizkernel keeps what wraps them (grants, approval, modules, HTTP).
5. afkconvoy v2 starts on factum + factum-sqlite.

## Open

**bizkernel's branches.** factum has one line (ADR 0001). When bizkernel
moves onto factum (step 2), its branches are either rebuilt on top of
factum or dropped: the owner decides.
