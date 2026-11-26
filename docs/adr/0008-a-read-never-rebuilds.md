# 8. A read never rebuilds

## Decision

A projection is **current** when it was built by the code running now:
same version, same tables. A new version, or any change to its tables, makes it not current, and the engine's loop rebuilds it from the whole log; a read never writes, and reads one that is not current as not yet built (no rows, no dead letters). An event it fails on, or that writes an undeclared table, becomes a **dead letter**: skipped, or (declared `stop`) held there.

**A read never rebuilds.** Rebuilding drops rows, so only the loop does it; every read (`query`, `rows`, `row`, `dead_letters`, a retry) takes a projection as it stands and answers one built by other code as not yet built. A read thus never serves, nor a retry writes to, rows another version made.

## Why

During a deploy, old and new code run side by side. A read that rebuilt
would drop the rows the other version serves; a read that served the
other version's rows would answer with code that is not running.
