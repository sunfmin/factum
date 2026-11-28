# 21. A sealed file store for hosts without a C compiler

## Decision

`factum-sealed` keeps the store (ADR 0010, ADR 0011) in one file of sealed
records, appended, in pure Rust (`chacha20poly1305`'s RustCrypto crates).
Nothing in its tree, for any target, needs `cc`.
`SealedStore::open(path, key)` takes a 32-byte key the caller derives.

**Format 1.** A header: magic `factumSL`, format version, a random file
id, and a known block sealed with the key, so a wrong key fails at open and says so.
Then records: a length, a random 24-byte nonce, and XChaCha20-Poly1305
over one commit's ops (JSON), with the file id and the record's index as
AAD. An op is an event, kept forever, or sets one piece of state whole, so
a later op on it supersedes the earlier. A newer format is `NewerSchema`.

**Damage.** A last record cut short or that does not open is a write a crash cut off: it is left out, and the next write cuts it off under the lock.
Any other record that does not open, or does not decode, is an error naming the file and the record, never a guessed state (ADR 0011).

**Several processes.** Each method is one transaction under an OS lock
(`File::lock`) on `<file>.lock`, a file of its own since Windows locks are
mandatory and a compaction replaces the data file. Under the lock a handle checks that its file is still the one at the path (by the file id), folds the records appended since it last looked, then checks for `Conflict`, `KeyTaken`, `LeaseLost` or `Stale`, appends and syncs (`sync_data`) before it unlocks.

**Size.** Waste is what no current op holds.
When it is over half the file and over 64 KiB, the write that finds it rewrites the file under the lock as every event and the current state, under a new file id, to `<file>.compacting`, synced, then renamed over the file.

**Mode.** Every file it makes (the file, `<file>.lock`, `<file>.compacting`) is owner-only from its open, and tightened if left wider: 0600 on Unix, on Windows a protected `D:P(A;;FA;;;<owner>)(A;;FA;;;SY)(A;;FA;;;BA)`. `unsafe_code` is denied, not forbidden, for those Windows calls.

Budget: 2,500 lines (ADR 0016). No new glossary words.

## Evidence

The measure example, M5 Max, SSD, two runs, medians of 100 (10
for opens). 2026-11-27, ~440-byte events: a one-event commit on 10,000,
3.1-3.9 ms; an open of that 4.6 MB file, 16.7 ms; the write compacting
9.1 MB, 29-37 ms; the file 2.5% over its JSON. 2026-11-28: one table's
1,000 rows of 100,000 in 100 tables, 39-79 µs by key range (703-720 µs
scanning all).

## Considered

`factum-sqlite` (bundled SQLite needs `cc`, and is not encrypted);
SurrealKV 1.4.0 (`zstd-sys` and `cc`, one process); Turso's Rust SQLite
(experimental encryption); redbx, redb-turbo (young redb forks, metadata
in clear, one process).

## Why

opcore keeps 1Password writes until 1Password takes them, and needs what
factum guarantees (`Unknown`, ordering keys, leases, retries, a read
model), encrypted, built for Windows with no C compiler. An appended log
keeps the crash story to one rule: only the last record may be torn.
