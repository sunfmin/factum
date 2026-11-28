//! The file: a header, then sealed records, appended (ADR 0021).
//!
//! The header is the magic, the format version, a random file id, and a
//! known block sealed with the key, so a wrong key fails at open. Each
//! record is its length (u32, little-endian), a random 24-byte nonce, and
//! XChaCha20-Poly1305 over a JSON array of ops, with the file id and the
//! record's index as its AAD: a record cannot be reordered, dropped from the
//! middle or moved to another file.
//!
//! Every transaction holds the OS lock on `<file>.lock` beside it
//! (exclusive to write, shared to read; a lock of its own, since a lock on
//! the file itself would stop reads on Windows and go with it when a
//! compaction replaces it), first makes sure its file is still the one at
//! the path, then folds the records appended since it last looked.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chacha20poly1305::aead::rand_core::RngCore;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use factum_store::{Result, StoreError};
use serde_json::value::RawValue;

use crate::state::{Op, State};

const MAGIC: &[u8; 8] = b"factumSL";
/// The format this code writes, and the newest it reads.
pub const FORMAT: u32 = 1;
const CHECK: &[u8; 16] = b"factum-sealed ok";
const NONCE: usize = 24;
const TAG: usize = 16;
/// Magic, format, file id.
const ID_END: usize = 8 + 4 + 16;
const HEADER: u64 = (ID_END + NONCE + CHECK.len() + TAG) as u64;
/// A compaction waits for waste over half the file and over this.
const MIN_WASTE: u64 = 64 * 1024;
/// A compacted file's records hold about this much each.
const CHUNK: usize = 1 << 20;

type FileId = [u8; 16];

pub struct Sealed {
    path: PathBuf,
    cipher: XChaCha20Poly1305,
    /// The file every transaction locks.
    lockfile: File,
    file: File,
    /// The id of the file `file` is, once its header was read.
    id: Option<FileId>,
    /// Where the records folded so far end.
    end: u64,
    /// How many records are folded.
    index: u64,
    pub state: State,
}

/// A write's ops, applied to the state as they are made and written
/// together as one record.
pub struct Tx<'a> {
    pub state: &'a mut State,
    ops: Vec<String>,
}

impl Tx<'_> {
    pub fn emit(&mut self, op: Op) -> Result<()> {
        let json = serde_json::to_string(&op).map_err(|e| StoreError::Backend(e.to_string()))?;
        self.state.apply(op, json.len());
        self.ops.push(json);
        Ok(())
    }
}

impl Sealed {
    pub fn open(path: &Path, key: [u8; 32]) -> Result<Sealed> {
        let mut sealed = Sealed {
            path: path.to_path_buf(),
            cipher: XChaCha20Poly1305::new(Key::from_slice(&key)),
            lockfile: open_file(&sibling(path, ".lock"))?,
            file: open_file(path)?,
            id: None,
            end: 0,
            index: 0,
            state: State::default(),
        };
        sealed.write(|_| Ok(()))?;
        Ok(sealed)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn err(&self, what: impl std::fmt::Display) -> StoreError {
        StoreError::Backend(format!("{}: {what}", self.path.display()))
    }

    /// Runs `f` on the state as the file holds it now, under a shared lock.
    pub fn read<T>(&mut self, f: impl FnOnce(&State) -> T) -> Result<T> {
        self.lock(false)?;
        let read = self.refresh().map(|()| f(&self.state));
        self.unlock();
        read
    }

    /// Runs `f` under the exclusive lock, then writes what it emitted as
    /// one record, synced before the lock is let go.
    pub fn write<T>(&mut self, f: impl FnOnce(&mut Tx) -> Result<T>) -> Result<T> {
        self.lock(true)?;
        let written = self.refresh().and_then(|()| {
            let mut tx = Tx {
                state: &mut self.state,
                ops: Vec::new(),
            };
            let answer = f(&mut tx);
            let ops = tx.ops;
            let done = answer.and_then(|a| self.append(&ops).map(|()| a));
            if done.is_err() && !ops.is_empty() {
                // The state took ops the file may not hold: fold it again.
                self.id = None;
            }
            let a = done?;
            // The commit is synced: a compaction that fails leaves the waste
            // for the next write to try again, and fails nothing.
            if let Err(e) = self.compact() {
                eprintln!("factum-sealed: compaction failed, the file keeps its waste: {e}");
            }
            Ok(a)
        });
        self.unlock();
        written
    }

    /// Takes the lock on the file at the path, reading its header first if
    /// this handle has not, or the file was replaced.
    fn lock(&mut self, exclusive: bool) -> Result<()> {
        loop {
            let locked = if exclusive || self.id.is_none() {
                self.lockfile.lock()
            } else {
                self.lockfile.lock_shared()
            };
            locked.map_err(|e| self.err(format!("cannot lock: {e}")))?;
            let held = self.attach().and_then(|()| self.at_path());
            match held {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    self.unlock();
                    self.file = open_file(&self.path)?;
                    self.id = None;
                }
                Err(e) => {
                    self.unlock();
                    return Err(e);
                }
            }
        }
    }

    fn unlock(&self) {
        let _ = self.lockfile.unlock();
    }

    /// Whether this handle's file is still the one at the path.
    fn at_path(&self) -> Result<bool> {
        let mut head = [0; ID_END];
        let mut file = File::open(&self.path).map_err(|e| self.err(e))?;
        file.read_exact(&mut head).map_err(|e| self.err(e))?;
        Ok(self.id.is_some_and(|id| head[12..] == id))
    }

    /// Reads the header of a file this handle has not read yet (writing it
    /// on a new, empty file) and starts its fold over.
    fn attach(&mut self) -> Result<()> {
        if self.id.is_some() {
            return Ok(());
        }
        let len = self.len()?;
        if len == 0 {
            let header = self.header(new_id())?;
            (self.file.write_all(&header))
                .and_then(|()| self.file.sync_all())
                .map_err(|e| self.err(e))?;
        }
        if len != 0 && len < HEADER {
            return Err(self.err("too short to be a factum sealed file"));
        }
        let mut head = [0; HEADER as usize];
        (self.file.seek(SeekFrom::Start(0)))
            .and_then(|_| self.file.read_exact(&mut head))
            .map_err(|e| self.err(e))?;
        if &head[..8] != MAGIC {
            return Err(self.err("not a factum sealed file"));
        }
        let found = u32::from_le_bytes(head[8..12].try_into().expect("4 bytes"));
        if found > FORMAT {
            return Err(StoreError::NewerSchema {
                found,
                known: FORMAT,
            });
        }
        let id: FileId = head[12..ID_END].try_into().expect("16 bytes");
        let nonce = XNonce::from_slice(&head[ID_END..ID_END + NONCE]);
        let check = Payload {
            msg: &head[ID_END + NONCE..],
            aad: &id,
        };
        if self.cipher.decrypt(nonce, check).as_deref() != Ok(CHECK) {
            return Err(self.err("the key does not open this file: a wrong key"));
        }
        self.id = Some(id);
        self.end = HEADER;
        self.index = 0;
        self.state = State::default();
        Ok(())
    }

    fn header(&self, id: FileId) -> Result<Vec<u8>> {
        let check = self.seal(CHECK, &id)?;
        Ok([&MAGIC[..], &FORMAT.to_le_bytes(), &id, &check].concat())
    }

    /// A random nonce, then `msg` sealed under it with `aad`.
    fn seal(&self, msg: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let sealed =
            (self.cipher.encrypt(&nonce, Payload { msg, aad })).map_err(|e| self.err(e))?;
        Ok([&nonce[..], &sealed].concat())
    }

    /// The bytes of a record of file `id` holding `ops`, at `index`.
    fn record(&self, id: FileId, index: u64, ops: &[String]) -> Result<Vec<u8>> {
        let sealed = self.seal(format!("[{}]", ops.join(",")).as_bytes(), &aad(id, index))?;
        Ok([&(sealed.len() as u32).to_le_bytes()[..], &sealed].concat())
    }

    fn len(&self) -> Result<u64> {
        Ok(self.file.metadata().map_err(|e| self.err(e))?.len())
    }

    /// Folds the records appended since this handle last looked. A last
    /// record cut short, or one that does not open, is a write a crash cut
    /// off: it is left out, and the next write cuts it off. Any other record
    /// that does not open is an error naming it.
    fn refresh(&mut self) -> Result<()> {
        let len = self.len()?;
        if len <= self.end {
            return Ok(());
        }
        let mut buf = vec![0; (len - self.end) as usize];
        (self.file.seek(SeekFrom::Start(self.end)))
            .and_then(|_| self.file.read_exact(&mut buf))
            .map_err(|e| self.err(e))?;
        let mut rest = &buf[..];
        while let Some((n, after)) = rest.split_first_chunk::<4>() {
            let Some((record, after)) = after.split_at_checked(u32::from_le_bytes(*n) as usize)
            else {
                break;
            };
            let ops = match self.unseal(record) {
                Some(plain) => self.decode(&plain)?,
                None if after.is_empty() => break,
                None => {
                    return Err(self.err(format!(
                        "record {} (at byte {}) does not open: the file is damaged",
                        self.index, self.end
                    )));
                }
            };
            for (op, size) in ops {
                self.state.apply(op, size);
            }
            self.index += 1;
            self.end += 4 + record.len() as u64;
            rest = after;
        }
        Ok(())
    }

    fn unseal(&self, record: &[u8]) -> Option<Vec<u8>> {
        let (nonce, sealed) = record.split_at_checked(NONCE)?;
        let aad = aad(self.id?, self.index);
        let payload = Payload {
            msg: sealed,
            aad: &aad,
        };
        self.cipher.decrypt(XNonce::from_slice(nonce), payload).ok()
    }

    fn decode(&self, plain: &[u8]) -> Result<Vec<(Op, usize)>> {
        let bad =
            |e: serde_json::Error| self.err(format!("record {} does not decode: {e}", self.index));
        let raw: Vec<&RawValue> = serde_json::from_slice(plain).map_err(bad)?;
        (raw.into_iter())
            .map(|r| Ok((serde_json::from_str(r.get()).map_err(bad)?, r.get().len())))
            .collect()
    }

    /// Appends one record and syncs it, cutting off a torn last record
    /// first.
    fn append(&mut self, ops: &[String]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let id = self.id.expect("a header read under the lock");
        let record = self.record(id, self.index, ops)?;
        if self.len()? != self.end {
            self.file.set_len(self.end).map_err(|e| self.err(e))?;
        }
        (self.file.seek(SeekFrom::Start(self.end)))
            .and_then(|_| self.file.write_all(&record))
            .and_then(|()| self.file.sync_data())
            .map_err(|e| self.err(e))?;
        self.end += record.len() as u64;
        self.index += 1;
        Ok(())
    }

    /// Rewrites the file as its current state and every event, under a new
    /// file id, when what the state no longer holds is over half the file
    /// and over 64 KiB. Other handles see the new id at their next lock and
    /// fold the new file.
    fn compact(&mut self) -> Result<()> {
        let len = self.len()?;
        let waste = len.saturating_sub(HEADER + self.state.held);
        if waste <= len / 2 || waste <= MIN_WASTE {
            return Ok(());
        }
        let id = new_id();
        let mut out = self.header(id)?;
        let mut index = 0;
        let mut chunk = Vec::new();
        let mut size = 0;
        let ops = self.state.ops().map(Some).chain([None]);
        for op in ops {
            if let Some(op) = &op {
                let json = serde_json::to_string(op).map_err(|e| self.err(e))?;
                size += json.len();
                chunk.push(json);
            }
            if !chunk.is_empty() && (op.is_none() || size >= CHUNK) {
                out.extend(self.record(id, index, &chunk)?);
                index += 1;
                chunk.clear();
                size = 0;
            }
        }
        let tmp = sibling(&self.path, ".compacting");
        let mut file = owner_only(&tmp, OpenOptions::new().write(true).truncate(true))
            .map_err(|e| self.err(e))?;
        (file.write_all(&out))
            .and_then(|()| file.sync_all())
            .and_then(|()| std::fs::rename(&tmp, &self.path))
            .map_err(|e| self.err(e))?;
        self.file = open_file(&self.path)?;
        self.id = Some(id);
        self.end = out.len() as u64;
        self.index = index;
        Ok(())
    }
}

fn new_id() -> FileId {
    let mut id = [0; 16];
    OsRng.fill_bytes(&mut id);
    id
}

/// What a record is sealed with: its file and its place in it.
fn aad(id: FileId, index: u64) -> Vec<u8> {
    [&id[..], &index.to_le_bytes()].concat()
}

/// `<path><suffix>`, beside the file.
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

fn open_file(path: &Path) -> Result<File> {
    owner_only(path, OpenOptions::new().read(true).write(true))
        .map_err(|e| StoreError::Backend(format!("{}: {e}", path.display())))
}

/// Opens `path` with `options`, creating it if missing: owner-only (0600
/// on Unix) if new, and tightened to that if it was left wider.
/// On Windows it keeps the access it inherits.
fn owner_only(path: &Path, options: &mut OpenOptions) -> std::io::Result<File> {
    options.create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let file = options.mode(0o600).open(path)?;
        if file.metadata()?.permissions().mode() & 0o077 != 0 {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(file)
    }
    #[cfg(not(unix))]
    options.open(path)
}
