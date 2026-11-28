//! The sealed store, held to the conformance suite: each scenario on a new
//! file, each runner on a handle of its own to it.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use factum_conformance::Backend;
use factum_sealed::SealedStore;

mod drive;
use drive::{KEY, claimed_row, event};

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A path for a new file, in a directory of this test run's own.
pub fn new_path() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("factum-sealed-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{}.sealed", NEXT.fetch_add(1, Ordering::SeqCst)));
    let _ = std::fs::remove_file(&path);
    path
}

struct Sealed;

impl Backend for Sealed {
    type S = SealedStore;
    fn open(&self) -> Arc<SealedStore> {
        Arc::new(SealedStore::open(new_path(), KEY).unwrap())
    }
    fn reopen(&self, store: &Arc<SealedStore>) -> Arc<SealedStore> {
        Arc::new(SealedStore::open(store.path(), KEY).unwrap())
    }
}

factum_conformance::suite!(Sealed);

// ---- what only this store promises (ADR 0021) ----

use std::process::Command;

use factum::{Store, StoreError};
use factum_core::{Instant, Position, StreamId, Uuid, Version, WorkKind};
use factum_store::{Append, Unit};
use serde_json::json;

/// Appends one event to `stream`, expected at `version`.
async fn note(store: &SealedStore, stream: &StreamId, version: u64, data: serde_json::Value) {
    let unit = Unit {
        appends: vec![Append {
            stream: stream.clone(),
            expected: Version(version),
            events: vec![event(stream, version + 1, data)],
        }],
        ..Unit::default()
    };
    store.commit(unit).await.unwrap();
}

async fn log(store: &SealedStore) -> Vec<serde_json::Value> {
    let events = store.read_log(Position::default(), usize::MAX).await;
    events.unwrap().into_iter().map(|e| e.data).collect()
}

fn open_err(path: &std::path::Path, key: [u8; 32]) -> String {
    match SealedStore::open(path, key) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("{} opened", path.display()),
    }
}

/// A file of three records, one event each, the last of them `last`.
async fn three_records(last: serde_json::Value) -> PathBuf {
    let path = new_path();
    let store = SealedStore::open(&path, KEY).unwrap();
    let s = StreamId::new("note", Uuid::now_v7());
    for (i, data) in [json!(0), json!(1), last].into_iter().enumerate() {
        note(&store, &s, i as u64, data).await;
    }
    path
}

/// Where each record of a file starts, and where the last one ends.
fn records(path: &std::path::Path) -> (Vec<usize>, usize) {
    let bytes = std::fs::read(path).unwrap();
    let mut at = 84;
    let mut starts = Vec::new();
    while at + 4 <= bytes.len() {
        starts.push(at);
        at += 4 + u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    }
    (starts, at)
}

fn flip(path: &std::path::Path, at: usize) {
    let mut bytes = std::fs::read(path).unwrap();
    bytes[at] ^= 1;
    std::fs::write(path, bytes).unwrap();
}

#[tokio::test]
async fn a_wrong_key_fails_at_open() {
    let path = three_records(json!(2)).await;
    let said = open_err(&path, [8; 32]);
    assert!(said.contains("wrong key"), "{said}");
    assert_eq!(log(&SealedStore::open(&path, KEY).unwrap()).await.len(), 3);
}

#[tokio::test]
async fn a_torn_last_record_is_dropped_and_cut_off_at_the_next_write() {
    for tear in ["short", "changed"] {
        // Longer than the record written after it.
        let path = three_records(json!("x".repeat(1000))).await;
        let last = *records(&path).0.last().unwrap();
        let len = std::fs::metadata(&path).unwrap().len();
        match tear {
            "short" => {
                let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
                file.set_len(len - 5).unwrap();
            }
            _ => flip(&path, last + 30),
        }
        let store = SealedStore::open(&path, KEY).unwrap();
        assert_eq!(log(&store).await, [json!(0), json!(1)], "{tear}");
        note(
            &store,
            &StreamId::new("note", Uuid::now_v7()),
            0,
            json!("after"),
        )
        .await;
        let (starts, end) = records(&path);
        let len = std::fs::metadata(&path).unwrap().len() as usize;
        assert_eq!((starts.len(), end), (3, len), "{tear}: nothing left of it");
        let again = SealedStore::open(&path, KEY).unwrap();
        assert_eq!(
            log(&again).await,
            [json!(0), json!(1), json!("after")],
            "{tear}"
        );
    }
}

#[tokio::test]
async fn a_changed_byte_in_a_middle_record_is_an_error_naming_it() {
    let path = three_records(json!(2)).await;
    flip(&path, records(&path).0[1] + 30);
    let said = open_err(&path, KEY);
    let file = path.display().to_string();
    assert!(said.contains(&file) && said.contains("record 1 "), "{said}");
}

/// Set in a child process: the file and which turns are its.
const CHILD: &str = "FACTUM_SEALED_CHILD";
const TURNS: u64 = 20;

fn shared_stream() -> StreamId {
    StreamId::new("turns", Uuid::from_u128(1))
}

/// One of two processes: commits on its turns, each after it has seen the
/// other's last commit.
#[tokio::test]
async fn child() {
    let Ok(given) = std::env::var(CHILD) else {
        return;
    };
    let (path, mine) = given.rsplit_once('|').unwrap();
    let mine: u64 = mine.parse().unwrap();
    let store = SealedStore::open(path, KEY).unwrap();
    let s = shared_stream();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    for turn in (mine..TURNS).step_by(2) {
        while store.read_stream(&s, Version(0)).await.unwrap().len() as u64 != turn {
            assert!(
                std::time::Instant::now() < deadline,
                "turn {turn} never came"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        note(&store, &s, turn, json!({ "by": mine, "turn": turn })).await;
    }
}

#[tokio::test]
async fn two_processes_commit_alternately_and_see_each_others_events() {
    let path = new_path();
    drop(SealedStore::open(&path, KEY).unwrap());
    let children: Vec<_> = (0..2)
        .map(|mine| {
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "child", "--test-threads=1"])
                .env(CHILD, format!("{}|{mine}", path.display()))
                .spawn()
                .unwrap()
        })
        .collect();
    for mut c in children {
        assert!(c.wait().unwrap().success());
    }
    let store = SealedStore::open(&path, KEY).unwrap();
    let events = store.read_stream(&shared_stream(), Version(0)).await;
    let turns: Vec<_> = events.unwrap().into_iter().map(|e| e.data).collect();
    let want: Vec<_> = (0..TURNS)
        .map(|t| json!({ "by": t % 2, "turn": t }))
        .collect();
    assert_eq!(turns, want);
}

#[tokio::test]
async fn compaction_keeps_every_event_and_the_current_state_and_shrinks_the_file() {
    let path = new_path();
    let store = SealedStore::open(&path, KEY).unwrap();
    let other = SealedStore::open(&path, KEY).unwrap();
    let s = StreamId::new("note", Uuid::now_v7());
    // 70 KB the file keeps, so a compaction waits for as much waste.
    let mut events: Vec<_> = (0..10)
        .map(|i| json!({ "n": i, "pad": "x".repeat(7_000) }))
        .collect();
    for (i, data) in events.iter().enumerate() {
        note(&store, &s, i as u64, data.clone()).await;
    }
    let (id, lease) = claimed_row(&store, 3_000).await;
    // Each renew writes the whole row again, over 3 KB: 100 of them write
    // over 300 KB the file does not keep.
    let (mut largest, mut last, mut shrank) = (0, 0, 0);
    for i in 1..=100 {
        let renewed = store.renew(WorkKind::Task, id, lease, Instant(0), Instant(i));
        assert!(renewed.await.unwrap());
        if i % 10 == 0 {
            events.push(json!(i));
            note(&other, &s, 9 + i as u64 / 10, json!(i)).await;
        }
        let size = std::fs::metadata(&path).unwrap().len();
        if size < last {
            shrank += 1;
            assert!(
                size * 2 < last + 4096,
                "compacted at {last} to {size}: waste under half"
            );
        }
        (largest, last) = (largest.max(size), size);
    }
    assert!(shrank >= 2, "compacted {shrank} times");
    assert!(largest < 200 * 1024, "the file grew to {largest}");
    let fresh = SealedStore::open(&path, KEY).unwrap();
    for handle in [&store, &other, &fresh] {
        assert_eq!(log(handle).await, events);
        let held = handle.work(WorkKind::Task, id).await.unwrap().unwrap();
        assert_eq!(held.lease_until, Some(Instant(100)));
    }
}

#[tokio::test]
async fn a_record_out_of_its_place_is_an_error_naming_it() {
    let path = three_records(json!(2)).await;
    let (starts, _) = records(&path);
    let mut bytes = std::fs::read(&path).unwrap();
    let first = bytes[starts[0]..starts[1]].to_vec();
    let second = bytes[starts[1]..starts[2]].to_vec();
    bytes.splice(starts[0]..starts[2], second.into_iter().chain(first));
    std::fs::write(&path, bytes).unwrap();
    let said = open_err(&path, KEY);
    assert!(said.contains("record 0 "), "{said}");
}

/// Set in a child process whose files may not grow past a limit: the file.
const FULL: &str = "FACTUM_SEALED_FULL";

/// A write that fails part way, the disk full, leaves the handle as the
/// file is: without it, and able to write once there is room.
#[tokio::test]
async fn full_child() {
    let Ok(path) = std::env::var(FULL) else {
        return;
    };
    let store = SealedStore::open(&path, KEY).unwrap();
    let s = StreamId::new("note", Uuid::now_v7());
    let unit = Unit {
        appends: vec![Append {
            stream: s.clone(),
            expected: Version(0),
            events: vec![event(&s, 1, json!("x".repeat(100_000)))],
        }],
        ..Unit::default()
    };
    assert!(store.commit(unit).await.is_err(), "no room for 100 KB");
    assert_eq!(log(&store).await, [json!("before")]);
    note(&store, &s, 0, json!("small")).await;
    assert_eq!(log(&store).await, [json!("before"), json!("small")]);
}

#[cfg(unix)]
#[tokio::test]
async fn a_write_that_fails_part_way_leaves_the_handle_as_the_file_is() {
    let path = new_path();
    let store = SealedStore::open(&path, KEY).unwrap();
    note(
        &store,
        &StreamId::new("note", Uuid::now_v7()),
        0,
        json!("before"),
    )
    .await;
    // Room for 8 to 16 KiB more, as the shell counts blocks of 512 or 1024.
    let blocks = std::fs::metadata(&path).unwrap().len() / 512 + 16;
    let exe = std::env::current_exe().unwrap();
    let script = format!("trap '' XFSZ; ulimit -f {blocks}; exec \"$0\" --exact full_child");
    let child = Command::new("/bin/sh")
        .args(["-c", &script])
        .arg(exe)
        .env(FULL, &path)
        // A coverage run's profile would be cut short by the limit too.
        .env("LLVM_PROFILE_FILE", "/dev/null")
        .status()
        .unwrap();
    assert!(child.success());
    let again = SealedStore::open(&path, KEY).unwrap();
    assert_eq!(log(&again).await, [json!("before"), json!("small")]);
}

#[tokio::test]
async fn a_file_of_a_newer_format_is_refused_at_open() {
    let path = three_records(json!(2)).await;
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[8..12].copy_from_slice(&(factum_sealed::FORMAT + 1).to_le_bytes());
    std::fs::write(&path, bytes).unwrap();
    match SealedStore::open(&path, KEY) {
        Err(StoreError::NewerSchema { found, known }) => {
            assert_eq!(
                (found, known),
                (factum_sealed::FORMAT + 1, factum_sealed::FORMAT)
            );
        }
        Err(e) => panic!("refused with {e}, not NewerSchema"),
        Ok(_) => panic!("a newer format was opened"),
    }
}

/// The mode of each file in `path`'s directory named after it.
#[cfg(unix)]
fn modes(path: &std::path::Path) -> Vec<(String, u32)> {
    use std::os::unix::fs::PermissionsExt;
    let name = path.file_name().unwrap().to_str().unwrap();
    let mut modes: Vec<_> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_name().to_str().unwrap().starts_with(name))
        .map(|e| {
            let mode = e.metadata().unwrap().permissions().mode() & 0o777;
            (e.file_name().into_string().unwrap(), mode)
        })
        .collect();
    modes.sort();
    modes
}

#[cfg(unix)]
#[tokio::test]
async fn its_files_are_owner_only_made_tightened_and_compacted() {
    use std::os::unix::fs::PermissionsExt;
    let path = new_path();
    let name = path.file_name().unwrap().to_str().unwrap().to_string();
    let want = vec![(name.clone(), 0o600), (format!("{name}.lock"), 0o600)];
    let store = SealedStore::open(&path, KEY).unwrap();
    let (id, lease) = claimed_row(&store, 300).await;
    assert_eq!(modes(&path), want, "made");
    drop(store);
    for (file, _) in &want {
        let wide = std::fs::Permissions::from_mode(0o644);
        std::fs::set_permissions(path.with_file_name(file), wide).unwrap();
    }
    let store = SealedStore::open(&path, KEY).unwrap();
    assert_eq!(modes(&path), want, "tightened at open");
    let mut last = 0;
    for i in 1.. {
        let renewed = store.renew(WorkKind::Task, id, lease, Instant(0), Instant(i));
        assert!(renewed.await.unwrap());
        let size = std::fs::metadata(&path).unwrap().len();
        if size < last {
            break;
        }
        last = size;
    }
    assert_eq!(modes(&path), want, "compacted");
}

/// The access list of each file in `path`'s directory named after it, as
/// SDDL, with the file owner's SID in its first entry written `OWNER`.
#[cfg(windows)]
#[allow(unsafe_code)]
fn acls(path: &std::path::Path) -> Vec<(String, String)> {
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::null_mut;
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertSecurityDescriptorToStringSecurityDescriptorW, GetNamedSecurityInfoW,
        SDDL_REVISION_1, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION};
    let name = path.file_name().unwrap().to_str().unwrap();
    let mut acls: Vec<_> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_name().to_str().unwrap().starts_with(name))
        .map(|e| {
            let wide: Vec<u16> = e.path().as_os_str().encode_wide().chain([0]).collect();
            let info = OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
            let (mut sd, mut text, mut len) = (null_mut(), null_mut(), 0);
            // SAFETY: a NUL-terminated path; the out pointers are written by the calls,
            // and what they allocate is freed once below.
            let sddl = unsafe {
                let status = GetNamedSecurityInfoW(
                    wide.as_ptr(),
                    SE_FILE_OBJECT,
                    info,
                    null_mut(),
                    null_mut(),
                    null_mut(),
                    null_mut(),
                    &mut sd,
                );
                assert_eq!(status, 0, "GetNamedSecurityInfoW");
                let ok = ConvertSecurityDescriptorToStringSecurityDescriptorW(
                    sd,
                    SDDL_REVISION_1,
                    info,
                    &mut text,
                    &mut len,
                );
                assert_ne!(
                    ok, 0,
                    "ConvertSecurityDescriptorToStringSecurityDescriptorW"
                );
                let s = String::from_utf16_lossy(std::slice::from_raw_parts(text, len as usize));
                LocalFree(text.cast());
                LocalFree(sd);
                s
            };
            // `O:<owner>D:<flags>(<ace>)...`; of the flags only P (protected) is kept.
            let sddl = sddl.trim_end_matches('\0');
            let (owner, dacl) = sddl.strip_prefix("O:").unwrap().split_once("D:").unwrap();
            let (flags, aces) = dacl.split_at(dacl.find('(').unwrap_or(dacl.len()));
            let protected = if flags.contains('P') { "P" } else { "" };
            let aces = aces.replacen(&format!(";{owner})"), ";OWNER)", 1);
            (
                e.file_name().into_string().unwrap(),
                format!("{protected}{aces}"),
            )
        })
        .collect();
    acls.sort();
    acls
}

/// Sets `file`'s access list to one inherited and open to everyone.
#[cfg(windows)]
#[allow(unsafe_code)]
fn widen(file: &std::path::Path) {
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::null_mut;
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1, SE_FILE_OBJECT,
        SetNamedSecurityInfoW,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GetSecurityDescriptorDacl, UNPROTECTED_DACL_SECURITY_INFORMATION,
    };
    let wide: Vec<u16> = file.as_os_str().encode_wide().chain([0]).collect();
    let sddl: Vec<u16> = "D:(A;;FA;;;WD)".encode_utf16().chain([0]).collect();
    let (mut sd, mut present, mut defaulted, mut dacl) = (null_mut(), 0, 0, null_mut());
    // SAFETY: NUL-terminated wide strings; the out pointers are written by the
    // calls, and the descriptor they allocate is freed once below.
    unsafe {
        let ok = ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &mut sd,
            null_mut(),
        );
        assert_ne!(
            ok, 0,
            "ConvertStringSecurityDescriptorToSecurityDescriptorW"
        );
        assert_ne!(
            GetSecurityDescriptorDacl(sd, &mut present, &mut dacl, &mut defaulted),
            0
        );
        let status = SetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | UNPROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            dacl,
            null_mut(),
        );
        assert_eq!(status, 0, "SetNamedSecurityInfoW");
        LocalFree(sd);
    }
}

#[cfg(windows)]
#[tokio::test]
async fn its_files_are_owner_only_made_tightened_and_compacted_on_windows() {
    let path = new_path();
    let name = path.file_name().unwrap().to_str().unwrap().to_string();
    let acl = "P(A;;FA;;;OWNER)(A;;FA;;;SY)(A;;FA;;;BA)".to_string();
    let want = vec![(name.clone(), acl.clone()), (format!("{name}.lock"), acl)];
    let store = SealedStore::open(&path, KEY).unwrap();
    let (id, lease) = claimed_row(&store, 300).await;
    assert_eq!(acls(&path), want, "made");
    drop(store);
    for (file, _) in &want {
        widen(&path.with_file_name(file));
        assert!(
            acls(&path)
                .iter()
                .any(|(f, a)| f == file && !a.starts_with('P'))
        );
    }
    let store = SealedStore::open(&path, KEY).unwrap();
    assert_eq!(acls(&path), want, "tightened at open");
    let mut last = 0;
    for i in 1.. {
        let renewed = store.renew(WorkKind::Task, id, lease, Instant(0), Instant(i));
        assert!(renewed.await.unwrap());
        let size = std::fs::metadata(&path).unwrap().len();
        if size < last {
            break;
        }
        last = size;
    }
    assert_eq!(acls(&path), want, "compacted");
}

#[cfg(windows)]
#[test]
fn opening_its_files_leaves_nothing_windows_allocated_on_windows() {
    let path = new_path();
    drop(SealedStore::open(&path, KEY).unwrap());
    let before = owner_sid_strings(&path);
    for _ in 0..20 {
        drop(SealedStore::open(&path, KEY).unwrap());
    }
    // 20 opens of 2 files each would leave 40; other tests opening at the
    // same time may hold a few for a moment.
    let after = owner_sid_strings(&path);
    assert!(
        after < before + 10,
        "{before} owner SID strings, then {after}"
    );
}

/// How many blocks of the process heap, where Windows allocates what its
/// calls hand back, hold `path`'s owner SID as a string: the one factum-sealed
/// asks for at each open and frees.
#[cfg(windows)]
#[allow(unsafe_code)]
fn owner_sid_strings(path: &std::path::Path) -> usize {
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::null_mut;
    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, GetNamedSecurityInfoW, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::OWNER_SECURITY_INFORMATION;
    use windows_sys::Win32::System::Memory::{
        GetProcessHeap, HeapLock, HeapUnlock, HeapWalk, PROCESS_HEAP_ENTRY,
    };
    const PROCESS_HEAP_ENTRY_BUSY: u16 = 4;
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain([0]).collect();
    let (mut owner, mut sd, mut text) = (null_mut(), null_mut(), null_mut());
    // SAFETY: a NUL-terminated path; the out pointers are written by the calls,
    // what they allocate is freed once below, and the heap is read only
    // between its lock and unlock, in blocks HeapWalk answers busy.
    unsafe {
        let status = GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            null_mut(),
            null_mut(),
            &mut sd,
        );
        assert_eq!(status, 0, "GetNamedSecurityInfoW");
        assert_ne!(ConvertSidToStringSidW(owner, &mut text), 0);
        let len = (0..).take_while(|&i| *text.add(i) != 0).count() + 1;
        let want = std::slice::from_raw_parts(text.cast::<u8>(), len * 2).to_vec();
        let heap = GetProcessHeap();
        assert_ne!(HeapLock(heap), 0, "HeapLock");
        let (mut entry, mut found) = (PROCESS_HEAP_ENTRY::default(), 0);
        while HeapWalk(heap, &mut entry) != 0 {
            if entry.wFlags & PROCESS_HEAP_ENTRY_BUSY != 0
                && entry.cbData as usize >= want.len()
                && std::slice::from_raw_parts(entry.lpData.cast::<u8>(), want.len()) == want
            {
                found += 1;
            }
        }
        HeapUnlock(heap);
        LocalFree(text.cast());
        LocalFree(sd);
        // The string just asked for is among them: the walk sees what Windows allocates.
        assert!(
            found >= 1,
            "the owner SID string asked for is not in the heap"
        );
        found - 1
    }
}
