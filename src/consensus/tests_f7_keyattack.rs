//! Throwaway adversarial probes against `Key::load` / `check_protection` / `unix_protection`.
//!
//! Two questions only: can `load` be made to SUCCEED on a key an attacker controls, and can it be
//! made to REFUSE a key an operator set up correctly. Everything here is Unix-only on purpose --
//! the Windows arm is a documented refusal and is not what these are aimed at.

#![cfg(unix)]

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use super::*;

fn chmod(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

/// 32 bytes that are recognisable in a string, so "did the key reach the message" is decidable.
const RECOGNISABLE: &[u8; 32] = b"KEYBYTESKEYBYTESKEYBYTESKEYBYTES";

fn write_key(dir: &Path, name: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, RECOGNISABLE).unwrap();
    chmod(&p, 0o600);
    p
}

// =============================================================================================
// A. Make it SUCCEED where it should refuse
// =============================================================================================

/// The directory rule is applied to the SYMLINK's parent, never to the parent of the file that is
/// actually opened. So a link in a locked-down directory launders a key that lives in a
/// world-writable one.
#[test]
fn symlink_launders_a_key_out_of_a_world_writable_directory() {
    let root = tempfile::tempdir().unwrap();

    let open = root.path().join("open");
    std::fs::create_dir(&open).unwrap();
    let real = write_key(&open, "k");
    chmod(&open, 0o777);

    let safe = root.path().join("safe");
    std::fs::create_dir(&safe).unwrap();
    chmod(&safe, 0o700);
    let link = safe.join("k");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    // Control: by its real name the rule fires, exactly as tests_signing asserts.
    let direct = Key::load(&real);
    assert!(direct.is_err(), "control: the real path must be refused");

    // The probe.
    let via_link = Key::load(&link);
    println!(
        "real={} (dir mode {:04o}) -> {:?}\nlink={} (dir mode {:04o}) -> {:?}",
        real.display(),
        mode_of(&open),
        direct.as_ref().err().map(|e| e.to_string()),
        link.display(),
        mode_of(&safe),
        via_link.as_ref().map(|k| k.len()).map_err(|e| e.to_string()),
    );
    assert!(
        via_link.is_err(),
        "PROBE HIT: the same 0600 key in a 0777 directory loads when reached through a symlink \
         whose own parent is 0700 -- the directory an attacker can write is never inspected"
    );
}

/// Same shape one level up: the key sits in a private directory, but that directory is reached
/// through a symlink from a world-writable one.
#[test]
fn symlink_to_the_holding_directory_is_still_inspected() {
    let root = tempfile::tempdir().unwrap();
    let open = root.path().join("open");
    std::fs::create_dir(&open).unwrap();
    let real_dir = root.path().join("realdir");
    std::fs::create_dir(&real_dir).unwrap();
    chmod(&real_dir, 0o777);
    let real = write_key(&real_dir, "k");
    let linkdir = open.join("d");
    std::os::unix::fs::symlink(&real_dir, &linkdir).unwrap();
    chmod(&open, 0o700);

    let via = Key::load(linkdir.join("k"));
    println!("through a symlinked directory -> {:?}", via.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    assert!(via.is_err(), "PROBE HIT: a symlinked holding directory escapes the mode check");
    let _ = real;
}

/// A bare relative path has an EMPTY parent, and the empty parent is filtered away, so the
/// directory check does not run at all. `"key"` and `"./key"` name the same file.
#[test]
fn a_bare_relative_path_has_an_empty_parent() {
    assert_eq!(Path::new("key").parent(), Some(Path::new("")), "this is what the filter drops");
    assert_eq!(Path::new("./key").parent(), Some(Path::new(".")), "and this is what it keeps");
}

/// The consequence, run for real in a child process whose CWD is a world-writable directory.
#[test]
fn a_bare_relative_path_skips_the_directory_check_entirely() {
    let root = tempfile::tempdir().unwrap();
    let open = root.path().join("open");
    std::fs::create_dir(&open).unwrap();
    let _ = write_key(&open, "key");
    chmod(&open, 0o777);

    let me = std::env::current_exe().unwrap();
    let name = format!(
        "{}::bare_relative_child",
        module_path!().split_once("::").expect("crate-qualified module path").1
    );
    let out = std::process::Command::new(&me)
        .args(["--ignored", "--exact", "--nocapture", "--test-threads", "1", &name])
        .current_dir(&open)
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    println!("child in CWD {} (mode {:04o}):\n{text}", open.display(), mode_of(&open));
    assert!(out.status.success(), "child reported the divergence:\n{text}");
}

#[test]
#[ignore = "driven by a_bare_relative_path_skips_the_directory_check_entirely with a chosen CWD"]
fn bare_relative_child() {
    let cwd = std::env::current_dir().unwrap();
    let dmode = mode_of(&cwd);
    let bare = Key::load("key");
    let dotted = Key::load("./key");
    println!("CWD {} mode {dmode:04o}", cwd.display());
    println!("  Key::load(\"key\")   -> {:?}", bare.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    println!("  Key::load(\"./key\") -> {:?}", dotted.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    assert_eq!(
        bare.is_ok(),
        dotted.is_ok(),
        "PROBE HIT: the same file in the same world-writable CWD loads under \"key\" and is \
         refused under \"./key\" -- the empty-parent filter skips the directory rule"
    );
}

/// A hard link puts a second name on the key's inode. If that name is in a directory an attacker
/// can write, does loading through it (or merely its existence) get past the rule?
#[test]
fn a_hard_link_does_not_launder_the_key() {
    let root = tempfile::tempdir().unwrap();
    let open = root.path().join("open");
    let safe = root.path().join("safe");
    std::fs::create_dir(&open).unwrap();
    std::fs::create_dir(&safe).unwrap();
    let real = write_key(&safe, "k");
    let hard = open.join("k");
    std::fs::hard_link(&real, &hard).unwrap();
    chmod(&open, 0o777);
    chmod(&safe, 0o700);

    let via_hard = Key::load(&hard);
    let via_safe = Key::load(&real);
    println!(
        "hard link in 0777 dir -> {:?}; original in 0700 dir -> {:?}",
        via_hard.as_ref().map(|k| k.len()).map_err(|e| e.to_string()),
        via_safe.as_ref().map(|k| k.len()).map_err(|e| e.to_string()),
    );
    assert!(via_hard.is_err(), "PROBE HIT: a hard link in a world-writable directory loads");
    assert!(via_safe.is_ok(), "the original must still load; a second link is not the operator's problem");
}

/// The reverse direction of the symlink probe: a link sitting in a world-writable directory is the
/// thing an attacker repoints, so it must be refused even though its target is fine.
#[test]
fn a_symlink_in_a_world_writable_directory_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let open = root.path().join("open");
    let safe = root.path().join("safe");
    std::fs::create_dir(&open).unwrap();
    std::fs::create_dir(&safe).unwrap();
    let real = write_key(&safe, "k");
    chmod(&safe, 0o700);
    let link = open.join("k");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    chmod(&open, 0o777);
    let r = Key::load(&link);
    println!("link in 0777 dir -> {:?}", r.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    assert!(r.is_err(), "PROBE HIT: a repointable symlink in a world-writable directory loads");
}

/// A symlink to a group-readable file: the mode inspected must be the target's.
#[test]
fn a_symlink_to_a_group_readable_key_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let real = write_key(root.path(), "k");
    chmod(&real, 0o644);
    let link = root.path().join("l");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    // A symlink's own mode is 0777 on every Unix; if the check read the link rather than the
    // target it would report 0777 or pass on a permissive target.
    let r = Key::load(&link);
    println!("symlink -> 0644 target: {:?}", r.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    let err = r.err().expect("PROBE HIT: a 0644 target loads through a symlink");
    assert!(err.to_string().contains("mode 0644"), "the target's mode must be the one named: {err}");
}

#[test]
fn dev_null_is_refused() {
    let r = Key::load("/dev/null");
    println!("/dev/null -> {:?}", r.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    let err = r.err().expect("PROBE HIT: /dev/null loads as a key");
    assert!(err.to_string().contains("not a regular file"), "{err}");
}

#[test]
fn dev_zero_is_refused() {
    // /dev/zero read_to_end would never terminate; the shape check must come first.
    let r = Key::load("/dev/zero");
    println!("/dev/zero -> {:?}", r.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    assert!(r.is_err(), "PROBE HIT: /dev/zero loads as a key");
}

/// A FIFO named where a key was expected. `is_file()` is false, so the shape check refuses it --
/// but only once `File::open` has RETURNED, and opening a FIFO for reading blocks until a writer
/// arrives. Measured with a bound, because "refuses" and "never returns" are different outcomes.
#[test]
fn a_fifo_named_as_the_key_does_not_hang_the_load() {
    let root = tempfile::tempdir().unwrap();
    let fifo = root.path().join("k");
    let st = std::process::Command::new("mkfifo")
        .arg("-m")
        .arg("600")
        .arg(&fifo)
        .status()
        .expect("mkfifo");
    assert!(st.success(), "mkfifo failed");

    let (tx, rx) = std::sync::mpsc::channel();
    let probe = fifo.clone();
    let h = std::thread::spawn(move || {
        let r = Key::load(&probe).map(|k| k.len()).map_err(|e| e.to_string());
        let _ = tx.send(r);
    });

    let verdict = rx.recv_timeout(std::time::Duration::from_millis(1500));
    let hung = verdict.is_err();
    println!("Key::load on a FIFO within 1500ms -> {verdict:?}");

    // Unblock the thread whatever happened, so the test binary can exit.
    if hung {
        let mut w = std::fs::OpenOptions::new().write(true).open(&fifo).unwrap();
        let _ = w.write_all(RECOGNISABLE);
        drop(w);
        let late = rx.recv_timeout(std::time::Duration::from_secs(5));
        println!("  after a writer arrived -> {late:?}");
    }
    h.join().unwrap();
    assert!(!hung, "PROBE HIT: Key::load blocks indefinitely on a FIFO instead of refusing it");
}

/// The mode is taken from the open descriptor, the contents from the same descriptor. A file that
/// gains bytes after the mode is approved yields the bytes present at read time, and nothing about
/// the size is carried across from the metadata call.
#[test]
fn a_file_that_grows_between_open_and_read() {
    let root = tempfile::tempdir().unwrap();
    let p = root.path().join("k");
    std::fs::write(&p, b"").unwrap();
    chmod(&p, 0o600);

    // Empty at open time: refused for length, and the refusal names 0, not a stale size.
    let empty = Key::load(&p);
    println!("empty file -> {:?}", empty.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    assert!(empty.is_err());

    std::fs::write(&p, RECOGNISABLE).unwrap();
    let grown = Key::load(&p).expect("32 bytes is a key");
    assert_eq!(grown.len(), 32, "the bytes read are the bytes present at read time");

    // And the file's mode is judged through the descriptor, so relaxing the mode after the open
    // cannot be what the check saw.
    let mut big = Vec::from(*RECOGNISABLE);
    big.extend_from_slice(&[7u8; 4096]);
    std::fs::write(&p, &big).unwrap();
    chmod(&p, 0o600);
    assert_eq!(Key::load(&p).unwrap().len(), 32 + 4096, "no size is cached from the metadata call");
}

/// Every directory mode the brief names, and the setuid/setgid ones, with the verdict spelled out.
#[test]
fn directory_mode_table() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("d");
    std::fs::create_dir(&dir).unwrap();
    let p = write_key(&dir, "k");

    // (mode, must_load) -- must_load is derived from the stated rule: refused iff group- or
    // other-writable AND not sticky.
    let table: &[(u32, bool)] = &[
        (0o0700, true),
        (0o0755, true),
        (0o0777, false),
        (0o0770, false),
        (0o0707, false),
        (0o0775, false),
        (0o0757, false),
        (0o1777, true),  // sticky: only the owner may replace the file
        (0o1755, true),
        (0o2777, false), // setgid, NOT sticky
        (0o2755, true),
        (0o4777, false), // setuid on a directory: no sticky semantics
        (0o4755, true),
        (0o6777, false),
        (0o3777, true), // setgid + sticky
        (0o0750, true),
        (0o0705, true),
    ];
    let mut wrong = Vec::new();
    for &(mode, must_load) in table {
        chmod(&dir, mode);
        let r = Key::load(&p);
        let got = r.is_ok();
        println!(
            "  dir {mode:04o} (as set: {:04o}) -> {}",
            mode_of(&dir),
            if got { "LOADS".to_string() } else { format!("refused: {}", r.as_ref().err().unwrap()) }
        );
        if got != must_load {
            wrong.push(format!("{mode:04o}: expected {}, got {}", if must_load { "load" } else { "refuse" }, if got { "load" } else { "refuse" }));
        }
    }
    chmod(&dir, 0o700);
    assert!(wrong.is_empty(), "PROBE HIT: {wrong:?}");
}

/// The directory check is `if let Ok(dmeta) = fs::metadata(dir)` -- an unreadable parent falls
/// through to `Ok(())`. Race the parent's name away between the open and that stat.
#[test]
fn racing_the_parent_directory_away_between_the_open_and_the_stat() {
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("open");
    let b = root.path().join("gone");
    std::fs::create_dir(&a).unwrap();
    let p = a.join("k");
    std::fs::write(&p, RECOGNISABLE).unwrap();
    chmod(&p, 0o600);
    chmod(&a, 0o777);

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flip = {
        let (a, b, stop) = (a.clone(), b.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = std::fs::rename(&a, &b);
                let _ = std::fs::rename(&b, &a);
            }
        })
    };

    let mut loaded = 0usize;
    let mut refused_dir = 0usize;
    let mut refused_uninspectable = 0usize;
    let mut refused_open = 0usize;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(6);
    while std::time::Instant::now() < deadline {
        match Key::load(&p) {
            Ok(_) => loaded += 1,
            Err(e) if e.to_string().contains("writable by group or other") => refused_dir += 1,
            // The post-fix branch: the open SUCCEEDED and the directory stat then failed. This is
            // the state the mutant table records as unreachable from a test.
            Err(e) if e.to_string().contains("could not be inspected") => refused_uninspectable += 1,
            Err(_) => refused_open += 1,
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    flip.join().unwrap();
    let _ = std::fs::rename(&b, &a);
    chmod(&a, 0o700);
    println!(
        "loaded={loaded} refused_mode={refused_dir} refused_uninspectable={refused_uninspectable} \
         refused_at_open={refused_open}"
    );
    assert_eq!(
        loaded, 0,
        "a key in a 0777 directory loaded {loaded} time(s): fs::metadata on the parent failed and \
         the check fell through to Ok(())"
    );
    assert!(
        refused_uninspectable > 0,
        "the open-succeeds-then-stat-fails window never opened, so this run proves nothing about \
         that branch; loaded={loaded} refused_mode={refused_dir} refused_at_open={refused_open}"
    );
}

// =============================================================================================
// B. Make it REFUSE where it should succeed
// =============================================================================================

#[test]
fn an_ordinary_0600_key_in_an_ordinary_directory_loads() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("etc");
    std::fs::create_dir(&dir).unwrap();
    chmod(&dir, 0o755);
    let p = write_key(&dir, "cluster.key");
    assert_eq!(Key::load(&p).expect("the ordinary case must load").len(), 32);
}

#[test]
fn a_key_directly_under_a_tempdir_loads() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_key(dir.path(), "k");
    println!("tempdir {} mode {:04o}", dir.path().display(), mode_of(dir.path()));
    Key::load(&p).expect("PROBE HIT: a key under tempfile::tempdir() is refused");
}

#[test]
fn paths_with_awkward_characters_load() {
    let root = tempfile::tempdir().unwrap();
    for (dirname, filename) in [
        ("a dir with spaces", "the cluster.key"),
        ("dir.with.dots", "k"),
        ("dir-with-dash", "key with spaces and a 'quote'"),
        ("ünïcode dir", "ключ.key"),
        ("dir\twith\ttabs", "k"),
        ("dir#with%weird&chars", "k=v"),
    ] {
        let d = root.path().join(dirname);
        std::fs::create_dir(&d).unwrap();
        let p = write_key(&d, filename);
        Key::load(&p).unwrap_or_else(|e| panic!("PROBE HIT: {} is refused: {e}", p.display()));
    }
}

#[test]
fn redundant_path_syntax_loads() {
    let root = tempfile::tempdir().unwrap();
    let sub = root.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    let _ = write_key(&sub, "k");
    for variant in [
        sub.join("./k"),
        sub.join("../sub/k"),
        PathBuf::from(format!("{}//k", sub.display())),
        PathBuf::from(format!("{}/./././k", sub.display())),
    ] {
        Key::load(&variant).unwrap_or_else(|e| panic!("PROBE HIT: {} is refused: {e}", variant.display()));
    }
}

#[test]
fn a_bare_relative_path_in_an_ordinary_cwd_loads() {
    // The crate root is the CWD under `cargo test`. Create, load, delete -- never left behind.
    let cwd = std::env::current_dir().unwrap();
    let name = format!(".f7-atk-probe-{}-{}", std::process::id(), line!());
    let p = cwd.join(&name);
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _guard = Cleanup(p.clone());
    std::fs::write(&p, RECOGNISABLE).unwrap();
    chmod(&p, 0o600);
    let r = Key::load(Path::new(&name));
    println!("bare relative in CWD {} (mode {:04o}) -> {:?}", cwd.display(), mode_of(&cwd), r.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    r.expect("PROBE HIT: a bare relative path is refused");
}

#[test]
fn a_root_level_key_path_does_not_panic_on_a_missing_parent() {
    // Path::new("/k").parent() is Some("/"); Path::new("/").parent() is None. Neither may panic.
    assert_eq!(Path::new("/k").parent(), Some(Path::new("/")));
    assert_eq!(Path::new("/").parent(), None);
    let _ = Key::load("/");
    let _ = Key::load("/f7-atk-nonexistent-key");
}

// =============================================================================================
// C. Can the key's bytes reach an error message?
// =============================================================================================

#[test]
fn no_error_or_rendering_carries_the_key_bytes() {
    let root = tempfile::tempdir().unwrap();
    let mut sightings = Vec::new();
    let mut check = |what: &str, text: String| {
        if text.contains("KEYBYTES") || text.contains("4b4559") || text.contains("75, 69, 89") {
            sightings.push(format!("{what}: {text}"));
        }
        println!("  {what}: {text}");
    };

    // Short key: 31 recognisable bytes.
    let short = root.path().join("short");
    std::fs::write(&short, &RECOGNISABLE[..31]).unwrap();
    chmod(&short, 0o600);
    check("under-length", Key::load(&short).err().unwrap().to_string());

    // Group-readable key with recognisable bytes.
    let readable = write_key(root.path(), "readable");
    chmod(&readable, 0o644);
    check("group-readable", Key::load(&readable).err().unwrap().to_string());

    // World-writable directory holding a recognisable key.
    let open = root.path().join("open");
    std::fs::create_dir(&open).unwrap();
    let inopen = write_key(&open, "k");
    chmod(&open, 0o777);
    check("world-writable-dir", Key::load(&inopen).err().unwrap().to_string());
    chmod(&open, 0o700);

    // Not a regular file, and a missing file whose NAME is the key material.
    let sub = root.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    check("not-a-regular-file", Key::load(&sub).err().unwrap().to_string());
    check("missing", Key::load(root.path().join("nope")).err().unwrap().to_string());

    // The Windows arm, reachable everywhere.
    check("accept-unverifiable", accept_unverifiable(PermissionCheck::Enforce, &inopen).err().unwrap().to_string());

    // A loaded key: Debug, source(), and the two frame-layer errors.
    let good = write_key(root.path(), "good");
    let key = Key::load(&good).unwrap();
    check("debug", format!("{key:?}"));
    check("debug-alternate", format!("{key:#?}"));
    check("source", format!("{:?}", key.source()));
    check("verify-too-short", verify_frame(&key, &[0u8; 4]).err().unwrap().to_string());
    let signed = sign_frame(&key, b"body").unwrap();
    let mut bad = signed.clone();
    bad[0] ^= 0xff;
    check("verify-bad-tag", verify_frame(&key, &bad).err().unwrap().to_string());
    check("sign-too-large", sign_frame(&key, &vec![0u8; MAX_FRAME_BYTES]).err().unwrap().to_string());

    // A panic message from an assertion that prints a Key, which is how a key most plausibly
    // escapes: through a test failure rather than through a log line.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let k = Key::load(&good).unwrap();
        assert!(false, "{k:?}");
    }))
    .err()
    .unwrap();
    let msg = panicked
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| "<non-string panic>".to_string());
    check("panic-payload", msg);

    assert!(sightings.is_empty(), "PROBE HIT: the key's bytes reached a message: {sightings:#?}");
}

/// The path is in every message, so a path that IS key material leaks it -- but that is the
/// operator naming their file, not the module. Recorded so the boundary is explicit.
#[test]
fn the_path_is_echoed_and_the_bytes_are_not() {
    let root = tempfile::tempdir().unwrap();
    let p = root.path().join("SECRETNAME");
    std::fs::write(&p, &RECOGNISABLE[..8]).unwrap();
    chmod(&p, 0o600);
    let text = Key::load(&p).err().unwrap().to_string();
    assert!(text.contains("SECRETNAME"), "the path is deliberately named: {text}");
    assert!(!text.contains("KEYBYTES"), "the contents are not: {text}");
}

// =============================================================================================
// D. The symlink hole, driven end to end: the attacker actually swaps the key
// =============================================================================================

/// Not just "the check is skipped" -- the substitution the check exists to stop, carried out.
///
/// `/safe/cluster.key` is a symlink into `/open`, which is 0777. The node loads it and signs a
/// frame. The attacker then replaces the file in `/open` with a key they chose. The node reloads,
/// still with `Key::load` and still without an error, and now the frames the ATTACKER signs verify.
#[test]
fn the_symlink_hole_lets_an_attacker_substitute_the_cluster_key() {
    let root = tempfile::tempdir().unwrap();
    let open = root.path().join("open");
    let safe = root.path().join("safe");
    std::fs::create_dir(&open).unwrap();
    std::fs::create_dir(&safe).unwrap();

    let real = open.join("k");
    std::fs::write(&real, b"THE-OPERATORS-REAL-CLUSTER-KEY!!").unwrap();
    chmod(&real, 0o600);
    chmod(&open, 0o777); // an attacker may create, rename and remove names here
    chmod(&safe, 0o700);

    let configured = safe.join("cluster.key");
    std::os::unix::fs::symlink(&real, &configured).unwrap();

    let before = Key::load(&configured).expect("loads through the link");
    let frame = sign_frame(&before, b"term=9 vote-for-me").unwrap();

    // The attacker's move: same directory, same final name, a key of their choosing. `rename` is
    // what the sticky-bit exception exists to prevent and what 0777-without-sticky permits.
    //
    // **Threat-model note, because this test writes the substitute as the SAME uid.** A
    // different-uid attacker cannot get a file they authored past the next check: 0644 is refused
    // by the `mode & 0o077` rule (pinned by `a_key_file_readable_by_group_or_other_is_refused`),
    // and 0600 owned by them is unreadable by the node, so `File::open` fails. What this test
    // therefore proves on its own is that the DIRECTORY GUARD DOES NOT RUN -- the substitution as
    // written needs a same-uid attacker (a compromised sidecar, a shared service account, a CI
    // job). The cross-uid version needs no ownership of anything and is
    // `the_canonicalize_fix_checks_both_ends_and_neither_hop_in_between`: point the name at a file
    // the node can ALREADY read, and the mode check sees the target's own 0600 and passes.
    let theirs = open.join(".theirs");
    std::fs::write(&theirs, b"ATTACKER-CHOSEN-CLUSTER-KEY!!!!!").unwrap();
    chmod(&theirs, 0o600);
    std::fs::rename(&theirs, &real).unwrap();

    let after = Key::load(&configured).expect("still loads, still without a complaint");
    let forged = sign_frame(&after, b"term=9 vote-for-me").unwrap();

    println!("frame signed before the swap == frame signed after: {}", forged == frame);
    println!("verify_frame(after-key, forged) -> {:?}", verify_frame(&after, &forged).map(|b| b.len()));
    assert_ne!(forged, frame, "sanity: the two keys must differ");
    assert!(
        verify_frame(&after, &forged).is_err(),
        "PROBE HIT: a node reached its key through a symlink, the key was replaced by anyone with \
         write access to the target's directory, and Key::load reported no problem either time"
    );
}

/// The directory mode the rule ACCEPTS -- sticky and world-writable, i.e. `/tmp` -- still lets any
/// user create a NEW name in it. A FIFO planted at the key's path before the operator writes the
/// key turns "refuses to start" into "never returns".
#[test]
fn a_planted_fifo_in_an_accepted_sticky_directory_hangs_the_node_instead_of_refusing_it() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("tmpish");
    std::fs::create_dir(&dir).unwrap();
    chmod(&dir, 0o1777); // accepted by unix_protection: sticky

    // Control: a real key here loads, which is the point of the sticky exception.
    let real = write_key(&dir, "real.key");
    Key::load(&real).expect("sticky world-writable is an accepted layout");

    let planted = dir.join("cluster.key");
    assert!(
        std::process::Command::new("mkfifo").arg("-m").arg("600").arg(&planted).status().unwrap().success()
    );

    let (tx, rx) = std::sync::mpsc::channel();
    let probe = planted.clone();
    let h = std::thread::spawn(move || {
        let _ = tx.send(Key::load(&probe).map(|k| k.len()).map_err(|e| e.to_string()));
    });
    let within_3s = rx.recv_timeout(std::time::Duration::from_secs(3));
    println!("Key::load on a planted FIFO, 3s bound -> {within_3s:?}");
    let hung = within_3s.is_err();
    if hung {
        let mut w = std::fs::OpenOptions::new().write(true).open(&planted).unwrap();
        let _ = w.write_all(b"x");
        drop(w);
        println!("  once a writer appeared -> {:?}", rx.recv_timeout(std::time::Duration::from_secs(5)));
    }
    h.join().unwrap();
    assert!(!hung, "PROBE HIT: startup blocks forever on a FIFO an unprivileged user planted in a directory the rule accepts");
}

// =============================================================================================
// E. Leftovers
// =============================================================================================

/// `unix_protection` takes no `PermissionCheck`, so the DIRECTORY rule must also be unbypassable
/// by the operator's assertion. tests_signing pins that for the file's own mode; this is the dir.
#[test]
fn accept_unverifiable_does_not_bypass_the_directory_rule_either() {
    let root = tempfile::tempdir().unwrap();
    let open = root.path().join("open");
    std::fs::create_dir(&open).unwrap();
    let p = write_key(&open, "k");
    chmod(&open, 0o777);
    let r = Key::load_with(&p, PermissionCheck::AcceptUnverifiable);
    println!("AcceptUnverifiable + 0777 dir -> {:?}", r.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
    chmod(&open, 0o700);
    assert!(r.is_err(), "PROBE HIT: the operator's assertion waives the directory rule on Unix");
}

#[test]
fn degenerate_paths_do_not_panic_or_load() {
    for p in ["", ".", "..", "/", "//", "/dev", "/dev/stdin", "/dev/fd/0"] {
        let r = Key::load(p);
        println!("{p:?} -> {:?}", r.as_ref().map(|k| k.len()).map_err(|e| e.to_string()));
        assert!(r.is_err(), "PROBE HIT: {p:?} loaded as a signing key");
    }
}

/// `Display` is what the tests above read. `Debug` on the error is what a `.unwrap()` prints, and
/// is a separate rendering that could carry different content.
#[test]
fn the_debug_rendering_of_every_refusal_is_also_free_of_the_key() {
    let root = tempfile::tempdir().unwrap();
    let short = root.path().join("s");
    std::fs::write(&short, &RECOGNISABLE[..31]).unwrap();
    chmod(&short, 0o600);
    let readable = write_key(root.path(), "r");
    chmod(&readable, 0o644);
    let open = root.path().join("open");
    std::fs::create_dir(&open).unwrap();
    let inopen = write_key(&open, "k");
    chmod(&open, 0o777);

    let mut sightings = Vec::new();
    for (what, e) in [
        ("short", Key::load(&short).err().unwrap()),
        ("readable", Key::load(&readable).err().unwrap()),
        ("open-dir", Key::load(&inopen).err().unwrap()),
    ] {
        for (kind, text) in [("Debug", format!("{e:?}")), ("Display", e.to_string())] {
            println!("  {what}/{kind}: {text}");
            if text.contains("KEYBYTES") || text.contains("4b4559") || text.contains("[75, 69") {
                sightings.push(format!("{what}/{kind}"));
            }
        }
    }
    chmod(&open, 0o700);
    assert!(sightings.is_empty(), "PROBE HIT: {sightings:?}");
}

// =============================================================================================
// F. Against the PROPOSED canonicalize fix
// =============================================================================================

/// Which of two known keys a loaded `Key` is, decided without reading its bytes.
fn identify(loaded: &Key, candidates: &[(&str, &[u8])]) -> String {
    let probe = loaded.tag(b"which key is this");
    for (name, bytes) in candidates {
        let k = Key::from_bytes_for_test(bytes.to_vec()).unwrap();
        if k.tag(b"which key is this") == probe {
            return (*name).to_string();
        }
    }
    "<neither>".to_string()
}

/// **The canonicalize fix checks the two ENDPOINTS of the resolution and no hop in between.**
///
/// `path.parent()` is the directory of the name as written. `canonicalize(path).parent()` is the
/// directory of the inode finally reached. An attacker whose write access is at an INTERMEDIATE
/// hop is in neither, so both checks pass and the resolution still ran through a directory they
/// control.
///
/// No race, no ownership assumption, no same-uid assumption: the attacker's only move is to point
/// a name they may write at a file the node can already read.
#[test]
fn the_canonicalize_fix_checks_both_ends_and_neither_hop_in_between() {
    let root = tempfile::tempdir().unwrap();
    chmod(root.path(), 0o700);

    const GOOD: &[u8; 32] = b"THE-OPERATORS-REAL-CLUSTER-KEY!!";
    const OTHER: &[u8; 32] = b"SOME-OTHER-NODE-READABLE-FILE!!!";

    // The operator's key, in a private directory.
    let safe = root.path().join("safe");
    std::fs::create_dir(&safe).unwrap();
    chmod(&safe, 0o700);
    let good = safe.join("good");
    std::fs::write(&good, GOOD).unwrap();
    chmod(&good, 0o600);

    // Any other file the node can already read: 0600, node-owned, in a private directory. A log, a
    // rotated key, a fixture -- the attacker needs no write access to it, only its name.
    let known = root.path().join("known");
    std::fs::write(&known, OTHER).unwrap();
    chmod(&known, 0o600);

    // The middle hop: a directory the attacker may write.
    let open = root.path().join("open");
    std::fs::create_dir(&open).unwrap();
    chmod(&open, 0o777);

    // The configured path, in a directory the operator locked down.
    let configured = safe.join("cluster.key");
    std::os::unix::fs::symlink(open.join("k"), &configured).unwrap();

    // Honest state: the chain lands on the operator's key.
    std::os::unix::fs::symlink(&good, open.join("k")).unwrap();
    let honest = Key::load(&configured).expect("the honest chain must load");
    assert_eq!(identify(&honest, &[("good", GOOD), ("other", OTHER)]), "good");

    // The attacker's whole move: repoint the middle name. `rename` over it, which 0777-non-sticky
    // permits and which needs no ownership of anything at either end.
    let tmp = open.join(".t");
    std::os::unix::fs::symlink(&known, &tmp).unwrap();
    std::fs::rename(&tmp, open.join("k")).unwrap();

    let after = Key::load(&configured);
    println!("configured  = {}  (parent mode {:04o})", configured.display(), mode_of(&safe));
    println!("middle hop  = {}  (parent mode {:04o})  <- the attacker writes here", open.join("k").display(), mode_of(&open));
    println!("canonical   = {:?}", std::fs::canonicalize(&configured));
    match &after {
        Ok(k) => println!("Key::load -> Ok, and it is the {} key", identify(k, &[("good", GOOD), ("other", OTHER)])),
        Err(e) => println!("Key::load -> {e}"),
    }
    let loaded = after.expect_err(
        "PROBE HIT: both endpoint checks passed and the node loaded a file the attacker chose -- \
         the world-writable directory was an intermediate hop and neither check looked at it",
    );
    let _ = loaded;
}
