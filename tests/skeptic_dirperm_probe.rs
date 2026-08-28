//! Skeptic probe: does the SHIPPED (unmutated) rule refuse a group-writable key directory?
#![cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn chmod(p: &Path, mode: u32) {
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[test]
fn shipped_rule_refuses_group_writable_dirs() {
    let outer = tempfile::tempdir().unwrap();
    let d = outer.path().join("d");
    std::fs::create_dir(&d).unwrap();
    let k = d.join("k");
    std::fs::write(&k, vec![7u8; 32]).unwrap();
    chmod(&k, 0o600);

    // The exact layouts the finding says "now load" under the mutant.
    for dmode in [0o770u32, 0o720, 0o730, 0o760] {
        chmod(&d, dmode);
        match ferrodb::consensus::signing::Key::load(&k) {
            Ok(_) => panic!("dir mode {dmode:04o} LOADED — group-writable was accepted"),
            Err(e) => {
                let t = e.to_string();
                assert!(t.contains("writable by group or other"), "wrong reason for {dmode:04o}: {t}");
                println!("dir {dmode:04o} -> REFUSED: ok");
            }
        }
    }
    chmod(&d, 0o1770);
    ferrodb::consensus::signing::Key::load(&k).expect("sticky group-writable must load");
    println!("dir 1770 -> LOADED: ok");
    chmod(&d, 0o700);
    ferrodb::consensus::signing::Key::load(&k).expect("0700 must load");
    println!("dir 0700 -> LOADED: ok");
}
