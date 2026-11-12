//! Behaviour of `gang_exec::break_hard_links` on real directory trees.
//!
//! Every tree lives under one temporary directory so that partners outside the root share the
//! filesystem with it, which hard links require. The assertions are about inode identity, link
//! counts, modes, contents and directory listings, so they hold whether the copy inside the
//! implementation is a reflink or a plain copy.

use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use gang_exec::{LinkBreakError, break_hard_links};

/// The mode the hard-linked pair is created with.
const PAIR_MODE: u32 = 0o640;

/// The content every hard-linked pair shares.
const PAYLOAD: &[u8] = b"payload";

/// Writes `content` to `path` and sets `mode` on it.
fn write_file(path: &Path, content: &[u8], mode: u32) -> Result<(), Box<dyn Error>> {
    fs::write(path, content)?;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

/// The inode number of `path`, without following a symlink.
fn ino(path: &Path) -> io::Result<u64> {
    Ok(fs::symlink_metadata(path)?.ino())
}

/// The link count of `path`, without following a symlink.
fn nlink(path: &Path) -> io::Result<u64> {
    Ok(fs::symlink_metadata(path)?.nlink())
}

/// The permission bits of `path`.
fn mode(path: &Path) -> io::Result<u32> {
    Ok(fs::metadata(path)?.permissions().mode() & 0o777)
}

/// The sorted names of the entries directly inside `dir`.
fn names(dir: &Path) -> Result<Vec<OsString>, Box<dyn Error>> {
    let mut names = fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

/// Creates `root` inside `parent` and returns it.
fn make_root(parent: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let root = parent.join("root");
    fs::create_dir(&root)?;
    Ok(root)
}

/// Creates `a` and `b` in `root` as a hard-linked pair with [`PAIR_MODE`] and [`PAYLOAD`].
fn make_pair(root: &Path) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
    let a = root.join("a");
    let b = root.join("b");
    write_file(&a, PAYLOAD, PAIR_MODE)?;
    fs::hard_link(&a, &b)?;
    Ok((a, b))
}

#[test]
fn hard_linked_pair_is_separated_by_breaking_one_link() -> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    let (a, b) = make_pair(&root)?;
    let before = ino(&a)?;
    assert_eq!(before, ino(&b)?, "precondition: the pair shares an inode");

    assert_eq!(break_hard_links(&root)?, 1);

    assert_ne!(ino(&a)?, ino(&b)?);
    let still_shared = [ino(&a)?, ino(&b)?]
        .iter()
        .filter(|&&inode| inode == before)
        .count();
    assert_eq!(
        still_shared, 1,
        "exactly one member keeps the original inode"
    );
    assert_eq!(nlink(&a)?, 1);
    assert_eq!(nlink(&b)?, 1);
    assert_eq!(fs::read(&a)?, PAYLOAD);
    assert_eq!(fs::read(&b)?, PAYLOAD);
    assert_eq!(mode(&a)?, PAIR_MODE);
    assert_eq!(mode(&b)?, PAIR_MODE);

    fs::write(&a, b"changed")?;
    assert_eq!(fs::read(&b)?, PAYLOAD, "overwriting a must not change b");
    Ok(())
}

#[test]
fn file_without_partner_keeps_its_inode_and_is_not_counted() -> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    let solo = root.join("solo");
    write_file(&solo, PAYLOAD, PAIR_MODE)?;
    let before = ino(&solo)?;

    assert_eq!(break_hard_links(&root)?, 0);

    assert_eq!(ino(&solo)?, before);
    assert_eq!(nlink(&solo)?, 1);
    assert_eq!(fs::read(&solo)?, PAYLOAD);
    Ok(())
}

#[test]
fn read_only_and_executable_modes_are_preserved() -> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    let read_only = root.join("read_only");
    let read_only_link = root.join("read_only_link");
    let executable = root.join("executable");
    let executable_link = root.join("executable_link");
    write_file(&read_only, PAYLOAD, 0o444)?;
    fs::hard_link(&read_only, &read_only_link)?;
    write_file(&executable, PAYLOAD, 0o755)?;
    fs::hard_link(&executable, &executable_link)?;

    assert_eq!(break_hard_links(&root)?, 2);

    assert_eq!(mode(&read_only)?, 0o444);
    assert_eq!(mode(&read_only_link)?, 0o444);
    assert_eq!(mode(&executable)?, 0o755);
    assert_eq!(mode(&executable_link)?, 0o755);
    assert_eq!(fs::read(&read_only_link)?, PAYLOAD);
    assert_eq!(fs::read(&executable_link)?, PAYLOAD);
    Ok(())
}

#[test]
fn nested_links_are_broken_including_those_whose_partner_is_outside_the_root()
-> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    let deep = root.join("sub").join("deep");
    fs::create_dir_all(&deep)?;
    let inside = deep.join("inside");
    write_file(&inside, PAYLOAD, PAIR_MODE)?;
    let inside_partner = deep.join("inside_partner");
    fs::hard_link(&inside, &inside_partner)?;

    let outside_dir = parent.path().join("outside");
    fs::create_dir(&outside_dir)?;
    let partner = outside_dir.join("partner");
    write_file(&partner, PAYLOAD, PAIR_MODE)?;
    let outside_ino = ino(&partner)?;
    let linked = deep.join("linked");
    fs::hard_link(&partner, &linked)?;
    assert_eq!(
        ino(&linked)?,
        outside_ino,
        "precondition: the link shares the partner's inode"
    );

    assert_eq!(break_hard_links(&root)?, 2);

    assert_ne!(ino(&inside)?, ino(&inside_partner)?);
    assert_eq!(nlink(&inside)?, 1);
    assert_eq!(nlink(&inside_partner)?, 1);
    assert_ne!(ino(&linked)?, outside_ino);
    assert_eq!(nlink(&linked)?, 1);
    assert_eq!(
        ino(&partner)?,
        outside_ino,
        "the outside partner keeps its inode"
    );
    assert_eq!(nlink(&partner)?, 1);
    assert_eq!(fs::read(&partner)?, PAYLOAD);
    Ok(())
}

#[test]
fn symlinks_are_left_alone_and_directories_outside_the_root_are_not_followed()
-> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    let (a, b) = make_pair(&root)?;
    let alias = root.join("alias");
    let target = a.clone();
    symlink(&target, &alias)?;

    let outside_dir = parent.path().join("outside");
    fs::create_dir(&outside_dir)?;
    let outside_file = outside_dir.join("file");
    let outside_other = outside_dir.join("other");
    write_file(&outside_file, PAYLOAD, PAIR_MODE)?;
    fs::hard_link(&outside_file, &outside_other)?;
    let dir_alias = root.join("dir_alias");
    symlink(&outside_dir, &dir_alias)?;

    assert_eq!(break_hard_links(&root)?, 1);

    assert!(fs::symlink_metadata(&alias)?.file_type().is_symlink());
    assert_eq!(fs::read_link(&alias)?, target);
    assert_ne!(ino(&a)?, ino(&b)?);
    assert_eq!(nlink(&a)?, 1);
    assert_eq!(nlink(&b)?, 1);
    assert_eq!(
        nlink(&outside_file)?,
        2,
        "a directory outside the root is not followed"
    );
    assert_eq!(nlink(&outside_other)?, 2);
    Ok(())
}

#[test]
fn no_temporary_entries_are_left_behind() -> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    make_pair(&root)?;
    write_file(&root.join("solo"), PAYLOAD, PAIR_MODE)?;
    let before = names(&root)?;

    break_hard_links(&root)?;

    assert_eq!(names(&root)?, before);
    assert_eq!(
        before,
        [
            OsString::from("a"),
            OsString::from("b"),
            OsString::from("solo")
        ]
    );
    Ok(())
}

#[test]
fn empty_root_has_nothing_to_replace() -> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;

    assert_eq!(break_hard_links(&root)?, 0);
    assert!(names(&root)?.is_empty());
    Ok(())
}

#[test]
fn missing_root_is_an_error_naming_the_root() -> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let missing = parent.path().join("missing");

    let err = break_hard_links(&missing)
        .err()
        .ok_or("expected an error for a missing root")?;

    assert_eq!(err.path, missing);
    Ok(())
}

/// A file occupying this process's temporary name is never removed, and its pair stays linked.
#[test]
fn a_file_occupying_the_temporary_name_of_this_process_is_never_removed()
-> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    let occupant = root.join(format!(".gang-exec-{}", std::process::id()));
    write_file(&occupant, b"keep", 0o600)?;
    let (a, b) = make_pair(&root)?;
    let occupant_ino = ino(&occupant)?;

    let outcome = break_hard_links(&root);
    assert!(
        matches!(outcome, Err(LinkBreakError { .. })),
        "a copy that cannot take the temporary name must fail the call"
    );
    assert_eq!(nlink(&a)?, 2, "the unbroken pair keeps its link count");
    assert_eq!(nlink(&b)?, 2);
    assert_eq!(ino(&a)?, ino(&b)?);
    assert_eq!(fs::read(&a)?, PAYLOAD);

    assert_eq!(fs::read(&occupant)?, b"keep");
    assert_eq!(ino(&occupant)?, occupant_ino);
    Ok(())
}

/// A failure below the root fails the call, and the error names the full path of the entry, whoever runs the test.
#[test]
fn a_nested_failure_names_its_full_path_in_the_error() -> Result<(), Box<dyn Error>> {
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    let nested = root.join("a").join("b");
    fs::create_dir_all(&nested)?;
    let linked = nested.join("linked");
    write_file(&linked, PAYLOAD, PAIR_MODE)?;
    fs::hard_link(&linked, parent.path().join("partner"))?;
    let occupant = nested.join(format!(".gang-exec-{}", std::process::id()));
    write_file(&occupant, b"keep", 0o600)?;

    let error = break_hard_links(&root)
        .err()
        .ok_or("expected an error for a copy that cannot take its temporary name")?;

    assert!(
        error.path.ends_with(Path::new("a/b/linked")),
        "the error names the full path, got {}",
        error.path.display()
    );
    Ok(())
}

/// Every link in a directory of two thousand files is broken, leaving no temporary entries.
#[test]
fn every_link_in_a_large_directory_is_broken() -> Result<(), Box<dyn Error>> {
    const COUNT: usize = 2000;
    let parent = tempfile::tempdir()?;
    let root = make_root(parent.path())?;
    let outside = parent.path().join("outside");
    fs::create_dir(&outside)?;
    let expected: Vec<OsString> = (0..COUNT)
        .map(|index| OsString::from(format!("f{index:04}")))
        .collect();
    for name in &expected {
        let file = root.join(name);
        let partner = outside.join(name);
        write_file(&file, PAYLOAD, PAIR_MODE)?;
        fs::hard_link(&file, &partner)?;
    }

    assert_eq!(break_hard_links(&root)?, COUNT);

    for name in &expected {
        let file = root.join(name);
        let partner = outside.join(name);
        assert_eq!(nlink(&file)?, 1);
        assert_eq!(nlink(&partner)?, 1);
        assert_ne!(ino(&file)?, ino(&partner)?);
    }
    assert_eq!(names(&root)?, expected);
    Ok(())
}
