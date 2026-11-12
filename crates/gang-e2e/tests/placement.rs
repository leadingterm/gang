//! Tests of the placement that holds one end-to-end run's scratch tree.

use std::fs;

use gang_e2e::Placement;

#[cfg(unix)]
#[test]
fn directories_exist_with_mode_0700() {
    use std::os::unix::fs::PermissionsExt;

    let placement = Placement::create(None).expect("placement is created");
    let key_directory = placement
        .ca_key()
        .parent()
        .expect("the key has a directory")
        .to_path_buf();
    let directories = [
        placement.state_dir(),
        placement.cache_dir(),
        key_directory,
        placement.noop_scratch(),
        placement.bazel_root(),
        placement.home(),
    ];
    for directory in directories {
        let metadata = fs::metadata(&directory).expect("directory exists");
        assert!(metadata.is_dir(), "{} is a directory", directory.display());
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o700,
            "{} has mode 0700",
            directory.display()
        );
    }
}

#[test]
fn workspace_and_database_are_not_created() {
    let placement = Placement::create(None).expect("placement is created");
    assert!(!placement.workspace().exists());
    assert!(!placement.database().exists());
}

#[test]
fn root_is_removed_on_drop() {
    let placement = Placement::create(None).expect("placement is created");
    let root = placement.root().to_path_buf();
    assert!(root.is_dir());
    drop(placement);
    assert!(!root.exists());
}

#[test]
fn root_is_placed_under_the_given_directory() {
    let scratch = tempfile::tempdir().expect("scratch directory is created");
    let placement = Placement::create(Some(scratch.path())).expect("placement is created");
    assert!(placement.root().starts_with(scratch.path()));
    assert!(placement.name().starts_with("gang-e2e-"));
    let last = placement
        .root()
        .file_name()
        .expect("the root has a name")
        .to_string_lossy()
        .into_owned();
    assert_eq!(placement.name(), last);
}

#[test]
fn placements_reserve_distinct_listener_ports() {
    let first = Placement::create(None).expect("first placement is created");
    let second = Placement::create(None).expect("second placement is created");
    let ports = [
        first.public_port(),
        first.worker_port(),
        first.imds_port(),
        second.public_port(),
        second.worker_port(),
        second.imds_port(),
    ];
    let unique: std::collections::HashSet<_> = ports.into_iter().collect();
    assert_eq!(unique.len(), ports.len());
}
