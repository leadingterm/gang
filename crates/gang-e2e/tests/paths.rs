//! Behaviour of path anchoring: absolute paths stay put, relative ones resolve against the base.

use std::path::{Path, PathBuf};

use gang_e2e::anchored;

#[test]
fn absolute_paths_are_unchanged() {
    assert_eq!(
        anchored(Path::new("/opt/gang/bin/gang"), Path::new("/work/tree")),
        PathBuf::from("/opt/gang/bin/gang")
    );
}

#[test]
fn relative_paths_are_joined_to_the_base() {
    assert_eq!(
        anchored(Path::new("out/gang"), Path::new("/work/tree")),
        PathBuf::from("/work/tree/out/gang")
    );
}

#[test]
fn an_empty_base_leaves_relative_paths_relative() {
    assert_eq!(
        anchored(Path::new("out/gang"), Path::new("")),
        PathBuf::from("out/gang")
    );
}
