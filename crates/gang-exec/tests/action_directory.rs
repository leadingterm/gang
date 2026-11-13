//! Behaviour of the action directory: the first component under the work root that holds the
//! working directory, with both paths canonicalized before they are compared.

use std::error::Error;
use std::fmt::Debug;
use std::fs;

use gang_exec::{ActionDirectory, ExecError};

/// The error of a lookup that must fail.
fn error_of<T: Debug>(result: Result<T, ExecError>) -> Result<ExecError, Box<dyn Error>> {
    match result {
        Ok(path) => Err(format!("expected an error, got {path:?}").into()),
        Err(error) => Ok(error),
    }
}

#[test]
fn returns_the_action_directory_of_a_nested_working_directory() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    let working_directory = root.join("id1/work/sub");
    fs::create_dir_all(&working_directory)?;

    let directory = ActionDirectory::try_new(&root, &working_directory)?;

    assert_eq!(directory.path, root.canonicalize()?.join("id1"));
    assert_eq!(
        directory.working_directory,
        root.canonicalize()?.join("id1/work/sub")
    );
    Ok(())
}

#[test]
fn returns_the_action_directory_of_a_working_directory_directly_under_it()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    let working_directory = root.join("id1");
    fs::create_dir_all(&working_directory)?;

    let directory = ActionDirectory::try_new(&root, &working_directory)?;

    assert_eq!(directory.path, root.canonicalize()?.join("id1"));
    assert_eq!(
        directory.working_directory,
        root.canonicalize()?.join("id1")
    );
    Ok(())
}

#[test]
fn accepts_a_root_given_through_a_symlink() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    let working_directory = root.join("id1/work");
    fs::create_dir_all(&working_directory)?;
    let link = temp.path().join("link");
    std::os::unix::fs::symlink(&root, &link)?;

    let directory = ActionDirectory::try_new(&link, &link.join("id1/work"))?;

    assert_eq!(directory.path, root.canonicalize()?.join("id1"));
    assert_eq!(
        directory.working_directory,
        root.canonicalize()?.join("id1/work")
    );
    Ok(())
}

#[test]
fn a_working_directory_outside_the_root_is_rejected() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    let outside = temp.path().join("outside");
    fs::create_dir_all(&root)?;
    fs::create_dir_all(&outside)?;

    let error = error_of(ActionDirectory::try_new(&root, &outside))?;

    assert!(matches!(error, ExecError::OutsideWorkDirectory { .. }));
    Ok(())
}

#[test]
fn a_working_directory_equal_to_the_root_is_rejected() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;

    let error = error_of(ActionDirectory::try_new(&root, &root))?;

    assert!(matches!(error, ExecError::OutsideWorkDirectory { .. }));
    Ok(())
}

#[test]
fn a_missing_root_is_a_work_directory_error() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("missing");
    let working_directory = temp.path().join("working");
    fs::create_dir_all(&working_directory)?;

    let error = error_of(ActionDirectory::try_new(&root, &working_directory))?;

    assert!(matches!(error, ExecError::WorkDirectory { .. }));
    Ok(())
}

#[test]
fn a_missing_working_directory_is_a_working_directory_error() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;

    let error = error_of(ActionDirectory::try_new(&root, &root.join("missing")))?;

    assert!(matches!(error, ExecError::WorkingDirectory(_)));
    Ok(())
}

#[test]
fn parent_components_that_leave_the_root_are_rejected() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(root.join("id1"))?;
    fs::create_dir_all(temp.path().join("outside"))?;
    // Lexically under the root, but `id1/../..` resolves to the temporary directory.
    let escaping = root.join("id1/../../outside");

    let error = error_of(ActionDirectory::try_new(&root, &escaping))?;

    assert!(matches!(error, ExecError::OutsideWorkDirectory { .. }));
    Ok(())
}
