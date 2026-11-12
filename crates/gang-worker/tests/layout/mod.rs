//! Behaviour of [`Layout`]: the directories, the sandbox file, the entrypoint link and the
//! credential files.

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;

use gang_spi::{EXEC_LINK_NAME, JOIN_LINK_NAME};
use gang_worker::{IdentityFiles, Layout};

use crate::support::{SANDBOX, credentials};

const DIRECTORY_MODE: u32 = 0o700;
const KEY_MODE: u32 = 0o600;
const PUBLIC_FILE_MODE: u32 = 0o644;

fn mode(path: &Path) -> u32 {
    fs::metadata(path)
        .expect("path exists")
        .permissions()
        .mode()
        & 0o777
}

fn executable_in(root: &Path) -> std::path::PathBuf {
    let executable = root.join("gang");
    fs::write(&executable, b"").expect("executable can be created");
    executable
}

#[test]
fn prepare_creates_the_directories() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    assert!(layout.work_directory().is_dir());
    assert!(layout.fast_cas_directory().is_dir());
    assert!(root.path().join("bin").is_dir());
    assert!(root.path().join("identity").is_dir());
    assert_eq!(mode(&root.path().join("identity")), DIRECTORY_MODE);
}

#[test]
fn prepare_writes_the_sandbox_file_exactly() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    assert_eq!(layout.sandbox_file(), root.path().join("bin/sandbox.toml"));
    assert_eq!(
        fs::read_to_string(layout.sandbox_file()).expect("sandbox file is readable"),
        SANDBOX
    );
}

#[test]
fn prepare_links_the_entrypoint_to_the_executable() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let entrypoint = layout.entrypoint();
    assert!(
        fs::symlink_metadata(&entrypoint)
            .expect("entrypoint exists")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read_link(&entrypoint).expect("entrypoint is a link"),
        executable
    );
}

#[test]
fn prepare_replaces_a_stale_entrypoint_link() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    fs::create_dir(root.path().join("bin")).expect("bin can be created");
    symlink(
        root.path().join("old-gang"),
        root.path().join("bin").join(EXEC_LINK_NAME),
    )
    .expect("stale link can be created");
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    assert_eq!(
        fs::read_link(layout.entrypoint()).expect("entrypoint is a link"),
        executable
    );
}

#[test]
fn prepare_links_the_join_link_to_the_executable() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let join = layout.join_link();
    assert_eq!(join, root.path().join("bin").join(JOIN_LINK_NAME));
    assert_eq!(join.parent(), layout.entrypoint().parent());
    assert!(
        fs::symlink_metadata(&join)
            .expect("join link exists")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read_link(&join).expect("join link is a link"),
        executable
    );
}

#[test]
fn prepare_replaces_a_stale_join_link() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    fs::create_dir(root.path().join("bin")).expect("bin can be created");
    symlink(
        root.path().join("old-gang"),
        root.path().join("bin").join(JOIN_LINK_NAME),
    )
    .expect("stale link can be created");
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    assert_eq!(
        fs::read_link(layout.join_link()).expect("join link is a link"),
        executable
    );
}

#[test]
fn write_credentials_stores_the_pem_text_with_private_key_mode() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let issued = credentials("FIRST");
    let files = layout
        .write_credentials(0, &issued)
        .expect("credentials are written");
    assert_eq!(files.directory, root.path().join("identity").join("0"));
    assert_eq!(
        fs::read_to_string(&files.certificate).expect("certificate is readable"),
        issued.certificate.as_str()
    );
    assert_eq!(
        fs::read_to_string(&files.private_key).expect("key is readable"),
        issued.private_key.expose()
    );
    assert_eq!(
        fs::read_to_string(&files.server_ca).expect("CA is readable"),
        issued.ca_certificate.as_str()
    );
    assert_eq!(mode(&files.private_key), KEY_MODE);
    assert_eq!(mode(&files.directory), DIRECTORY_MODE);
}

#[test]
fn writing_the_same_generation_again_replaces_its_files() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    layout
        .write_credentials(0, &credentials("FIRST"))
        .expect("first write succeeds");
    let second = credentials("SECOND");
    let files = layout
        .write_credentials(0, &second)
        .expect("second write succeeds");
    assert_eq!(
        fs::read_to_string(&files.certificate).expect("certificate is readable"),
        second.certificate.as_str()
    );
    assert_eq!(
        fs::read_to_string(&files.private_key).expect("key is readable"),
        second.private_key.expose()
    );
    assert_eq!(mode(&files.private_key), KEY_MODE);
}

#[test]
fn each_generation_has_its_own_directory() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let first = layout
        .write_credentials(0, &credentials("FIRST"))
        .expect("first generation is written");
    let second = layout
        .write_credentials(1, &credentials("SECOND"))
        .expect("second generation is written");
    assert_eq!(first.directory, root.path().join("identity").join("0"));
    assert_eq!(second.directory, root.path().join("identity").join("1"));
    assert_eq!(second.certificate, second.directory.join("certificate.pem"));
    assert_eq!(second.private_key, second.directory.join("private-key.pem"));
    assert_eq!(second.server_ca, second.directory.join("ca.pem"));
}

#[test]
fn writing_a_later_generation_leaves_the_earlier_files_untouched() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let first = layout
        .write_credentials(0, &credentials("FIRST"))
        .expect("first generation is written");
    let before = [&first.certificate, &first.private_key, &first.server_ca]
        .map(|path| fs::read(path).expect("file is readable"));
    layout
        .write_credentials(1, &credentials("SECOND"))
        .expect("second generation is written");
    let after = [&first.certificate, &first.private_key, &first.server_ca]
        .map(|path| fs::read(path).expect("file is readable"));
    assert_eq!(after, before);
}

#[test]
fn remove_credentials_removes_only_that_generation() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let first = layout
        .write_credentials(0, &credentials("FIRST"))
        .expect("first generation is written");
    let second_credentials = credentials("SECOND");
    let second = layout
        .write_credentials(1, &second_credentials)
        .expect("second generation is written");
    layout
        .remove_credentials(&first)
        .expect("first generation is removed");
    assert!(!first.directory.exists());
    assert_eq!(
        fs::read_to_string(&second.certificate).expect("certificate is readable"),
        second_credentials.certificate.as_str()
    );
    assert_eq!(mode(&second.private_key), KEY_MODE);
    assert_eq!(mode(&second.directory), DIRECTORY_MODE);
}

#[test]
fn removing_a_generation_twice_succeeds() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let files = layout
        .write_credentials(0, &credentials("FIRST"))
        .expect("generation is written");
    layout
        .remove_credentials(&files)
        .expect("generation is removed");
    layout
        .remove_credentials(&files)
        .expect("a removed generation is not an error");
}

#[test]
fn prepare_removes_the_generations_of_an_earlier_process() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let stale = root.path().join("identity").join("3");
    fs::create_dir_all(&stale).expect("stale generation can be created");
    fs::write(stale.join("private-key.pem"), "STALE").expect("stale key can be written");
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    assert!(!stale.exists());
    assert_eq!(mode(&root.path().join("identity")), DIRECTORY_MODE);
    let files = layout
        .write_credentials(0, &credentials("FIRST"))
        .expect("generation numbers restart at zero");
    assert_eq!(mode(&files.private_key), KEY_MODE);
}

#[test]
fn credentials_files_have_private_modes() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let files = layout
        .write_credentials(0, &credentials("FIRST"))
        .expect("credentials are written");
    assert_eq!(mode(&files.private_key), KEY_MODE);
    assert_eq!(mode(&files.certificate), PUBLIC_FILE_MODE);
    assert_eq!(mode(&files.server_ca), PUBLIC_FILE_MODE);
    assert_eq!(mode(&files.directory), DIRECTORY_MODE);
}

#[test]
fn write_credentials_replaces_a_partial_generation_left_behind() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let partial = root.path().join("identity").join("0");
    fs::create_dir(&partial).expect("partial generation can be created");
    fs::write(partial.join("stray"), "STRAY").expect("stray file can be written");
    let issued = credentials("FIRST");
    let files = layout
        .write_credentials(0, &issued)
        .expect("credentials are written");
    assert!(!partial.join("stray").exists());
    assert_eq!(
        fs::read_to_string(&files.certificate).expect("certificate is readable"),
        issued.certificate.as_str()
    );
}

#[test]
fn remove_credentials_of_a_missing_directory_is_ok() {
    let root = tempfile::tempdir().expect("temporary directory can be created");
    let executable = executable_in(root.path());
    let layout = Layout::new(root.path());
    layout
        .prepare(SANDBOX, &executable)
        .expect("layout prepares");
    let directory = root.path().join("identity").join("9");
    let never_written = IdentityFiles {
        certificate: directory.join("certificate.pem"),
        private_key: directory.join("private-key.pem"),
        server_ca: directory.join("ca.pem"),
        directory: directory.clone(),
    };
    layout
        .remove_credentials(&never_written)
        .expect("a missing generation is not an error");
    assert!(!directory.exists());
}
