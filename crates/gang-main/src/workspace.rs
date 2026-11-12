//! Location of the default local configuration file, resolved against the Bazel workspace root.

use std::ffi::OsString;
use std::path::PathBuf;

/// Environment variable that `bazel run` sets to the workspace root.
pub(crate) const WORKSPACE_ENV: &str = "BUILD_WORKSPACE_DIRECTORY";
/// Environment variable that `bazel run` sets to the directory the user ran it from.
pub(crate) const WORKING_DIRECTORY_ENV: &str = "BUILD_WORKING_DIRECTORY";
/// Default local configuration, relative to the workspace root.
pub(crate) const LOCAL_CONFIG: &str = "configs/gang.local.toml";
/// Workspace root used when `WORKSPACE_ENV` is unset or empty.
const CURRENT_DIR: &str = ".";

/// The variable's value as a path when it is set and non-empty.
fn non_empty(env: &impl Fn(&str) -> Option<OsString>, key: &str) -> Option<PathBuf> {
    match env(key) {
        Some(value) if !value.is_empty() => Some(PathBuf::from(value)),
        _ => None,
    }
}

/// The directory the user ran the command from: `WORKING_DIRECTORY_ENV` when set and non-empty,
/// otherwise the current directory.
pub(crate) fn working_directory(env: impl Fn(&str) -> Option<OsString>) -> Option<PathBuf> {
    non_empty(&env, WORKING_DIRECTORY_ENV).or_else(|| std::env::current_dir().ok())
}

/// Makes `path` absolute: an absolute path is returned unchanged, and a relative one is joined onto
/// the `working_directory`. When no working directory can be found, the relative path is returned as
/// it was given.
pub(crate) fn absolute(path: PathBuf, env: impl Fn(&str) -> Option<OsString>) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    let Some(base) = working_directory(env) else {
        return path;
    };
    base.join(path)
}

/// Workspace root: `WORKSPACE_ENV` when set and non-empty, otherwise the current directory.
pub(crate) fn workspace_root(env: impl Fn(&str) -> Option<OsString>) -> PathBuf {
    non_empty(&env, WORKSPACE_ENV).unwrap_or_else(|| PathBuf::from(CURRENT_DIR))
}

/// Local configuration path: `explicit` made absolute when given, otherwise `LOCAL_CONFIG` under
/// the workspace root, made absolute.
pub(crate) fn local_config(
    explicit: Option<PathBuf>,
    env: impl Fn(&str) -> Option<OsString>,
) -> PathBuf {
    match explicit {
        Some(path) => absolute(path, env),
        None => absolute(workspace_root(&env).join(LOCAL_CONFIG), env),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An environment holding exactly `vars`.
    fn env_of(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let owned: Vec<(String, OsString)> = vars
            .iter()
            .map(|(key, value)| ((*key).to_owned(), OsString::from(value)))
            .collect();
        move |key: &str| {
            owned
                .iter()
                .find(|(name, _)| name.as_str() == key)
                .map(|(_, value)| value.clone())
        }
    }

    /// The process's current directory, panicking when it cannot be read.
    fn current_dir() -> PathBuf {
        std::env::current_dir().expect("current directory")
    }

    #[test]
    fn absolute_path_is_unchanged() {
        assert_eq!(
            absolute(PathBuf::from("/etc/gang.toml"), env_of(&[])),
            PathBuf::from("/etc/gang.toml")
        );
    }

    #[test]
    fn absolute_path_is_unchanged_even_with_working_directory() {
        assert_eq!(
            absolute(
                PathBuf::from("/etc/gang.toml"),
                env_of(&[(WORKING_DIRECTORY_ENV, "/bwd")])
            ),
            PathBuf::from("/etc/gang.toml")
        );
    }

    #[test]
    fn relative_path_joins_working_directory() {
        assert_eq!(
            absolute(
                PathBuf::from("configs/gang.toml"),
                env_of(&[(WORKING_DIRECTORY_ENV, "/bwd")])
            ),
            PathBuf::from("/bwd").join("configs/gang.toml")
        );
    }

    #[test]
    fn empty_working_directory_is_ignored() {
        assert_eq!(
            absolute(
                PathBuf::from("gang.toml"),
                env_of(&[(WORKING_DIRECTORY_ENV, "")])
            ),
            current_dir().join("gang.toml")
        );
    }

    #[test]
    fn relative_path_without_env_joins_current_dir() {
        assert_eq!(
            absolute(PathBuf::from("gang.toml"), env_of(&[])),
            current_dir().join("gang.toml")
        );
    }

    #[test]
    fn working_directory_prefers_the_bazel_variable_and_falls_back_to_the_current_directory() {
        assert_eq!(
            working_directory(env_of(&[(WORKING_DIRECTORY_ENV, "/bwd")])),
            Some(PathBuf::from("/bwd"))
        );
        assert_eq!(
            working_directory(env_of(&[(WORKING_DIRECTORY_ENV, "")])),
            Some(current_dir())
        );
        assert_eq!(working_directory(env_of(&[])), Some(current_dir()));
    }

    #[test]
    fn workspace_root_honours_non_empty_env() {
        assert_eq!(
            workspace_root(env_of(&[(WORKSPACE_ENV, "/ws")])),
            PathBuf::from("/ws")
        );
    }

    #[test]
    fn workspace_root_ignores_empty_env() {
        assert_eq!(
            workspace_root(env_of(&[(WORKSPACE_ENV, "")])),
            PathBuf::from(".")
        );
    }

    #[test]
    fn workspace_root_falls_back_to_current_dir() {
        assert_eq!(workspace_root(env_of(&[])), PathBuf::from("."));
    }

    #[test]
    fn local_config_prefers_explicit_path() {
        assert_eq!(
            local_config(
                Some(PathBuf::from("/etc/gang.toml")),
                env_of(&[(WORKSPACE_ENV, "/ws")])
            ),
            PathBuf::from("/etc/gang.toml")
        );
    }

    #[test]
    fn local_config_honours_working_directory_for_relative_explicit_path() {
        assert_eq!(
            local_config(
                Some(PathBuf::from("gang.toml")),
                env_of(&[(WORKING_DIRECTORY_ENV, "/bwd")])
            ),
            PathBuf::from("/bwd").join("gang.toml")
        );
    }

    #[test]
    fn local_config_joins_workspace_root() {
        assert_eq!(
            local_config(None, env_of(&[(WORKSPACE_ENV, "/ws")])),
            PathBuf::from("/ws").join(LOCAL_CONFIG)
        );
    }

    #[test]
    fn local_config_falls_back_to_current_dir() {
        assert_eq!(
            local_config(None, env_of(&[])),
            current_dir().join(LOCAL_CONFIG)
        );
    }
}
