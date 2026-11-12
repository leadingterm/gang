//! Filesystem path type that is statically known to be absolute.

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};

/// Error returned when a path fails the absolute-path invariant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AbsolutePathError {
    /// The supplied path is relative.
    #[error("path `{}` is not absolute", path.display())]
    Relative {
        /// The rejected relative path.
        path: PathBuf,
    },
}

/// A filesystem path that is known to be absolute.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AbsolutePathBuf(PathBuf);

impl AbsolutePathBuf {
    /// Wraps `path` if it is absolute.
    ///
    /// # Errors
    ///
    /// Returns [`AbsolutePathError::Relative`] when `path` is not absolute.
    pub fn try_new(path: impl Into<PathBuf>) -> Result<Self, AbsolutePathError> {
        let path = path.into();
        if path.is_absolute() {
            Ok(Self(path))
        } else {
            Err(AbsolutePathError::Relative { path })
        }
    }

    /// `path` anchored at the filesystem root: a relative `path` is taken relative to `/`, an
    /// absolute one is kept as is. Always absolute, so it cannot fail.
    #[must_use]
    pub fn rooted(path: impl AsRef<Path>) -> Self {
        Self(Path::new("/").join(path))
    }

    /// Borrows the underlying path.
    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }

    /// Consumes the wrapper and returns the owned path.
    #[must_use]
    pub fn into_path_buf(self) -> PathBuf {
        self.0
    }
}

impl AsRef<Path> for AbsolutePathBuf {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Deref for AbsolutePathBuf {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl TryFrom<PathBuf> for AbsolutePathBuf {
    type Error = AbsolutePathError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Self::try_new(path)
    }
}

impl TryFrom<&Path> for AbsolutePathBuf {
    type Error = AbsolutePathError;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        Self::try_new(path)
    }
}

impl FromStr for AbsolutePathBuf {
    type Err = AbsolutePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_new(s)
    }
}

impl From<AbsolutePathBuf> for PathBuf {
    fn from(path: AbsolutePathBuf) -> Self {
        path.0
    }
}

impl Serialize for AbsolutePathBuf {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AbsolutePathBuf {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let path = PathBuf::deserialize(deserializer)?;
        Self::try_new(path).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Holder {
        path: AbsolutePathBuf,
    }

    #[test]
    fn accepts_absolute_path() {
        let path = AbsolutePathBuf::try_new("/var/lib/x").expect("absolute path accepted");
        assert_eq!(path.as_path(), Path::new("/var/lib/x"));
    }

    #[test]
    fn rejects_relative_path() {
        let err = AbsolutePathBuf::try_new("relative/p").expect_err("relative path rejected");
        assert_eq!(
            err,
            AbsolutePathError::Relative {
                path: PathBuf::from("relative/p"),
            }
        );
    }

    #[test]
    fn rejects_empty_path() {
        assert!(AbsolutePathBuf::try_new("").is_err());
    }

    #[test]
    fn rooted_anchors_a_relative_path_at_the_root() {
        assert_eq!(AbsolutePathBuf::rooted("tmp").as_path(), Path::new("/tmp"));
    }

    #[test]
    fn rooted_keeps_an_absolute_path() {
        assert_eq!(
            AbsolutePathBuf::rooted("/sys/fs/cgroup").as_path(),
            Path::new("/sys/fs/cgroup")
        );
    }

    #[test]
    fn toml_struct_field_round_trip() {
        let holder = Holder {
            path: AbsolutePathBuf::try_new("/var/lib/x").expect("absolute path accepted"),
        };
        let encoded = toml::to_string(&holder).expect("serializes");
        let decoded: Holder = toml::from_str(&encoded).expect("deserializes");
        assert_eq!(decoded, holder);
    }

    #[test]
    fn toml_rejects_relative_field() {
        let err =
            toml::from_str::<Holder>("path = \"relative/p\"").expect_err("relative path rejected");
        assert!(
            err.to_string().contains("is not absolute"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deref_allows_join() {
        let path = AbsolutePathBuf::try_new("/var").expect("absolute path accepted");
        assert_eq!(path.join("lib"), PathBuf::from("/var/lib"));
    }

    #[test]
    fn from_str_parses_absolute_and_rejects_relative() {
        let parsed: AbsolutePathBuf = "/var/lib".parse().expect("absolute path parses");
        assert_eq!(parsed.as_path(), Path::new("/var/lib"));
        assert!("relative".parse::<AbsolutePathBuf>().is_err());
    }

    #[test]
    fn into_path_buf_returns_the_wrapped_path() {
        let path = AbsolutePathBuf::try_new("/var/lib/x").expect("absolute path accepted");
        assert_eq!(path.into_path_buf(), PathBuf::from("/var/lib/x"));
    }

    #[test]
    fn owned_conversion_keeps_the_same_location() {
        let path = AbsolutePathBuf::try_new("/var/lib/x").expect("absolute path accepted");
        let owned: PathBuf = path.into();
        assert_eq!(owned.parent(), Some(Path::new("/var/lib")));
        assert_eq!(owned.file_name(), Some(OsStr::new("x")));
    }

    #[test]
    fn as_ref_exposes_the_same_file_name() {
        fn file_name_via_as_ref<P: AsRef<Path>>(path: &P) -> Option<&OsStr> {
            path.as_ref().file_name()
        }
        let path = AbsolutePathBuf::try_new("/var/lib/x").expect("absolute path accepted");
        assert_eq!(file_name_via_as_ref(&path), Some(OsStr::new("x")));
    }

    #[test]
    fn try_from_pathbuf_accepts_absolute() {
        let path =
            AbsolutePathBuf::try_from(PathBuf::from("/var/lib")).expect("absolute path accepted");
        assert_eq!(path.parent(), Some(Path::new("/var")));
    }

    #[test]
    fn try_from_pathbuf_rejects_relative() {
        let err =
            AbsolutePathBuf::try_from(PathBuf::from("rel/x")).expect_err("relative path rejected");
        assert_eq!(
            err,
            AbsolutePathError::Relative {
                path: PathBuf::from("rel/x"),
            }
        );
    }

    #[test]
    fn try_from_borrowed_path_accepts_absolute() {
        let path =
            AbsolutePathBuf::try_from(Path::new("/var/lib")).expect("absolute path accepted");
        assert_eq!(path.parent(), Some(Path::new("/var")));
    }

    #[test]
    fn try_from_borrowed_path_rejects_relative() {
        let err =
            AbsolutePathBuf::try_from(Path::new("rel/x")).expect_err("relative path rejected");
        assert_eq!(
            err,
            AbsolutePathError::Relative {
                path: PathBuf::from("rel/x"),
            }
        );
    }
}
