//! Binaries sharing a database during a rolling deploy may know different schemas. A binary
//! migrates an older schema, uses its own or a newer compatible one, and refuses a newer
//! incompatible one.

/// The version of a database schema, as a backend stores it beside the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaVersion {
    /// The newest migration applied; migrations are numbered from 1.
    pub current: u32,
    /// The oldest `current` a binary may know and still use this schema: every change after it is additive.
    pub oldest_compatible: u32,
}

/// What a binary does with the schema it finds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPlan {
    /// Apply the migrations after `from` (0 for an empty database), then store the binary's own version.
    Migrate {
        /// The stored schema's current version, or zero for an empty database.
        from: u32,
    },
    /// Use the schema as stored, writing nothing: it is the binary's version, or a newer one the binary is compatible with.
    Use,
}

/// The stored schema is newer than this binary can use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("database schema {} needs a binary that knows schema {} or newer; this binary knows schema {supported}", stored.current, stored.oldest_compatible)]
pub struct SchemaTooNew {
    /// The version stored in the database.
    pub stored: SchemaVersion,
    /// The newest schema this binary knows.
    pub supported: u32,
}

impl SchemaVersion {
    /// Returns whether the version describes an applied schema with a valid compatibility range.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.current >= 1 && self.oldest_compatible >= 1 && self.oldest_compatible <= self.current
    }

    /// Decides what a binary whose newest schema is `self` does with the stored schema (`None`: an empty database).
    ///
    /// # Errors
    ///
    /// [`SchemaTooNew`] when the stored schema is newer than `self.current` and not compatible with it.
    pub const fn plan(self, stored: Option<SchemaVersion>) -> Result<SchemaPlan, SchemaTooNew> {
        match stored {
            None => Ok(SchemaPlan::Migrate { from: 0 }),
            Some(stored) if stored.current < self.current => Ok(SchemaPlan::Migrate {
                from: stored.current,
            }),
            Some(stored)
                if stored.current == self.current || stored.oldest_compatible <= self.current =>
            {
                Ok(SchemaPlan::Use)
            }
            Some(stored) => Err(SchemaTooNew {
                stored,
                supported: self.current,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SchemaPlan, SchemaTooNew, SchemaVersion};

    const BINARY: SchemaVersion = SchemaVersion {
        current: 3,
        oldest_compatible: 2,
    };

    #[test]
    fn schema_version_validity() {
        for (current, oldest_compatible, valid) in [
            (0, 0, false),
            (1, 0, false),
            (1, 2, false),
            (1, 1, true),
            (3, 1, true),
        ] {
            assert_eq!(
                SchemaVersion {
                    current,
                    oldest_compatible,
                }
                .is_valid(),
                valid
            );
        }
    }

    #[test]
    fn empty_database_is_migrated_from_zero() {
        assert_eq!(BINARY.plan(None), Ok(SchemaPlan::Migrate { from: 0 }));
    }

    #[test]
    fn older_schema_is_migrated_from_its_current_version() {
        let stored = SchemaVersion {
            current: 2,
            oldest_compatible: 1,
        };
        assert_eq!(
            BINARY.plan(Some(stored)),
            Ok(SchemaPlan::Migrate { from: 2 })
        );
    }

    #[test]
    fn equal_schema_is_used() {
        assert_eq!(BINARY.plan(Some(BINARY)), Ok(SchemaPlan::Use));
    }

    #[test]
    fn newer_compatible_schema_is_used() {
        for oldest_compatible in [2, 3] {
            let stored = SchemaVersion {
                current: 4,
                oldest_compatible,
            };
            assert_eq!(BINARY.plan(Some(stored)), Ok(SchemaPlan::Use));
        }
    }

    #[test]
    fn newer_incompatible_schema_is_rejected() {
        let stored = SchemaVersion {
            current: 5,
            oldest_compatible: 4,
        };
        let error = SchemaTooNew {
            stored,
            supported: 3,
        };
        assert_eq!(BINARY.plan(Some(stored)), Err(error));
        assert_eq!(
            error.to_string(),
            "database schema 5 needs a binary that knows schema 4 or newer; this binary knows schema 3"
        );
    }
}
