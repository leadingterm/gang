//! Recognition of wire values by this version of the schema.

use std::collections::BTreeMap;
use std::time::Duration;

/// Whether every value in a wire value is one this version knows.
pub trait Known {
    /// Returns whether every value is known.
    fn is_known(&self) -> bool;
}

macro_rules! known_leaves {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Known for $ty {
                fn is_known(&self) -> bool {
                    true
                }
            }
        )*
    };
}

known_leaves!(
    String,
    bool,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    Duration,
    gang_clock::Timestamp,
    gang_types::Url,
    gang_types::CertificatePem,
    gang_types::CertificateRequestPem,
    gang_types::Proof,
    gang_types::ProviderName,
    gang_types::AccessToken,
    gang_types::Principal,
    serde_json::Value,
);

impl<T: Known> Known for Option<T> {
    fn is_known(&self) -> bool {
        self.as_ref().is_none_or(Known::is_known)
    }
}

impl<T: Known> Known for [T] {
    fn is_known(&self) -> bool {
        self.iter().all(Known::is_known)
    }
}

impl<T: Known> Known for Vec<T> {
    fn is_known(&self) -> bool {
        self.as_slice().is_known()
    }
}

impl<K, V: Known> Known for BTreeMap<K, V> {
    fn is_known(&self) -> bool {
        self.values().all(Known::is_known)
    }
}

impl<T: Known + ?Sized> Known for Box<T> {
    fn is_known(&self) -> bool {
        self.as_ref().is_known()
    }
}

impl<T: Known + ?Sized> Known for &T {
    fn is_known(&self) -> bool {
        (*self).is_known()
    }
}

#[cfg(test)]
mod tests {
    use super::Known;
    use crate::Permission;

    #[derive(gang_macros::Known)]
    struct Nested {
        values: Vec<Option<Permission>>,
    }

    #[test]
    fn unknown_enum_inside_vector_and_option_is_not_known() {
        let known = Nested {
            values: vec![None, Some(Permission::from("state.read"))],
        };
        assert!(known.is_known());
        let unknown = Nested {
            values: vec![Some(Permission::from("future.permission"))],
        };
        assert!(!unknown.is_known());
    }
}
