//! What the host must know about an authentication plugin's configuration.

use crate::ProviderName;

/// Configuration facts the host reads before it builds an authentication plugin.
/// `#[settings]` records the methods, so a composed configuration enum delegates every one of
/// them to the selected plugin's settings.
#[gang_macros::settings]
pub trait AuthenticationSettings {
    /// Whether the server must terminate TLS itself for this plugin to work.
    fn requires_tls(&self) -> bool {
        false
    }

    /// The identity providers the authentication section names, borrowed without allocation.
    fn providers(&self) -> &[ProviderName] {
        &[]
    }

    /// The provider whose proofs enroll workers, when the plugin has one.
    fn enrollment_provider(&self) -> Option<&ProviderName> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Providers(Vec<ProviderName>);

    impl AuthenticationSettings for Providers {
        fn providers(&self) -> &[ProviderName] {
            &self.0
        }
    }

    #[test]
    fn unoverridden_defaults_require_no_tls_and_no_enrollment_provider() {
        let settings = Providers(vec![
            ProviderName::try_new("github-actions").expect("valid provider name"),
        ]);

        assert!(!settings.requires_tls());
        assert_eq!(settings.enrollment_provider(), None);
    }
}
