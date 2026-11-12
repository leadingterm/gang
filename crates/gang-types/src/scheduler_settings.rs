//! What the host must read and set in a scheduler plugin's configuration without knowing which
//! scheduler plugin it is.

use crate::Url;

/// What the host reads and sets in a scheduler plugin's configuration without knowing the plugin.
/// `#[settings]` records the methods, so a composed configuration enum delegates every one of
/// them to the selected plugin's settings.
#[gang_macros::settings]
pub trait SchedulerSettings {
    /// The endpoint workers dial the scheduler at, when the configuration names or derives one.
    fn worker_endpoint(&self) -> Option<&Url> {
        None
    }

    /// Sets the worker endpoint (None clears it); schedulers without the concept ignore it.
    fn set_worker_endpoint(&mut self, endpoint: Option<Url>) {
        let _ = endpoint;
    }

    /// The key of the worker endpoint inside the plugin's own section, when the plugin has one.
    fn worker_endpoint_key(&self) -> Option<&'static str> {
        None
    }

    /// Fills the worker endpoint with `derived` when the configuration omits it; an explicit
    /// value is kept.
    #[must_use]
    fn with_worker_endpoint(mut self, derived: Url) -> Self
    where
        Self: Sized,
    {
        if self.worker_endpoint().is_none() {
            self.set_worker_endpoint(Some(derived));
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Endpoint(Option<Url>);

    impl SchedulerSettings for Endpoint {
        fn worker_endpoint(&self) -> Option<&Url> {
            self.0.as_ref()
        }

        fn set_worker_endpoint(&mut self, endpoint: Option<Url>) {
            self.0 = endpoint;
        }
    }

    struct Unconfigurable;

    impl SchedulerSettings for Unconfigurable {}

    fn url(text: &str) -> Url {
        Url::parse(text).expect("valid URL literal")
    }

    #[test]
    fn with_worker_endpoint_fills_an_absent_endpoint() {
        let settings = Endpoint(None).with_worker_endpoint(url("https://scheduler.example/"));

        assert_eq!(
            settings.worker_endpoint(),
            Some(&url("https://scheduler.example/"))
        );
    }

    #[test]
    fn with_worker_endpoint_keeps_an_explicit_endpoint() {
        let settings = Endpoint(Some(url("https://explicit.example/")))
            .with_worker_endpoint(url("https://derived.example/"));

        assert_eq!(
            settings.worker_endpoint(),
            Some(&url("https://explicit.example/"))
        );
    }

    #[test]
    fn default_methods_ignore_set_and_report_no_endpoint() {
        let mut settings = Unconfigurable;
        settings.set_worker_endpoint(Some(url("https://scheduler.example/")));

        assert_eq!(settings.worker_endpoint(), None);
        assert_eq!(Unconfigurable.worker_endpoint_key(), None);
    }

    #[test]
    fn default_with_worker_endpoint_leaves_settings_without_endpoint() {
        let settings = Unconfigurable.with_worker_endpoint(url("https://scheduler.example/"));

        assert_eq!(settings.worker_endpoint(), None);
    }
}
