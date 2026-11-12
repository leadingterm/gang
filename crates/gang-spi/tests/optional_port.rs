//! `#[optional]` and default-bodied port methods: a plugin that does not override them gets the
//! generated default, directly and through the dispatch enum; an override wins in both paths.

use gang_testkit::must_ready as ready;

/// The probe port, re-exported at the root for the plugins and tests below.
pub use ports::Probe;

#[macro_use]
mod ports {
    use gang_spi::port;

    use crate::__private::PortError;
    use crate::Ticket;

    /// A port with a required method, an `#[optional]` method, and a method with a default body.
    #[port]
    pub trait Probe: Send + Sync {
        /// Returns the ticket's number; every plugin implements it.
        async fn required(&self, ticket: &Ticket) -> Result<u32, PortError>;

        /// Returns the ticket's number plus one when a plugin overrides it; otherwise fails as
        /// unsupported.
        #[optional]
        async fn optional(&self, ticket: &Ticket) -> Result<u32, PortError>;

        /// Returns seven unless a plugin overrides it.
        async fn fallback(&self) -> u32 {
            7
        }
    }
}

/// The ticket every probe method receives.
pub struct Ticket(pub u32);

/// Hides the `PortError` path that the delegation macro resolves through `$crate::__private`.
mod __private {
    pub use gang_spi::PortError;
}

/// A plugin that implements only the required method.
pub struct Bare;

impl Probe for Bare {
    async fn required(&self, ticket: &Ticket) -> Result<u32, gang_spi::PortError> {
        Ok(ticket.0)
    }
}

/// A plugin that implements the required method and overrides the optional one.
pub struct Overriding;

impl Probe for Overriding {
    async fn required(&self, ticket: &Ticket) -> Result<u32, gang_spi::PortError> {
        Ok(ticket.0)
    }

    async fn optional(&self, ticket: &Ticket) -> Result<u32, gang_spi::PortError> {
        Ok(ticket.0 + 1)
    }
}

/// The dispatch enum that selects a plugin at run time.
enum Dispatch {
    Bare(Bare),
    Overriding(Overriding),
}

__gang_delegate_Probe!(Dispatch { Bare => as_bare, into_bare, Overriding => as_overriding, into_overriding } []);

#[test]
fn bare_plugin_optional_reports_unsupported() {
    assert!(matches!(
        ready(Bare.optional(&Ticket(41))),
        Err(gang_spi::PortError::Unsupported {
            operation: "optional"
        })
    ));
}

#[test]
fn bare_plugin_fallback_is_the_default() {
    assert_eq!(ready(Bare.fallback()), 7);
}

#[test]
fn dispatch_bare_optional_reports_unsupported() {
    assert!(matches!(
        ready(Dispatch::Bare(Bare).optional(&Ticket(41))),
        Err(gang_spi::PortError::Unsupported {
            operation: "optional"
        })
    ));
}

#[test]
fn dispatch_bare_fallback_is_the_default() {
    assert_eq!(ready(Dispatch::Bare(Bare).fallback()), 7);
}

#[test]
fn dispatch_overriding_optional_wins_over_the_default() {
    assert_eq!(
        ready(Dispatch::Overriding(Overriding).optional(&Ticket(41))).ok(),
        Some(42)
    );
}

#[test]
fn dispatch_forwards_required_to_the_selected_plugin() {
    assert_eq!(
        ready(Dispatch::Bare(Bare).required(&Ticket(41))).ok(),
        Some(41)
    );
    assert_eq!(
        ready(Dispatch::Overriding(Overriding).required(&Ticket(41))).ok(),
        Some(41)
    );
}

#[test]
fn arc_bare_optional_reports_unsupported() {
    assert!(matches!(
        ready(Probe::optional(&std::sync::Arc::new(Bare), &Ticket(41))),
        Err(gang_spi::PortError::Unsupported {
            operation: "optional"
        })
    ));
}

#[test]
fn arc_overriding_optional_wins_over_the_default() {
    assert_eq!(
        ready(Probe::optional(
            &std::sync::Arc::new(Overriding),
            &Ticket(41)
        ))
        .ok(),
        Some(42)
    );
}

#[test]
fn arc_bare_fallback_is_the_default() {
    assert_eq!(ready(Probe::fallback(&std::sync::Arc::new(Bare))), 7);
}
