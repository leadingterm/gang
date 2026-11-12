//! The user-facing side of an interactive login: showing the authorization URL and reading a
//! redirect URL the user pasted when the browser cannot reach this machine.

use std::future::Future;

use gang_types::Url;

/// What an interactive login needs from the user.
pub trait Console: Send + Sync {
    /// Shows the authorization URL to the user and offers whatever else is needed to finish a
    /// login without a local browser; may open a browser. Best effort, infallible.
    fn announce(&self, url: &Url);

    /// Resolves with a redirect URL the user pasted; stays pending for as long as no URL can
    /// arrive (input closed).
    fn pasted_redirect(&self) -> impl Future<Output = Url> + Send;
}
