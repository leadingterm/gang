//! Identity providers: verifying proofs on the server, obtaining them on the client.

use gang_types::{LoginSession, Principal, Proof, RefreshToken, Url};

use crate::{PortError, port};

/// The server side of an identity provider.
#[port]
pub trait ProofVerifierPort: Send + Sync {
    /// Verifies a proof and returns who it identifies; [`PortError::Rejected`] if it fails.
    ///
    /// # Errors
    ///
    /// [`PortError::Rejected`] when the proof fails verification, other [`PortError`] variants
    /// when the provider cannot be reached or fails.
    async fn verify(&self, proof: &Proof) -> Result<Principal, PortError>;
}

/// The client side of an identity provider: an interactive login in two steps, and renewal.
/// A provider without an interactive login reports [`LoginSupport::Unsupported`] from
/// [`login`](Self::login) and leaves [`begin_login`](Self::begin_login),
/// [`complete_login`](Self::complete_login), and [`renew`](Self::renew) at their default,
/// [`PortError::Unsupported`]. A provider with an interactive login overrides them.
#[port]
pub trait LoginPort: Send + Sync {
    /// Whether the provider has an interactive login. The server lists only providers that
    /// report [`LoginSupport::Interactive`] in `ServerInfo.login`.
    fn login(&self) -> LoginSupport;

    /// Starts a login and returns the URL the user must visit.
    ///
    /// # Errors
    ///
    /// [`PortError::Unsupported`] (the default) when the provider has no interactive login, other
    /// [`PortError`] variants when the provider fails.
    #[optional]
    async fn begin_login(&self, request: &LoginRequest<'_>) -> Result<LoginChallenge, PortError>;

    /// Finishes a login from the URL the provider redirected the browser to.
    ///
    /// # Errors
    ///
    /// [`PortError::Unsupported`] (the default) when the provider has no interactive login,
    /// [`PortError::Rejected`] when the callback does not verify, other [`PortError`] variants
    /// when the provider fails.
    #[optional]
    async fn complete_login(
        &self,
        session: LoginSession,
        callback_url: &Url,
    ) -> Result<LoginGrant, PortError>;

    /// Obtains a fresh proof from a stored refresh token. `client` selects the OAuth client the
    /// refresh runs under: `None` uses the provider's own configured client, `Some` uses that
    /// client's id and secret, as a login does.
    ///
    /// # Errors
    ///
    /// [`PortError::Unsupported`] (the default) when the provider has no interactive login,
    /// [`PortError::Rejected`] when the refresh token is refused, other [`PortError`] variants
    /// when the provider fails.
    #[optional]
    async fn renew(
        &self,
        refresh_token: &RefreshToken,
        client: Option<&LoginClient>,
    ) -> Result<LoginGrant, PortError>;
}

/// Whether a provider has an interactive login.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoginSupport {
    /// The provider has an interactive login; the user can sign in through a browser.
    Interactive,
    /// The provider has no interactive login.
    Unsupported,
}

/// An OAuth client a login runs under, in place of the provider's configured one.
#[derive(Debug, Clone)]
pub struct LoginClient {
    /// The client id, sent as `client_id` to the authorization and token endpoints.
    pub id: String,
    /// The client secret, sent to the token endpoint.
    pub secret: gang_types::ClientSecret,
}

/// Where the provider sends the browser after the user approves.
#[derive(Debug, Clone, Copy)]
pub struct LoginRequest<'a> {
    /// The redirect URI, for example `http://127.0.0.1:{port}/callback`.
    pub redirect_uri: &'a Url,
    /// The client to log in with; `None` uses the provider's own configured client.
    pub client: Option<&'a LoginClient>,
}

/// A started login.
#[derive(Debug)]
pub struct LoginChallenge {
    /// The URL to open in a browser.
    pub authorization_url: Url,
    /// The OAuth state sent in the authorization URL and expected in the callback.
    pub state: String,
    /// The state to hand back to [`LoginPort::complete_login`].
    pub session: LoginSession,
}

/// What a completed login or a renewal yields.
#[derive(Debug, Clone)]
pub struct LoginGrant {
    /// The proof to present at the certificate endpoint.
    pub proof: Proof,
    /// The token for the next renewal, when the provider issued or rotated one.
    pub refresh_token: Option<RefreshToken>,
}
