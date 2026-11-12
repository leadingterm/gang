//! What the Gate 2 scenarios see of a deployment whose clients authenticate with bearer tokens, as
//! one trait.

use std::path::PathBuf;

use crate::build_output::BuildOutcome;
use crate::errors::{E2eError, Result};
use crate::issuer::TokenSpec;
use crate::session::SessionCookie;
use crate::sse::EventStream;

/// A logged-in client session: its principal, its client home, and a bearer token for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    /// The principal, as `provider:principal`.
    pub principal: String,
    /// The client home directory of the logged-in session.
    pub home: PathBuf,
    /// A bearer token for the session, as `gang get` returns it.
    pub token: String,
    /// Path of the Gang CA PEM when the server terminates TLS with it.
    pub authority: Option<PathBuf>,
}

impl Credentials {
    /// Parses the provider-scoped identity of this session's principal.
    ///
    /// # Errors
    ///
    /// Returns an error if the principal is not a valid provider-scoped identity.
    pub fn identity(&self) -> Result<gang_types::Identity> {
        self.principal
            .parse()
            .map_err(E2eError::parse("the principal identity"))
    }
}

/// Whether a client sends its bearer token with a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presenting {
    /// The client sends its bearer token.
    Token,
    /// The client sends no credential.
    Nothing,
}

/// How a login request ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Issuance {
    /// The login succeeded and produced a session.
    Issued(Credentials),
    /// The login was refused; the text is what `gang login` reported.
    Refused(String),
}

/// What one poll of an established connection observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelEvent {
    /// The server acknowledged a ping.
    PingAcknowledged,
    /// The connection is gone.
    Closed,
}

/// How one request on a connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    /// The server served the request.
    Served,
    /// The server answered gRPC status 16: the request carried no valid token.
    Unauthenticated,
    /// The server answered gRPC status 7: the principal is refused.
    Denied,
    /// The connection is gone and no answer arrived.
    Closed,
}

/// A connection the server accepted, pinged and sent requests on.
pub trait Channel {
    /// Sends a ping and waits for the server's answer: [`ChannelEvent::PingAcknowledged`] when it
    /// acknowledges the ping, or [`ChannelEvent::Closed`] when the connection ends first.
    ///
    /// # Errors
    ///
    /// When the ping cannot be sent.
    fn ping(&mut self) -> Result<ChannelEvent>;

    /// Sends one REAPI `Capabilities` request on this connection, with the token the channel was
    /// opened with (or none), and reports how the server answered.
    ///
    /// # Errors
    ///
    /// When the request cannot be written or the answer cannot be understood.
    fn request(&mut self) -> Result<Reply>;
}

/// The answer to a connection attempt.
#[derive(Debug)]
pub enum Admission<C> {
    /// The server accepted the TLS connection.
    Accepted(C),
    /// The server refused the TLS connection.
    Refused,
}

/// A deployment whose clients authenticate with bearer tokens, with an in-process issuer that
/// vouches for the test identities.
pub trait Identity: Sync {
    /// The connection type this deployment's clients open.
    type Channel: Channel;

    /// The open response of the state stream this deployment serves.
    type Stream: EventStream;

    /// Mints a token for `spec` and logs it in as a new client session.
    ///
    /// # Errors
    ///
    /// When the token cannot be minted or the login cannot be run. A login the server refuses is
    /// `Ok` with [`Issuance::Refused`].
    fn issue(&self, spec: &TokenSpec) -> Result<Issuance>;

    /// Opens a connection to the public endpoint, presenting `credentials` when `presenting` says
    /// so.
    ///
    /// # Errors
    ///
    /// When the TLS configuration cannot be built or the connection cannot be attempted. A TLS
    /// refusal is `Ok` with [`Admission::Refused`].
    fn connect(
        &self,
        credentials: &Credentials,
        presenting: Presenting,
    ) -> Result<Admission<Self::Channel>>;

    /// Builds the sample workspace over the public endpoint, as `credentials` would present
    /// itself.
    ///
    /// # Errors
    ///
    /// When Bazel cannot be run; a build that fails is `Ok` with a failed outcome.
    fn build_as(&self, credentials: &Credentials, presenting: Presenting) -> Result<BuildOutcome>;

    /// Revokes the principal of `target`, as an administrator, without restarting the server.
    ///
    /// # Errors
    ///
    /// When the revocation cannot be requested or the server rejects it.
    fn revoke(&self, target: &Credentials) -> Result<()>;

    /// Restores the principal of `target` after a revocation, as an administrator.
    ///
    /// # Errors
    ///
    /// When the restoration cannot be requested or the server rejects it.
    fn restore(&self, target: &Credentials) -> Result<()>;

    /// The process id of the running server.
    fn server_pid(&self) -> Option<u32>;

    /// The administrator of the deployment, logged in: the principal that its configuration
    /// grants every permission.
    ///
    /// # Errors
    ///
    /// When the administrator cannot be logged in.
    fn administrator(&self) -> Result<Credentials>;

    /// Mints the browser session cookie of the principal of `who`, which the server accepts as
    /// a session of that principal.
    ///
    /// # Errors
    ///
    /// When the cookie cannot be minted.
    fn session_cookie(&self, who: &Credentials) -> Result<SessionCookie>;

    /// The HTTP status of a `GET` of `path` on the public endpoint, presenting `cookie` when
    /// given and no other credential.
    ///
    /// # Errors
    ///
    /// When the request cannot be made.
    fn status(&self, path: &str, cookie: Option<&SessionCookie>) -> Result<u16>;

    /// Opens the state stream presenting `cookie`.
    ///
    /// # Errors
    ///
    /// When the request cannot be made.
    fn open_state_stream(&self, cookie: &SessionCookie) -> Result<Self::Stream>;
}
