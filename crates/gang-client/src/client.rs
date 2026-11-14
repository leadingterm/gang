//! The client operations: interactive and workload login, the bearer tokens that Bazel's
//! credential helper presents, the Bazel settings of a server, and the administrative calls.
//!
//! Operations that rewrite the client's files hold the client lock while they do so, so
//! concurrent processes never interleave their writes.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use gang_clock::{Clock, Timestamp};
use gang_spi::{LoginGrant, LoginPort, LoginRequest, LoginSupport, PortError};
use gang_tls::ServerTrust;
use gang_types::{
    AccessToken, CaPin, CertSerial, Identity, Proof, ProviderName, RefreshToken, Url,
};
use gang_wire::{
    AuthenticateRequest, AuthenticateResponse, Authentication, Certificate, Credential, Principal,
    RevokePrincipalResponse, Server,
};
use serde_json::Value;

use crate::api::ServerApi;
use crate::bazelrc;
use crate::browser::LocalBrowser;
use crate::console::Console;
use crate::credential_helper::{CredentialRequest, credential_reply};
use crate::errors::ClientError;
use crate::home::ClientHome;
use crate::loopback::LoopbackListener;
use crate::options::ClientOptions;
use crate::proof_source::ProofSource;
use crate::providers::LoginProviders;
use crate::session::{Session, SessionKind};
use crate::stored::{StoredToken, jwt_expiry};

/// The client: keeps the session of one Gang client in its home directory and mints the
/// bearer tokens that the client presents to the server.
pub struct Client<P, C, K> {
    home: ClientHome,
    options: ClientOptions,
    providers: P,
    console: C,
    clock: Arc<K>,
}

/// The successful authorization from either login path.
enum LoginOutcome {
    /// A browser returned an identity-provider grant.
    Interactive(LoginGrant),
    /// The server redeemed a device authorization.
    Device(AuthenticateResponse),
}

/// An interactive login through a browser, in the server's identity-provider list.
pub struct InteractiveLogin<'a> {
    /// The Gang server URL.
    pub server: &'a Url,
    /// The provider to log in with; `None` requires the server to offer exactly one.
    pub provider: Option<&'a ProviderName>,
    /// The pin of the Gang CA public key; `None` trusts the system roots.
    pub ca_pin: Option<&'a CaPin>,
    /// Whether a local browser can be opened.
    pub browser: LocalBrowser,
}

/// A login from a proof source the caller provides, such as a CI job token.
pub struct WorkloadLogin<'a> {
    /// The Gang server URL.
    pub server: &'a Url,
    /// The identity provider that vouches for the proof.
    pub provider: &'a ProviderName,
    /// Where the proof of identity is read from each time a token is minted.
    pub source: ProofSource,
    /// The pin of the Gang CA public key; `None` trusts the system roots.
    pub ca_pin: Option<&'a CaPin>,
}

/// The outcome of a successful login.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedIn {
    /// The identity provider the login used.
    pub provider: ProviderName,
    /// Expiry of the bearer token minted by the login.
    pub expires_at: Timestamp,
}

/// A bearer token for the server, with its expiry.
#[derive(Debug, Clone)]
pub struct BearerToken {
    /// The token text, sent as `authorization: Bearer <value>`.
    pub value: AccessToken,
    /// When the server stops accepting the token.
    pub expires_at: Timestamp,
}

/// The server and the Bazel helper a `.bazelrc` is rendered for.
pub struct BazelrcRequest<'a> {
    /// The Gang server URL.
    pub server: &'a Url,
    /// The pin of the Gang CA public key; `None` trusts the system roots.
    pub ca_pin: Option<&'a CaPin>,
    /// The credential helper command Bazel runs for the server's host.
    pub helper: &'a str,
}

/// The Bazel settings a server calls for, with the CA certificate the rc file refers to.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bazelrc {
    /// The rc file text.
    pub text: String,
    /// The CA certificate to write next to the rc file, when the server has one.
    pub ca_certificate: Option<gang_types::CertificatePem>,
}

impl<P: LoginProviders, C: Console, K: Clock + 'static> Client<P, C, K> {
    /// Creates a client that keeps its state in `home`.
    #[must_use]
    pub fn new(
        home: ClientHome,
        options: ClientOptions,
        providers: P,
        console: C,
        clock: K,
    ) -> Self {
        Self {
            home,
            options,
            providers,
            console,
            clock: Arc::new(clock),
        }
    }

    /// Logs in through the browser with one of the server's interactive identity providers,
    /// stores the session, and mints the first bearer token.
    ///
    /// The client lock is taken only once the browser has answered, so a login that waits for
    /// the user blocks no other process.
    ///
    /// # Errors
    ///
    /// [`ClientError::NoAuthentication`] when the server has no login; the transport and server
    /// errors of [`ClientError::Transport`], [`ClientError::Server`], and [`ClientError::Body`];
    /// [`ClientError::ProviderNotOffered`], [`ClientError::AmbiguousProvider`], or
    /// [`ClientError::NoInteractiveProvider`] when the provider cannot be chosen;
    /// [`ClientError::LoginTimedOut`] when no browser redirect arrives in time;
    /// [`ClientError::DeviceApprovalTimedOut`] when device approval does not arrive in time;
    /// [`ClientError::Provider`] when the identity provider rejects the login; and the storage
    /// errors of [`ClientError::Io`] and [`ClientError::Session`].
    pub async fn login(&self, request: &InteractiveLogin<'_>) -> Result<LoggedIn, ClientError> {
        let api = ServerApi::try_new(
            request.server,
            ServerTrust::from(request.ca_pin),
            &self.options,
            self.clock.clone(),
        )?;
        let info = api.server(None).await?;
        let (logins, device) = match info.authentication {
            Authentication::Token(token) => (token.login, token.device_login),
            Authentication::None(_) => return Err(ClientError::NoAuthentication),
            Authentication::Unrecognized(_) => return Err(ClientError::UnsupportedAuthentication),
        };
        let device =
            device.filter(|device| request.provider.is_none_or(|name| *name == device.provider));
        let name = match &device {
            Some(device) => device.provider.clone(),
            None => choose_provider(&logins, request.provider)?,
        };
        let outcome = if device.is_some() {
            let mut poll = Box::pin(self.device_login(&api));
            if request.browser == LocalBrowser::Available {
                let mut browser = Box::pin(self.loopback_login(&name, &logins, false));
                tokio::select! {
                    result = &mut browser => match result {
                        Ok(grant) => LoginOutcome::Interactive(grant),
                        Err(_) => LoginOutcome::Device(poll.await?),
                    },
                    result = &mut poll => match result {
                        Ok(response) => LoginOutcome::Device(response),
                        Err(_) => LoginOutcome::Interactive(browser.await?),
                    },
                }
            } else {
                LoginOutcome::Device(poll.await?)
            }
        } else {
            LoginOutcome::Interactive(Box::pin(self.loopback_login(&name, &logins, true)).await?)
        };
        let _lock = self.home.lock().await?;
        let grant = match outcome {
            LoginOutcome::Interactive(grant) => grant,
            LoginOutcome::Device(response) => {
                let refresh = response
                    .refresh_token
                    .as_ref()
                    .ok_or(ClientError::NoRefreshToken)?;
                self.forget_interactive_login()?;
                self.write_refresh_token(refresh)?;
                let token = self.store_response(response)?;
                Session {
                    server: request.server.clone(),
                    ca_pin: request.ca_pin.copied(),
                    provider: name.clone(),
                    kind: SessionKind::Device,
                }
                .store(&self.home)?;
                return Ok(LoggedIn {
                    provider: name,
                    expires_at: token.expires_at,
                });
            }
        };
        match &grant.refresh_token {
            Some(token) => self.write_refresh_token(token)?,
            None => self.remove_refresh_token()?,
        }
        self.write_id_token(&grant.proof)?;
        let session = Session {
            server: request.server.clone(),
            ca_pin: request.ca_pin.copied(),
            provider: name.clone(),
            kind: SessionKind::Interactive {
                login: section_of(&logins, &name)?.clone(),
            },
        };
        let token = self.mint(&api, &session).await?;
        session.store(&self.home)?;
        Ok(LoggedIn {
            provider: name,
            expires_at: token.expires_at,
        })
    }

    /// Runs a browser login, optionally accepting a pasted callback.
    async fn loopback_login(
        &self,
        name: &ProviderName,
        logins: &BTreeMap<ProviderName, Value>,
        paste: bool,
    ) -> Result<LoginGrant, ClientError> {
        let provider = self.interactive_provider(name, logins)?;
        let listener = LoopbackListener::bind().await?;
        let challenge = provider
            .begin_login(&LoginRequest {
                redirect_uri: listener.redirect_uri(),
                client: None,
                offline: true,
            })
            .await
            .map_err(ClientError::Provider)?;
        self.console.announce(&challenge.authorization_url);
        let callback = if paste {
            self.await_redirect(listener).await?
        } else {
            Clock::timeout(
                &self.clock,
                self.options.login_timeout.get(),
                listener.callback(
                    &self.clock,
                    self.options.callback_read_timeout.get(),
                    self.options.callback_max_head_size.get().as_usize(),
                ),
            )
            .await
            .map_err(|_| ClientError::LoginTimedOut)??
        };
        provider
            .complete_login(challenge.session, &callback)
            .await
            .map_err(ClientError::Provider)
    }

    /// Creates and polls a device authorization until approval or expiry.
    async fn device_login(&self, api: &ServerApi) -> Result<AuthenticateResponse, ClientError> {
        let started = self.clock.monotonic();
        let operation = async {
            let authorization = api.create_device_authorization().await?;
            let created = self.clock.monotonic();
            self.console.announce_device(&authorization);
            let secret = authorization
                .secret
                .as_ref()
                .ok_or(ClientError::DeviceLoginInvalid)?;
            let code = authorization
                .name
                .strip_prefix("deviceAuthorizations/")
                .ok_or(ClientError::DeviceLoginInvalid)?;
            let remaining = authorization
                .expire_time
                .saturating_duration_since(authorization.create_time)
                .saturating_sub(self.clock.monotonic().saturating_duration_since(created));
            let poll = async {
                let mut delay = authorization.poll_interval;
                loop {
                    self.clock.sleep(delay).await;
                    match api.redeem_device_authorization(code, secret).await {
                        Err(ClientError::AuthorizationPending) => {
                            delay = authorization.poll_interval;
                        }
                        Err(ClientError::RateLimited { retry_after }) => {
                            delay = retry_after.unwrap_or(authorization.poll_interval);
                        }
                        result => return result,
                    }
                }
            };
            Clock::timeout(&self.clock, remaining, poll)
                .await
                .map_err(|_| ClientError::DeviceApprovalTimedOut)?
        };
        let remaining = self
            .options
            .login_timeout
            .get()
            .saturating_sub(self.clock.monotonic().saturating_duration_since(started));
        Clock::timeout(&self.clock, remaining, operation)
            .await
            .map_err(|_| ClientError::DeviceApprovalTimedOut)?
    }

    /// Logs in with a proof source and stores the session. A workload session has no refresh
    /// token: every bearer token is minted from a fresh read of the source.
    ///
    /// The client lock is taken once the server has answered, and held until the session is
    /// stored.
    ///
    /// # Errors
    ///
    /// [`ClientError::NoAuthentication`] when the server has no login; the transport and server
    /// errors of [`ClientError::Transport`], [`ClientError::Server`], and [`ClientError::Body`];
    /// the proof-source errors of [`ClientError::EmptyToken`], [`ClientError::TokenCommand`],
    /// [`ClientError::TokenCommandFailed`], and [`ClientError::Io`]; and the storage errors of
    /// [`ClientError::Session`].
    pub async fn login_workload(
        &self,
        request: WorkloadLogin<'_>,
    ) -> Result<LoggedIn, ClientError> {
        let WorkloadLogin {
            server,
            provider,
            source,
            ca_pin,
        } = request;
        let api = ServerApi::try_new(
            server,
            ServerTrust::from(ca_pin),
            &self.options,
            self.clock.clone(),
        )?;
        token_logins(api.server(None).await?)?;
        let session = Session {
            server: server.clone(),
            ca_pin: ca_pin.copied(),
            provider: provider.clone(),
            kind: SessionKind::Workload { source },
        };

        let _lock = self.home.lock().await?;
        self.forget_interactive_login()?;
        let token = self.mint(&api, &session).await?;
        session.store(&self.home)?;
        Ok(LoggedIn {
            provider: provider.clone(),
            expires_at: token.expires_at,
        })
    }

    /// Returns a bearer token for the stored session: the cached one unless it is inside its
    /// refresh margin, otherwise a freshly minted one.
    ///
    /// The client lock is held until this returns, so concurrent callers mint at most once.
    ///
    /// # Errors
    ///
    /// [`ClientError::NotLoggedIn`] when no session is stored; [`ClientError::NoRefreshToken`]
    /// and [`ClientError::RenewalRejected`] when an interactive session cannot be renewed; the
    /// proof-source errors of a workload session; [`ClientError::Transport`],
    /// [`ClientError::Server`], and [`ClientError::Body`] when the server refuses or cannot be
    /// reached; and the storage errors of [`ClientError::Io`] and [`ClientError::Session`].
    pub async fn token(&self) -> Result<BearerToken, ClientError> {
        self.session_token(|_| Ok(())).await.map(|(_, token)| token)
    }

    /// Loads and checks the session and returns its bearer under one hold of the client lock.
    async fn session_token(
        &self,
        check: impl FnOnce(&Session) -> Result<(), ClientError>,
    ) -> Result<(Session, BearerToken), ClientError> {
        let _lock = self.home.lock().await?;
        let session = Session::load(&self.home)?.ok_or(ClientError::NotLoggedIn)?;
        check(&session)?;
        let now = self.clock.wall();
        if let Some(stored) = self.stored_token()?
            && reusable(&stored, now, &self.options)
        {
            return Ok((
                session,
                BearerToken {
                    value: stored.token,
                    expires_at: stored.expires_at,
                },
            ));
        }
        let api = ServerApi::try_new(
            &session.server,
            ServerTrust::from(session.ca_pin.as_ref()),
            &self.options,
            self.clock.clone(),
        )?;
        let token = self.mint(&api, &session).await?;
        Ok((session, token))
    }

    /// Answers one Bazel credential helper request: the JSON request on standard input, the
    /// JSON reply on standard output.
    ///
    /// # Errors
    ///
    /// [`ClientError::CredentialRequest`] when the request is not valid JSON of the expected
    /// shape; [`ClientError::NotLoggedIn`] when no session is stored; [`ClientError::HostMismatch`]
    /// when the request is for another host than the session's server; and the errors of
    /// [`Client::token`].
    pub async fn get(&self, request_json: &str) -> Result<String, ClientError> {
        let request: CredentialRequest =
            serde_json::from_str(request_json).map_err(ClientError::CredentialRequest)?;
        let (_, token) = self
            .session_token(|session| check_host(&request.uri, &session.server))
            .await?;
        Ok(credential_reply(&token))
    }

    /// Renders the Bazel settings of a server, without touching the client's state.
    ///
    /// # Errors
    ///
    /// [`ClientError::UnsupportedServerUrl`] for a server URL that is not `http` or `https`, or
    /// that has no host; [`ClientError::Tls`] when the trust anchor cannot be used; and the
    /// transport and server errors of [`ClientError::Transport`], [`ClientError::Server`], and
    /// [`ClientError::Body`].
    pub async fn bazelrc(&self, request: &BazelrcRequest<'_>) -> Result<Bazelrc, ClientError> {
        let api = ServerApi::try_new(
            request.server,
            ServerTrust::from(request.ca_pin),
            &self.options,
            self.clock.clone(),
        )?;
        let host = request
            .server
            .host_str()
            .ok_or_else(|| ClientError::UnsupportedServerUrl {
                url: request.server.clone(),
            })?;
        let info = api.server(None).await?;
        let text = bazelrc::render(&info, host, request.helper);
        Ok(Bazelrc {
            text,
            ca_certificate: info.ca_certificate,
        })
    }

    /// Revokes one certificate through the server, with the stored session's bearer token.
    /// The server decides whether the caller is an administrator.
    ///
    /// # Errors
    ///
    /// The errors of [`Client::token`] and [`ServerApi::revoke_certificate`].
    pub async fn revoke_certificate(
        &self,
        serial: &CertSerial,
    ) -> Result<Certificate, ClientError> {
        let (api, bearer) = self.authorized().await?;
        api.revoke_certificate(Some(&bearer.value), serial).await
    }

    /// Revokes a principal and its certificates through the server, with the stored session's
    /// bearer token. The server decides whether the caller is an administrator.
    ///
    /// # Errors
    ///
    /// The errors of [`Client::token`] and [`ServerApi::revoke_principal`].
    pub async fn revoke_principal(
        &self,
        identity: &Identity,
    ) -> Result<RevokePrincipalResponse, ClientError> {
        let (api, bearer) = self.authorized().await?;
        api.revoke_principal(Some(&bearer.value), identity).await
    }

    /// Restores a principal the server revoked, with the stored session's bearer token.
    ///
    /// # Errors
    ///
    /// The errors of [`Client::token`] and [`ServerApi::restore_principal`].
    pub async fn restore_principal(&self, identity: &Identity) -> Result<Principal, ClientError> {
        let (api, bearer) = self.authorized().await?;
        api.restore_principal(Some(&bearer.value), identity).await
    }

    /// A server API and a fresh bearer token from the stored session. The bearer is presented
    /// per request, not stored in the API.
    async fn authorized(&self) -> Result<(ServerApi, BearerToken), ClientError> {
        let (session, bearer) = self.session_token(|_| Ok(())).await?;
        let api = ServerApi::try_new(
            &session.server,
            ServerTrust::from(session.ca_pin.as_ref()),
            &self.options,
            self.clock.clone(),
        )?;
        Ok((api, bearer))
    }

    /// Mints a bearer token from the session's proof, stores it, and returns it. An interactive
    /// session presents its stored identity-provider proof while it is fresh; the server's
    /// refusal of a stored proof renews it once. The caller holds the client lock.
    async fn mint(&self, api: &ServerApi, session: &Session) -> Result<BearerToken, ClientError> {
        if matches!(&session.kind, SessionKind::Device) {
            let request = AuthenticateRequest {
                provider: session.provider.clone(),
                credential: Credential::RefreshToken(self.stored_refresh_token()?),
            };
            let response = match api.authenticate(&request).await {
                Err(ClientError::LoginRejected) => {
                    ClientHome::remove(&self.home.session_file())?;
                    ClientHome::remove(&self.home.token_file())?;
                    self.forget_interactive_login()?;
                    return Err(ClientError::LoginRejected);
                }
                result => result?,
            };
            if let Some(refresh) = &response.refresh_token {
                self.write_refresh_token(refresh)?;
            }
            return self.store_response(response);
        }
        let stored = match &session.kind {
            SessionKind::Interactive { .. } => self.stored_id_token()?,
            SessionKind::Workload { .. } | SessionKind::Device => None,
        };
        let mut from_store = stored.is_some();
        let mut proof = match stored {
            Some(proof) => proof,
            None => self.renew_proof(session).await?,
        };
        let response = loop {
            let request = AuthenticateRequest {
                provider: session.provider.clone(),
                credential: Credential::Proof(proof.clone()),
            };
            match api.authenticate(&request).await {
                Err(
                    ClientError::LoginRejected
                    | ClientError::Server {
                        status: 401 | 403, ..
                    },
                ) if from_store => {
                    from_store = false;
                    proof = self.renew_proof(session).await?;
                }
                result => break result?,
            }
        };
        self.store_response(response)
    }

    /// Stores a server-issued bearer token.
    fn store_response(&self, response: AuthenticateResponse) -> Result<BearerToken, ClientError> {
        let issued_at = self.clock.wall();
        let stored = StoredToken {
            token: response.token,
            issued_at,
            expires_at: issued_at + response.lifetime,
        };
        let bytes = serde_json::to_vec(&stored).map_err(|source| ClientError::Session {
            path: self.home.token_file(),
            source,
        })?;
        self.home.write_private(&self.home.token_file(), &bytes)?;
        Ok(BearerToken {
            value: stored.token,
            expires_at: stored.expires_at,
        })
    }

    /// The proof a session presents when no stored one is usable: a fresh identity-provider
    /// proof for an interactive session, a read of the source for a workload session.
    async fn renew_proof(&self, session: &Session) -> Result<Proof, ClientError> {
        match &session.kind {
            SessionKind::Interactive { login } => self.renew(&session.provider, login).await,
            SessionKind::Workload { source } => source.read(&self.clock, &self.options).await,
            SessionKind::Device => Err(ClientError::DeviceLoginInvalid),
        }
    }

    /// Renews an interactive login from its stored refresh token. A rotated refresh token and
    /// the new identity-provider proof are stored as soon as the provider returns them.
    async fn renew(&self, name: &ProviderName, login: &Value) -> Result<Proof, ClientError> {
        let token = self.stored_refresh_token()?;
        let provider = self.build_provider(name, login)?;
        let grant = provider.renew(&token, None).await.map_err(renewal_error)?;
        if let Some(rotated) = grant.refresh_token.as_ref() {
            self.write_refresh_token(rotated)?;
        }
        self.write_id_token(&grant.proof)?;
        Ok(grant.proof)
    }

    /// The stored identity-provider proof, when it stays valid beyond the configured margin.
    fn stored_id_token(&self) -> Result<Option<Proof>, ClientError> {
        let Some(bytes) = ClientHome::read(&self.home.id_token_file())? else {
            return Ok(None);
        };
        let text = String::from_utf8_lossy(&bytes).trim().to_owned();
        let now = self.clock.wall();
        let fresh = id_token_fresh(&text, now, self.options.id_token_refresh_margin);
        Ok(fresh.then(|| Proof::new(text)))
    }

    /// The stored bearer token, when one has been minted.
    fn stored_token(&self) -> Result<Option<StoredToken>, ClientError> {
        let path = self.home.token_file();
        ClientHome::read(&path)?
            .map(|bytes| {
                serde_json::from_slice::<StoredToken>(&bytes).map_err(|source| {
                    ClientError::Session {
                        path: path.clone(),
                        source,
                    }
                })
            })
            .transpose()
    }

    /// Writes the identity-provider proof the session presents.
    fn write_id_token(&self, proof: &Proof) -> Result<(), ClientError> {
        self.home
            .write_private(&self.home.id_token_file(), proof.expose().as_bytes())
    }

    /// Writes the refresh token the provider issued.
    fn write_refresh_token(&self, token: &RefreshToken) -> Result<(), ClientError> {
        self.home
            .write_private(&self.home.refresh_token_file(), token.expose().as_bytes())
    }

    /// Removes the stored refresh token, for a login that brings none.
    fn remove_refresh_token(&self) -> Result<(), ClientError> {
        ClientHome::remove(&self.home.refresh_token_file())
    }

    /// Removes the interactive login's refresh token and identity-provider proof.
    fn forget_interactive_login(&self) -> Result<(), ClientError> {
        self.remove_refresh_token()?;
        ClientHome::remove(&self.home.id_token_file())
    }

    /// Reads the stored refresh token.
    fn stored_refresh_token(&self) -> Result<RefreshToken, ClientError> {
        let bytes = ClientHome::read(&self.home.refresh_token_file())?
            .ok_or(ClientError::NoRefreshToken)?;
        Ok(RefreshToken::new(String::from_utf8_lossy(&bytes).trim()))
    }

    /// Builds the provider of an interactive login and checks that it offers one.
    fn interactive_provider(
        &self,
        name: &ProviderName,
        logins: &BTreeMap<ProviderName, Value>,
    ) -> Result<P::Provider, ClientError> {
        let provider = self.build_provider(name, section_of(logins, name)?)?;
        if provider.login() == LoginSupport::Interactive {
            Ok(provider)
        } else {
            Err(ClientError::ProviderNotOffered {
                provider: name.clone(),
            })
        }
    }

    /// Turns a provider's published login section into a provider.
    fn build_provider(
        &self,
        name: &ProviderName,
        section: &Value,
    ) -> Result<P::Provider, ClientError> {
        let config: P::Config =
            serde_json::from_value(section.clone()).map_err(|source| ClientError::LoginConfig {
                provider: name.clone(),
                source,
            })?;
        self.providers.build(config).map_err(ClientError::Provider)
    }

    /// Waits for the browser redirect, either on the loopback listener or as a pasted URL.
    async fn await_redirect(&self, listener: LoopbackListener) -> Result<Url, ClientError> {
        let read_timeout = self.options.callback_read_timeout.get();
        let max_head = self.options.callback_max_head_size.get().as_usize();
        first_redirect(
            &self.clock,
            self.options.login_timeout.get(),
            listener.callback(&self.clock, read_timeout, max_head),
            self.console.pasted_redirect(),
        )
        .await
    }
}

/// Waits for the first login redirect within `timeout`: the loopback callback or a pasted URL,
/// whichever completes first. A failed callback ends the wait with its error.
async fn first_redirect(
    clock: &impl Clock,
    timeout: Duration,
    callback: impl Future<Output = Result<Url, ClientError>>,
    pasted: impl Future<Output = Url>,
) -> Result<Url, ClientError> {
    let redirect = async {
        tokio::select! {
            url = callback => url,
            url = pasted => Ok(url),
        }
    };
    match Clock::timeout(clock, timeout, redirect).await {
        Ok(result) => result,
        Err(_) => Err(ClientError::LoginTimedOut),
    }
}

/// The providers of a server that authenticates with bearer tokens.
fn token_logins(info: Server<Value>) -> Result<BTreeMap<ProviderName, Value>, ClientError> {
    match info.authentication {
        Authentication::Token(token) => Ok(token.login),
        Authentication::None(_) => Err(ClientError::NoAuthentication),
        Authentication::Unrecognized(_) => Err(ClientError::UnsupportedAuthentication),
    }
}

/// Whether an ID token stays valid for more than the configured margin.
fn id_token_fresh(token: &str, now: Timestamp, margin: Duration) -> bool {
    jwt_expiry(token).is_some_and(|expiry| expiry.saturating_duration_since(now) > margin)
}

/// Whether a stored token is still good to present: it has not expired, and at least the
/// refresh margin remains, capped by the configured percentage of its lifetime.
fn reusable(stored: &StoredToken, now: Timestamp, options: &ClientOptions) -> bool {
    let lifetime = stored
        .expires_at
        .saturating_duration_since(stored.issued_at);
    let threshold = options
        .token_refresh_margin
        .min(options.token_refresh_cap.of_duration(lifetime));
    let remaining = stored.expires_at.saturating_duration_since(now);
    !remaining.is_zero() && remaining >= threshold
}

/// Requires a credential request's host to be the session server's host.
fn check_host(requested: &Url, server: &Url) -> Result<(), ClientError> {
    if requested.host_str() == server.host_str() {
        return Ok(());
    }
    Err(ClientError::HostMismatch {
        requested: requested.host_str().unwrap_or_default().to_owned(),
        server: server.host_str().unwrap_or_default().to_owned(),
    })
}

/// Chooses the provider of an interactive login from the server's offered providers.
fn choose_provider(
    login: &BTreeMap<ProviderName, Value>,
    requested: Option<&ProviderName>,
) -> Result<ProviderName, ClientError> {
    if let Some(name) = requested {
        return if login.contains_key(name) {
            Ok(name.clone())
        } else {
            Err(ClientError::ProviderNotOffered {
                provider: name.clone(),
            })
        };
    }
    let mut names = login.keys();
    match (names.next(), names.next()) {
        (None, _) => Err(ClientError::NoInteractiveProvider),
        (Some(only), None) => Ok(only.clone()),
        _ => Err(ClientError::AmbiguousProvider {
            offered: login
                .keys()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        }),
    }
}

/// The login section the server published for `name`.
fn section_of<'a>(
    login: &'a BTreeMap<ProviderName, Value>,
    name: &ProviderName,
) -> Result<&'a Value, ClientError> {
    login
        .get(name)
        .ok_or_else(|| ClientError::ProviderNotOffered {
            provider: name.clone(),
        })
}

/// Maps a renewal failure: a refused refresh token is reported as such, anything else as a
/// provider failure.
fn renewal_error(error: PortError) -> ClientError {
    if matches!(error, PortError::Rejected { .. }) {
        ClientError::RenewalRejected(error)
    } else {
        ClientError::Provider(error)
    }
}

#[cfg(test)]
mod tests {
    use gang_clock::TestClock;

    use super::*;

    #[test]
    fn host_mismatch_is_refused_and_matching_host_accepted() {
        let server = Url::parse("https://gang.example.test:8443/").expect("valid url");
        let same = Url::parse("https://gang.example.test/cache/x").expect("valid url");
        let other = Url::parse("https://cache.example.test/").expect("valid url");
        assert!(check_host(&same, &server).is_ok());
        assert!(matches!(
            check_host(&other, &server),
            Err(ClientError::HostMismatch { requested, server })
                if requested == "cache.example.test" && server == "gang.example.test"
        ));
    }

    #[tokio::test]
    async fn silent_login_times_out_at_the_login_timeout() {
        let clock = TestClock::default();
        let started = clock.monotonic();
        let timeout = Duration::from_mins(5);
        let (result, ()) = tokio::join!(
            first_redirect(
                &clock,
                timeout,
                std::future::pending::<Result<Url, ClientError>>(),
                std::future::pending::<Url>(),
            ),
            async {
                clock.advance(timeout);
            },
        );
        let error = result.expect_err("nothing arrives, so the login times out");
        assert!(
            matches!(error, ClientError::LoginTimedOut),
            "unexpected error: {error}"
        );
        assert_eq!(
            clock.monotonic().saturating_duration_since(started),
            timeout
        );
    }

    #[tokio::test]
    async fn callback_within_the_timeout_is_the_redirect() {
        let callback = Url::parse("http://127.0.0.1:1/callback?code=a&state=b").expect("valid url");
        let redirect = first_redirect(
            &TestClock::default(),
            Duration::from_mins(5),
            async { Ok(callback.clone()) },
            std::future::pending::<Url>(),
        )
        .await
        .expect("the callback completes the login");
        assert_eq!(redirect, callback);
    }

    #[tokio::test]
    async fn pasted_url_within_the_timeout_is_the_redirect() {
        let pasted = Url::parse("http://127.0.0.1:1/callback?code=a&state=b").expect("valid url");
        let redirect = first_redirect(
            &TestClock::default(),
            Duration::from_mins(5),
            std::future::pending::<Result<Url, ClientError>>(),
            async { pasted.clone() },
        )
        .await
        .expect("the pasted URL completes the login");
        assert_eq!(redirect, pasted);
    }

    #[tokio::test]
    async fn failed_callback_ends_the_wait_with_its_error() {
        let error = first_redirect(
            &TestClock::default(),
            Duration::from_mins(5),
            async {
                Err(ClientError::Loopback(std::io::Error::other(
                    "listener failed",
                )))
            },
            std::future::pending::<Url>(),
        )
        .await
        .expect_err("the callback error is returned");
        assert!(
            matches!(error, ClientError::Loopback(_)),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn unknown_authentication_scheme_is_unsupported() {
        let info = Server {
            name: gang_wire::name::SERVER.to_owned(),
            remote_executor: Url::parse("grpcs://exec.example.test:443").expect("valid url"),
            remote_cache: Url::parse("grpcs://cache.example.test:443").expect("valid url"),
            ca_certificate: None,
            default_isa: gang_wire::Isa::X86_64,
            authentication: Authentication::Unrecognized(serde_json::Map::new()),
        };
        assert!(matches!(
            token_logins(info),
            Err(ClientError::UnsupportedAuthentication)
        ));
    }

    fn stored_token(issued_at: Timestamp, lifetime: Duration) -> StoredToken {
        StoredToken {
            token: AccessToken::new("bearer"),
            issued_at,
            expires_at: issued_at + lifetime,
        }
    }

    #[test]
    fn id_token_at_the_margin_is_not_fresh() {
        let token = "e30.eyJleHAiOjE4OTM0NTYwMDB9.c2ln";
        let expiry = Timestamp::from_unix_millis(1_893_456_000_000);
        let margin = Duration::from_secs(15);

        assert!(!id_token_fresh(token, expiry - margin, margin));
        assert!(id_token_fresh(
            token,
            expiry - margin - Duration::from_millis(1),
            margin
        ));
        assert!(!id_token_fresh(token, expiry, Duration::ZERO));
    }

    #[test]
    fn token_with_exactly_the_threshold_left_is_reusable() {
        let issued = Timestamp::from_unix_millis(1_000_000);
        let stored = stored_token(issued, Duration::from_mins(10));
        let options = ClientOptions::default();
        let margin = options.token_refresh_margin;
        assert!(reusable(&stored, stored.expires_at - margin, &options));
    }

    #[test]
    fn token_one_millisecond_short_of_the_threshold_is_not_reusable() {
        let issued = Timestamp::from_unix_millis(1_000_000);
        let stored = stored_token(issued, Duration::from_mins(10));
        let options = ClientOptions::default();
        let now = stored.expires_at - options.token_refresh_margin + Duration::from_millis(1);
        assert!(!reusable(&stored, now, &options));
    }

    #[test]
    fn short_lived_token_uses_the_configured_refresh_cap() {
        let issued = Timestamp::from_unix_millis(1_000_000);
        let stored = stored_token(issued, Duration::from_secs(20));
        let options = ClientOptions {
            token_refresh_cap: gang_types::Percent::from_percent(50),
            ..ClientOptions::default()
        };
        let threshold = Duration::from_secs(10);
        assert!(reusable(&stored, stored.expires_at - threshold, &options));
        assert!(!reusable(
            &stored,
            stored.expires_at - threshold + Duration::from_millis(1),
            &options
        ));
    }

    #[test]
    fn token_expiring_exactly_now_is_not_reusable() {
        let issued = Timestamp::from_unix_millis(1_000_000);
        let stored = stored_token(issued, Duration::from_mins(10));
        let options = ClientOptions {
            token_refresh_margin: Duration::ZERO,
            ..ClientOptions::default()
        };
        assert!(!reusable(&stored, stored.expires_at, &options));
    }

    #[test]
    fn token_with_zero_lifetime_is_not_reusable() {
        let issued = Timestamp::from_unix_millis(1_000_000);
        let stored = stored_token(issued, Duration::ZERO);
        let options = ClientOptions::default();
        assert!(!reusable(&stored, issued, &options));
        let zero_margin = ClientOptions {
            token_refresh_margin: Duration::ZERO,
            ..options
        };
        assert!(!reusable(&stored, issued, &zero_margin));
    }
}
