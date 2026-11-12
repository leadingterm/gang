//! A proof verifier and login provider that answers from a table of configured proofs.

use std::collections::BTreeMap;
use std::sync::Arc;

use gang_spi::{
    LoginChallenge, LoginClient, LoginGrant, LoginPort, LoginRequest, LoginSupport, PortError,
    ProofVerifierPort, Rejection,
};
use gang_types::{LoginSession, Principal, Proof, RefreshToken, Url};
use parking_lot::Mutex;

const AUTHORIZE_URL: &str = "https://idp.example.test/authorize";

#[derive(Clone)]
enum Outcome {
    Accept(Principal),
    Reject(Rejection),
    Unavailable,
    Hang,
}

/// A login started through `begin_login`, as the verifier saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BegunLogin {
    /// The redirect URI the login asked the provider to return to.
    pub redirect_uri: Url,
    /// The client id and secret text the login ran under; `None` uses the configured client.
    pub client: Option<(String, String)>,
}

/// How a [`FakeVerifier`] fails every `begin_login` after [`FakeVerifier::fail_begin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeginFailure {
    /// The identity provider cannot be reached.
    Unavailable,
    /// The identity provider refuses the login with this rejection.
    Rejected(Rejection),
}

#[derive(Default)]
struct LoginFlow {
    begins: Vec<BegunLogin>,
    completes: Vec<Url>,
    begin_failure: Option<BeginFailure>,
    started: u64,
}

/// A proof verifier and login provider that answers from a table of configured proofs.
#[derive(Clone)]
pub struct FakeVerifier {
    login: LoginSupport,
    outcomes: Arc<Mutex<BTreeMap<String, Outcome>>>,
    flow: Arc<Mutex<LoginFlow>>,
}

impl FakeVerifier {
    /// A verifier with no configured proofs whose login port reports `login`.
    #[must_use]
    pub fn new(login: LoginSupport) -> Self {
        Self {
            login,
            outcomes: Arc::default(),
            flow: Arc::default(),
        }
    }

    /// Accepts `proof` as `principal`.
    pub fn accept(&self, proof: &str, principal: &str) {
        let principal = Principal::try_new(principal).expect("test principal is valid");
        self.set(proof, Outcome::Accept(principal));
    }

    /// Rejects `proof` with `rejection`.
    pub fn reject(&self, proof: &str, rejection: Rejection) {
        self.set(proof, Outcome::Reject(rejection));
    }

    /// Makes verification of `proof` fail as an unavailable provider.
    pub fn unavailable(&self, proof: &str) {
        self.set(proof, Outcome::Unavailable);
    }

    /// Makes verification of `proof` never answer: the verification stays pending.
    pub fn hang(&self, proof: &str) {
        self.set(proof, Outcome::Hang);
    }

    /// Every login started through `begin_login`, oldest first.
    #[must_use]
    pub fn begun(&self) -> Vec<BegunLogin> {
        self.flow.lock().begins.clone()
    }

    /// Every callback URL passed to `complete_login`, oldest first.
    #[must_use]
    pub fn completed(&self) -> Vec<Url> {
        self.flow.lock().completes.clone()
    }

    /// Makes every later `begin_login` fail with `failure`.
    pub fn fail_begin(&self, failure: BeginFailure) {
        self.flow.lock().begin_failure = Some(failure);
    }

    fn set(&self, proof: &str, outcome: Outcome) {
        self.outcomes.lock().insert(proof.to_owned(), outcome);
    }

    fn start_login(&self, request: &LoginRequest<'_>) -> Result<LoginChallenge, PortError> {
        let mut flow = self.flow.lock();
        flow.begins.push(BegunLogin {
            redirect_uri: request.redirect_uri.clone(),
            client: request
                .client
                .map(|client| (client.id.clone(), client.secret.expose().to_owned())),
        });
        match flow.begin_failure {
            Some(BeginFailure::Unavailable) => {
                return Err(PortError::unavailable("identity provider is unavailable"));
            }
            Some(BeginFailure::Rejected(rejection)) => {
                return Err(PortError::refused(rejection));
            }
            None => {}
        }
        flow.started += 1;
        let state = format!("state-{}", flow.started);
        let client_id = request
            .client
            .map_or("configured", |client| client.id.as_str());
        let authorization_url = Url::parse_with_params(
            AUTHORIZE_URL,
            &[
                ("client_id", client_id),
                ("redirect_uri", request.redirect_uri.as_str()),
                ("state", state.as_str()),
            ],
        )
        .expect("authorization url of the fake provider parses");
        Ok(LoginChallenge {
            authorization_url,
            state: state.clone(),
            session: LoginSession::new(state),
        })
    }
}

impl ProofVerifierPort for FakeVerifier {
    async fn verify(&self, proof: &Proof) -> Result<Principal, PortError> {
        let outcome = self.outcomes.lock().get(proof.expose()).cloned();
        match outcome {
            Some(Outcome::Accept(principal)) => Ok(principal),
            Some(Outcome::Reject(rejection)) => Err(PortError::refused(rejection)),
            Some(Outcome::Unavailable) => {
                Err(PortError::unavailable("identity provider is unavailable"))
            }
            Some(Outcome::Hang) => std::future::pending().await,
            None => Err(PortError::refused(Rejection::InvalidProof)),
        }
    }
}

impl LoginPort for FakeVerifier {
    fn login(&self) -> LoginSupport {
        self.login
    }

    async fn begin_login(&self, request: &LoginRequest<'_>) -> Result<LoginChallenge, PortError> {
        if self.login != LoginSupport::Interactive {
            return Err(PortError::Unsupported {
                operation: "begin_login",
            });
        }
        self.start_login(request)
    }

    async fn complete_login(
        &self,
        session: LoginSession,
        callback_url: &Url,
    ) -> Result<LoginGrant, PortError> {
        if self.login != LoginSupport::Interactive {
            return Err(PortError::Unsupported {
                operation: "complete_login",
            });
        }
        self.flow.lock().completes.push(callback_url.clone());
        let query = |name: &str| {
            callback_url
                .query_pairs()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.into_owned())
        };
        if query("error").as_deref() == Some("access_denied") {
            return Err(PortError::refused(Rejection::PolicyDenied));
        }
        if query("state").as_deref() != Some(session.expose()) {
            return Err(PortError::refused(Rejection::InvalidProof));
        }
        let code = query("code").ok_or(PortError::refused(Rejection::InvalidProof))?;
        Ok(LoginGrant {
            proof: Proof::new(code),
            refresh_token: None,
        })
    }

    async fn renew(
        &self,
        _refresh_token: &RefreshToken,
        _client: Option<&LoginClient>,
    ) -> Result<LoginGrant, PortError> {
        Err(PortError::Unsupported { operation: "renew" })
    }
}
