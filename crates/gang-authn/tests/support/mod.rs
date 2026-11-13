//! The authentication host the plugin tests lend to plugins: a scripted identity provider, a
//! fixed clock, and the shared fakes from `gang-testkit`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use gang_clock::{Clock, TestClock, Timestamp};
use gang_spi::{
    AuthenticationHost, AuthorityRequest, CertStatus, Evidence, IssuedCertRecord,
    IssuedCertificate, Peer, PortError, Rejection, StateRepositoryPort, Subject,
};
use gang_testkit::{FakeRepository, TestSigner};
use gang_types::{CertSerial, CertificatePem, Identity, Principal, Proof, ProviderName, Url};
use http::HeaderMap;
use http::header::{AUTHORIZATION, HeaderValue};
use parking_lot::Mutex;

/// The public URL the test host serves on.
const ENDPOINT: &str = "https://gang.example.com";
/// The PEM the test authority returns for every certificate it signs.
const CERTIFICATE_PEM: &str = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----";

/// The collaborators a plugin test lends to a plugin: a fixed-start clock, a ledger, an
/// authority signer and a scripted identity provider.
pub struct TestHost {
    clock: Arc<TestClock>,
    endpoint: Url,
    repository: FakeRepository,
    signer: TestSigner,
    providers: Mutex<BTreeSet<ProviderName>>,
    proofs: Mutex<BTreeMap<(ProviderName, String), Principal>>,
    revoked: Mutex<BTreeSet<Identity>>,
    rejecting: AtomicBool,
    next_serial: AtomicU64,
    authority: Mutex<Vec<(Identity, Duration)>>,
}

impl TestHost {
    /// A host whose clock reads `start`, served from `https://gang.example.com`, that knows no
    /// identity provider until one is scripted with [`TestHost::accept_proof`].
    ///
    /// # Panics
    ///
    /// Never in practice: the built-in endpoint is a valid URL and the authority seed is a valid
    /// P-256 scalar.
    #[must_use]
    pub fn new(start: Timestamp) -> Self {
        Self {
            clock: Arc::new(TestClock::new(start)),
            endpoint: Url::parse(ENDPOINT).expect("test endpoint parses"),
            repository: FakeRepository::default(),
            signer: TestSigner::authority().expect("the authority seed is a valid scalar"),
            providers: Mutex::new(BTreeSet::new()),
            proofs: Mutex::new(BTreeMap::new()),
            revoked: Mutex::new(BTreeSet::new()),
            rejecting: AtomicBool::new(false),
            next_serial: AtomicU64::new(0),
            authority: Mutex::new(Vec::new()),
        }
    }

    /// Moves the clock forward by `by`.
    pub fn advance(&self, by: Duration) {
        self.clock.advance(by);
    }

    /// Makes `proof` from `provider` verify as `principal`, registering `provider` as known.
    ///
    /// # Panics
    ///
    /// Panics when `provider` or `principal` is not a valid name.
    pub fn accept_proof(&self, provider: &str, proof: &str, principal: &str) {
        let provider = ProviderName::try_new(provider).expect("test provider is a valid name");
        let principal = Principal::try_new(principal).expect("test principal is a valid principal");
        self.providers.lock().insert(provider.clone());
        self.proofs
            .lock()
            .insert((provider, proof.to_owned()), principal);
    }

    /// Makes every proof fail verification while `reject` is true.
    pub fn reject_proofs(&self, reject: bool) {
        self.rejecting.store(reject, Ordering::SeqCst);
    }

    /// Makes `verify` refuse `identity` with a policy denial, as a revoked principal.
    pub fn revoke_principal_in_host(&self, identity: &Identity) {
        self.revoked.lock().insert(identity.clone());
    }

    /// Every certificate request the authority has been asked to sign, oldest first.
    #[must_use]
    pub fn authority_requests(&self) -> Vec<(Identity, Duration)> {
        self.authority.lock().clone()
    }

    /// How many times the authority's signer has been asked for its public key.
    #[must_use]
    pub fn signer_key_reads(&self) -> usize {
        self.signer.public_key_reads()
    }

    fn check_proof(&self, provider: &ProviderName, proof: &Proof) -> Result<Principal, PortError> {
        if !self.providers.lock().contains(provider) {
            return Err(PortError::NotFound(Subject::Provider(provider.clone())));
        }
        if self.rejecting.load(Ordering::SeqCst) {
            return Err(PortError::refused(Rejection::InvalidProof));
        }
        let key = (provider.clone(), proof.expose().to_owned());
        let principal = self
            .proofs
            .lock()
            .get(&key)
            .cloned()
            .ok_or(PortError::refused(Rejection::InvalidProof))?;
        let identity = Identity::new(provider.clone(), principal.clone());
        if self.revoked.lock().contains(&identity) {
            return Err(PortError::refused(Rejection::PrincipalRevoked));
        }
        Ok(principal)
    }
}

impl AuthenticationHost for TestHost {
    type Repository = FakeRepository;
    type Signer = TestSigner;

    fn repository(&self) -> &FakeRepository {
        &self.repository
    }

    fn signer(&self) -> &TestSigner {
        &self.signer
    }

    fn now(&self) -> Timestamp {
        self.clock.wall()
    }

    fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    async fn verify(&self, provider: &ProviderName, proof: &Proof) -> Result<Principal, PortError> {
        let principal = self.check_proof(provider, proof)?;
        let identity = Identity::new(provider.clone(), principal.clone());
        self.repository
            .observe_principal(&identity, self.now())
            .await?;
        Ok(principal)
    }

    async fn issue_certificate(
        &self,
        request: &AuthorityRequest<'_>,
    ) -> Result<IssuedCertificate, PortError> {
        self.authority
            .lock()
            .push((request.identity.clone(), request.lifetime.get()));
        let number = self.next_serial.fetch_add(1, Ordering::SeqCst) + 1;
        let serial = CertSerial::from_be_bytes(&number.to_be_bytes())
            .map_err(|error| PortError::backend(error.to_string()))?;
        let certificate = CertificatePem::try_new(CERTIFICATE_PEM)
            .map_err(|error| PortError::backend(error.to_string()))?;
        Ok(IssuedCertificate {
            ca_certificate: certificate.clone(),
            certificate,
            record: IssuedCertRecord {
                serial,
                identity: request.identity.clone(),
                issued_at: self.now(),
                expires_at: self.now() + request.lifetime.get(),
                status: CertStatus::Active,
            },
        })
    }
}

/// The identity `provider` names `principal` as.
///
/// # Panics
///
/// Panics when either name is not valid.
#[must_use]
pub fn identity(provider: &str, principal: &str) -> Identity {
    Identity::new(
        ProviderName::try_new(provider).expect("test provider is a valid name"),
        Principal::try_new(principal).expect("test principal is a valid principal"),
    )
}

/// A request header map carrying `token` as `authorization: Bearer <token>`.
///
/// # Panics
///
/// Panics when `token` is not a valid header value.
#[must_use]
pub fn bearer(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))
            .expect("bearer token is a valid header value"),
    );
    headers
}

/// What a plugin sees of a request that arrived on `peer` with `headers`.
#[must_use]
pub fn evidence<'a>(peer: &'a Peer, headers: &'a HeaderMap) -> Evidence<'a> {
    Evidence {
        peer,
        headers,
        session: None,
    }
}

/// What a plugin sees of a request that arrived on `peer` with `headers` and the session cookie
/// `session`.
#[must_use]
pub fn session_evidence<'a>(
    peer: &'a Peer,
    headers: &'a HeaderMap,
    session: &'a str,
) -> Evidence<'a> {
    Evidence {
        peer,
        headers,
        session: Some(session),
    }
}
