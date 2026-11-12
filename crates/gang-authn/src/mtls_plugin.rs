//! The `mtls` authentication plugin: client certificates that Gang's authority signs for proofs
//! from one identity provider.
//!
//! Issuance verifies the proof, signs the request and records the certificate in the ledger before
//! it is returned. Authentication identifies a presented certificate by its serial, looked up in
//! the ledger, reported valid until the certificate expires, and memoized until then. Revocation
//! writes the ledger and drops the memoized entry.

use std::num::NonZeroUsize;
use std::sync::Arc;

use gang_spi::{
    Authenticated, AuthenticationHost, AuthenticationPort, AuthorityRequest, Capabilities,
    CertStatus, CertificateIssue, CertificateTarget, ClientCertificates, CredentialLifecyclePort,
    Evidence, IssuedCertRecord, IssuedCertificate, Plugin, PluginContext, PortError, Rejection,
    Scheme, StateRepositoryPort, Subject,
};
use gang_types::{AuthenticationSettings, CertSerial, Identity, NonZeroDuration, ProviderName};
use serde::{Deserialize, Serialize};

use crate::Memo;

/// Default for [`MtlsConfig::certificate_ttl`]: one day.
const DEFAULT_CERTIFICATE_TTL: NonZeroDuration = NonZeroDuration::from_hours(24);

/// Default for [`MtlsConfig::max_cached_certificates`].
const DEFAULT_MAX_CACHED_CERTIFICATES: NonZeroUsize =
    NonZeroUsize::new(4096).expect("the default cache capacity is not zero");

fn default_certificate_ttl() -> NonZeroDuration {
    DEFAULT_CERTIFICATE_TTL
}

fn default_max_cached_certificates() -> NonZeroUsize {
    DEFAULT_MAX_CACHED_CERTIFICATES
}

/// Configuration of the `mtls` plugin.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MtlsConfig {
    /// The identity provider whose proofs workers present to obtain a certificate. Required.
    pub provider: ProviderName,
    /// How long an issued certificate is valid. Default: `1d`.
    #[serde(default = "default_certificate_ttl")]
    pub certificate_ttl: NonZeroDuration,
    /// How many certificate lookups are kept in memory. Default: `4096`.
    #[serde(default = "default_max_cached_certificates")]
    pub max_cached_certificates: NonZeroUsize,
}

impl AuthenticationSettings for MtlsConfig {
    fn requires_tls(&self) -> bool {
        true
    }

    fn providers(&self) -> &[ProviderName] {
        std::slice::from_ref(&self.provider)
    }

    fn enrollment_provider(&self) -> Option<&ProviderName> {
        Some(&self.provider)
    }
}

/// The `mtls` plugin: client certificates signed by Gang's authority for one identity provider.
pub struct Mtls {
    config: MtlsConfig,
    memo: Memo<CertSerial, Authenticated>,
}

impl Mtls {
    /// Records the revocation of `serial` and drops its memoized identity.
    async fn revoke_record<H: AuthenticationHost>(
        &self,
        host: &H,
        serial: &CertSerial,
    ) -> Result<IssuedCertRecord, PortError> {
        let revoked = host.repository().revoke(serial, host.now()).await?;
        self.memo.remove(serial);
        Ok(revoked)
    }
}

impl Plugin for Mtls {
    type Config = MtlsConfig;

    fn build(config: MtlsConfig, _context: &PluginContext<'_>) -> Result<Self, PortError> {
        let memo = Memo::new(config.max_cached_certificates);
        Ok(Self { config, memo })
    }
}

impl AuthenticationPort for Mtls {
    fn scheme(&self) -> Scheme {
        Scheme::Mtls
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            certificates: true,
            tokens: false,
            sessions: false,
        }
    }

    fn client_certificates(&self) -> ClientCertificates {
        ClientCertificates::Required
    }

    async fn authenticate<H: AuthenticationHost>(
        &self,
        host: &H,
        evidence: &Evidence<'_>,
    ) -> Result<Option<Authenticated>, PortError> {
        let now = host.now();
        let Some(serial) = evidence.peer.certificate.as_ref() else {
            return Ok(None);
        };
        if let Some(authenticated) = self.memo.get(serial, now) {
            return Ok(Some(authenticated));
        }
        // Read the generation before the ledger: a revocation that removes the entry after this
        // point makes the insert below decline, so a lookup that raced it is never cached.
        let generation = self.memo.generation();
        let Some(record) = host.repository().certificate(serial).await? else {
            return Err(PortError::refused(Rejection::InvalidProof));
        };
        // An expired certificate is not a proof.
        if now >= record.expires_at {
            return Err(PortError::refused(Rejection::InvalidProof));
        }
        if matches!(record.status, CertStatus::Revoked { .. })
            || record.identity.provider != self.config.provider
        {
            return Err(PortError::refused(Rejection::PolicyDenied));
        }
        // The certificate is accepted until it expires, so nothing that holds this identity
        // outlives the certificate.
        let authenticated = Authenticated {
            identity: Arc::new(record.identity),
            valid_until: Some(record.expires_at),
        };
        self.memo.insert_if_unchanged(
            generation,
            serial.clone(),
            authenticated.clone(),
            record.expires_at,
            now,
        );
        Ok(Some(authenticated))
    }
}

impl CredentialLifecyclePort for Mtls {
    async fn issue_certificate<H: AuthenticationHost>(
        &self,
        host: &H,
        request: &CertificateIssue<'_>,
    ) -> Result<IssuedCertificate, PortError> {
        if request.provider != &self.config.provider {
            return Err(PortError::NotFound(Subject::Provider(
                request.provider.clone(),
            )));
        }
        let principal = host.verify(request.provider, request.proof).await?;
        let identity = Identity::new(request.provider.clone(), principal);
        let issued = host
            .issue_certificate(&AuthorityRequest {
                csr: request.csr,
                identity: &identity,
                lifetime: self.config.certificate_ttl,
            })
            .await?;
        host.repository().record_issued(&issued.record).await?;
        Ok(issued)
    }

    async fn revoke_certificates<H: AuthenticationHost>(
        &self,
        host: &H,
        target: &CertificateTarget<'_>,
    ) -> Result<Vec<IssuedCertRecord>, PortError> {
        match target {
            CertificateTarget::Serial(serial) => Ok(vec![self.revoke_record(host, serial).await?]),
            CertificateTarget::Identity(identity) => {
                let records = host.repository().certificates_of(identity).await?;
                if records.is_empty() {
                    let subject = Identity::clone(identity);
                    return Err(PortError::NotFound(Subject::Principal(subject)));
                }
                let mut revoked = Vec::with_capacity(records.len());
                for record in records {
                    revoked.push(self.revoke_record(host, &record.serial).await?);
                }
                Ok(revoked)
            }
        }
    }

    async fn revoked_serials<H: AuthenticationHost>(
        &self,
        host: &H,
    ) -> Result<Vec<CertSerial>, PortError> {
        Ok(host
            .repository()
            .revoked()
            .await?
            .into_iter()
            .map(|record| record.serial)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use gang_clock::Timestamp;
    use gang_spi::{
        Authenticated, AuthenticationHost, AuthenticationPort, AuthorityRequest, CertStatus,
        CertificateTarget, CredentialLifecyclePort, Evidence, IssuedCertRecord, IssuedCertificate,
        Peer, Plugin, PluginContext, PortError, PrincipalRecord, RecordedScalingEvent, Rejection,
        ScalingAuditEvent, ScalingEventSeq, StateRepositoryPort,
    };
    use gang_testkit::{FakeRepository, TestSigner, park_then_signal};
    use gang_types::{
        AuthenticationSettings, CertSerial, Identity, NonZeroDuration, Principal, Proof,
        ProviderName, Url,
    };
    use http::HeaderMap;
    use parking_lot::Mutex;
    use tokio::sync::oneshot;

    use super::{DEFAULT_CERTIFICATE_TTL, DEFAULT_MAX_CACHED_CERTIFICATES, Mtls, MtlsConfig};

    fn context() -> PluginContext<'static> {
        PluginContext {
            config_path: Path::new("gang.toml"),
        }
    }

    fn parse(json: &str) -> Result<MtlsConfig, serde_json::Error> {
        serde_json::from_str(json)
    }

    #[test]
    fn defaults_fill_omitted_fields() {
        let config = parse(r#"{"provider":"corp"}"#).expect("a provider alone is a valid config");
        assert_eq!(config.certificate_ttl, DEFAULT_CERTIFICATE_TTL);
        assert_eq!(
            config.max_cached_certificates,
            DEFAULT_MAX_CACHED_CERTIFICATES
        );
        assert_eq!(DEFAULT_CERTIFICATE_TTL, NonZeroDuration::from_hours(24));
    }

    #[test]
    fn explicit_fields_are_read_and_round_trip() {
        let config =
            parse(r#"{"provider":"corp","certificate_ttl":"2h","max_cached_certificates":16}"#)
                .expect("every field is valid");
        assert_eq!(config.certificate_ttl, NonZeroDuration::from_hours(2));
        assert_eq!(config.max_cached_certificates.get(), 16);
        let serialized = serde_json::to_value(&config).expect("the config serializes");
        assert_eq!(
            serialized,
            serde_json::json!({
                "provider": "corp",
                "certificate_ttl": "2h",
                "max_cached_certificates": 16,
            })
        );
    }

    #[test]
    fn provider_is_required() {
        assert!(parse(r#"{"certificate_ttl":"1h"}"#).is_err());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(parse(r#"{"provider":"corp","unknown":true}"#).is_err());
    }

    #[test]
    fn zero_cache_capacity_is_rejected() {
        assert!(parse(r#"{"provider":"corp","max_cached_certificates":0}"#).is_err());
    }

    #[test]
    fn zero_certificate_ttl_is_rejected() {
        assert!(parse(r#"{"provider":"corp","certificate_ttl":"0s"}"#).is_err());
    }

    #[test]
    fn valid_config_builds() {
        let config = parse(r#"{"provider":"corp"}"#).expect("a provider alone is a valid config");
        assert!(Mtls::build(config, &context()).is_ok());
    }

    #[test]
    fn mtls_requires_tls_and_names_its_provider() {
        let config = parse(r#"{"provider":"corp"}"#).expect("a provider alone is a valid config");
        let providers: Vec<&str> = config
            .providers()
            .iter()
            .map(ProviderName::as_str)
            .collect();
        assert!(config.requires_tls());
        assert_eq!(providers, ["corp"]);
        assert_eq!(
            config.enrollment_provider().map(ProviderName::as_str),
            Some("corp")
        );
    }

    #[test]
    fn memo_caches_a_certificate_until_it_expires_or_is_removed() {
        let config = parse(r#"{"provider":"corp"}"#).expect("a provider alone is a valid config");
        let plugin = Mtls::build(config, &context()).expect("the config builds");
        let serial = CertSerial::from_be_bytes(&[1, 2, 3]).expect("test serial is non-empty");
        let identity = Arc::new(Identity::new(
            ProviderName::try_new("corp").expect("test provider is a valid name"),
            Principal::try_new("alice").expect("test principal is a valid name"),
        ));
        let at = |seconds: u64| Timestamp::UNIX_EPOCH + Duration::from_secs(seconds);
        let authenticated = Authenticated {
            identity: Arc::clone(&identity),
            valid_until: Some(at(100)),
        };

        plugin
            .memo
            .insert(serial.clone(), authenticated.clone(), at(100), at(0));
        assert_eq!(
            plugin.memo.get(&serial, at(50)),
            Some(authenticated.clone())
        );
        assert_eq!(plugin.memo.get(&serial, at(100)), None);

        plugin
            .memo
            .insert(serial.clone(), authenticated.clone(), at(200), at(0));
        plugin.memo.remove(&serial);
        assert_eq!(plugin.memo.get(&serial, at(50)), None);
    }

    fn instant(seconds: u64) -> Timestamp {
        Timestamp::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    /// A ledger that can park one certificate lookup after it has read its record.
    struct ParkingRepository {
        inner: FakeRepository,
        parked: Mutex<Option<ParkedLookup>>,
    }

    /// The lookup to park: `armed` is signalled once it waits on `release`.
    struct ParkedLookup {
        armed: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    }

    impl ParkingRepository {
        fn new() -> Self {
            Self {
                inner: FakeRepository::default(),
                parked: Mutex::new(None),
            }
        }

        /// Parks the next certificate lookup once it has read the ledger, until `release` fires.
        fn park_next_lookup(&self, armed: oneshot::Sender<()>, release: oneshot::Receiver<()>) {
            *self.parked.lock() = Some(ParkedLookup { armed, release });
        }
    }

    impl StateRepositoryPort for ParkingRepository {
        async fn record_issued(&self, record: &IssuedCertRecord) -> Result<(), PortError> {
            self.inner.record_issued(record).await
        }

        async fn certificate(
            &self,
            serial: &CertSerial,
        ) -> Result<Option<IssuedCertRecord>, PortError> {
            let parked = self.parked.lock().take();
            let Some(ParkedLookup { armed, release }) = parked else {
                return self.inner.certificate(serial).await;
            };
            park_then_signal(
                async {
                    let record = self.inner.certificate(serial).await;
                    release.await.ok();
                    record
                },
                armed,
            )
            .await
        }

        async fn revoke(
            &self,
            serial: &CertSerial,
            at: Timestamp,
        ) -> Result<IssuedCertRecord, PortError> {
            self.inner.revoke(serial, at).await
        }

        async fn revoked(&self) -> Result<Vec<IssuedCertRecord>, PortError> {
            self.inner.revoked().await
        }

        async fn certificates_of(
            &self,
            identity: &Identity,
        ) -> Result<Vec<IssuedCertRecord>, PortError> {
            self.inner.certificates_of(identity).await
        }

        async fn record_scaling_event(
            &self,
            event: &ScalingAuditEvent,
        ) -> Result<ScalingEventSeq, PortError> {
            self.inner.record_scaling_event(event).await
        }

        async fn scaling_events(
            &self,
            since: ScalingEventSeq,
            before: Option<ScalingEventSeq>,
            limit: NonZeroUsize,
            into: &mut Vec<RecordedScalingEvent>,
        ) -> Result<bool, PortError> {
            StateRepositoryPort::scaling_events(&self.inner, since, before, limit, into).await
        }

        async fn observe_principal(
            &self,
            identity: &Identity,
            at: Timestamp,
        ) -> Result<(), PortError> {
            self.inner.observe_principal(identity, at).await
        }

        async fn principal(
            &self,
            identity: &Identity,
        ) -> Result<Option<PrincipalRecord>, PortError> {
            self.inner.principal(identity).await
        }

        async fn revoke_principal(
            &self,
            identity: &Identity,
            at: Timestamp,
        ) -> Result<PrincipalRecord, PortError> {
            self.inner.revoke_principal(identity, at).await
        }

        async fn restore_principal(
            &self,
            identity: &Identity,
        ) -> Result<PrincipalRecord, PortError> {
            self.inner.restore_principal(identity).await
        }

        async fn revoked_principals(&self) -> Result<Vec<PrincipalRecord>, PortError> {
            self.inner.revoked_principals().await
        }

        async fn principals(
            &self,
            after: Option<&Identity>,
            limit: NonZeroUsize,
            only_revoked: bool,
            into: &mut Vec<PrincipalRecord>,
        ) -> Result<bool, PortError> {
            StateRepositoryPort::principals(&self.inner, after, limit, only_revoked, into).await
        }
    }

    /// A host that lends a [`ParkingRepository`] and a fixed clock to the plugin.
    struct ParkingHost {
        repository: ParkingRepository,
        signer: TestSigner,
        endpoint: Url,
        now: Timestamp,
    }

    impl ParkingHost {
        fn new(now: Timestamp) -> Self {
            Self {
                repository: ParkingRepository::new(),
                signer: TestSigner::authority().expect("the authority seed is a valid scalar"),
                endpoint: Url::parse("https://gang.example.com").expect("test endpoint parses"),
                now,
            }
        }
    }

    impl AuthenticationHost for ParkingHost {
        type Repository = ParkingRepository;
        type Signer = TestSigner;

        fn repository(&self) -> &ParkingRepository {
            &self.repository
        }

        fn signer(&self) -> &TestSigner {
            &self.signer
        }

        fn now(&self) -> Timestamp {
            self.now
        }

        fn endpoint(&self) -> &Url {
            &self.endpoint
        }

        async fn verify(
            &self,
            _provider: &ProviderName,
            _proof: &Proof,
        ) -> Result<Principal, PortError> {
            Err(PortError::unavailable("this host verifies no proofs"))
        }

        async fn issue_certificate(
            &self,
            _request: &AuthorityRequest<'_>,
        ) -> Result<IssuedCertificate, PortError> {
            Err(PortError::unavailable("this host signs no certificates"))
        }
    }

    fn corp_alice_record(serial: &CertSerial) -> IssuedCertRecord {
        IssuedCertRecord {
            serial: serial.clone(),
            identity: Identity::new(
                ProviderName::try_new("corp").expect("test provider is a valid name"),
                Principal::try_new("alice").expect("test principal is a valid name"),
            ),
            issued_at: instant(0),
            expires_at: instant(3600),
            status: CertStatus::Active,
        }
    }

    #[tokio::test]
    async fn lookup_parked_across_a_revocation_does_not_undo_it() {
        let config = parse(r#"{"provider":"corp"}"#).expect("a provider alone is a valid config");
        let plugin = Mtls::build(config, &context()).expect("the config builds");
        let host = ParkingHost::new(instant(0));
        let serial = CertSerial::from_be_bytes(&[1, 2, 3]).expect("test serial is non-empty");
        host.repository
            .record_issued(&corp_alice_record(&serial))
            .await
            .expect("the ledger accepts the record");
        let peer = Peer {
            certificate: Some(serial.clone()),
            ..Peer::default()
        };
        let headers = HeaderMap::new();
        let evidence = Evidence {
            peer: &peer,
            headers: &headers,
            session: None,
        };
        let (armed_tx, armed_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        host.repository.park_next_lookup(armed_tx, release_rx);

        let (_in_flight, ()) = tokio::join!(plugin.authenticate(&host, &evidence), async {
            armed_rx
                .await
                .expect("the lookup parks before the revocation");
            plugin
                .revoke_certificates(&host, &CertificateTarget::Serial(&serial))
                .await
                .expect("the parked certificate is revoked");
            release_tx
                .send(())
                .expect("the parked lookup is still waiting");
        },);

        let next = plugin.authenticate(&host, &evidence).await;
        assert!(
            matches!(
                next,
                Err(PortError::Rejected {
                    reason: Rejection::PolicyDenied,
                    ..
                })
            ),
            "a certificate revoked during a lookup must be refused, got {next:?}"
        );
    }
}
