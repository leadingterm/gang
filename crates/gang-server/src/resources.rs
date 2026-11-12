//! The wire form of the records the server keeps: principals and certificates.

use gang_spi::{CertStatus, IssuedCertRecord, PrincipalRecord};
use gang_types::CertificatePem;
use gang_wire::name::{self, Certificates, Principals};
use gang_wire::{Certificate, Principal};

/// The wire form of one principal record.
pub(crate) fn wire_principal(record: PrincipalRecord) -> Principal {
    let PrincipalRecord {
        identity,
        first_seen,
        last_seen,
        revoked_at,
    } = record;
    Principal {
        name: name::of::<Principals>(&identity),
        provider: identity.provider,
        principal: identity.principal,
        create_time: first_seen,
        last_seen_time: last_seen,
        revoke_time: revoked_at,
    }
}

/// The wire form of one issued certificate. `pem_certificate` is the certificate itself, which
/// only the answer that issues it carries.
pub(crate) fn wire_certificate(
    record: IssuedCertRecord,
    pem_certificate: Option<CertificatePem>,
) -> Certificate {
    let IssuedCertRecord {
        serial,
        identity,
        issued_at,
        expires_at,
        status,
    } = record;
    Certificate {
        name: name::of::<Certificates>(&serial),
        principal: name::of::<Principals>(&identity),
        create_time: issued_at,
        expire_time: expires_at,
        revoke_time: match status {
            CertStatus::Active => None,
            CertStatus::Revoked { at } => Some(at),
        },
        pem_certificate,
    }
}

#[cfg(test)]
mod tests {
    use gang_clock::Timestamp;
    use gang_types::{CertSerial, Identity, Principal as PrincipalId, ProviderName};

    use super::*;

    fn provider() -> ProviderName {
        ProviderName::try_new("google").expect("provider name is valid")
    }

    fn principal() -> PrincipalId {
        PrincipalId::try_new("ops@example.org").expect("principal is valid")
    }

    #[test]
    fn principals_are_named_by_their_percent_encoded_identity() {
        let wire = wire_principal(PrincipalRecord {
            identity: Identity::new(provider(), principal()),
            first_seen: Timestamp::from_unix_millis(1_000),
            last_seen: Timestamp::from_unix_millis(2_000),
            revoked_at: Some(Timestamp::from_unix_millis(3_000)),
        });

        assert_eq!(wire.name, "principals/google%3Aops%40example.org");
        assert_eq!(wire.create_time, Timestamp::from_unix_millis(1_000));
        assert_eq!(wire.last_seen_time, Timestamp::from_unix_millis(2_000));
        assert_eq!(wire.revoke_time, Some(Timestamp::from_unix_millis(3_000)));
    }

    #[test]
    fn certificates_refer_to_their_principal_and_carry_the_revocation() {
        let record = |status| IssuedCertRecord {
            serial: CertSerial::try_new("3fa9").expect("serial is valid"),
            identity: Identity::new(provider(), principal()),
            issued_at: Timestamp::from_unix_millis(1_000),
            expires_at: Timestamp::from_unix_millis(9_000),
            status,
        };

        let active = wire_certificate(record(CertStatus::Active), None);
        assert_eq!(active.name, "certificates/3fa9");
        assert_eq!(active.principal, "principals/google%3Aops%40example.org");
        assert_eq!(active.revoke_time, None);
        assert_eq!(active.pem_certificate, None);

        let revoked = wire_certificate(
            record(CertStatus::Revoked {
                at: Timestamp::from_unix_millis(4_000),
            }),
            None,
        );
        assert_eq!(
            revoked.revoke_time,
            Some(Timestamp::from_unix_millis(4_000))
        );
    }
}
