//! Extension sets for the certificate authority, client and server profiles.

use std::net::IpAddr;

use const_oid::AssociatedOid;
use const_oid::db::rfc5280::{ID_KP_CLIENT_AUTH, ID_KP_SERVER_AUTH};
use der::Encode;
use der::asn1::{Ia5String, OctetString};
use sha2::{Digest, Sha256};
use spki::SubjectPublicKeyInfoOwned;
use x509_cert::ext::Extension;
use x509_cert::ext::pkix::name::GeneralName;
use x509_cert::ext::pkix::{
    AuthorityKeyIdentifier, BasicConstraints, ExtendedKeyUsage, KeyUsage, KeyUsages,
    SubjectAltName, SubjectKeyIdentifier,
};

use crate::errors::MtlsError;
use crate::subject_name::SubjectName;

/// Number of leading bytes of the SHA-256 digest used as a key identifier (RFC 7093 method 1).
const KEY_IDENTIFIER_LENGTH: usize = 20;

/// Returns the key identifier of `spki`: the leftmost 160 bits of the SHA-256 digest of the
/// subject public key bit string (RFC 7093 method 1).
pub(crate) fn key_identifier(spki: &SubjectPublicKeyInfoOwned) -> Vec<u8> {
    Sha256::digest(spki.subject_public_key.raw_bytes())
        .iter()
        .take(KEY_IDENTIFIER_LENGTH)
        .copied()
        .collect()
}

/// Returns the extensions of the self-signed certificate authority certificate.
///
/// # Errors
///
/// Returns [`MtlsError::Encoding`] when an extension cannot be encoded.
pub(crate) fn ca_extensions(subject_key_id: &[u8]) -> Result<Vec<Extension>, MtlsError> {
    Ok(vec![
        extension(
            &BasicConstraints {
                ca: true,
                path_len_constraint: Some(0),
            },
            true,
        )?,
        extension(&KeyUsage(KeyUsages::KeyCertSign.into()), true)?,
        extension(&subject_key_identifier(subject_key_id)?, false)?,
    ])
}

/// Returns the extensions of a client certificate.
///
/// # Errors
///
/// Returns [`MtlsError::Encoding`] when an extension cannot be encoded.
pub(crate) fn client_extensions(
    subject_key_id: &[u8],
    authority_key_id: &[u8],
    spiffe_id: &str,
) -> Result<Vec<Extension>, MtlsError> {
    Ok(vec![
        extension(&end_entity_constraints(), true)?,
        extension(&KeyUsage(KeyUsages::DigitalSignature.into()), true)?,
        extension(&ExtendedKeyUsage(vec![ID_KP_CLIENT_AUTH]), false)?,
        extension(&subject_key_identifier(subject_key_id)?, false)?,
        extension(&authority_key_identifier(authority_key_id)?, false)?,
        extension(
            &SubjectAltName(vec![GeneralName::UniformResourceIdentifier(
                Ia5String::new(spiffe_id)?,
            )]),
            false,
        )?,
    ])
}

/// Returns the extensions of a server certificate whose subject alternative names are `names`.
///
/// # Errors
///
/// Returns [`MtlsError::Encoding`] when an extension cannot be encoded.
pub(crate) fn server_extensions(
    subject_key_id: &[u8],
    authority_key_id: &[u8],
    names: &[SubjectName],
) -> Result<Vec<Extension>, MtlsError> {
    let general_names = names
        .iter()
        .map(general_name)
        .collect::<Result<Vec<_>, MtlsError>>()?;
    Ok(vec![
        extension(&end_entity_constraints(), true)?,
        extension(&KeyUsage(KeyUsages::DigitalSignature.into()), true)?,
        extension(&ExtendedKeyUsage(vec![ID_KP_SERVER_AUTH]), false)?,
        extension(&subject_key_identifier(subject_key_id)?, false)?,
        extension(&authority_key_identifier(authority_key_id)?, false)?,
        extension(&SubjectAltName(general_names), false)?,
    ])
}

/// Basic constraints of an end-entity certificate: not a CA.
fn end_entity_constraints() -> BasicConstraints {
    BasicConstraints {
        ca: false,
        path_len_constraint: None,
    }
}

fn subject_key_identifier(subject_key_id: &[u8]) -> Result<SubjectKeyIdentifier, MtlsError> {
    Ok(SubjectKeyIdentifier(OctetString::new(subject_key_id)?))
}

fn authority_key_identifier(authority_key_id: &[u8]) -> Result<AuthorityKeyIdentifier, MtlsError> {
    Ok(AuthorityKeyIdentifier {
        key_identifier: Some(OctetString::new(authority_key_id)?),
        ..Default::default()
    })
}

fn general_name(name: &SubjectName) -> Result<GeneralName, MtlsError> {
    Ok(match name {
        SubjectName::Dns(dns) => GeneralName::DnsName(Ia5String::new(dns)?),
        SubjectName::Ip(address) => GeneralName::IpAddress(OctetString::new(ip_octets(*address))?),
    })
}

fn ip_octets(address: IpAddr) -> Vec<u8> {
    match address {
        IpAddr::V4(address) => address.octets().to_vec(),
        IpAddr::V6(address) => address.octets().to_vec(),
    }
}

/// Encodes `value` as an extension with its own object identifier.
fn extension<T: AssociatedOid + Encode>(value: &T, critical: bool) -> Result<Extension, MtlsError> {
    Ok(Extension {
        extn_id: T::OID,
        critical,
        extn_value: OctetString::new(value.to_der()?)?,
    })
}
