//! SPIFFE identifiers for client certificates.

use gang_types::{Principal, ProviderName, TrustDomain};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

/// Every byte except the RFC 3986 unreserved characters, so a principal is always one path segment.
const PRINCIPAL_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Builds the SPIFFE identifier `spiffe://{trust_domain}/{provider}/{principal}`.
///
/// The principal is percent-encoded as a single path segment, so a principal containing `/`
/// or `@` cannot add path segments or change the trust domain.
#[must_use]
pub fn spiffe_id(
    trust_domain: &TrustDomain,
    provider: &ProviderName,
    principal: &Principal,
) -> String {
    format!(
        "spiffe://{trust_domain}/{provider}/{}",
        utf8_percent_encode(principal.as_str(), PRINCIPAL_SEGMENT)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn domain() -> TrustDomain {
        TrustDomain::try_new("gang.example").expect("valid trust domain")
    }

    fn provider() -> ProviderName {
        ProviderName::try_new("alpha").expect("valid provider name")
    }

    fn id_for(principal: &str) -> String {
        let principal = Principal::try_new(principal).expect("valid principal");
        spiffe_id(&domain(), &provider(), &principal)
    }

    #[test]
    fn email_principal_encodes_at_sign() {
        assert_eq!(
            id_for("dev@example.test"),
            "spiffe://gang.example/alpha/dev%40example.test"
        );
    }

    #[test]
    fn unreserved_characters_pass_through() {
        assert_eq!(
            id_for("a-b.c_d~E9"),
            "spiffe://gang.example/alpha/a-b.c_d~E9"
        );
    }

    #[test]
    fn slash_cannot_add_a_path_segment() {
        let id = id_for("team/dev");

        assert_eq!(id, "spiffe://gang.example/alpha/team%2Fdev");
        assert_eq!(id.split('/').count(), 5);
    }

    #[test]
    fn colon_is_encoded() {
        assert_eq!(
            id_for("repo:org/name:ref:refs/heads/main"),
            "spiffe://gang.example/alpha/repo%3Aorg%2Fname%3Aref%3Arefs%2Fheads%2Fmain"
        );
    }

    #[test]
    fn unicode_is_encoded_as_utf8_bytes() {
        assert_eq!(id_for("dév"), "spiffe://gang.example/alpha/d%C3%A9v");
    }

    #[test]
    fn empty_segment_injection_stays_one_segment() {
        let id = id_for("x/../../y");

        assert_eq!(id, "spiffe://gang.example/alpha/x%2F..%2F..%2Fy");
        assert_eq!(id.split('/').count(), 5);
        assert!(!id["spiffe://".len()..].contains("//"));
    }
}
