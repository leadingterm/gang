//! Subject names a server certificate is valid for.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

use crate::errors::MtlsError;

const MAX_DNS_NAME_LENGTH: usize = 253;
const MAX_DNS_LABEL_LENGTH: usize = 63;

/// A subject name a server certificate is valid for: an IP address or a DNS name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectName {
    /// A DNS name, lowercase ASCII, such as `gang.example.test`.
    Dns(String),
    /// An IP address.
    Ip(IpAddr),
}

impl FromStr for SubjectName {
    type Err = MtlsError;

    /// Parses an IP address literal, or else an ASCII DNS name, which is lowercased.
    ///
    /// # Errors
    ///
    /// Returns [`MtlsError::InvalidServerName`] when the text is neither.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Ok(address) = text.parse::<IpAddr>() {
            Ok(Self::Ip(address))
        } else if is_dns_name(text) {
            Ok(Self::Dns(text.to_ascii_lowercase()))
        } else {
            Err(MtlsError::InvalidServerName(text.to_owned()))
        }
    }
}

impl fmt::Display for SubjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dns(name) => f.write_str(name),
            Self::Ip(address) => fmt::Display::fmt(address, f),
        }
    }
}

fn is_dns_name(text: &str) -> bool {
    text.len() <= MAX_DNS_NAME_LENGTH && text.split('.').all(is_dns_label)
}

fn is_dns_label(label: &str) -> bool {
    (1..=MAX_DNS_LABEL_LENGTH).contains(&label.len())
        && !label.starts_with('-')
        && !label.ends_with('-')
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    fn parse(text: &str) -> Result<SubjectName, MtlsError> {
        text.parse()
    }

    #[test]
    fn ip_literals_parse_as_addresses() {
        assert_eq!(
            parse("192.0.2.1").expect("ipv4 parses"),
            SubjectName::Ip(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)))
        );
        assert_eq!(
            parse("::1").expect("ipv6 parses"),
            SubjectName::Ip(IpAddr::V6(Ipv6Addr::LOCALHOST))
        );
    }

    #[test]
    fn dns_names_are_lowercased() {
        assert_eq!(
            parse("Gang-Server.Example.TEST").expect("dns name parses"),
            SubjectName::Dns("gang-server.example.test".to_owned())
        );
    }

    #[test]
    fn display_round_trips_both_variants() {
        for text in ["192.0.2.1", "::1", "gang.example.test"] {
            let name = parse(text).expect("valid server name");
            assert_eq!(name.to_string(), text);
        }
    }

    #[test]
    fn invalid_names_are_rejected() {
        let cases = [
            "",
            "example.test.",
            "-leading.test",
            "trailing-.test",
            "under_score.test",
            "space name.test",
            "ünicode.test",
            "double..dot.test",
            "http://example.test",
        ];
        for text in cases {
            let result = parse(text);
            assert!(
                matches!(&result, Err(MtlsError::InvalidServerName(echoed)) if echoed == text),
                "{text:?} must be rejected"
            );
        }
    }

    #[test]
    fn label_length_limit_is_sixty_three() {
        let longest = format!("{}.test", "a".repeat(63));
        let too_long = format!("{}.test", "a".repeat(64));

        assert!(parse(&longest).is_ok());
        assert!(parse(&too_long).is_err());
    }

    #[test]
    fn name_length_limit_is_two_hundred_fifty_three() {
        let label = "a".repeat(63);
        let fits = format!("{label}.{label}.{label}.{}", "b".repeat(61));
        let too_long = format!("{fits}.c");

        assert_eq!(fits.len(), 253);
        assert!(parse(&fits).is_ok());
        assert!(parse(&too_long).is_err());
    }
}
