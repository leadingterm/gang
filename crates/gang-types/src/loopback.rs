//! Loopback URL host classification.

use std::net::IpAddr;

use crate::Url;

/// Returns whether the host of `url` is `localhost` in any case or a loopback IP address.
/// A URL without a host is not loopback.
#[must_use]
pub fn is_loopback_url(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Classifies localhost and loopback addresses without admitting other hosts.
    #[test]
    fn classifies_url_hosts() {
        for (url, expected) in [
            ("http://localhost", true),
            ("http://LOCALHOST", true),
            ("http://LocalHost", true),
            ("http://127.0.0.2", true),
            ("http://[::1]", true),
            ("http://192.0.2.1", false),
            ("http://example.com", false),
            ("mailto:user@example.com", false),
        ] {
            let parsed = Url::parse(url).expect("test URL parses");
            assert_eq!(is_loopback_url(&parsed), expected, "{url}");
        }
    }
}
