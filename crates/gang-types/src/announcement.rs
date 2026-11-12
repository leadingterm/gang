//! The startup lines that `gang server` prints on stdout before serving: its CA pin under TLS,
//! its plaintext endpoint otherwise, and each listener's bound address. Every line has the form
//! `<name>: <value>`. `gang local up` and the test harnesses read them back from the server log.

use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::{CaPin, Url};

crate::string_enum! {
    /// The listener an announcement names.
    pub enum ListenerRole("listener role") {
        /// The public API and REAPI listener.
        Public = "public",
        /// The worker listener.
        Worker = "worker",
        /// The local provisioner's mock instance metadata listener.
        Imds = "imds",
    }
}

/// The name of the announcement that carries the CA pin of a TLS server.
pub const CA_PIN: &str = "ca-pin";
/// The name of the announcement that carries the endpoint of a plaintext server.
pub const PLAINTEXT: &str = "plaintext";
/// The name of the announcement that carries a listener's bound address.
const LISTENING: &str = "listening";

/// One line of the server's startup announcements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Announcement {
    /// The CA pin of a server that serves TLS.
    CaPin(CaPin),
    /// The endpoint of a server that serves plaintext.
    Plaintext(Url),
    /// The address a listener bound, with the port the operating system chose.
    Listening(ListenerRole, SocketAddr),
}

/// A line that is not one of the server's announcements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotAnAnnouncement;

impl fmt::Display for NotAnAnnouncement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("not an announcement")
    }
}

impl std::error::Error for NotAnAnnouncement {}

impl fmt::Display for Announcement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CaPin(pin) => write!(formatter, "{CA_PIN}: {pin}"),
            Self::Plaintext(endpoint) => write!(formatter, "{PLAINTEXT}: {endpoint}"),
            Self::Listening(role, address) => write!(formatter, "{LISTENING}: {role} {address}"),
        }
    }
}

impl FromStr for Announcement {
    type Err = NotAnAnnouncement;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let (name, value) = line.split_once(": ").ok_or(NotAnAnnouncement)?;
        match name {
            CA_PIN => value
                .parse()
                .map(Self::CaPin)
                .map_err(|_| NotAnAnnouncement),
            PLAINTEXT => value
                .parse()
                .map(Self::Plaintext)
                .map_err(|_| NotAnAnnouncement),
            LISTENING => {
                let (role, address) = value.split_once(' ').ok_or(NotAnAnnouncement)?;
                Ok(Self::Listening(
                    role.parse().map_err(|_| NotAnAnnouncement)?,
                    address.parse().map_err(|_| NotAnAnnouncement)?,
                ))
            }
            _ => Err(NotAnAnnouncement),
        }
    }
}

/// Every valid announcement in a log, in order, ignoring lines that are not announcements.
fn announcements(log: &str) -> impl Iterator<Item = Announcement> + '_ {
    log.lines().filter_map(|line| line.trim().parse().ok())
}

/// The last valid announced CA pin in a log.
#[must_use]
pub fn last_ca_pin(log: &str) -> Option<CaPin> {
    announcements(log)
        .filter_map(|announcement| match announcement {
            Announcement::CaPin(pin) => Some(pin),
            _ => None,
        })
        .last()
}

/// The last valid announced plaintext endpoint in a log.
#[must_use]
pub fn last_plaintext(log: &str) -> Option<Url> {
    announcements(log)
        .filter_map(|announcement| match announcement {
            Announcement::Plaintext(endpoint) => Some(endpoint),
            _ => None,
        })
        .last()
}

/// The address the last valid announcement of `role` in a log names.
#[must_use]
pub fn last_listening(log: &str, role: ListenerRole) -> Option<SocketAddr> {
    announcements(log)
        .filter_map(|announcement| match announcement {
            Announcement::Listening(named, address) if named == role => Some(address),
            _ => None,
        })
        .last()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::str::FromStr;

    use super::{
        Announcement, ListenerRole, NotAnAnnouncement, last_ca_pin, last_listening, last_plaintext,
    };
    use crate::{CaPin, Url};

    /// Builds a valid pin whose digest repeats one byte, written as two hex characters.
    fn pin(byte_hex: &str) -> CaPin {
        CaPin::from_str(&format!("sha256:{}", byte_hex.repeat(32))).expect("test pin text is valid")
    }

    fn address(text: &str) -> SocketAddr {
        text.parse().expect("test address is valid")
    }

    fn url(text: &str) -> Url {
        text.parse().expect("test url is valid")
    }

    #[test]
    fn ca_pin_round_trips_through_display_and_from_str() {
        let original = Announcement::CaPin(pin("ab"));
        assert_eq!(original.to_string().parse::<Announcement>(), Ok(original));
    }

    #[test]
    fn plaintext_round_trips_through_display_and_from_str() {
        let original = Announcement::Plaintext(url("http://127.0.0.1:9443/"));
        assert_eq!(original.to_string().parse::<Announcement>(), Ok(original));
    }

    #[test]
    fn listening_round_trips_through_display_and_from_str() {
        let original = Announcement::Listening(ListenerRole::Public, address("127.0.0.1:9443"));
        assert_eq!(original.to_string().parse::<Announcement>(), Ok(original));
    }

    #[test]
    fn announcement_lines_have_exact_text() {
        assert_eq!(
            Announcement::Listening(ListenerRole::Public, address("127.0.0.1:9443")).to_string(),
            "listening: public 127.0.0.1:9443"
        );
        assert_eq!(
            Announcement::Plaintext(url("http://127.0.0.1:9443/")).to_string(),
            "plaintext: http://127.0.0.1:9443/"
        );
        assert_eq!(
            Announcement::CaPin(pin("ab")).to_string(),
            format!("ca-pin: sha256:{}", "ab".repeat(32))
        );
    }

    #[test]
    fn last_ca_pin_wins_among_valid_lines() {
        let first = pin("ab");
        let second = pin("cd");
        let log = format!(
            "starting\n{}\nserving\n{}\n",
            Announcement::CaPin(first),
            Announcement::CaPin(second),
        );
        assert_eq!(last_ca_pin(&log), Some(second));
    }

    #[test]
    fn last_plaintext_wins_among_valid_lines() {
        let first = url("http://127.0.0.1:1/");
        let second = url("http://127.0.0.1:2/");
        let log = format!(
            "{}\n{}\n",
            Announcement::Plaintext(first.clone()),
            Announcement::Plaintext(second.clone()),
        );
        assert_eq!(last_plaintext(&log), Some(second));
    }

    #[test]
    fn last_listening_wins_per_role() {
        let first = address("127.0.0.1:4");
        let last = address("127.0.0.1:5");
        let log = format!(
            "{}\n{}\n{}\n",
            Announcement::Listening(ListenerRole::Public, first),
            Announcement::Listening(ListenerRole::Worker, first),
            Announcement::Listening(ListenerRole::Public, last),
        );
        assert_eq!(last_listening(&log, ListenerRole::Public), Some(last));
        assert_eq!(last_listening(&log, ListenerRole::Worker), Some(first));
        assert_eq!(last_listening(&log, ListenerRole::Imds), None);
    }

    #[test]
    fn malformed_ca_pin_line_is_ignored() {
        let valid = pin("ab");
        let log = format!(
            "{}\nca-pin: sha256:not-hex\nca-pin: sha256:abcd\n",
            Announcement::CaPin(valid),
        );
        assert_eq!(last_ca_pin(&log), Some(valid));
    }

    #[test]
    fn malformed_plaintext_line_is_ignored() {
        let valid = url("http://127.0.0.1:1/");
        let log = format!(
            "{}\nplaintext: not a url\n",
            Announcement::Plaintext(valid.clone())
        );
        assert_eq!(last_plaintext(&log), Some(valid));
    }

    #[test]
    fn malformed_listening_lines_are_ignored() {
        let log = "listening: admin 127.0.0.1:1\nlistening: public not-an-address\nlistening: imds 127.0.0.1:2\nlistening: public\n";
        assert_eq!(last_listening(log, ListenerRole::Public), None);
        assert_eq!(
            last_listening(log, ListenerRole::Imds),
            Some(address("127.0.0.1:2"))
        );
    }

    #[test]
    fn ca_pin_line_is_not_a_plaintext_line() {
        let log = format!("{}\n", Announcement::CaPin(pin("ab")));
        assert_eq!(last_plaintext(&log), None);
        assert_eq!(last_listening(&log, ListenerRole::Public), None);
    }

    #[test]
    fn plaintext_line_is_not_a_ca_pin_line() {
        let log = format!("{}\n", Announcement::Plaintext(url("http://127.0.0.1:1/")));
        assert_eq!(last_ca_pin(&log), None);
    }

    #[test]
    fn line_with_surrounding_whitespace_is_accepted() {
        let original = pin("ab");
        let log = format!("   {}\t  \r\n", Announcement::CaPin(original));
        assert_eq!(last_ca_pin(&log), Some(original));
    }

    #[test]
    fn prefix_in_middle_of_line_is_not_accepted() {
        let log = format!("log: {}\n", Announcement::CaPin(pin("ab")));
        assert_eq!(last_ca_pin(&log), None);
    }

    #[test]
    fn non_announcement_text_is_rejected() {
        assert_eq!(
            "server ready".parse::<Announcement>(),
            Err(NotAnAnnouncement)
        );
        assert_eq!(
            format!("sha256:{}", "ab".repeat(32)).parse::<Announcement>(),
            Err(NotAnAnnouncement)
        );
        assert_eq!(last_ca_pin(""), None);
    }

    #[test]
    fn log_without_announcement_has_no_pin() {
        let log = format!("server ready\nno pin here\nsha256:{}\n", "ab".repeat(32));
        assert_eq!(last_ca_pin(&log), None);
    }
}
