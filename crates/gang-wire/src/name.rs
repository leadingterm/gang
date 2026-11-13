//! Resource names: one path segment per identifier.
//!
//! An ID is exactly one path segment. Every byte outside the RFC 3986 unreserved set
//! (`A-Z a-z 0-9 - . _ ~`) is percent-encoded with uppercase hex, so an encoded ID never
//! contains a literal `:` or `/`. The AIP-136 verb is the text after the last `:` of the final
//! segment, which [`split_verb`] separates from the ID.

use std::borrow::Cow;
use std::fmt::{self, Display};
use std::str::FromStr;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

use crate::ResourceNameError;

/// Collection of the singleton server resource.
pub const SERVER: &str = "server";

/// Collection of the singleton caller session resource.
pub const SESSION: &str = "session";

/// Collection of the singleton state resource.
pub const STATE: &str = "state";

/// A resource collection: the segment that names it, and the type of the identifier that names
/// a member of it.
pub trait Collection {
    /// The collection segment, such as `fleets`.
    const NAME: &'static str;
    /// The type of the identifier that names a member: written with Display, read with `FromStr`.
    type Id: Display + FromStr;
}

/// Collection of principal resources, named by identity.
#[derive(Debug, Clone, Copy)]
pub enum Principals {}

impl Collection for Principals {
    const NAME: &'static str = "principals";
    type Id = gang_types::Identity;
}

/// Collection of certificate resources, named by serial.
#[derive(Debug, Clone, Copy)]
pub enum Certificates {}

impl Collection for Certificates {
    const NAME: &'static str = "certificates";
    type Id = gang_types::CertSerial;
}

/// Collection of role resources, named by validated role name.
#[derive(Debug, Clone, Copy)]
pub enum Roles {}

impl Collection for Roles {
    const NAME: &'static str = "roles";
    type Id = gang_types::RoleName;
}

/// Collection of scaling events, named by ledger sequence number.
#[derive(Debug, Clone, Copy)]
pub enum Events {}

impl Collection for Events {
    const NAME: &'static str = "events";
    type Id = u64;
}

/// Collection of fleet resources, named by fleet name.
#[derive(Debug, Clone, Copy)]
pub enum Fleets {}

impl Collection for Fleets {
    const NAME: &'static str = "fleets";
    type Id = gang_types::FleetName;
}

/// Collection of worker resources, named by worker identifier.
#[derive(Debug, Clone, Copy)]
pub enum Workers {}

impl Collection for Workers {
    const NAME: &'static str = "workers";
    type Id = gang_types::WorkerId;
}

/// Collection of instance resources, named by instance identifier.
#[derive(Debug, Clone, Copy)]
pub enum Instances {}

impl Collection for Instances {
    const NAME: &'static str = "instances";
    type Id = gang_types::InstanceId;
}

/// Collection of action resources, named by an unstructured string.
#[derive(Debug, Clone, Copy)]
pub enum Actions {}

impl Collection for Actions {
    const NAME: &'static str = "actions";
    type Id = String;
}

/// Bytes that stay literal in an encoded ID: everything alphanumeric plus the unreserved
/// punctuation `-`, `.`, `_` and `~`.
const ID_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encodes `id` so it forms exactly one path segment.
#[must_use]
pub fn encode(id: &str) -> String {
    encoded_chunks(id).collect()
}

/// Produces the same encoded chunks for writing and comparing path segments.
fn encoded_chunks(id: &str) -> impl Iterator<Item = &str> {
    utf8_percent_encode(id, ID_SEGMENT)
}

/// Compares resource names for string IDs without allocating the encoded names.
#[must_use]
pub fn cmp_of<C: Collection<Id = String>>(a: &str, b: &str) -> std::cmp::Ordering {
    encoded_chunks(a)
        .flat_map(str::bytes)
        .cmp(encoded_chunks(b).flat_map(str::bytes))
}

/// Percent-decodes one encoded path segment back into an ID.
///
/// The result borrows from `segment` when it contains no percent escape, and owns the decoded
/// text otherwise.
///
/// Decoding is strict: the segment must be non-empty and contain no literal `:` or `/`, every
/// `%` must be followed by two hex digits (either case), and the decoded bytes must be UTF-8.
///
/// # Errors
///
/// Returns [`ResourceNameError::MalformedId`] for an empty segment, a literal `:` or `/`, or a
/// `%` that is not followed by two hex digits, and [`ResourceNameError::NotUtf8`] when the
/// decoded bytes are not valid UTF-8.
pub fn decode(segment: &str) -> Result<Cow<'_, str>, ResourceNameError> {
    if segment.is_empty() || segment.contains([':', '/']) {
        return Err(ResourceNameError::MalformedId);
    }
    let bytes = segment.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if !bytes
                .get(index + 1..index + 3)
                .is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit))
            {
                return Err(ResourceNameError::MalformedId);
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    percent_decode_str(segment)
        .decode_utf8()
        .map_err(|_| ResourceNameError::NotUtf8)
}

/// Builds the resource name `{collection}/{id}` for `id`, encoding it as one segment.
#[must_use]
pub fn of<C: Collection>(id: &C::Id) -> String {
    let mut name = String::new();
    write_of::<C>(&mut name, id);
    name
}

/// Replaces the contents of `into` with the resource name `{collection}/{id}`, encoding `id` as
/// one segment exactly as [`of`] does.
///
/// The name is streamed into `into`, so its existing capacity is reused and nothing is allocated
/// when it is large enough.
pub fn write_of<C: Collection>(into: &mut String, id: &C::Id) {
    into.clear();
    into.push_str(C::NAME);
    into.push('/');
    push_segment(into, id);
}

/// Appends `id` percent-encoded as exactly one path segment, without clearing `into`.
///
/// [`write_of`] uses it after the collection prefix, and route builders use it to append an ID
/// to a path.
///
/// # Panics
///
/// Panics if `id`'s `Display` implementation returns an error.
pub fn push_segment(into: &mut String, id: &(impl Display + ?Sized)) {
    let mut sink = SegmentSink { out: into };
    let written = fmt::write(&mut sink, format_args!("{id}"));
    // `SegmentSink::write_str` never fails, so only a faulty `Display` impl of the ID can fail here.
    assert!(written.is_ok(), "Display for an ID returned an error");
}

/// Adapter that percent-encodes every chunk written to it, appending the result to `out`.
///
/// Percent-encoding works byte by byte, so encoding each chunk separately yields the same bytes
/// as encoding the whole string.
struct SegmentSink<'a> {
    out: &'a mut String,
}

impl fmt::Write for SegmentSink<'_> {
    fn write_str(&mut self, chunk: &str) -> fmt::Result {
        self.out.extend(encoded_chunks(chunk));
        Ok(())
    }
}

/// Parses the resource name `{collection}/{id}` into the typed ID of collection `C`.
///
/// The name must begin with `C::NAME` followed by `/`; the remaining segment is decoded with
/// [`decode`] and then parsed with `C::Id::from_str`.
///
/// # Errors
///
/// Returns [`ResourceNameError::WrongCollection`] when the name does not start with `{C::NAME}/`,
/// [`ResourceNameError::MalformedId`] or [`ResourceNameError::NotUtf8`] when the segment does not
/// decode, and [`ResourceNameError::InvalidId`] when `C::Id::from_str` rejects the decoded ID.
pub fn parse<C: Collection>(name: &str) -> Result<C::Id, ResourceNameError> {
    let Some(segment) = name
        .strip_prefix(C::NAME)
        .and_then(|rest| rest.strip_prefix('/'))
    else {
        return Err(ResourceNameError::WrongCollection {
            collection: C::NAME,
        });
    };
    let id = decode(segment)?;
    <C::Id as FromStr>::from_str(&id).map_err(|_| ResourceNameError::InvalidId {
        collection: C::NAME,
    })
}

/// Splits an AIP-136 verb off the final segment: the text after its last `:`.
///
/// Returns the segment unchanged with no verb when it contains no `:`.
#[must_use]
pub fn split_verb(segment: &str) -> (&str, Option<&str>) {
    match segment.rsplit_once(':') {
        Some((id, verb)) => (id, Some(verb)),
        None => (segment, None),
    }
}

#[cfg(test)]
mod tests {
    use gang_types::{CertSerial, Identity};

    use super::*;

    #[test]
    fn invalid_role_name_is_refused() {
        assert_eq!(
            parse::<Roles>("roles/invalid%20role"),
            Err(ResourceNameError::InvalidId {
                collection: "roles"
            })
        );
    }

    fn identity() -> Identity {
        "google:ops@example.org".parse().expect("identity parses")
    }

    #[test]
    fn encode_percent_encodes_reserved_bytes() {
        assert_eq!(
            encode("google:ops@example.org"),
            "google%3Aops%40example.org"
        );
    }

    #[test]
    fn encode_leaves_unreserved_bytes_unchanged() {
        assert_eq!(encode("aZ09-._~"), "aZ09-._~");
    }

    #[test]
    fn encode_uses_bytewise_uppercase_utf8() {
        assert_eq!(encode("é"), "%C3%A9");
    }

    #[test]
    fn principal_name_round_trips_identity() {
        let name = of::<Principals>(&identity());
        assert_eq!(name, "principals/google%3Aops%40example.org");
        assert_eq!(
            parse::<Principals>(&name).expect("principal parses"),
            identity()
        );
    }

    #[test]
    fn certificate_serial_round_trips() {
        let serial: CertSerial = "3fa9".parse().expect("serial parses");
        let name = of::<Certificates>(&serial);
        assert_eq!(name, "certificates/3fa9");
        assert_eq!(
            parse::<Certificates>(&name).expect("serial round-trips"),
            serial
        );
    }

    #[test]
    fn fleet_name_round_trips() {
        let fleet = gang_types::FleetName::try_new("gang-x86").expect("fleet name is valid");
        let name = of::<Fleets>(&fleet);
        assert_eq!(name, "fleets/gang-x86");
        assert_eq!(parse::<Fleets>(&name).expect("fleet parses"), fleet);
    }

    #[test]
    fn event_name_round_trips_sequence() {
        assert_eq!(of::<Events>(&995_u64), "events/995");
        assert_eq!(parse::<Events>("events/995"), Ok(995));
    }

    #[test]
    fn event_name_rejects_non_numeric_id() {
        assert_eq!(
            parse::<Events>("events/abc"),
            Err(ResourceNameError::InvalidId {
                collection: "events"
            })
        );
    }

    #[test]
    fn parse_rejects_other_collection() {
        assert_eq!(
            parse::<Events>("principals/1"),
            Err(ResourceNameError::WrongCollection {
                collection: "events"
            })
        );
        assert_eq!(
            parse::<Workers>("fleets/x"),
            Err(ResourceNameError::WrongCollection {
                collection: "workers"
            })
        );
    }

    #[test]
    fn parse_requires_slash_after_collection() {
        assert_eq!(
            parse::<Events>("events"),
            Err(ResourceNameError::WrongCollection {
                collection: "events"
            })
        );
        assert_eq!(
            parse::<Events>("eventsx/1"),
            Err(ResourceNameError::WrongCollection {
                collection: "events"
            })
        );
    }

    #[test]
    fn parse_rejects_empty_or_extra_segments() {
        assert_eq!(
            parse::<Events>("events/"),
            Err(ResourceNameError::MalformedId)
        );
        assert_eq!(
            parse::<Events>("events/1/2"),
            Err(ResourceNameError::MalformedId)
        );
    }

    #[test]
    fn decode_rejects_literal_separators() {
        assert_eq!(decode("a:b"), Err(ResourceNameError::MalformedId));
        assert_eq!(decode("a/b"), Err(ResourceNameError::MalformedId));
    }

    #[test]
    fn decode_rejects_bad_percent_escapes() {
        assert_eq!(decode("%zz"), Err(ResourceNameError::MalformedId));
        assert_eq!(decode("%4"), Err(ResourceNameError::MalformedId));
        assert_eq!(decode("a%"), Err(ResourceNameError::MalformedId));
    }

    #[test]
    fn decode_rejects_empty() {
        assert_eq!(decode(""), Err(ResourceNameError::MalformedId));
    }

    #[test]
    fn decode_rejects_invalid_utf8() {
        assert_eq!(decode("%FF"), Err(ResourceNameError::NotUtf8));
    }

    #[test]
    fn decode_accepts_lowercase_hex() {
        assert_eq!(decode("%3a").expect("escape decodes"), ":");
        assert_eq!(
            decode("google%3aops").expect("escape decodes"),
            "google:ops"
        );
    }

    #[test]
    fn decode_borrows_plain_segment() {
        let decoded = decode("google-ops").expect("plain segment decodes");
        assert!(matches!(decoded, Cow::Borrowed(_)));
        assert_eq!(decoded, "google-ops");
    }

    #[test]
    fn decode_owns_escaped_segment() {
        let decoded = decode("google%3Aops").expect("escaped segment decodes");
        assert!(matches!(decoded, Cow::Owned(_)));
        assert_eq!(decoded, "google:ops");
    }

    #[test]
    fn split_verb_separates_last_colon() {
        assert_eq!(
            split_verb("google%3Aops:revoke"),
            ("google%3Aops", Some("revoke"))
        );
    }

    #[test]
    fn split_verb_without_colon_has_no_verb() {
        assert_eq!(split_verb("abc"), ("abc", None));
    }

    #[test]
    fn awkward_ids_round_trip() {
        for id in ["a b", "x/y", "%", "é", ":", "aZ09-._~"] {
            let encoded = encode(id);
            assert!(!encoded.contains([':', '/']), "{encoded} has a separator");
            assert_eq!(decode(&encoded).expect("encoded id decodes"), id);
        }
    }

    #[test]
    fn push_segment_appends_without_clearing() {
        let mut buf = String::from("x/");
        push_segment(&mut buf, "a b");
        assert_eq!(buf, "x/a%20b");
    }

    #[test]
    fn write_of_matches_of() {
        let mut buf = String::new();
        for id in ["w-1", "a b/c:d%", "é"] {
            let id = id.to_owned();
            write_of::<Actions>(&mut buf, &id);
            assert_eq!(buf, of::<Actions>(&id));
        }
    }

    #[test]
    fn cmp_of_matches_encoded_name_order() {
        let ids = ["a-1", "a/1", "", "a%", "é", "a~", "a b"];
        for a in ids {
            for b in ids {
                assert_eq!(
                    cmp_of::<Actions>(a, b),
                    of::<Actions>(&a.to_owned()).cmp(&of::<Actions>(&b.to_owned()))
                );
            }
        }
        assert_eq!(cmp_of::<Actions>("a-1", "a/1"), std::cmp::Ordering::Greater);
    }

    #[test]
    fn write_of_replaces_previous_contents() {
        let mut buf = String::new();
        write_of::<Actions>(&mut buf, &"long-identifier".to_owned());
        write_of::<Actions>(&mut buf, &"1".to_owned());
        assert_eq!(buf, "actions/1");
    }
}
