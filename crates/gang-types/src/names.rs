//! Validated string newtypes for identifiers used across Gang.

use std::borrow::{Borrow, Cow};

use crate::NameError;

fn validate(
    value: &str,
    kind: &'static str,
    allowed: fn(char) -> bool,
    max: Option<usize>,
    hyphen_edges: bool,
) -> Result<(), NameError> {
    if value.is_empty() {
        return Err(NameError::Empty { kind });
    }
    if let Some(character) = value.chars().find(|&character| !allowed(character)) {
        return Err(NameError::InvalidCharacter { kind, character });
    }
    if hyphen_edges && (value.starts_with('-') || value.ends_with('-')) {
        return Err(NameError::HyphenAtEdge { kind });
    }
    match max {
        Some(max) if value.chars().count() > max => Err(NameError::TooLong { kind, max }),
        _ => Ok(()),
    }
}

fn is_slug_char(character: char) -> bool {
    character.is_ascii_lowercase()
        || character.is_ascii_digit()
        || matches!(character, '-' | '_' | '.')
}

fn is_identifier_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
}

fn is_cert_serial_char(character: char) -> bool {
    character.is_ascii_digit() || matches!(character, 'a'..='f')
}

fn is_fleet_name_char(character: char) -> bool {
    character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
}

fn is_free_text_char(character: char) -> bool {
    !character.is_control()
}

fn is_scheduler_name_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
}

macro_rules! define_name {
    ($(#[$meta:meta])* $name:ident, $kind:literal, $allowed:path) => {
        define_name!($(#[$meta])* $name, $kind, $allowed, None, false);
    };
    ($(#[$meta:meta])* $name:ident, $kind:literal, $allowed:path, $max:expr) => {
        define_name!($(#[$meta])* $name, $kind, $allowed, $max, false);
    };
    ($(#[$meta:meta])* $name:ident, $kind:literal, $allowed:path, $max:expr, $edges:expr) => {
        crate::validated_string!(
            $(#[$meta])*
            pub $name,
            |value: &str| validate(value, $kind, $allowed, $max, $edges),
            NameError
        );
    };
}

define_name! {
    /// Operator's name for an identity provider, such as `aws-workers`.
    ///
    /// Allowed characters: `a-z`, `0-9`, `-`, `_` and `.`.
    ProviderName, "provider name", is_slug_char
}

/// Text of the reserved provider name that stands for no authentication.
const ANONYMOUS_TEXT: &str = "none";

impl ProviderName {
    /// The reserved provider name that stands for no authentication.
    pub const ANONYMOUS: Self = Self(Cow::Borrowed(ANONYMOUS_TEXT));

    /// Returns whether this is the reserved name that stands for no authentication.
    #[must_use]
    pub fn is_anonymous(&self) -> bool {
        self.as_str() == ANONYMOUS_TEXT
    }
}

define_name! {
    /// Name of a role that grants permissions to principals, such as `administrator`.
    ///
    /// Allowed characters: `a-z`, `0-9`, `-`, `_` and `.`.
    RoleName, "role name", is_slug_char
}

impl RoleName {
    /// The role that grants every permission when the server is configured without
    /// authentication.
    pub const ADMINISTRATOR: Self = Self(Cow::Borrowed("administrator"));
}

/// Longest allowed scheduler name, in characters.
const SCHEDULER_NAME_MAX_LENGTH: usize = 64;

define_name! {
    /// Name of a scheduler instance; allowed characters and maximum length.
    ///
    /// Allowed characters: `A-Z`, `a-z`, `0-9`, `-` and `_`. Hyphens and underscores are allowed
    /// at either edge. At most 64 characters.
    SchedulerName, "scheduler name", is_scheduler_name_char, Some(SCHEDULER_NAME_MAX_LENGTH)
}

/// Longest allowed audience, in characters.
const AUDIENCE_MAX_LENGTH: usize = 1000;

define_name! {
    /// Audience that a workload credential is issued for, such as `sts.amazonaws.com`.
    ///
    /// Any non-control character is allowed. 1 to 1000 characters.
    Audience, "audience", is_free_text_char, Some(AUDIENCE_MAX_LENGTH)
}

/// Text of the audience a workload token is issued for unless it is configured.
const AUDIENCE_DEFAULT_TEXT: &str = "gang";

impl Audience {
    /// The audience a workload token is issued for unless it is configured: `gang`.
    pub const DEFAULT: Self = Self(Cow::Borrowed(AUDIENCE_DEFAULT_TEXT));
}

define_name! {
    /// Trust domain that identifies a certificate authority, such as `gang.internal`.
    ///
    /// Allowed characters: `a-z`, `0-9`, `-`, `_` and `.`.
    TrustDomain, "trust domain", is_slug_char
}

define_name! {
    /// Cloud instance identifier, such as `i-0abc123`.
    ///
    /// Allowed characters: `A-Z`, `a-z`, `0-9`, `-`, `_` and `.`.
    InstanceId, "instance id", is_identifier_char
}

define_name! {
    /// Identifier of a `NativeLink` worker.
    ///
    /// A worker id is the worker name followed by a UUID. Gang names every worker `{instance}-`
    /// plus a UUID, so [`WorkerId::instance`] recovers the [`InstanceId`] of the host that runs it.
    ///
    /// Allowed characters: `A-Z`, `a-z`, `0-9`, `-`, `_` and `.`.
    WorkerId, "worker id", is_identifier_char
}

/// Longest allowed fleet name, in characters.
const FLEET_NAME_MAX_LENGTH: usize = 40;

define_name! {
    /// Name of a fleet as the provisioner knows it, such as `gang-worker-x86-64`.
    ///
    /// Allowed characters: `a-z`, `0-9` and `-`. Must start and end with a letter or digit.
    /// At most 40 characters.
    FleetName, "fleet name", is_fleet_name_char, Some(FLEET_NAME_MAX_LENGTH), true
}

define_name! {
    /// Qualitative class of a machine, such as `gpu`.
    ///
    /// Allowed characters: `a-z`, `0-9` and `-`. Must start and end with a letter or digit.
    /// At most 40 characters.
    MachineClass, "machine class", is_fleet_name_char, Some(FLEET_NAME_MAX_LENGTH), true
}

/// Why certificate serial text is not canonical.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CertSerialError {
    /// The serial is empty.
    #[error("certificate serial is empty")]
    Empty,
    /// The serial contains a character outside lowercase hexadecimal.
    #[error("invalid certificate serial character: {0}")]
    InvalidCharacter(char),
    /// The serial does not contain whole octets.
    #[error("certificate serial has odd length")]
    OddLength,
    /// The serial has a redundant leading zero byte.
    #[error("certificate serial has a leading zero byte")]
    LeadingZero,
}

/// Serial number of a certificate, as canonical lowercase hexadecimal octets.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CertSerial(Cow<'static, str>);

impl_validated_text!(CertSerial);
impl_validated_parse!(CertSerial, CertSerialError);
impl_validated_serde!(CertSerial, CertSerialError);

impl Borrow<str> for CertSerial {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl CertSerial {
    /// Validates canonical lowercase hexadecimal serial text.
    ///
    /// # Errors
    ///
    /// Returns an error for empty text, invalid characters, partial octets, or leading zero bytes.
    pub fn try_new(value: impl Into<String>) -> Result<Self, CertSerialError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CertSerialError::Empty);
        }
        if let Some(character) = value
            .chars()
            .find(|&character| !is_cert_serial_char(character))
        {
            return Err(CertSerialError::InvalidCharacter(character));
        }
        if value.len() % 2 != 0 {
            return Err(CertSerialError::OddLength);
        }
        if value.len() > 2 && value.starts_with("00") {
            return Err(CertSerialError::LeadingZero);
        }
        Ok(Self(Cow::Owned(value)))
    }

    /// Returns the serial as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Builds a serial from the big-endian bytes of a certificate serial number.
    ///
    /// Leading zero bytes are dropped, keeping at least one byte, so zero is `00`. The text is
    /// lowercase hexadecimal.
    ///
    /// # Errors
    ///
    /// Returns [`CertSerialError::Empty`] when `bytes` is empty.
    pub fn from_be_bytes(bytes: &[u8]) -> Result<Self, CertSerialError> {
        let Some(last) = bytes.len().checked_sub(1) else {
            return Err(CertSerialError::Empty);
        };
        let start = bytes.iter().position(|&byte| byte != 0).unwrap_or(last);
        let significant = &bytes[start..];
        Self::try_new(hex::encode(significant))
    }
}

define_name! {
    /// Identity that a credential or certificate is issued to, such as `dev@example.com`
    /// or `repo:org/name:ref:refs/heads/main`.
    ///
    /// Any non-control character is allowed.
    Principal, "principal", is_free_text_char
}

impl Principal {
    /// The principal that stands for the unauthenticated caller.
    pub const ANONYMOUS: Self = Self(Cow::Borrowed("anonymous"));
}

/// Separator between a worker name and the UUID that `NativeLink` appends to it.
const WORKER_NAME_SEPARATOR: char = '-';

/// Length of a hyphenated UUID, such as `0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b`.
const UUID_LENGTH: usize = 36;

/// Returns whether `text` has the shape of a hyphenated UUID.
fn is_hyphenated_uuid(text: &str) -> bool {
    text.len() == UUID_LENGTH
        && text.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

/// Returns the instance-id slice of a worker id named `{instance}-{uuid}`, without allocating.
///
/// Returns `None` when the id has no trailing UUID, when the separator is missing before it,
/// or when the text before the separator is empty or not valid instance-id text.
fn instance_of(worker: &str) -> Option<&str> {
    let uuid_start = worker.len().checked_sub(UUID_LENGTH)?;
    let (head, uuid) = worker.split_at_checked(uuid_start)?;
    if !is_hyphenated_uuid(uuid) {
        return None;
    }
    let instance = head.strip_suffix(WORKER_NAME_SEPARATOR)?;
    if instance.is_empty() || !instance.chars().all(is_identifier_char) {
        return None;
    }
    Some(instance)
}

impl InstanceId {
    /// Returns the `NativeLink` worker `name` that Gang gives every worker on this instance.
    ///
    /// `NativeLink` appends a UUID to the name, so a worker id is this name followed by a UUID.
    #[must_use]
    pub fn worker_name(&self) -> String {
        format!("{self}{WORKER_NAME_SEPARATOR}")
    }

    /// Returns whether `worker` is hosted on this instance.
    ///
    /// This is the Gang worker-naming rule: the instance recovered from the worker id is this
    /// instance. Does not allocate.
    #[must_use]
    pub fn hosts(&self, worker: &WorkerId) -> bool {
        instance_of(worker.as_str()) == Some(self.as_str())
    }
}

impl WorkerId {
    /// Returns the instance-id text that hosts this worker, borrowed from the id.
    ///
    /// Does not allocate. Returns `None` when the id does not name an instance: it must end in
    /// the separator and a hyphenated UUID, and the text before them must be valid instance-id
    /// text.
    #[must_use]
    pub fn instance_name(&self) -> Option<&str> {
        instance_of(self.as_str())
    }

    /// Returns the instance that hosts this worker.
    ///
    /// The id must end in the separator and a hyphenated UUID; the text before them must be a
    /// valid [`InstanceId`]. Returns `None` otherwise.
    #[must_use]
    pub fn instance(&self) -> Option<InstanceId> {
        self.instance_name()
            .and_then(|name| InstanceId::try_new(name).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn provider_name_accepts_slugs_and_rejects_bad_input() {
        assert!(ProviderName::try_new("github-actions").is_ok());
        assert_eq!(
            ProviderName::try_new(""),
            Err(NameError::Empty {
                kind: "provider name"
            })
        );
        assert_eq!(
            ProviderName::try_new("gh/actions"),
            Err(NameError::InvalidCharacter {
                kind: "provider name",
                character: '/',
            })
        );
    }

    #[test]
    fn role_name_accepts_slugs_and_rejects_bad_input() {
        assert!(RoleName::try_new("administrator").is_ok());
        assert_eq!(
            RoleName::try_new(""),
            Err(NameError::Empty { kind: "role name" })
        );
        assert_eq!(
            RoleName::try_new("Admin"),
            Err(NameError::InvalidCharacter {
                kind: "role name",
                character: 'A',
            })
        );
    }

    #[test]
    fn scheduler_name_accepts_identifiers_and_rejects_bad_input() {
        assert!(SchedulerName::try_new("a").is_ok());
        assert!(SchedulerName::try_new("Sched_1-x").is_ok());
        assert!(SchedulerName::try_new("-edge_").is_ok());
        assert_eq!(
            SchedulerName::try_new(""),
            Err(NameError::Empty {
                kind: "scheduler name"
            })
        );
        assert_eq!(
            SchedulerName::try_new("a b"),
            Err(NameError::InvalidCharacter {
                kind: "scheduler name",
                character: ' ',
            })
        );
        assert_eq!(
            SchedulerName::try_new("a.b"),
            Err(NameError::InvalidCharacter {
                kind: "scheduler name",
                character: '.',
            })
        );
        assert!(SchedulerName::try_new("a".repeat(64)).is_ok());
        assert_eq!(
            SchedulerName::try_new("a".repeat(65)),
            Err(NameError::TooLong {
                kind: "scheduler name",
                max: 64,
            })
        );
    }

    #[test]
    fn audience_accepts_audiences_and_rejects_bad_input() {
        assert!(Audience::try_new("sts.amazonaws.com").is_ok());
        assert_eq!(
            Audience::try_new(""),
            Err(NameError::Empty { kind: "audience" })
        );
        assert_eq!(
            Audience::try_new("sts\namazonaws.com"),
            Err(NameError::InvalidCharacter {
                kind: "audience",
                character: '\n',
            })
        );
        assert!(Audience::try_new("a".repeat(1000)).is_ok());
        assert_eq!(
            Audience::try_new("a".repeat(1001)),
            Err(NameError::TooLong {
                kind: "audience",
                max: 1000,
            })
        );
    }

    #[test]
    fn audience_limit_counts_characters_not_bytes() {
        assert!(Audience::try_new("é".repeat(1000)).is_ok());
        assert!(matches!(
            Audience::try_new("é".repeat(1001)),
            Err(NameError::TooLong { .. })
        ));
    }

    #[test]
    fn audience_default_matches_parsed_gang() {
        assert_eq!(Audience::try_new("gang"), Ok(Audience::DEFAULT));
    }

    #[test]
    fn trust_domain_accepts_domains_and_rejects_bad_input() {
        assert!(TrustDomain::try_new("gang.internal").is_ok());
        assert_eq!(
            TrustDomain::try_new(""),
            Err(NameError::Empty {
                kind: "trust domain"
            })
        );
        assert_eq!(
            TrustDomain::try_new("gang/internal"),
            Err(NameError::InvalidCharacter {
                kind: "trust domain",
                character: '/',
            })
        );
    }

    #[test]
    fn instance_id_accepts_cloud_ids_and_rejects_bad_input() {
        assert!(InstanceId::try_new("i-0abc123").is_ok());
        assert!(InstanceId::try_new("Host.A_1").is_ok());
        assert_eq!(
            InstanceId::try_new(""),
            Err(NameError::Empty {
                kind: "instance id"
            })
        );
        assert_eq!(
            InstanceId::try_new("i/0abc"),
            Err(NameError::InvalidCharacter {
                kind: "instance id",
                character: '/',
            })
        );
    }

    #[test]
    fn worker_id_accepts_name_and_uuid_and_rejects_bad_input() {
        assert!(WorkerId::try_new("i-0abc123-6c5e4f3a-9b1d-4e2a-8f7c-0d3b2a1e9f58").is_ok());
        assert_eq!(
            WorkerId::try_new(""),
            Err(NameError::Empty { kind: "worker id" })
        );
        assert_eq!(
            WorkerId::try_new("i-0abc:123"),
            Err(NameError::InvalidCharacter {
                kind: "worker id",
                character: ':',
            })
        );
    }

    #[test]
    fn cert_serial_accepts_only_canonical_hex() {
        for text in ["00", "3fa9", "0f"] {
            assert!(CertSerial::try_new(text).is_ok());
        }
        assert_eq!(
            CertSerial::try_new("003fa9"),
            Err(CertSerialError::LeadingZero)
        );
        assert_eq!(CertSerial::try_new("f"), Err(CertSerialError::OddLength));
        assert_eq!(CertSerial::try_new("0"), Err(CertSerialError::OddLength));
        assert_eq!(
            CertSerial::try_new("0F"),
            Err(CertSerialError::InvalidCharacter('F'))
        );
        assert_eq!(CertSerial::try_new(""), Err(CertSerialError::Empty));
    }

    #[test]
    fn cert_serial_bytes_produce_canonical_roundtrippable_text() {
        for bytes in [
            &[0][..],
            &[0, 0],
            &[0, 15],
            &[0, 0xff],
            &[0x3f, 0xa9],
            &[0x10, 0, 1],
        ] {
            let serial = CertSerial::from_be_bytes(bytes).expect("the byte array is nonempty");
            assert_eq!(CertSerial::try_new(serial.as_str()), Ok(serial.clone()));
            let decoded = hex::decode(serial.as_str()).expect("serial is hexadecimal");
            assert_eq!(CertSerial::from_be_bytes(&decoded), Ok(serial));
        }
    }

    #[test]
    fn cert_serial_from_be_bytes_strips_leading_zeros() {
        let serial = CertSerial::from_be_bytes(&[0x00, 0x00, 0x0a, 0xff])
            .expect("the serial bytes are nonempty");
        assert_eq!(serial.as_str(), "0aff");
    }

    #[test]
    fn cert_serial_from_be_bytes_keeps_zero_as_one_byte() {
        assert_eq!(
            CertSerial::from_be_bytes(&[0x00])
                .expect("the serial bytes are nonempty")
                .as_str(),
            "00"
        );
        assert_eq!(
            CertSerial::from_be_bytes(&[0x00, 0x00])
                .expect("the serial bytes are nonempty")
                .as_str(),
            "00"
        );
    }

    #[test]
    fn cert_serial_from_be_bytes_keeps_interior_zeros() {
        let serial =
            CertSerial::from_be_bytes(&[0x10, 0x00, 0x01]).expect("the serial bytes are nonempty");
        assert_eq!(serial.as_str(), "100001");
    }

    #[test]
    fn cert_serial_from_be_bytes_rejects_empty() {
        assert_eq!(CertSerial::from_be_bytes(&[]), Err(CertSerialError::Empty));
    }

    #[test]
    fn principal_accepts_free_text_and_rejects_bad_input() {
        assert!(Principal::try_new("dev@example.com").is_ok());
        assert!(Principal::try_new("repo:org/name:ref:refs/heads/main").is_ok());
        assert_eq!(
            Principal::try_new(""),
            Err(NameError::Empty { kind: "principal" })
        );
        assert_eq!(
            Principal::try_new("dev\n@example.com"),
            Err(NameError::InvalidCharacter {
                kind: "principal",
                character: '\n',
            })
        );
    }

    #[test]
    fn principal_rejects_control_characters() {
        assert_eq!(
            Principal::try_new("a\tb"),
            Err(NameError::InvalidCharacter {
                kind: "principal",
                character: '\t',
            })
        );
    }

    const WORKER_UUID: &str = "0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b";

    fn worker(name: &str) -> WorkerId {
        WorkerId::try_new(format!("{name}-{WORKER_UUID}")).unwrap()
    }

    #[test]
    fn instance_hosts_only_workers_named_after_it() {
        let instance = InstanceId::try_new("local-1").unwrap();
        assert!(instance.hosts(&worker("local-1")));
        assert!(!instance.hosts(&worker("local-10")));
        let sibling = InstanceId::try_new("local-10").unwrap();
        assert!(sibling.hosts(&worker("local-10")));
    }

    #[test]
    fn worker_without_uuid_has_no_instance() {
        let candidate = WorkerId::try_new("local-1").unwrap();
        assert_eq!(candidate.instance(), None);
    }

    #[test]
    fn worker_with_non_hex_uuid_has_no_instance() {
        let candidate = WorkerId::try_new("local-1-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6g").unwrap();
        assert_eq!(candidate.instance(), None);
    }

    #[test]
    fn worker_uuid_requires_separator_before_it() {
        let candidate = WorkerId::try_new("local-10194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b").unwrap();
        assert_eq!(candidate.instance(), None);
    }

    #[test]
    fn instance_name_is_the_instance_text_of_a_worker_id() {
        assert_eq!(worker("local-1").instance_name(), Some("local-1"));
        assert_eq!(worker("i-0a.b_c-1").instance_name(), Some("i-0a.b_c-1"));
    }

    #[test]
    fn instance_name_is_none_for_ids_that_do_not_name_an_instance() {
        let no_uuid = WorkerId::try_new("local-1").unwrap();
        let bad_hex = WorkerId::try_new("local-1-0194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6g").unwrap();
        let no_separator =
            WorkerId::try_new("local-10194d5a2-7b3c-6f10-8a4e-1c2d3e4f5a6b").unwrap();
        let empty_instance = WorkerId::try_new(format!("-{WORKER_UUID}")).unwrap();
        for candidate in [&no_uuid, &bad_hex, &no_separator, &empty_instance] {
            assert_eq!(candidate.instance_name(), None);
        }
    }

    #[test]
    fn instance_name_allocates_nothing() {
        const LOOKUPS: usize = 100;
        let candidate = worker("local-1");
        let counts = allocation_counter::measure(|| {
            for _ in 0..LOOKUPS {
                std::hint::black_box(candidate.instance_name());
            }
        });
        assert_eq!(counts.count_total, 0, "instance_name allocated");
    }

    #[test]
    fn worker_name_round_trips_through_worker_id_for_dotted_instances() {
        let instance = InstanceId::try_new("i-0a.b_c-1").unwrap();
        let id = format!("{}{WORKER_UUID}", instance.worker_name());
        let worker = WorkerId::try_new(id).unwrap();
        assert_eq!(worker.instance(), Some(instance));
    }

    #[test]
    fn btree_map_lookup_by_str() {
        let mut providers = BTreeMap::new();
        providers.insert(ProviderName::try_new("aws-workers").unwrap(), 3_u32);
        assert_eq!(providers.get("aws-workers"), Some(&3));
        assert_eq!(providers.get("gcp-workers"), None);
    }

    #[test]
    fn deserialize_rejects_invalid_names() {
        assert!(serde_json::from_str::<ProviderName>("\"gh/actions\"").is_err());
        assert!(serde_json::from_str::<ProviderName>("\"\"").is_err());
        assert_eq!(
            serde_json::from_str::<ProviderName>("\"github-actions\"")
                .unwrap()
                .as_str(),
            "github-actions"
        );
    }

    #[test]
    fn serialize_as_plain_string() {
        let name = TrustDomain::try_new("gang.internal").unwrap();
        assert_eq!(serde_json::to_string(&name).unwrap(), "\"gang.internal\"");
    }

    #[test]
    fn fleet_name_accepts_up_to_40_characters_and_rejects_bad_input() {
        assert!(FleetName::try_new("gang-worker-x86-64").is_ok());
        assert!(FleetName::try_new("a".repeat(40)).is_ok());
        assert_eq!(
            FleetName::try_new("a".repeat(41)),
            Err(NameError::TooLong {
                kind: "fleet name",
                max: 40,
            })
        );
        assert_eq!(
            FleetName::try_new(""),
            Err(NameError::Empty { kind: "fleet name" })
        );
        assert_eq!(
            FleetName::try_new("Gang"),
            Err(NameError::InvalidCharacter {
                kind: "fleet name",
                character: 'G',
            })
        );
        assert_eq!(
            FleetName::try_new("gang_worker"),
            Err(NameError::InvalidCharacter {
                kind: "fleet name",
                character: '_',
            })
        );
    }

    #[test]
    fn fleet_name_rejects_hyphen_at_either_edge() {
        assert_eq!(
            FleetName::try_new("-workers"),
            Err(NameError::HyphenAtEdge { kind: "fleet name" })
        );
        assert_eq!(
            FleetName::try_new("workers-"),
            Err(NameError::HyphenAtEdge { kind: "fleet name" })
        );
        assert_eq!(
            FleetName::try_new("-"),
            Err(NameError::HyphenAtEdge { kind: "fleet name" })
        );
        assert!(FleetName::try_new("gang-worker-x86-64").is_ok());
        assert!(FleetName::try_new("a").is_ok());
    }

    #[test]
    fn machine_class_accepts_up_to_40_characters_and_rejects_bad_input() {
        assert!(MachineClass::try_new("gpu").is_ok());
        assert!(MachineClass::try_new("a".repeat(40)).is_ok());
        assert_eq!(
            MachineClass::try_new("a".repeat(41)),
            Err(NameError::TooLong {
                kind: "machine class",
                max: 40,
            })
        );
        assert_eq!(
            MachineClass::try_new(""),
            Err(NameError::Empty {
                kind: "machine class"
            })
        );
        assert_eq!(
            MachineClass::try_new("GPU"),
            Err(NameError::InvalidCharacter {
                kind: "machine class",
                character: 'G',
            })
        );
    }

    #[test]
    fn machine_class_rejects_hyphen_at_either_edge() {
        assert_eq!(
            MachineClass::try_new("-gpu"),
            Err(NameError::HyphenAtEdge {
                kind: "machine class"
            })
        );
        assert_eq!(
            MachineClass::try_new("gpu-"),
            Err(NameError::HyphenAtEdge {
                kind: "machine class"
            })
        );
        assert!(MachineClass::try_new("gpu-large").is_ok());
    }

    #[test]
    fn only_the_reserved_provider_name_is_anonymous() {
        let anonymous = ProviderName::ANONYMOUS;
        assert!(anonymous.is_anonymous());
        let named = ProviderName::try_new("aws-workers").expect("test provider name is valid");
        assert!(!named.is_anonymous());
    }

    #[test]
    fn provider_anonymous_constant_is_its_text() {
        assert_eq!(ProviderName::try_new("none"), Ok(ProviderName::ANONYMOUS));
        assert_eq!(ProviderName::ANONYMOUS.as_str(), "none");
    }

    #[test]
    fn principal_and_role_constants_are_their_text() {
        assert_eq!(Principal::try_new("anonymous"), Ok(Principal::ANONYMOUS));
        assert_eq!(
            RoleName::try_new("administrator"),
            Ok(RoleName::ADMINISTRATOR)
        );
    }

    #[test]
    fn constants_serialize_to_their_text() {
        assert_eq!(
            serde_json::to_string(&ProviderName::ANONYMOUS).unwrap(),
            "\"none\""
        );
        assert_eq!(
            serde_json::to_string(&Principal::ANONYMOUS).unwrap(),
            "\"anonymous\""
        );
        assert_eq!(
            serde_json::to_string(&RoleName::ADMINISTRATOR).unwrap(),
            "\"administrator\""
        );
    }

    #[test]
    fn name_built_from_string_equals_and_hashes_like_constant() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_of<T: Hash>(value: &T) -> u64 {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        }

        let built = ProviderName::try_new(String::from("none")).unwrap();
        assert_eq!(built, ProviderName::ANONYMOUS);
        assert_eq!(hash_of(&built), hash_of(&ProviderName::ANONYMOUS));
        assert!(built.is_anonymous());
    }
}
