//! Content-addressed SHA-256 blob and action digests.

use std::fmt;
use std::str::FromStr;

use sha2::{Digest as _, Sha256};

/// Number of bytes in a SHA-256 hash.
const HASH_LEN: usize = 32;

/// Number of hexadecimal characters in a formatted SHA-256 hash.
const HEX_LEN: usize = HASH_LEN * 2;

/// SHA-256 of the empty byte slice:
/// `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
const EMPTY_HASH: [u8; HASH_LEN] = [
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
    0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
];

/// Errors from parsing a [`Digest`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DigestError {
    /// The text has no `/` separating the hash and byte size.
    #[error("missing '/' in digest, expected <hex>/<size>")]
    MissingSeparator,
    /// The hash does not have 64 hexadecimal characters.
    #[error("digest hash must have 64 hex characters, got {found}")]
    HashLength {
        /// The number of characters found in the hash part.
        found: usize,
    },
    /// A character in the hash is not an ASCII hexadecimal digit.
    #[error("invalid character '{character}' in digest hash, expected a hex digit")]
    InvalidHashCharacter {
        /// The first disallowed character.
        character: char,
    },
    /// The byte size is empty, contains non-digits, or overflows `u64`.
    #[error("invalid byte size in digest: `{input}`")]
    InvalidSize {
        /// The rejected size text.
        input: String,
    },
}

/// A SHA-256 content digest paired with the object's byte length.
///
/// Formats as `<lowercase-hex>/<size>` and parses both lowercase and uppercase hexadecimal hashes
/// in `<hex>/<size>` form.
///
/// ```
/// use gang_types::Digest;
///
/// let digest = Digest::from_bytes(b"");
/// assert_eq!(digest, Digest::EMPTY);
/// assert!(digest.is_empty());
/// assert_eq!(
///     digest.to_string(),
///     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0"
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest {
    hash: [u8; HASH_LEN],
    size: u64,
}

impl Digest {
    /// The SHA-256 digest of zero bytes (`e3b0c442...b855/0`).
    pub const EMPTY: Self = Self {
        hash: EMPTY_HASH,
        size: 0,
    };

    /// Wraps a raw 32-byte SHA-256 hash and byte size.
    #[must_use]
    pub const fn new(hash: [u8; HASH_LEN], size: u64) -> Self {
        Self { hash, size }
    }

    /// Computes the SHA-256 [`Digest`] of `bytes`.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let hash: [u8; HASH_LEN] = Sha256::digest(bytes).into();
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        Self { hash, size }
    }

    /// Parses a 64-character hexadecimal SHA-256 hash and byte size into a [`Digest`].
    ///
    /// Both lowercase and uppercase hexadecimal digits are accepted; [`Digest::hex`] and
    /// [`fmt::Display`] always emit lowercase hexadecimal.
    ///
    /// # Errors
    ///
    /// Returns [`DigestError::HashLength`] if `hex_hash` is not 64 characters long, or
    /// [`DigestError::InvalidHashCharacter`] if any character is not an ASCII hexadecimal digit.
    pub fn from_hex_and_size(hex_hash: &str, size: u64) -> Result<Self, DigestError> {
        let found = hex_hash.chars().count();
        if found != HEX_LEN {
            return Err(DigestError::HashLength { found });
        }
        if let Some(character) = hex_hash.chars().find(|c| !c.is_ascii_hexdigit()) {
            return Err(DigestError::InvalidHashCharacter { character });
        }
        let mut hash = [0_u8; HASH_LEN];
        match hex::decode_to_slice(hex_hash, &mut hash) {
            Ok(()) => Ok(Self { hash, size }),
            Err(hex::FromHexError::InvalidHexCharacter { c, .. }) => {
                Err(DigestError::InvalidHashCharacter { character: c })
            }
            Err(_) => Err(DigestError::HashLength { found }),
        }
    }

    /// Returns whether this digest equals [`Digest::EMPTY`].
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.size == 0 && bytes_equal(&self.hash, &EMPTY_HASH)
    }

    /// Returns the raw 32-byte SHA-256 hash.
    #[must_use]
    pub const fn hash(&self) -> &[u8; HASH_LEN] {
        &self.hash
    }

    /// Returns the 64-character lowercase hexadecimal representation of the hash.
    #[must_use]
    pub fn hex(&self) -> String {
        hex::encode(self.hash)
    }

    /// Returns the byte length of the digested content.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
}

const fn bytes_equal(a: &[u8; HASH_LEN], b: &[u8; HASH_LEN]) -> bool {
    let mut index = 0;
    while index < HASH_LEN {
        if a[index] != b[index] {
            return false;
        }
        index += 1;
    }
    true
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut hex_buf = [0_u8; HEX_LEN];
        hex::encode_to_slice(self.hash, &mut hex_buf).map_err(|_| fmt::Error)?;
        let hex_str = std::str::from_utf8(&hex_buf).map_err(|_| fmt::Error)?;
        write!(formatter, "{hex_str}/{}", self.size)
    }
}

impl FromStr for Digest {
    type Err = DigestError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (hex_hash, size_str) = text.split_once('/').ok_or(DigestError::MissingSeparator)?;
        if size_str.is_empty() || !size_str.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(DigestError::InvalidSize {
                input: size_str.to_owned(),
            });
        }
        let size = size_str
            .parse::<u64>()
            .map_err(|_| DigestError::InvalidSize {
                input: size_str.to_owned(),
            })?;
        Self::from_hex_and_size(hex_hash, size)
    }
}

impl_parsed_serde!(Digest, "a SHA-256 digest in <hex>/<size> format");

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_TEXT: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0";
    const ABC_HEX: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn size_is_forty_bytes() {
        assert_eq!(size_of::<Digest>(), 40);
    }

    #[test]
    fn empty_digest_matches_empty_bytes() {
        let empty = Digest::from_bytes(b"");
        assert_eq!(empty, Digest::EMPTY);
        assert!(empty.is_empty());
        assert_eq!(empty.size(), 0);
        assert_eq!(empty.to_string(), EMPTY_TEXT);
        assert_eq!(EMPTY_TEXT.parse::<Digest>(), Ok(Digest::EMPTY));
    }

    #[test]
    fn zero_size_with_non_empty_hash_is_not_empty() {
        let bogus = Digest::new([0_u8; 32], 0);
        assert!(!bogus.is_empty());
        assert_ne!(bogus, Digest::EMPTY);
    }

    #[test]
    fn from_bytes_and_accessors_match_sha256() {
        let digest = Digest::from_bytes(b"abc");
        assert!(!digest.is_empty());
        assert_eq!(digest.size(), 3);
        assert_eq!(digest.hex(), ABC_HEX);
        assert_eq!(digest.to_string(), format!("{ABC_HEX}/3"));
        assert_eq!(digest.hash()[0], 0xba);
        assert_eq!(digest.hash()[31], 0xad);
    }

    #[test]
    fn from_str_accepts_uppercase_and_prints_lowercase() {
        let upper = format!("{}/42", ABC_HEX.to_ascii_uppercase());
        let parsed: Digest = upper.parse().expect("uppercase hex parses");
        assert_eq!(parsed.hex(), ABC_HEX);
        assert_eq!(parsed.size(), 42);
        assert_eq!(parsed.to_string(), format!("{ABC_HEX}/42"));
    }

    #[test]
    fn rejects_malformed_text() {
        assert_eq!(
            ABC_HEX.parse::<Digest>(),
            Err(DigestError::MissingSeparator)
        );
        assert_eq!(
            format!("{}/0", &ABC_HEX[..63]).parse::<Digest>(),
            Err(DigestError::HashLength { found: 63 })
        );
        assert_eq!(
            format!("{ABC_HEX}0/0").parse::<Digest>(),
            Err(DigestError::HashLength { found: 65 })
        );
        assert_eq!(
            format!("g{}/0", &ABC_HEX[1..]).parse::<Digest>(),
            Err(DigestError::InvalidHashCharacter { character: 'g' })
        );
        for bad_size in ["", "-1", "+1", "1a", "18446744073709551616"] {
            assert_eq!(
                format!("{ABC_HEX}/{bad_size}").parse::<Digest>(),
                Err(DigestError::InvalidSize {
                    input: bad_size.to_owned(),
                }),
                "{bad_size}"
            );
        }
    }

    #[test]
    fn ordering_and_serde_round_trip() {
        let small = Digest::from_bytes(b"a");
        let large = Digest::from_bytes(b"b");
        assert_ne!(small, large);
        assert_eq!(small.cmp(&large), small.hash().cmp(large.hash()));

        let json = serde_json::to_string(&small).expect("serialize digest");
        assert_eq!(json, format!("\"{small}\""));
        assert_eq!(
            serde_json::from_str::<Digest>(&json).expect("deserialize digest"),
            small
        );
        assert!(serde_json::from_str::<Digest>("\"not-a-digest\"").is_err());
    }

    #[test]
    fn display_allocates_nothing_beyond_string_formatting() {
        use std::fmt::Write as _;

        let digest = Digest::from_bytes(b"abc");
        let mut buf = String::with_capacity(128);
        let counts = allocation_counter::measure(|| {
            for _ in 0..50 {
                buf.clear();
                let _ = write!(buf, "{digest}");
                std::hint::black_box(&buf);
            }
        });
        assert_eq!(counts.count_total, 0, "Display allocated");
    }
}
