//! The SHA-256 pin of a certificate authority's public key.

use std::fmt;
use std::str::FromStr;

use sha2::{Digest, Sha256};

use crate::CaPinError;

/// Number of bytes in a SHA-256 digest.
const DIGEST_LEN: usize = 32;

/// Number of hex characters that encode a digest.
const HEX_LEN: usize = DIGEST_LEN * 2;

/// The prefix that marks the text form of a `CaPin`.
const PREFIX: &str = "sha256:";

/// The SHA-256 of a certificate authority's `SubjectPublicKeyInfo`.
///
/// Parsed from `sha256:` followed by 64 hex characters (either case) and printed as the same
/// prefix with lowercase hex. Any other prefix is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CaPin([u8; DIGEST_LEN]);

impl CaPin {
    /// Wraps a SHA-256 digest of a certificate authority's `SubjectPublicKeyInfo`.
    #[must_use]
    pub const fn new(digest: [u8; DIGEST_LEN]) -> Self {
        Self(digest)
    }

    /// The raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DIGEST_LEN] {
        &self.0
    }

    /// The pin of a certificate authority's `SubjectPublicKeyInfo` DER bytes: the SHA-256 of
    /// `spki_der`.
    #[must_use]
    pub fn of_spki(spki_der: &[u8]) -> Self {
        Self::new(Sha256::digest(spki_der).into())
    }

    /// The lowercase hex of the digest, without the `sha256:` prefix.
    #[must_use]
    pub fn digest_hex(&self) -> impl fmt::Display + '_ {
        DigestHex(&self.0)
    }
}

impl fmt::Display for CaPin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{PREFIX}{}", self.digest_hex())
    }
}

/// Writes a digest as lowercase hex, without allocating.
struct DigestHex<'a>(&'a [u8; DIGEST_LEN]);

impl fmt::Display for DigestHex<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut text = [0_u8; 2 * DIGEST_LEN];
        hex::encode_to_slice(self.0, &mut text).map_err(|_| fmt::Error)?;
        formatter.write_str(std::str::from_utf8(&text).map_err(|_| fmt::Error)?)
    }
}

impl FromStr for CaPin {
    type Err = CaPinError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = text.strip_prefix(PREFIX).ok_or(CaPinError::MissingPrefix)?;
        let found = digits.chars().count();
        if found != HEX_LEN {
            return Err(CaPinError::Length { found });
        }
        if let Some(character) = digits
            .chars()
            .find(|character| !character.is_ascii_hexdigit())
        {
            return Err(CaPinError::InvalidCharacter { character });
        }
        let mut digest = [0_u8; DIGEST_LEN];
        match hex::decode_to_slice(digits, &mut digest) {
            Ok(()) => Ok(Self(digest)),
            Err(hex::FromHexError::InvalidHexCharacter { c, .. }) => {
                Err(CaPinError::InvalidCharacter { character: c })
            }
            Err(_) => Err(CaPinError::Length { found }),
        }
    }
}

impl_parsed_serde!(CaPin, "a SHA-256 certificate authority pin");

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn json_deserializes_borrowed_and_owned_strings() {
        let expected = LOWER.parse::<CaPin>().expect("valid pin");
        let json = format!("\"{LOWER}\"");
        assert_eq!(
            serde_json::from_str::<CaPin>(&json).expect("valid pin"),
            expected
        );
        assert_eq!(
            serde_json::from_value::<CaPin>(serde_json::Value::String(LOWER.to_owned()))
                .expect("valid pin"),
            expected
        );
    }

    const LOWER: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const UPPER: &str = "sha256:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF";

    #[test]
    fn round_trips_through_its_text() -> Result<(), CaPinError> {
        let pin: CaPin = LOWER.parse()?;
        assert_eq!(pin.to_string(), LOWER);
        assert_eq!(pin.as_bytes()[0], 0x01);
        assert_eq!(pin.as_bytes()[31], 0xef);
        Ok(())
    }

    #[test]
    fn uppercase_hex_is_accepted_and_printed_lowercase() -> Result<(), CaPinError> {
        let pin: CaPin = UPPER.parse()?;
        assert_eq!(pin.to_string(), LOWER);
        Ok(())
    }

    #[test]
    fn rejects_a_missing_or_wrong_prefix() {
        let bare = &LOWER[7..];
        for text in [
            bare.to_owned(),
            format!("SHA256:{bare}"),
            format!("sha512:{bare}"),
        ] {
            assert_eq!(text.parse::<CaPin>(), Err(CaPinError::MissingPrefix));
        }
    }

    #[test]
    fn rejects_sixty_three_characters() {
        let short = &LOWER[..LOWER.len() - 1];
        assert_eq!(
            short.parse::<CaPin>(),
            Err(CaPinError::Length { found: 63 })
        );
    }

    #[test]
    fn rejects_sixty_five_hex_digits() {
        let long = format!("{LOWER}0");
        assert_eq!(long.parse::<CaPin>(), Err(CaPinError::Length { found: 65 }));
    }

    #[test]
    fn rejects_a_trailing_newline_from_a_file() {
        let text = format!("{LOWER}\n");
        assert_eq!(text.parse::<CaPin>(), Err(CaPinError::Length { found: 65 }));
    }

    #[test]
    fn rejects_a_non_hex_character() {
        let bad = format!("{}g{}", &LOWER[..17], &LOWER[18..]);
        assert_eq!(
            bad.parse::<CaPin>(),
            Err(CaPinError::InvalidCharacter { character: 'g' })
        );
    }

    #[test]
    fn toml_field_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Holder {
            ca_pin: CaPin,
        }
        let text = format!("ca_pin = \"{LOWER}\"\n");
        let holder: Holder = toml::from_str(&text)?;
        assert_eq!(holder.ca_pin.to_string(), LOWER);
        assert_eq!(toml::to_string(&holder)?, text);
        Ok(())
    }

    #[test]
    fn deserialize_rejects_a_short_value() {
        assert!(serde_json::from_str::<CaPin>("\"abcd\"").is_err());
    }

    #[test]
    fn of_spki_is_the_sha256_of_its_input() -> Result<(), CaPinError> {
        let expected: CaPin =
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".parse()?;
        assert_eq!(CaPin::of_spki(b"abc"), expected);
        Ok(())
    }

    #[test]
    fn digest_hex_is_64_lowercase_hex_without_prefix() {
        let hex = CaPin::of_spki(b"abc").digest_hex().to_string();
        assert_eq!(hex.len(), HEX_LEN);
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(!hex.contains(':'));
    }

    #[test]
    fn display_is_prefix_plus_digest_hex() {
        let pin = CaPin::of_spki(b"abc");
        assert_eq!(pin.to_string(), format!("sha256:{}", pin.digest_hex()));
    }
}
