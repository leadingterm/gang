//! Hex digests: git object ids and the SHA-256 of a saved plan.

use sha2::{Digest, Sha256};

use crate::errors::{Expected, Result};
use crate::newtype::{lower_hex, text_newtype};

/// Length of a git object id in hex characters.
const SHA_LENGTH: usize = 40;
/// Length of a SHA-256 digest in hex characters.
const SHA256_LENGTH: usize = 64;

/// Accepts a git object id.
fn check_sha(text: &str) -> Result<()> {
    lower_hex(text, SHA_LENGTH, Expected::Sha)
}

/// Accepts a SHA-256 digest.
fn check_sha256(text: &str) -> Result<()> {
    lower_hex(text, SHA256_LENGTH, Expected::PlanSha256)
}

text_newtype! {
    /// A git commit or tree id: 40 lowercase hex characters.
    Sha, check_sha
}

text_newtype! {
    /// The SHA-256 of a saved plan file: 64 lowercase hex characters.
    PlanSha256, check_sha256
}

impl PlanSha256 {
    /// The digest of the plan file `bytes`.
    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        Self(hex::encode(digest))
    }
}

#[cfg(test)]
mod tests {
    use super::{PlanSha256, Sha};

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn sha_accepts_a_40_character_lowercase_hex_sha() {
        assert_eq!(SHA.parse::<Sha>().unwrap().as_str(), SHA);
    }

    #[test]
    fn sha_rejects_uppercase_short_long_and_non_hex_text() {
        let upper = SHA.to_uppercase();
        let short = &SHA[1..];
        let long = format!("{SHA}0");
        let non_hex = format!("{short}g");
        for text in [upper.as_str(), short, long.as_str(), non_hex.as_str()] {
            let error = text.parse::<Sha>().unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("`{text}` is not a 40-character lowercase hex sha")
            );
        }
    }

    #[test]
    fn plan_sha256_digest_is_the_lowercase_hex_sha256_of_the_bytes() {
        assert_eq!(
            PlanSha256::digest(b"abc").as_str(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
