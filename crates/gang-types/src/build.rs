//! Build keys and their stable identifiers.

use std::fmt::{self, Write};
use std::str::FromStr;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::{BuildIdError, Identity, InvocationId};

const ID_BYTES: usize = 10;
const ID_CHARS: usize = 16;
const BITS_PER_CHAR: usize = 5;
const ID_BITS: usize = ID_CHARS * BITS_PER_CHAR;
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// One build: a Bazel command, keyed by its authenticated principal and client-asserted invocation id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BuildKey {
    /// The authenticated principal.
    pub principal: Arc<Identity>,
    /// The client-asserted invocation id.
    pub invocation: InvocationId,
}

impl BuildKey {
    /// The build's id, derived from the key.
    #[must_use]
    pub fn id(&self) -> BuildId {
        let mut hash = Sha256::new();
        hash.update(self.principal.provider.as_str().as_bytes());
        hash.update(b":");
        hash.update(self.principal.principal.as_str().as_bytes());
        hash.update([0]);
        hash.update(self.invocation.as_str().as_bytes());
        let digest = hash.finalize();
        let mut bytes = [0; ID_BYTES];
        bytes.copy_from_slice(&digest[..ID_BYTES]);
        BuildId(bytes)
    }
}

/// The first 80 bits of the build key's SHA-256 hash, in lowercase Crockford base32.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BuildId([u8; ID_BYTES]);

impl fmt::Display for BuildId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = [0u8; size_of::<u128>()];
        bytes[size_of::<u128>() - ID_BYTES..].copy_from_slice(&self.0);
        let value = u128::from_be_bytes(bytes);
        for index in 0..ID_CHARS {
            let shift = ID_BITS - BITS_PER_CHAR * (index + 1);
            let digit = usize::try_from((value >> shift) & 31).map_err(|_| fmt::Error)?;
            f.write_char(char::from(ALPHABET[digit]))?;
        }
        Ok(())
    }
}

impl FromStr for BuildId {
    type Err = BuildIdError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != ID_CHARS {
            return Err(BuildIdError);
        }
        let mut value = 0u128;
        for character in text.bytes() {
            let digit = ALPHABET
                .iter()
                .position(|&candidate| candidate == character)
                .ok_or(BuildIdError)?;
            value = (value << BITS_PER_CHAR) | u128::try_from(digit).map_err(|_| BuildIdError)?;
        }
        let encoded = value.to_be_bytes();
        let mut bytes = [0u8; ID_BYTES];
        bytes.copy_from_slice(&encoded[size_of::<u128>() - ID_BYTES..]);
        Ok(Self(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(principal: &str, invocation: &str) -> BuildKey {
        BuildKey {
            principal: Arc::new(principal.parse().expect("valid identity")),
            invocation: InvocationId::try_new(invocation).expect("valid invocation"),
        }
    }

    #[test]
    fn build_id_has_pinned_canonical_encoding() {
        let key = key("example:alice", "invocation-1");
        let id = key.id();
        let text = id.to_string();
        assert_eq!(text, "12rsfjsg79g6h7f3");
        assert_eq!(text.len(), ID_CHARS);
        assert!(text.bytes().all(|byte| ALPHABET.contains(&byte)));
        assert_eq!(text.parse::<BuildId>(), Ok(id));
        assert_eq!(key.id(), id);
    }

    #[test]
    fn build_id_separates_principals_and_key_components() {
        assert_ne!(key("example:alice", "x").id(), key("example:bob", "x").id());
        assert_ne!(key("example:ab", "c").id(), key("example:a", "bc").id());
    }

    #[test]
    fn build_id_rejects_noncanonical_text() {
        for text in [
            "0123456789abcde",
            "0123456789abcdef0",
            "0123456789abcdeA",
            "0123456789abcdei",
            "0123456789abcdel",
            "0123456789abcdeo",
            "0123456789abcdeu",
        ] {
            assert_eq!(text.parse::<BuildId>(), Err(BuildIdError));
        }
    }
}
