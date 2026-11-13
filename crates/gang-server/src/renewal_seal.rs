//! Authenticated encryption of browser renewal cookies (ADR-39).

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use gang_clock::Timestamp;
use gang_types::{ClientSecret, ProviderName, RefreshToken};
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};

/// What the renewal cookie holds, sealed (ADR-39).
#[derive(Debug, Clone)]
pub(crate) struct Renewal {
    /// The sign-in provider.
    pub(crate) provider: ProviderName,
    /// The principal text the sign-in verified (`Principal::as_str`).
    pub(crate) principal: String,
    /// The provider's refresh token.
    pub(crate) refresh_token: RefreshToken,
    /// The absolute expiry of this renewal.
    pub(crate) expire_time: Timestamp,
}

impl PartialEq for Renewal {
    fn eq(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.principal == other.principal
            && self.refresh_token.matches(other.refresh_token.expose())
            && self.expire_time == other.expire_time
    }
}

impl Eq for Renewal {}

/// Why a renewal cookie could not be sealed or opened.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum SealError {
    /// The cookie is invalid or cannot be authenticated.
    #[error("the renewal cookie cannot be opened")]
    Unopenable,
    /// The encoded cookie exceeds the size limit.
    #[error("the sealed renewal is over {MAX_SEALED_BYTES} bytes")]
    TooLarge,
    /// Randomness was unavailable or the renewal could not be sealed.
    #[error("the renewal cannot be sealed")]
    Crypto,
}

/// Maximum encoded renewal cookie value length.
pub(crate) const MAX_SEALED_BYTES: usize = 4000;

/// JSON representation inside the authenticated ciphertext.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Sealed {
    /// Provider name.
    provider: String,
    /// Verified principal text.
    principal: String,
    /// Provider refresh token.
    refresh_token: String,
    /// Expiration in milliseconds since the Unix epoch.
    expire_time_ms: u64,
}

/// Encrypts and authenticates renewal cookies for one endpoint and client secret.
pub(crate) struct RenewalSeal {
    /// Key derived from the client secret and endpoint origin.
    key: [u8; 32],
    /// Associated data binding cookies to this endpoint.
    origin: String,
    /// Source of fresh nonces.
    rng: SystemRandom,
}

impl RenewalSeal {
    /// Derives the cookie encryption key for the given endpoint.
    ///
    /// Uses HKDF-SHA256 with the endpoint origin as salt, the client secret as input key
    /// material, and `gang-browser-renewal` as info. One SHA-256 block supplies the key.
    pub(crate) fn new(client_secret: &ClientSecret, origin: &str) -> Self {
        let prk = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, origin.as_bytes()),
            client_secret.expose().as_bytes(),
        );
        let okm = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, prk.as_ref()),
            b"gang-browser-renewal\x01",
        );
        let mut key = [0; 32];
        key.copy_from_slice(&okm.as_ref()[..32]);
        Self {
            key,
            origin: origin.to_owned(),
            rng: SystemRandom::new(),
        }
    }

    /// Creates a cipher from the derived key.
    fn cipher(&self) -> Option<LessSafeKey> {
        UnboundKey::new(&AES_256_GCM, &self.key)
            .ok()
            .map(LessSafeKey::new)
    }

    /// Encrypts a renewal with a fresh nonce and returns its cookie value.
    pub(crate) fn seal(&self, renewal: &Renewal) -> Result<String, SealError> {
        let value = Sealed {
            provider: renewal.provider.as_str().to_owned(),
            principal: renewal.principal.clone(),
            refresh_token: renewal.refresh_token.expose().to_owned(),
            expire_time_ms: renewal.expire_time.as_unix_millis(),
        };
        let mut plaintext = serde_json::to_vec(&value).map_err(|_| SealError::Crypto)?;
        let mut nonce = [0; NONCE_LEN];
        self.rng.fill(&mut nonce).map_err(|_| SealError::Crypto)?;
        self.cipher()
            .ok_or(SealError::Crypto)?
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(self.origin.as_bytes()),
                &mut plaintext,
            )
            .map_err(|_| SealError::Crypto)?;
        let mut bytes = Vec::with_capacity(NONCE_LEN + plaintext.len());
        bytes.extend_from_slice(&nonce);
        bytes.extend_from_slice(&plaintext);
        let encoded = URL_SAFE_NO_PAD.encode(bytes);
        if encoded.len() > MAX_SEALED_BYTES {
            return Err(SealError::TooLarge);
        }
        Ok(encoded)
    }

    /// Authenticates and opens a renewal cookie.
    pub(crate) fn open(&self, sealed: &str) -> Result<Renewal, SealError> {
        if sealed.len() > MAX_SEALED_BYTES {
            return Err(SealError::Unopenable);
        }
        let mut bytes = URL_SAFE_NO_PAD
            .decode(sealed)
            .map_err(|_| SealError::Unopenable)?;
        if bytes.len() < NONCE_LEN + AES_256_GCM.tag_len() {
            return Err(SealError::Unopenable);
        }
        let (nonce, ciphertext) = bytes.split_at_mut(NONCE_LEN);
        let nonce: [u8; NONCE_LEN] = nonce.try_into().map_err(|_| SealError::Unopenable)?;
        let cipher = self.cipher().ok_or(SealError::Unopenable)?;
        let plaintext = cipher
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(self.origin.as_bytes()),
                ciphertext,
            )
            .map_err(|_| SealError::Unopenable)?;
        let value: Sealed = serde_json::from_slice(plaintext).map_err(|_| SealError::Unopenable)?;
        Ok(Renewal {
            provider: ProviderName::try_new(value.provider).map_err(|_| SealError::Unopenable)?,
            principal: value.principal,
            refresh_token: RefreshToken::new(value.refresh_token),
            expire_time: Timestamp::from_unix_millis(value.expire_time_ms),
        })
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use gang_clock::Timestamp;
    use gang_types::{ClientSecret, ProviderName, RefreshToken};

    use super::{Renewal, RenewalSeal, SealError};

    /// A representative renewal.
    fn renewal() -> Renewal {
        Renewal {
            provider: ProviderName::try_new("provider").expect("valid provider"),
            principal: "person@example.org".to_owned(),
            refresh_token: RefreshToken::new("refresh-token"),
            expire_time: Timestamp::from_unix_millis(123_456),
        }
    }

    /// Builds a seal with the given secret and origin.
    fn seal(secret: &str, origin: &str) -> RenewalSeal {
        RenewalSeal::new(&ClientSecret::new(secret), origin)
    }

    /// A sealed renewal round-trips all its fields.
    #[test]
    fn a_sealed_renewal_opens_to_the_same_renewal() {
        let seal = seal("secret", "https://a.example");
        let renewal = renewal();
        assert_eq!(
            seal.open(&seal.seal(&renewal).expect("seal succeeds")),
            Ok(renewal)
        );
    }

    /// Altering the ciphertext invalidates its authentication tag.
    #[test]
    fn a_tampered_byte_is_refused() {
        let seal = seal("secret", "https://a.example");
        let encoded = seal.seal(&renewal()).expect("seal succeeds");
        let mut bytes = URL_SAFE_NO_PAD.decode(encoded).expect("valid encoding");
        bytes[12] ^= 1;
        assert_eq!(
            seal.open(&URL_SAFE_NO_PAD.encode(bytes)),
            Err(SealError::Unopenable)
        );
    }

    /// A different origin cannot decrypt the cookie.
    #[test]
    fn another_origin_cannot_open_it() {
        let sealed = seal("secret", "https://a.example")
            .seal(&renewal())
            .expect("seal succeeds");
        assert_eq!(
            seal("secret", "https://b.example").open(&sealed),
            Err(SealError::Unopenable)
        );
    }

    /// A different client secret cannot decrypt the cookie.
    #[test]
    fn another_client_secret_cannot_open_it() {
        let sealed = seal("secret", "https://a.example")
            .seal(&renewal())
            .expect("seal succeeds");
        assert_eq!(
            seal("other", "https://a.example").open(&sealed),
            Err(SealError::Unopenable)
        );
    }

    /// Oversized encoded cookies are rejected.
    #[test]
    fn a_seal_over_the_size_cap_is_refused() {
        let mut renewal = renewal();
        renewal.refresh_token = RefreshToken::new("x".repeat(3000));
        assert_eq!(
            seal("secret", "https://a.example").seal(&renewal),
            Err(SealError::TooLarge)
        );
    }

    /// Invalid encodings and truncated cookies cannot be opened.
    #[test]
    fn garbage_is_unopenable() {
        let seal = seal("secret", "https://a.example");
        for value in ["", "not base64!", "YQ"] {
            assert_eq!(seal.open(value), Err(SealError::Unopenable));
        }
    }
}
