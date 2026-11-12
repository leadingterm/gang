use std::sync::Arc;

use gang_clock::Clock;
use gang_types::{NonZeroDuration, Url};
use jsonwebtoken::DecodingKey;
use jsonwebtoken::jwk::{JwkSet, PublicKeyUse};

use crate::errors::OidcError;
use crate::http::Http;
use crate::refresh_cache::RefreshCache;

/// The issuer's signing keys, fetched lazily and refreshed for rotation.
pub(crate) struct KeyCache {
    keys: RefreshCache<KeySet>,
    /// Minimum spacing between refetches triggered by an unknown key id.
    refresh_interval: NonZeroDuration,
}

impl KeyCache {
    pub(crate) fn new(ttl: NonZeroDuration, refresh_interval: NonZeroDuration) -> Self {
        Self {
            keys: RefreshCache::new(ttl, refresh_interval, || OidcError::KeysUnavailable),
            refresh_interval,
        }
    }

    /// Returns the key for `kid`, fetching the key set when none is held or when the held set
    /// has no such key and the last refetch on a miss is old enough.
    pub(crate) async fn decoding_key(
        &self,
        http: &Http,
        jwks_uri: &Url,
        clock: &impl Clock,
        kid: Option<&str>,
    ) -> Result<SelectedKey, OidcError> {
        let set = self.keys.get(clock, || fetch(http, jwks_uri)).await?;
        if let Some(index) = set.select(kid)? {
            return Ok(SelectedKey { set, index });
        }
        let set = self
            .keys
            .replace(clock, self.refresh_interval, || fetch(http, jwks_uri))
            .await?;
        let index = set.select(kid)?.ok_or(OidcError::UnknownKey)?;
        Ok(SelectedKey { set, index })
    }
}

/// The issuer's keys, parsed once per fetch.
struct KeySet {
    keys: Vec<ParsedKey>,
}

/// One published key. A key that cannot be parsed is kept, so that a token naming it fails
/// with that key's error rather than as an unknown key.
struct ParsedKey {
    id: Option<String>,
    key: Result<DecodingKey, jsonwebtoken::errors::Error>,
}

/// The key chosen for one token. Holds the key set it was chosen from, so it outlives the
/// cache's replacement of that set.
pub(crate) struct SelectedKey {
    set: Arc<KeySet>,
    index: usize,
}

impl SelectedKey {
    /// The decoding key.
    ///
    /// # Errors
    ///
    /// [`OidcError::Jwks`] when the issuer's key for this token cannot be parsed.
    pub(crate) fn key(&self) -> Result<&DecodingKey, OidcError> {
        let parsed = self.set.keys.get(self.index).ok_or(OidcError::UnknownKey)?;
        parsed
            .key
            .as_ref()
            .map_err(|error| OidcError::Jwks(error.clone()))
    }
}

impl KeySet {
    fn from_jwks(jwks: JwkSet) -> Self {
        let keys = jwks
            .keys
            .into_iter()
            // RFC 7517 §4.2 reserves "enc" keys for encryption rather than signatures.
            .filter(|jwk| {
                matches!(
                    jwk.common.public_key_use,
                    None | Some(PublicKeyUse::Signature)
                )
            })
            .map(|jwk| ParsedKey {
                key: DecodingKey::from_jwk(&jwk),
                id: jwk.common.key_id,
            })
            .collect();
        Self { keys }
    }

    /// Picks the signing key named by `kid`; a token without a `kid` is only resolvable when the
    /// issuer publishes exactly one signing key or key without a declared use.
    fn select(&self, kid: Option<&str>) -> Result<Option<usize>, OidcError> {
        match kid {
            Some(kid) => Ok(self
                .keys
                .iter()
                .position(|parsed| parsed.id.as_deref() == Some(kid))),
            None => match self.keys.len() {
                0 => Ok(None),
                1 => Ok(Some(0)),
                _ => Err(OidcError::AmbiguousKey),
            },
        }
    }
}

async fn fetch(http: &Http, jwks_uri: &Url) -> Result<KeySet, OidcError> {
    http.get_json::<JwkSet>(jwks_uri)
        .await
        .map(KeySet::from_jwks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key_set(kids: &[&str]) -> KeySet {
        let keys: Vec<_> = kids
            .iter()
            .map(|kid| {
                json!({
                    "kty": "EC",
                    "crv": "P-256",
                    "x": "f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
                    "y": "x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0",
                    "kid": kid,
                    "alg": "ES256",
                })
            })
            .collect();
        let jwks: JwkSet =
            serde_json::from_value(json!({ "keys": keys })).expect("test key set is valid JWKS");
        KeySet::from_jwks(jwks)
    }

    fn selected_id(set: &KeySet, kid: Option<&str>) -> Option<String> {
        let index = set.select(kid).expect("selection succeeds")?;
        set.keys[index].id.clone()
    }

    #[test]
    fn kid_selects_the_matching_key() {
        let set = key_set(&["alpha", "beta"]);
        let selected = selected_id(&set, Some("beta"));
        assert_eq!(selected, Some("beta".to_owned()));
    }

    #[test]
    fn unknown_kid_selects_nothing() {
        let set = key_set(&["alpha"]);
        assert!(matches!(set.select(Some("gamma")), Ok(None)));
    }

    #[test]
    fn missing_kid_selects_the_only_key() {
        let set = key_set(&["alpha"]);
        let selected = selected_id(&set, None);
        assert_eq!(selected, Some("alpha".to_owned()));
    }

    #[test]
    fn missing_kid_with_several_keys_is_ambiguous() {
        let set = key_set(&["alpha", "beta"]);
        assert!(matches!(set.select(None), Err(OidcError::AmbiguousKey)));
    }

    #[test]
    fn kidless_selection_ignores_encryption_keys() {
        let jwks: JwkSet = serde_json::from_value(json!({
            "keys": [
                {
                    "kty": "EC",
                    "crv": "P-256",
                    "x": "f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
                    "y": "x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0",
                    "use": "enc"
                },
                {
                    "kty": "EC",
                    "crv": "P-256",
                    "x": "f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
                    "y": "x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0",
                    "use": "sig"
                }
            ]
        }))
        .expect("test key set is valid JWKS");
        let set = KeySet::from_jwks(jwks);
        assert_eq!(set.keys.len(), 1);
        assert!(matches!(set.select(None), Ok(Some(0))));
    }

    #[test]
    fn missing_kid_with_no_keys_selects_nothing() {
        let set = key_set(&[]);
        assert!(matches!(set.select(None), Ok(None)));
    }
}
