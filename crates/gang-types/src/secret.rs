//! Secret newtypes whose `Debug` output never reveals their contents.

/// Defines a public secret newtype.
///
/// Takes the type's doc comments and name. The plain form generates serde
/// implementations that encode the secret as a plain string; prefixing the
/// doc comments with `non_wire` omits serde.
///
/// The generated struct wraps a `Zeroizing<String>` and provides `new`,
/// `expose`, `matches`, `From<String>`, and `Clone`. Its
/// `Debug` output is redacted, and its owned string is scrubbed on drop.
/// Secrets deliberately implement neither `PartialEq` nor `Eq`: compare them with the constant-time `matches`, never with `==`.
///
/// ```
/// gang_types::define_secret! {
///     /// An example secret.
///     ExampleToken
/// }
///
/// let token = ExampleToken::new("secret");
/// assert_eq!(token.expose(), "secret");
/// assert!(token.matches("secret"));
/// assert!(!token.matches("other"));
/// assert_eq!(format!("{token:?}"), "ExampleToken(<redacted>)");
/// ```
#[macro_export]
macro_rules! define_secret {
    ($(#[$meta:meta])* $name:ident) => {
        $crate::define_secret!(@base $(#[$meta])* $name);

        impl $crate::__private::serde::Serialize for $name {
            fn serialize<S: $crate::__private::serde::Serializer>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error> {
                <str as $crate::__private::serde::Serialize>::serialize(self.expose(), serializer)
            }
        }

        impl<'de> $crate::__private::serde::Deserialize<'de> for $name {
            fn deserialize<D: $crate::__private::serde::Deserializer<'de>>(deserializer: D) -> ::core::result::Result<Self, D::Error> {
                <::std::string::String as $crate::__private::serde::Deserialize>::deserialize(deserializer).map(Self::new)
            }
        }
    };
    (non_wire $(#[$meta:meta])* $name:ident) => {
        $crate::define_secret!(@base $(#[$meta])* $name);
    };
    (@base $(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(::std::clone::Clone)]
        pub struct $name($crate::__private::zeroize::Zeroizing<::std::string::String>);

        impl $name {
            /// Wraps `value` as a secret.
            #[must_use]
            pub fn new(value: impl ::std::convert::Into<::std::string::String>) -> Self {
                Self($crate::__private::zeroize::Zeroizing::new(::std::convert::Into::into(value)))
            }

            /// Returns the secret text. Callers must not log or persist the result.
            #[must_use]
            pub fn expose(&self) -> &str {
                &self.0
            }

            /// Whether `presented` equals this secret. Every byte is compared, so the time
            /// taken does not reveal how long a matching prefix is.
            #[must_use]
            pub fn matches(&self, presented: &str) -> bool {
                let secret = self.expose().as_bytes();
                let presented = presented.as_bytes();
                if secret.len() != presented.len() {
                    return false;
                }

                let difference = secret
                    .iter()
                    .zip(presented)
                    .fold(0_u8, |difference, (a, b)| difference | (a ^ b));
                ::std::hint::black_box(difference) == 0
            }
        }

        impl ::std::convert::From<::std::string::String> for $name {
            fn from(value: ::std::string::String) -> Self {
                Self::new(value)
            }
        }

        impl ::std::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(::std::concat!(::std::stringify!($name), "(<redacted>)"))
            }
        }
    };
}

define_secret! {
    /// Bearer token that proves identity to the certificate endpoint.
    Proof
}

define_secret! {
    /// Stored login credential used to obtain new proofs.
    RefreshToken
}

define_secret! {
    /// Secret required to redeem a device authorization.
    DeviceSecret
}

define_secret! {
    /// Secret a confidential client presents to the identity provider to prove its own identity.
    ClientSecret
}

define_secret! {
    /// Bearer token Gang issues to a client after it proved its identity.
    AccessToken
}

define_secret! {
    non_wire
    /// The plugin's secret state of a login in progress (anti-forgery values and verifiers).
    ///
    /// The owned state is scrubbed on drop by `Zeroizing`; copies made through `expose` are
    /// the caller's responsibility.
    LoginSession
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "tok-s3cr3t-value";

    #[test]
    fn every_secret_matches_only_equal_values() {
        macro_rules! check_matches {
            ($type:ty) => {{
                let secret = <$type>::new("secret");
                assert!(secret.matches("secret"));
                assert!(!secret.matches("secrex"));
                assert!(!secret.matches("secre"));
                assert!(!secret.matches("secret!"));
                assert!(!secret.matches(""));
                assert!(<$type>::new("").matches(""));
            }};
        }

        check_matches!(Proof);
        check_matches!(RefreshToken);
        check_matches!(ClientSecret);
        check_matches!(AccessToken);
        check_matches!(LoginSession);
        check_matches!(DeviceSecret);
    }

    #[test]
    fn login_session_exposes_state_but_debug_redacts_it() {
        let session = LoginSession::new("verifier=s3cr3t");
        assert_eq!(session.expose(), "verifier=s3cr3t");
        assert_eq!(format!("{session:?}"), "LoginSession(<redacted>)");
    }

    #[test]
    fn debug_never_contains_the_token() {
        let proof = Proof::new(SECRET);
        let refresh = RefreshToken::new(SECRET);
        let access = AccessToken::new(SECRET);
        let client = ClientSecret::new(SECRET);
        let proof_debug = format!("{proof:?}");
        let refresh_debug = format!("{refresh:?}");
        let access_debug = format!("{access:?}");
        let client_debug = format!("{client:?}");
        assert!(!proof_debug.contains(SECRET));
        assert!(!refresh_debug.contains(SECRET));
        assert!(!access_debug.contains(SECRET));
        assert!(!client_debug.contains(SECRET));
        assert_eq!(proof_debug, "Proof(<redacted>)");
        assert_eq!(refresh_debug, "RefreshToken(<redacted>)");
        assert_eq!(access_debug, "AccessToken(<redacted>)");
        assert_eq!(client_debug, "ClientSecret(<redacted>)");
    }

    #[test]
    fn serde_is_transparent() {
        let refresh = RefreshToken::new(SECRET);
        let access = AccessToken::new(SECRET);
        let text = serde_json::to_string(&refresh).expect("refresh token serializes");
        assert_eq!(text, format!("\"{SECRET}\""));
        assert!(
            refresh.matches(
                serde_json::from_str::<RefreshToken>(&text)
                    .expect("refresh token parses")
                    .expose()
            )
        );
        let text = serde_json::to_string(&access).expect("access token serializes");
        assert_eq!(text, format!("\"{SECRET}\""));
        assert!(
            access.matches(
                serde_json::from_str::<AccessToken>(&text)
                    .expect("access token parses")
                    .expose()
            )
        );
        let client = ClientSecret::new(SECRET);
        let text = serde_json::to_string(&client).expect("client secret serializes");
        assert_eq!(text, format!("\"{SECRET}\""));
        assert!(
            client.matches(
                serde_json::from_str::<ClientSecret>(&text)
                    .expect("client secret parses")
                    .expose()
            )
        );
    }

    #[test]
    fn proof_drops_and_debug_stays_redacted() {
        let proof = Proof::new(SECRET);
        assert_eq!(format!("{proof:?}"), "Proof(<redacted>)");
        drop(proof);
    }

    #[test]
    fn refresh_token_drops_and_debug_stays_redacted() {
        let refresh = RefreshToken::new(SECRET);
        assert_eq!(format!("{refresh:?}"), "RefreshToken(<redacted>)");
        drop(refresh);
    }

    #[test]
    fn client_secret_drops_and_debug_stays_redacted() {
        let client = ClientSecret::new(SECRET);
        assert_eq!(format!("{client:?}"), "ClientSecret(<redacted>)");
        drop(client);
    }

    #[test]
    fn access_token_drops_and_debug_stays_redacted() {
        let access = AccessToken::new(SECRET);
        assert_eq!(format!("{access:?}"), "AccessToken(<redacted>)");
        drop(access);
    }
}
