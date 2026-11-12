//! Validated PEM-encoded certificate and certificate request text.

use std::fmt;

use crate::PemError;

/// Checks framing and labels without decoding the payload.
fn is_pem_block(value: &str, begin: &str, end: &str) -> bool {
    enum State {
        Outside,
        Inside { saw_payload: bool },
    }

    let mut state = State::Outside;
    let mut saw_block = false;
    for line in value.lines().map(str::trim) {
        match state {
            State::Outside if line.is_empty() => {}
            State::Outside if line == begin => state = State::Inside { saw_payload: false },
            State::Outside => return false,
            State::Inside { saw_payload } if line == end => {
                if !saw_payload {
                    return false;
                }
                saw_block = true;
                state = State::Outside;
            }
            State::Inside { .. }
                if line.starts_with("-----BEGIN ") || line.starts_with("-----END ") =>
            {
                return false;
            }
            State::Inside { .. } if !line.is_empty() => {
                state = State::Inside { saw_payload: true };
            }
            State::Inside { .. } => {}
        }
    }
    saw_block && matches!(state, State::Outside)
}

macro_rules! define_pem {
    (secret $(#[$meta:meta])* $name:ident, $label:literal) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq)]
        pub struct $name(zeroize::Zeroizing<String>);

        define_pem!(@constructor $name, $label);

        impl $name {
            /// Returns the PEM text. Callers must not log or persist the result.
            #[must_use]
            pub fn expose(&self) -> &str {
                &self.0
            }
        }

        impl_validated_parse!($name, PemError);

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "(<redacted>)"))
            }
        }
    };
    ($(#[$meta:meta])* $name:ident, $label:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name(String);

        define_pem!(@constructor $name, $label);

        impl $name {
            /// Returns the PEM text as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl_validated_text!($name);
        impl_validated_parse!($name, PemError);
        impl_validated_serde!($name, PemError);
    };
    (@constructor $name:ident, $label:literal) => {
        impl $name {
            /// Validates `value` as one or more PEM blocks and stores the text as given.
            ///
            /// # Errors
            ///
            /// Returns [`PemError::Malformed`] if the PEM framing or label is invalid.
            pub fn try_new(value: impl Into<String>) -> Result<Self, PemError> {
                let value = value.into();
                if is_pem_block(
                    &value,
                    concat!("-----BEGIN ", $label, "-----"),
                    concat!("-----END ", $label, "-----"),
                ) {
                    Ok(Self(value.into()))
                } else {
                    Err(PemError::Malformed { label: $label })
                }
            }
        }
    };
}

define_pem! {
    /// PEM-encoded X.509 certificate, labelled `CERTIFICATE`.
    CertificatePem, "CERTIFICATE"
}

define_pem! {
    /// PEM-encoded certificate signing request, labelled `CERTIFICATE REQUEST`.
    CertificateRequestPem, "CERTIFICATE REQUEST"
}

define_pem! {
    secret
    /// PEM-encoded PKCS#8 private key, labelled `PRIVATE KEY`.
    ///
    /// The text is never printed by `Debug`, and the type has no `Display` or `Serialize`, so the
    /// key only leaves through [`PrivateKeyPem::expose`]. The owned text is scrubbed on drop;
    /// copies made through `expose` are the caller's responsibility.
    PrivateKeyPem, "PRIVATE KEY"
}

#[cfg(test)]
mod tests {
    use super::*;

    const CERTIFICATE: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
    const REQUEST: &str =
        "-----BEGIN CERTIFICATE REQUEST-----\nMIIB\n-----END CERTIFICATE REQUEST-----\n";

    #[test]
    fn certificate_rejects_unbalanced_markers_and_junk_between_blocks() {
        for text in [
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n-----END CERTIFICATE-----\n",
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\njunk\n-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
            "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n",
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----END PRIVATE KEY-----\n",
        ] {
            assert!(CertificatePem::try_new(text).is_err(), "accepted {text:?}");
        }
    }

    #[test]
    fn certificate_accepts_one_or_two_complete_blocks() {
        assert!(CertificatePem::try_new(CERTIFICATE).is_ok());
        assert!(CertificatePem::try_new(format!("{CERTIFICATE}{CERTIFICATE}")).is_ok());
    }

    #[test]
    fn certificate_accepts_a_minimal_block_and_keeps_the_text() {
        let certificate = CertificatePem::try_new(CERTIFICATE).unwrap();
        assert_eq!(certificate.as_str(), CERTIFICATE);
    }

    #[test]
    fn certificate_rejects_empty_text() {
        assert_eq!(
            CertificatePem::try_new(""),
            Err(PemError::Malformed {
                label: "CERTIFICATE"
            })
        );
    }

    #[test]
    fn certificate_rejects_a_request_block() {
        assert_eq!(
            CertificatePem::try_new(REQUEST),
            Err(PemError::Malformed {
                label: "CERTIFICATE"
            })
        );
    }

    #[test]
    fn request_rejects_a_certificate_block() {
        assert_eq!(
            CertificateRequestPem::try_new(CERTIFICATE),
            Err(PemError::Malformed {
                label: "CERTIFICATE REQUEST"
            })
        );
    }

    #[test]
    fn rejects_a_block_missing_its_end_line() {
        let truncated = "-----BEGIN CERTIFICATE-----\nMIIB\n";
        assert!(CertificatePem::try_new(truncated).is_err());
    }

    #[test]
    fn serde_round_trips_as_a_plain_string() {
        let certificate = CertificatePem::try_new(CERTIFICATE).unwrap();
        let text = serde_json::to_string(&certificate).unwrap();
        assert_eq!(text, serde_json::to_string(CERTIFICATE).unwrap());
        assert_eq!(
            serde_json::from_str::<CertificatePem>(&text).unwrap(),
            certificate
        );
    }

    #[test]
    fn deserialize_rejects_text_that_is_not_pem() {
        assert!(serde_json::from_str::<CertificateRequestPem>("\"not pem\"").is_err());
    }

    const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n";

    #[test]
    fn private_key_accepts_a_pkcs8_block_and_exposes_the_text() -> Result<(), PemError> {
        let key = PrivateKeyPem::try_new(PRIVATE_KEY)?;

        assert_eq!(key.expose(), PRIVATE_KEY);
        Ok(())
    }

    #[test]
    fn private_key_rejects_a_certificate_block() {
        assert_eq!(
            PrivateKeyPem::try_new(CERTIFICATE),
            Err(PemError::Malformed {
                label: "PRIVATE KEY"
            })
        );
    }

    #[test]
    fn private_key_debug_is_redacted() -> Result<(), PemError> {
        let key = PrivateKeyPem::try_new(PRIVATE_KEY)?;
        let debug = format!("{key:?}");

        assert_eq!(debug, "PrivateKeyPem(<redacted>)");
        assert!(!debug.contains("MIIB"));
        Ok(())
    }

    #[test]
    fn private_key_drops_and_debug_stays_redacted() -> Result<(), PemError> {
        let key = PrivateKeyPem::try_new(PRIVATE_KEY)?;
        assert_eq!(format!("{key:?}"), "PrivateKeyPem(<redacted>)");
        drop(key);
        Ok(())
    }

    #[test]
    fn private_key_rejects_an_rsa_block() {
        let rsa = "-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----\n";

        assert_eq!(
            PrivateKeyPem::try_new(rsa),
            Err(PemError::Malformed {
                label: "PRIVATE KEY"
            })
        );
    }

    #[test]
    fn private_key_rejects_a_certificate_block_in_the_middle() {
        let bundle = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----\nMIID\n-----END PRIVATE KEY-----\n";

        assert_eq!(
            PrivateKeyPem::try_new(bundle),
            Err(PemError::Malformed {
                label: "PRIVATE KEY"
            })
        );
    }

    #[test]
    fn certificate_accepts_a_chain_of_blocks_with_the_same_label() -> Result<(), PemError> {
        let chain = format!("{CERTIFICATE}{CERTIFICATE}");
        let certificate = CertificatePem::try_new(chain.clone())?;

        assert_eq!(certificate.as_str(), chain.as_str());
        Ok(())
    }

    #[test]
    fn certificate_request_keeps_its_own_block() -> Result<(), PemError> {
        let request = CertificateRequestPem::try_new(REQUEST)?;

        assert_eq!(request.as_str(), REQUEST);
        Ok(())
    }
}
