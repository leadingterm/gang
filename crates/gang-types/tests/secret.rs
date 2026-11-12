//! Exercises secret definitions from a downstream crate.

gang_types::define_secret! {
    /// A secret serialized as a string.
    WireSecret
}

gang_types::define_secret! {
    non_wire
    /// A secret that is not serialized.
    LocalSecret
}

#[test]
fn generated_secrets_expose_match_and_redact() {
    macro_rules! check_secret {
        ($secret:ty, $debug:literal) => {{
            let secret = <$secret>::new("secret");
            assert_eq!(secret.expose(), "secret");
            assert!(secret.matches("secret"));
            assert!(!secret.matches("secrex"));
            assert!(!secret.matches("secre"));
            assert!(!secret.matches("secret!"));
            assert!(!secret.matches(""));
            let debug = format!("{secret:?}");
            assert_eq!(debug, $debug);
            assert!(!debug.contains("secret"));
            let from_string = <$secret>::from(String::from("secret"));
            assert!(secret.matches(from_string.expose()));
            assert!(secret.matches(secret.clone().expose()));
            assert!(!secret.matches(<$secret>::new("different").expose()));
        }};
    }

    check_secret!(WireSecret, "WireSecret(<redacted>)");
    check_secret!(LocalSecret, "LocalSecret(<redacted>)");
}

#[test]
fn wire_secret_is_a_json_string() -> Result<(), Box<dyn std::error::Error>> {
    let secret = WireSecret::new("secret");
    let json = serde_json::to_string(&secret)?;
    assert_eq!(json, "\"secret\"");
    assert!(secret.matches(serde_json::from_str::<WireSecret>(&json)?.expose()));
    Ok(())
}
