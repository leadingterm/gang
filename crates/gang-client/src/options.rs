//! Settings of the client: how long one request to the server may take, how large a response
//! may be, how long a login waits for the browser, how early a bearer token is renewed, which
//! program opens the browser, and how much of a server error text is quoted.

use std::num::NonZeroUsize;
use std::time::Duration;

use gang_types::{NonZeroBytes, NonZeroDuration, Percent};
use serde::{Deserialize, Serialize};

/// Default for [`ClientOptions::max_error_message_chars`]: two hundred characters. Built from
/// `MIN` so that the constant is non-zero by construction.
const DEFAULT_MAX_ERROR_MESSAGE_CHARS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(199);

/// The program that opens a URL in the browser when [`ClientOptions::browser_command`] is
/// unset: `open` on macOS, `xdg-open` elsewhere.
pub const DEFAULT_BROWSER_COMMAND: &str = if cfg!(target_os = "macos") {
    "open"
} else {
    "xdg-open"
};

/// Client settings. Every field is optional in serialized form; a missing field takes the value
/// given by [`ClientOptions::default`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClientOptions {
    /// The total time allowed for one request to the server, including reading the body.
    /// Must not be zero.
    pub http_timeout: NonZeroDuration,
    /// The largest response body accepted from the server. Must not be zero.
    pub max_response_size: NonZeroBytes,
    /// The largest request head the loopback login server reads from one connection; a larger
    /// head is answered `431` (browsers send every cookie of `127.0.0.1` to every port, so this
    /// must leave room for them). Must not be zero.
    pub callback_max_head_size: NonZeroBytes,
    /// The longest text of a server error body quoted in an error message, in characters;
    /// longer text is cut and marked with `...`. Must not be zero.
    pub max_error_message_chars: NonZeroUsize,
    /// The time a login waits for the browser redirect, a pasted URL, or device approval.
    /// Must not be zero.
    pub login_timeout: NonZeroDuration,
    /// The time allowed for one loopback connection to deliver its request. Must not be zero.
    pub callback_read_timeout: NonZeroDuration,
    /// A cached bearer token is minted again once its remaining life is below this margin,
    /// capped by `token_refresh_cap`. Zero is a valid margin.
    #[serde(with = "gang_types::duration")]
    pub token_refresh_margin: Duration,
    /// Maximum bearer-token refresh margin as a percentage of its lifetime. Defaults to 20%.
    pub token_refresh_cap: Percent,
    /// A stored ID token is fresh only while more than this margin remains. Defaults to one
    /// minute; zero is valid.
    #[serde(with = "gang_types::duration")]
    pub id_token_refresh_margin: Duration,
    /// The program that opens the authorization URL, run with the URL as its only argument.
    /// `None` uses [`DEFAULT_BROWSER_COMMAND`].
    pub browser_command: Option<String>,
    /// The largest difference between this machine's clock and the server's that `gang doctor`
    /// accepts. Must not be zero.
    pub max_clock_skew: NonZeroDuration,
    /// The time `gang doctor` waits for Bazel to report its effective options. Must not be zero.
    pub bazel_timeout: NonZeroDuration,
    /// The time allowed for a workload proof command. Must not be zero.
    pub proof_command_timeout: NonZeroDuration,
    /// The largest output accepted from a workload proof command. Must not be zero.
    pub proof_command_max_output: NonZeroBytes,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            http_timeout: NonZeroDuration::from_secs(10),
            max_response_size: NonZeroBytes::mib(1),
            callback_max_head_size: NonZeroBytes::kib(64),
            max_error_message_chars: DEFAULT_MAX_ERROR_MESSAGE_CHARS,
            login_timeout: NonZeroDuration::from_mins(5),
            callback_read_timeout: NonZeroDuration::from_secs(5),
            token_refresh_margin: Duration::from_mins(1),
            token_refresh_cap: Percent::from_percent(20),
            id_token_refresh_margin: Duration::from_mins(1),
            browser_command: None,
            max_clock_skew: NonZeroDuration::from_secs(30),
            bazel_timeout: NonZeroDuration::from_mins(10),
            proof_command_timeout: NonZeroDuration::from_secs(10),
            proof_command_max_output: NonZeroBytes::kib(64),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use gang_types::{NonZeroBytes, NonZeroDuration, Percent};

    use super::ClientOptions;

    #[test]
    fn default_values_are_the_documented_ones() {
        let options = ClientOptions::default();

        assert_eq!(options.http_timeout, NonZeroDuration::from_secs(10));
        assert_eq!(options.max_response_size, NonZeroBytes::mib(1));
        assert_eq!(options.callback_max_head_size, NonZeroBytes::kib(64));
        assert_eq!(
            options.max_error_message_chars,
            NonZeroUsize::new(200).expect("200 is non-zero")
        );
        assert_eq!(options.login_timeout, NonZeroDuration::from_mins(5));
        assert_eq!(options.callback_read_timeout, NonZeroDuration::from_secs(5));
        assert_eq!(options.token_refresh_margin, Duration::from_mins(1));
        assert_eq!(options.token_refresh_cap, Percent::from_percent(20));
        assert_eq!(options.id_token_refresh_margin, Duration::from_mins(1));
        assert_eq!(options.browser_command, None);
        assert_eq!(options.max_clock_skew, NonZeroDuration::from_secs(30));
        assert_eq!(options.bazel_timeout, NonZeroDuration::from_mins(10));
        assert_eq!(
            options.proof_command_timeout,
            NonZeroDuration::from_secs(10)
        );
        assert_eq!(options.proof_command_max_output, NonZeroBytes::kib(64));
    }

    #[test]
    fn json_round_trip_keeps_every_field() {
        let options = ClientOptions {
            http_timeout: NonZeroDuration::from_secs(3),
            max_response_size: NonZeroBytes::kib(4),
            callback_max_head_size: NonZeroBytes::kib(2),
            max_error_message_chars: NonZeroUsize::new(77).expect("77 is non-zero"),
            login_timeout: NonZeroDuration::from_secs(90),
            callback_read_timeout: NonZeroDuration::from_secs(2),
            token_refresh_margin: Duration::from_secs(30),
            token_refresh_cap: Percent::from_percent(35),
            id_token_refresh_margin: Duration::from_secs(15),
            browser_command: Some("firefox".to_owned()),
            max_clock_skew: NonZeroDuration::from_secs(7),
            bazel_timeout: NonZeroDuration::from_secs(90),
            proof_command_timeout: NonZeroDuration::from_secs(4),
            proof_command_max_output: NonZeroBytes::kib(8),
        };

        let json = serde_json::to_string(&options).expect("options serialize");
        let back: ClientOptions = serde_json::from_str(&json).expect("options deserialize");

        assert_eq!(back, options);
    }

    #[test]
    fn unknown_field_is_rejected() {
        let result = serde_json::from_str::<ClientOptions>(r#"{"http_timeout":"3s","retries":2}"#);

        assert!(result.is_err());
    }

    #[test]
    fn partial_override_keeps_the_other_defaults() {
        let options: ClientOptions =
            serde_json::from_str(r#"{"login_timeout":"1m"}"#).expect("partial options parse");

        assert_eq!(options.login_timeout, NonZeroDuration::from_mins(1));
        assert_eq!(options.http_timeout, NonZeroDuration::from_secs(10));
        assert_eq!(options.max_response_size, NonZeroBytes::mib(1));
        assert_eq!(options.callback_max_head_size, NonZeroBytes::kib(64));
        assert_eq!(options.callback_read_timeout, NonZeroDuration::from_secs(5));
        assert_eq!(options.token_refresh_margin, Duration::from_mins(1));
        assert_eq!(options.token_refresh_cap, Percent::from_percent(20));
        assert_eq!(options.id_token_refresh_margin, Duration::from_mins(1));
        assert_eq!(
            options.max_error_message_chars,
            NonZeroUsize::new(200).expect("200 is non-zero")
        );
        assert_eq!(options.browser_command, None);
        assert_eq!(options.max_clock_skew, NonZeroDuration::from_secs(30));
        assert_eq!(options.bazel_timeout, NonZeroDuration::from_mins(10));
    }

    #[test]
    fn token_refresh_margin_can_be_overridden() {
        let options: ClientOptions =
            serde_json::from_str(r#"{"token_refresh_margin":"15s"}"#).expect("options parse");

        assert_eq!(options.token_refresh_margin, Duration::from_secs(15));
        assert_eq!(options.login_timeout, NonZeroDuration::from_mins(5));
    }

    #[test]
    fn zero_token_refresh_margin_is_a_valid_margin() {
        let options: ClientOptions =
            serde_json::from_str(r#"{"token_refresh_margin":"0s"}"#).expect("options parse");

        assert_eq!(options.token_refresh_margin, Duration::ZERO);
    }

    #[test]
    fn id_token_refresh_margin_and_token_refresh_cap_can_be_overridden() {
        let options: ClientOptions =
            serde_json::from_str(r#"{"id_token_refresh_margin":"15s","token_refresh_cap":35}"#)
                .expect("options parse");

        assert_eq!(options.id_token_refresh_margin, Duration::from_secs(15));
        assert_eq!(options.token_refresh_cap, Percent::from_percent(35));
    }

    #[test]
    fn zero_id_token_refresh_margin_is_valid() {
        let options: ClientOptions =
            serde_json::from_str(r#"{"id_token_refresh_margin":"0s"}"#).expect("options parse");

        assert_eq!(options.id_token_refresh_margin, Duration::ZERO);
    }

    #[test]
    fn callback_max_head_size_can_be_overridden() {
        let options: ClientOptions =
            serde_json::from_str(r#"{"callback_max_head_size":"1KiB"}"#).expect("options parse");

        assert_eq!(options.callback_max_head_size, NonZeroBytes::kib(1));
        assert_eq!(options.login_timeout, NonZeroDuration::from_mins(5));
    }

    #[test]
    fn misspelled_callback_head_field_is_rejected() {
        let result = serde_json::from_str::<ClientOptions>(r#"{"callback_head_size":"1KiB"}"#);

        assert!(result.is_err());
    }

    #[test]
    fn max_error_message_chars_can_be_overridden() {
        let options: ClientOptions =
            serde_json::from_str(r#"{"max_error_message_chars":50}"#).expect("options parse");

        assert_eq!(options.max_error_message_chars.get(), 50);
        assert_eq!(options.login_timeout, NonZeroDuration::from_mins(5));
    }

    #[test]
    fn zero_max_error_message_chars_is_rejected() {
        let result = serde_json::from_str::<ClientOptions>(r#"{"max_error_message_chars":0}"#);

        assert!(result.is_err());
    }

    #[test]
    fn proof_command_limits_can_be_overridden() {
        let options: ClientOptions = serde_json::from_str(
            r#"{"proof_command_timeout":"2s","proof_command_max_output":"1KiB"}"#,
        )
        .expect("options parse");

        assert_eq!(options.proof_command_timeout, NonZeroDuration::from_secs(2));
        assert_eq!(options.proof_command_max_output, NonZeroBytes::kib(1));
    }

    #[test]
    fn zero_durations_and_sizes_are_rejected() {
        assert!(serde_json::from_str::<ClientOptions>(r#"{"http_timeout":"0s"}"#).is_err());
        assert!(serde_json::from_str::<ClientOptions>(r#"{"max_clock_skew":"0s"}"#).is_err());
        assert!(serde_json::from_str::<ClientOptions>(r#"{"max_response_size":"0B"}"#).is_err());
        assert!(
            serde_json::from_str::<ClientOptions>(r#"{"proof_command_timeout":"0s"}"#).is_err()
        );
        assert!(
            serde_json::from_str::<ClientOptions>(r#"{"proof_command_max_output":"0B"}"#).is_err()
        );
    }

    #[test]
    fn doctor_limits_default_and_parse_from_durations() {
        let defaults: ClientOptions = serde_json::from_str("{}").expect("empty options parse");

        assert_eq!(defaults, ClientOptions::default());
        assert_eq!(defaults.max_clock_skew, NonZeroDuration::from_secs(30));
        assert_eq!(defaults.bazel_timeout, NonZeroDuration::from_mins(10));

        let overridden: ClientOptions =
            serde_json::from_str(r#"{"max_clock_skew":"1m","bazel_timeout":"2m"}"#)
                .expect("options parse");

        assert_eq!(overridden.max_clock_skew, NonZeroDuration::from_mins(1));
        assert_eq!(overridden.bazel_timeout, NonZeroDuration::from_mins(2));
        assert_eq!(overridden.login_timeout, NonZeroDuration::from_mins(5));
    }
}
