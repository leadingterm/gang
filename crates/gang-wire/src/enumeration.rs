//! Macro for string-valued wire enums that keep values this version does not know.

/// Declares a string-valued wire enum with an `Unrecognized` variant that keeps unknown text, so
/// that values added by newer servers round-trip unchanged.
///
/// `KNOWN` lists the wire text this version knows and `is_known` tells a known value from an
/// `Unrecognized` one. `enum Name default Variant` also implements `Default`, and
/// `enum Name from Domain` also generates the exhaustive `From<Domain>`; every variant must then
/// exist under the same name in `Domain`.
macro_rules! wire_enum {
    (@from $name:ident; ; $($variant:ident)*) => {};
    (@from $name:ident; $domain:path; $($variant:ident)*) => {
        impl ::std::convert::From<$domain> for $name {
            fn from(value: $domain) -> Self {
                match value {
                    $(<$domain>::$variant => Self::$variant,)*
                }
            }
        }
    };
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $name:ident $(default $default:ident)? $(from $domain:path)? {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $text:literal
            ),* $(,)?
        }
    ) => {
        wire_enum!(@from $name; $($domain)?; $($variant)*);

        $(
            impl ::std::default::Default for $name {
                fn default() -> Self {
                    Self::$default
                }
            }
        )?

        $(#[$enum_meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        $vis enum $name {
            $(
                $(#[$variant_meta])*
                $variant,
            )*
            /// A value this version does not know; kept so it round-trips.
            Unrecognized(::std::string::String),
        }

        impl $name {
            /// The wire text of the values this version knows, in declaration order.
            $vis const KNOWN: &'static [&'static str] = &[$($text),*];

            /// The wire text of this value.
            #[must_use]
            $vis fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $text,)*
                    Self::Unrecognized(text) => text.as_str(),
                }
            }
        }

        impl $crate::Known for $name {
            fn is_known(&self) -> bool {
                !matches!(self, Self::Unrecognized(_))
            }
        }

        impl ::std::convert::From<::std::string::String> for $name {
            fn from(text: ::std::string::String) -> Self {
                match text.as_str() {
                    $($text => Self::$variant,)*
                    _ => Self::Unrecognized(text),
                }
            }
        }

        impl ::std::convert::From<&str> for $name {
            fn from(text: &str) -> Self {
                match text {
                    $($text => Self::$variant,)*
                    _ => Self::Unrecognized(::std::string::String::from(text)),
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(
                &self,
                serializer: S,
            ) -> ::std::result::Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> ::std::result::Result<Self, D::Error> {
                <::std::string::String as ::serde::Deserialize>::deserialize(deserializer)
                    .map(Self::from)
            }
        }

        impl ::ts_rs::TS for $name {
            type WithoutGenerics = Self;
            type OptionInnerType = Self;
            const IS_ENUM: bool = true;

            fn name(_: &::ts_rs::Config) -> ::std::string::String {
                ::std::string::String::from(stringify!($name))
            }

            fn inline(_: &::ts_rs::Config) -> ::std::string::String {
                [$(concat!("\"", $text, "\"")),*].join(" | ")
            }

            fn decl(cfg: &::ts_rs::Config) -> ::std::string::String {
                format!(
                    "type {} = {};",
                    <Self as ::ts_rs::TS>::name(cfg),
                    <Self as ::ts_rs::TS>::inline(cfg)
                )
            }

            fn decl_concrete(cfg: &::ts_rs::Config) -> ::std::string::String {
                <Self as ::ts_rs::TS>::decl(cfg)
            }

            fn output_path() -> Option<::std::path::PathBuf> {
                Some(::std::path::PathBuf::from(concat!(stringify!($name), ".ts")))
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::Known;
    wire_enum! {
        /// Lifecycle phase used to exercise the macro.
        enum Phase {
            /// Doing work.
            Busy = "BUSY",
            Idle = "IDLE",
        }
    }

    #[test]
    fn known_values_round_trip_through_serde() {
        assert_eq!(
            serde_json::to_string(&Phase::Busy).expect("serializes"),
            "\"BUSY\""
        );
        let parsed: Phase = serde_json::from_str("\"IDLE\"").expect("parses");
        assert_eq!(parsed, Phase::Idle);
    }

    #[test]
    fn unknown_value_round_trips_unchanged() {
        let parsed: Phase = serde_json::from_str("\"DRAINING\"").expect("parses");
        assert_eq!(parsed, Phase::Unrecognized("DRAINING".into()));
        assert_eq!(
            serde_json::to_string(&parsed).expect("serializes"),
            "\"DRAINING\""
        );
    }

    #[test]
    fn display_and_as_str_use_wire_text() {
        assert_eq!(Phase::Busy.as_str(), "BUSY");
        assert_eq!(Phase::Busy.to_string(), "BUSY");
        let unknown = Phase::Unrecognized("DRAINING".into());
        assert_eq!(unknown.as_str(), "DRAINING");
        assert_eq!(unknown.to_string(), "DRAINING");
    }

    #[test]
    fn from_str_maps_known_and_unknown_text() {
        assert_eq!(Phase::from("BUSY"), Phase::Busy);
        assert_eq!(Phase::from("IDLE"), Phase::Idle);
        assert_eq!(
            Phase::from("DRAINING"),
            Phase::Unrecognized("DRAINING".into())
        );
    }

    #[test]
    fn from_string_maps_known_and_unknown_text() {
        assert_eq!(Phase::from(String::from("IDLE")), Phase::Idle);
        assert_eq!(
            Phase::from(String::from("DRAINING")),
            Phase::Unrecognized("DRAINING".into())
        );
    }

    #[test]
    fn known_lists_the_known_wire_text() {
        assert_eq!(Phase::KNOWN, ["BUSY", "IDLE"]);
    }

    #[test]
    fn ts_declaration_lists_the_known_wire_text() {
        assert_eq!(
            <Phase as ts_rs::TS>::decl(&ts_rs::Config::new()),
            r#"type Phase = "BUSY" | "IDLE";"#
        );
    }

    #[test]
    fn is_known_tells_known_from_unrecognized() {
        assert!(Phase::Busy.is_known());
        assert!(Phase::Idle.is_known());
        assert!(!Phase::Unrecognized("DRAINING".into()).is_known());
    }

    #[derive(Clone, Copy)]
    enum Mode {
        Busy,
        Idle,
    }

    wire_enum! {
        enum Activity from Mode {
            Busy = "BUSY",
            Idle = "IDLE",
        }
    }

    #[test]
    fn from_domain_is_exhaustive() {
        assert_eq!(Activity::from(Mode::Busy), Activity::Busy);
        assert_eq!(Activity::from(Mode::Idle), Activity::Idle);
        assert_eq!(Activity::KNOWN, ["BUSY", "IDLE"]);
        assert!(Activity::from(Mode::Idle).is_known());
    }
}
