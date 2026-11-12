//! `string_enum!`: closed enums whose values are fixed strings.

use std::error::Error;
use std::fmt;

/// A string that names none of the values of a closed string enum.
///
/// The message lists the accepted names, built from the enum's own table, so it cannot drift from
/// the parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownVariantError {
    kind: &'static str,
    input: String,
    expected: &'static [&'static str],
}

impl UnknownVariantError {
    /// Creates the error for `input`, which is not one of `expected` names of the `kind` of value.
    #[must_use]
    pub fn new(kind: &'static str, input: &str, expected: &'static [&'static str]) -> Self {
        Self {
            kind,
            input: input.to_owned(),
            expected,
        }
    }

    /// What the rejected text was meant to name, such as `isa`.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        self.kind
    }

    /// The rejected text.
    #[must_use]
    pub fn input(&self) -> &str {
        &self.input
    }

    /// The accepted names, in declaration order.
    #[must_use]
    pub const fn expected(&self) -> &'static [&'static str] {
        self.expected
    }
}

impl fmt::Display for UnknownVariantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown {} `{}`, expected one of ",
            self.kind, self.input
        )?;
        for (index, name) in self.expected.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            f.write_str(name)?;
        }
        Ok(())
    }
}

impl Error for UnknownVariantError {}

/// Declares a closed enum of fixed strings.
///
/// One table of `Variant = "text"` rows generates the enum (`Debug`, `Clone`, `Copy`, `Eq`,
/// `Ord`, `Hash`), `ALL` and `NAMES` as fixed-size arrays in declaration order, `as_str`,
/// `Display`, `FromStr` with an [`UnknownVariantError`] that lists the accepted names. The string
/// in parentheses names the kind of value in that error message. An optional `default Variant`
/// clause after the kind implements `Default` with that variant.
///
/// ```
/// gang_types::string_enum! {
///     /// How a thing is stored.
///     pub enum Storage("storage") {
///         /// Kept in memory.
///         Memory = "memory",
///         /// Kept on disk.
///         Disk = "on-disk",
///     }
/// }
///
/// assert_eq!(Storage::Disk.as_str(), "on-disk");
/// assert_eq!("memory".parse::<Storage>(), Ok(Storage::Memory));
/// assert_eq!(
///     "tape".parse::<Storage>().unwrap_err().to_string(),
///     "unknown storage `tape`, expected one of memory, on-disk"
/// );
/// ```
#[macro_export]
macro_rules! string_enum {
    (@unit $variant:tt) => { () };
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $name:ident($kind:literal) $(default $default:ident)? {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $text:literal
            ),+ $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis enum $name {
            $(
                $(#[$variant_meta])*
                $variant,
            )+
        }

        $(
            impl ::std::default::Default for $name {
                fn default() -> Self {
                    Self::$default
                }
            }
        )?

        impl $name {
            /// Every value, in declaration order.
            $vis const ALL: [Self; <[()]>::len(&[$($crate::string_enum!(@unit $variant)),+])] =
                [$(Self::$variant),+];

            /// The text of every value, in declaration order.
            $vis const NAMES: [&'static str; <[()]>::len(&[$($crate::string_enum!(@unit $variant)),+])] =
                [$($text),+];

            /// The text of this value.
            #[must_use]
            $vis const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $crate::UnknownVariantError;

            fn from_str(input: &str) -> ::std::result::Result<Self, Self::Err> {
                match input {
                    $($text => ::std::result::Result::Ok(Self::$variant),)+
                    _ => ::std::result::Result::Err($crate::UnknownVariantError::new(
                        $kind,
                        input,
                        &Self::NAMES,
                    )),
                }
            }
        }

        impl $crate::__private::serde::Serialize for $name {
            fn serialize<S: $crate::__private::serde::Serializer>(
                &self,
                serializer: S,
            ) -> ::std::result::Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> $crate::__private::serde::Deserialize<'de> for $name {
            fn deserialize<D: $crate::__private::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> ::std::result::Result<Self, D::Error> {
                struct Visitor;

                impl $crate::__private::serde::de::Visitor<'_> for Visitor {
                    type Value = $name;

                    fn expecting(
                        &self,
                        formatter: &mut ::std::fmt::Formatter<'_>,
                    ) -> ::std::fmt::Result {
                        formatter.write_str(concat!("a ", $kind, " name"))
                    }

                    fn visit_str<E: $crate::__private::serde::de::Error>(
                        self,
                        value: &str,
                    ) -> ::std::result::Result<$name, E> {
                        value.parse().map_err(E::custom)
                    }
                }

                deserializer.deserialize_str(Visitor)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::UnknownVariantError;

    crate::string_enum! {
        /// A mode used to exercise the macro.
        enum Mode("mode") {
            /// Fast.
            Fast = "fast",
            /// Slow and steady.
            SlowSteady = "slow-steady",
        }
    }

    crate::string_enum! {
        /// A mode with an explicit default.
        enum DefaultMode("default mode") default Slow {
            /// Fast.
            Fast = "fast",
            /// Slow.
            Slow = "slow",
        }
    }

    #[test]
    fn explicit_default_selects_the_declared_variant() {
        assert_eq!(DefaultMode::default(), DefaultMode::Slow);
        assert_eq!(DefaultMode::ALL, [DefaultMode::Fast, DefaultMode::Slow]);
        assert_eq!(DefaultMode::NAMES, ["fast", "slow"]);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        mode: Mode,
    }

    #[test]
    fn all_and_names_follow_declaration_order() {
        assert_eq!(Mode::ALL, [Mode::Fast, Mode::SlowSteady]);
        assert_eq!(Mode::NAMES, ["fast", "slow-steady"]);
    }

    #[test]
    fn display_and_from_str_round_trip_every_value() {
        for mode in Mode::ALL {
            assert_eq!(mode.to_string().parse::<Mode>(), Ok(mode));
        }
        assert_eq!(Mode::SlowSteady.as_str(), "slow-steady");
    }

    #[test]
    fn an_unknown_name_lists_the_accepted_ones() {
        let error = "other".parse::<Mode>().expect_err("not a mode");

        assert_eq!(
            error,
            UnknownVariantError::new("mode", "other", &Mode::NAMES)
        );
        assert_eq!(
            error.to_string(),
            "unknown mode `other`, expected one of fast, slow-steady"
        );
    }

    #[test]
    fn serde_uses_the_text() {
        let holder: Holder = toml::from_str("mode = \"slow-steady\"").expect("parses");

        assert_eq!(holder.mode, Mode::SlowSteady);
        assert_eq!(
            serde_json::to_string(&holder).expect("serializes"),
            r#"{"mode":"slow-steady"}"#
        );
    }

    #[test]
    fn a_parse_error_reports_its_kind_input_and_accepted_names() {
        let error = "other".parse::<Mode>().expect_err("not a mode");

        assert_eq!(error.kind(), "mode");
        assert_eq!(error.input(), "other");
        assert_eq!(error.expected(), Mode::NAMES.as_slice());
    }

    #[test]
    fn serde_rejects_an_unknown_name() {
        assert!(toml::from_str::<Holder>("mode = \"other\"").is_err());
    }

    #[test]
    fn serde_rejects_a_non_string_value() {
        assert!(toml::from_str::<Holder>("mode = 5").is_err());
    }
}
