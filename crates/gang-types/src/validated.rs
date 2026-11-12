//! Macros for validated text newtypes and their conversions.
/// Defines a string newtype whose constructors and deserializer validate its contents.
///
/// The check must be a function or non-capturing closure with signature
/// `fn(&str) -> Result<(), Error>`. The error must implement `Display`.
///
/// ```
/// use gang_types::validated_string;
///
/// #[derive(Debug, PartialEq, Eq)]
/// struct EmptyName;
///
/// impl std::fmt::Display for EmptyName {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         f.write_str("name is empty")
///     }
/// }
///
/// validated_string!(
///     /// A nonempty name.
///     Name, |value: &str| {
///         if value.is_empty() { Err(EmptyName) } else { Ok(()) }
///     }, EmptyName
/// );
///
/// let name = Name::try_new("alice").expect("the name is nonempty");
/// assert_eq!(name.as_str(), "alice");
/// assert_eq!(Name::try_new(""), Err(EmptyName));
/// ```
#[macro_export]
macro_rules! validated_string {
    ($(#[$meta:meta])* $vis:vis $name:ident, $check:expr, $error:ty) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $vis struct $name(::std::borrow::Cow<'static, str>);

        impl $name {
            /// Validates and wraps the supplied string.
            ///
            /// # Errors
            ///
            /// Returns the validation error if the string is invalid.
            pub fn try_new(
                value: impl ::std::convert::Into<::std::string::String>,
            ) -> ::std::result::Result<Self, $error> {
                let value = value.into();
                let check: fn(&str) -> ::std::result::Result<(), $error> = $check;
                check(&value)?;
                ::std::result::Result::Ok(Self(::std::borrow::Cow::Owned(value)))
            }

            /// Returns the validated string.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::std::convert::AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl ::std::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $error;

            fn from_str(value: &str) -> ::std::result::Result<Self, Self::Err> {
                Self::try_new(value)
            }
        }

        impl ::std::convert::TryFrom<::std::string::String> for $name {
            type Error = $error;

            fn try_from(value: ::std::string::String) -> ::std::result::Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }

        impl ::std::convert::TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(value: &str) -> ::std::result::Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }

        impl ::std::convert::From<$name> for ::std::string::String {
            fn from(value: $name) -> Self {
                value.0.into_owned()
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
                let value = <::std::string::String as $crate::__private::serde::Deserialize>::deserialize(
                    deserializer,
                )?;
                Self::try_new(value).map_err($crate::__private::serde::de::Error::custom)
            }
        }
    };
}

/// Implements `FromStr`, `TryFrom<String>` and `TryFrom<&str>` through `try_new`.
macro_rules! impl_validated_parse {
    ($name:ident, $error:ty) => {
        impl ::std::str::FromStr for $name {
            type Err = $error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::try_new(value)
            }
        }

        impl ::std::convert::TryFrom<String> for $name {
            type Error = $error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }

        impl ::std::convert::TryFrom<&str> for $name {
            type Error = $error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::try_new(value)
            }
        }
    };
}

/// Implements `Display` and `AsRef<str>` over the type's inherent `as_str`.
macro_rules! impl_validated_text {
    ($name:ident) => {
        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::std::convert::AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }
    };
}

/// Implements `Serialize` over `as_str` and `Deserialize` through `try_new`.
macro_rules! impl_validated_serde {
    ($name:ident, $error:ty) => {
        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                let value = <String as ::serde::Deserialize>::deserialize(deserializer)?;
                let parsed: Result<Self, $error> = Self::try_new(value);
                parsed.map_err(::serde::de::Error::custom)
            }
        }
    };
}

/// Implements string serde through `Display` and `FromStr`.
macro_rules! impl_parsed_serde {
    ([$($generics:tt)*] $ty:ty, $expecting:literal) => {
        impl_parsed_serde!(@impl [$($generics)*] $ty, $expecting);
    };
    ($ty:ty, $expecting:literal) => {
        impl_parsed_serde!(@impl [] $ty, $expecting);
    };
    (@impl [$($generics:tt)*] $ty:ty, $expecting:literal) => {
        impl<$($generics)*> ::serde::Serialize for $ty {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de, $($generics)*> ::serde::Deserialize<'de> for $ty {
            fn deserialize<D: ::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                struct ParsedVisitor<T>(::std::marker::PhantomData<T>);

                impl<$($generics)*> ::serde::de::Visitor<'_> for ParsedVisitor<$ty> {
                    type Value = $ty;

                    fn expecting(
                        &self,
                        formatter: &mut ::std::fmt::Formatter<'_>,
                    ) -> ::std::fmt::Result {
                        formatter.write_str($expecting)
                    }

                    fn visit_str<E: ::serde::de::Error>(
                        self,
                        value: &str,
                    ) -> Result<Self::Value, E> {
                        value.parse::<$ty>().map_err(E::custom)
                    }
                }

                deserializer.deserialize_str(ParsedVisitor::<$ty>(::std::marker::PhantomData))
            }
        }
    };
}

/// Generates an optional serde adapter using the parent module's adapter functions.
#[macro_export]
#[doc(hidden)]
macro_rules! serde_option_module {
    ($ty:ty, $expecting:literal) => {
        /// Serde adapter for an optional value using the parent module's format.
        pub mod option {
            use $crate::__private::serde::de::{self, Visitor};
            use $crate::__private::serde::{Deserializer, Serializer};

            /// Serializes `Some` using the parent adapter and `None` as null.
            ///
            /// # Errors
            ///
            /// Returns the serializer's error if writing fails.
            pub fn serialize<S: Serializer>(
                value: &Option<$ty>,
                serializer: S,
            ) -> Result<S::Ok, S::Error> {
                match value {
                    Some(value) => super::serialize(value, serializer),
                    None => serializer.serialize_none(),
                }
            }

            /// Deserializes null as `None` and other values using the parent adapter.
            ///
            /// # Errors
            ///
            /// Returns the deserializer's error if the value cannot be parsed.
            pub fn deserialize<'de, D: Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Option<$ty>, D::Error> {
                struct OptionVisitor;

                impl<'de> Visitor<'de> for OptionVisitor {
                    type Value = Option<$ty>;

                    fn expecting(
                        &self,
                        formatter: &mut ::std::fmt::Formatter<'_>,
                    ) -> ::std::fmt::Result {
                        formatter.write_str($expecting)
                    }

                    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                        Ok(None)
                    }

                    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                        Ok(None)
                    }

                    fn visit_some<D: Deserializer<'de>>(
                        self,
                        deserializer: D,
                    ) -> Result<Self::Value, D::Error> {
                        super::deserialize(deserializer).map(Some)
                    }
                }

                deserializer.deserialize_option(OptionVisitor)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    #[error("name is empty")]
    struct EmptyName;

    fn check_name(value: &str) -> Result<(), EmptyName> {
        if value.is_empty() {
            Err(EmptyName)
        } else {
            Ok(())
        }
    }

    crate::validated_string!(
        /// A nonempty name used to exercise the macro.
        Name, check_name, EmptyName
    );

    #[test]
    fn validates_construction_and_supports_borrowed_lookup() -> Result<(), EmptyName> {
        let name = Name::try_new("alice")?;
        assert_eq!(name.as_str(), "alice");
        assert_eq!(Name::try_new(""), Err(EmptyName));

        let names = HashSet::from([name]);
        assert!(names.contains("alice"));
        assert!(!names.contains("bob"));
        Ok(())
    }

    #[test]
    fn serde_round_trips_and_rejects_invalid_text() -> Result<(), Box<dyn std::error::Error>> {
        let name = Name::try_new("alice")?;
        let json = serde_json::to_string(&name)?;
        assert_eq!(json, "\"alice\"");
        assert_eq!(serde_json::from_str::<Name>(&json)?, name);
        assert!(serde_json::from_str::<Name>("\"\"").is_err());
        Ok(())
    }
}
