//! `#[derive(Fix)]`: static and formatted fixes, field binding, escaping, and generics.

use std::borrow::Cow;
use std::fmt::Display;
use std::path::PathBuf;

use gang_types::Fix;

/// Errors covering every shape of variant and fix.
#[derive(Debug, thiserror::Error, Fix)]
pub enum Failure {
    /// A unit variant with static text.
    #[error("config missing")]
    #[fix("create the config file")]
    Missing,
    /// A named variant naming a field in a placeholder.
    #[error("unknown provider {name} ({known} known)")]
    #[fix("add `{name}` to the providers table")]
    Unknown {
        /// The provider that was asked for.
        name: String,
        /// A field the fix never mentions.
        known: usize,
    },
    /// A variant naming its fields in both the error and the fix.
    #[error("exit status {status}: {log}")]
    #[fix("rerun after fixing exit status {status}")]
    Exited {
        /// The exit status.
        status: i32,
        /// The captured log.
        log: String,
    },
    /// A variant whose fix calls a method on a field.
    #[error("cannot start {}", program.display())]
    #[fix("make {} executable", program.display())]
    Start {
        /// The program that did not start.
        program: PathBuf,
    },
    /// A variant with a format spec taking its width from a field.
    #[error("padded")]
    #[fix("{label:>width$}")]
    Padded {
        /// The text to pad.
        label: String,
        /// The width to pad to.
        width: usize,
    },
    /// A variant with literal braces and no placeholder.
    #[error("braces")]
    #[fix("write {{name}} in the template")]
    Braces,
    /// A variant with literal braces next to a placeholder.
    #[error("braces and field")]
    #[fix("write {{{key}}}")]
    BracesAndField {
        /// The key to wrap in braces.
        key: String,
    },
    /// A named variant with conditional guidance.
    #[error("named computed guidance")]
    #[fix(with = if *enabled {
        Some(Cow::Owned(format!("check {name}")))
    } else {
        None
    })]
    ComputedNamed {
        /// Whether to provide guidance.
        enabled: bool,
        /// The item to check.
        name: String,
        /// A field not used by the guidance.
        unused: usize,
    },
    /// A tuple variant with computed guidance.
    #[error("tuple computed guidance")]
    #[fix(with = Some(Cow::Owned(format!("check {f0} {f1}"))))]
    ComputedTuple(String, u8),
    /// A named variant without a fix.
    #[error("opaque {detail}")]
    Opaque {
        /// What went wrong.
        detail: String,
    },
    /// A tuple variant without a fix.
    #[error("opaque tuple {0}")]
    OpaqueTuple(u8),
    /// A unit variant without a fix.
    #[error("opaque unit")]
    OpaqueUnit,
}

#[test]
fn computed_guidance_can_use_named_fields_and_return_none() {
    let failure = Failure::ComputedNamed {
        enabled: true,
        name: "config".into(),
        unused: 0,
    };
    assert_eq!(failure.fix().as_deref(), Some("check config"));
    let silent = Failure::ComputedNamed {
        enabled: false,
        name: "config".into(),
        unused: 0,
    };
    assert_eq!(silent.fix(), None);
}

#[test]
fn computed_guidance_can_use_tuple_fields() {
    assert_eq!(
        Failure::ComputedTuple("item".into(), 7).fix().as_deref(),
        Some("check item 7")
    );
}

#[test]
fn unit_variant_yields_borrowed_static_text() {
    let fix = Failure::Missing.fix();
    assert!(matches!(fix, Some(Cow::Borrowed(_))));
    assert_eq!(fix.as_deref(), Some("create the config file"));
}

#[test]
fn named_field_placeholder_is_formatted() {
    let failure = Failure::Unknown {
        name: "nomad".to_owned(),
        known: 3,
    };
    let fix = failure.fix();
    assert!(matches!(fix, Some(Cow::Owned(_))));
    assert_eq!(fix.as_deref(), Some("add `nomad` to the providers table"));
    assert_eq!(failure.to_string(), "unknown provider nomad (3 known)");
}

#[test]
fn named_placeholder_binds_field_for_fix() {
    let failure = Failure::Exited {
        status: 7,
        log: "log".to_owned(),
    };
    assert_eq!(
        failure.fix().as_deref(),
        Some("rerun after fixing exit status 7")
    );
}

#[test]
fn format_argument_calls_method_on_field() {
    let failure = Failure::Start {
        program: PathBuf::from("/usr/bin/worker"),
    };
    assert_eq!(
        failure.fix().as_deref(),
        Some("make /usr/bin/worker executable")
    );
}

#[test]
fn width_argument_binds_its_field() {
    let failure = Failure::Padded {
        label: "ab".to_owned(),
        width: 4,
    };
    assert_eq!(failure.fix().as_deref(), Some("  ab"));
}

#[test]
fn doubled_braces_in_static_text_are_unescaped_and_borrowed() {
    let fix = Failure::Braces.fix();
    assert!(matches!(fix, Some(Cow::Borrowed(_))));
    assert_eq!(fix.as_deref(), Some("write {name} in the template"));
}

#[test]
fn doubled_braces_around_a_placeholder_format() {
    let failure = Failure::BracesAndField {
        key: "name".to_owned(),
    };
    assert_eq!(failure.fix().as_deref(), Some("write {name}"));
}

#[test]
fn variants_without_attribute_yield_none() {
    let detail = Failure::Opaque {
        detail: "x".to_owned(),
    };
    assert_eq!(detail.fix(), None);
    assert_eq!(Failure::OpaqueTuple(1).fix(), None);
    assert_eq!(Failure::OpaqueUnit.fix(), None);
}

/// A generic enum with a `where` clause.
#[derive(Debug, Fix)]
pub enum Wrapped<T>
where
    T: Display,
{
    /// A variant formatting its payload.
    #[fix("check {value}")]
    Bad {
        /// The value that the message and the fix both name.
        value: T,
    },
    /// A variant with static text.
    #[fix("nothing to do")]
    Fine,
    /// A variant without a fix.
    Silent,
}

/// A source error whose own fix is static text.
#[derive(Debug, thiserror::Error, Fix)]
pub enum Inner {
    /// A variant with a fix.
    #[error("inner missing")]
    #[fix("fix the inner thing")]
    Missing,
    /// A variant without a fix.
    #[error("inner opaque")]
    Opaque,
}

/// An error whose fix is delegated to its source field.
#[derive(Debug, thiserror::Error, Fix)]
pub enum Outer {
    /// A named source field.
    #[error("delegated")]
    #[fix(source)]
    Delegated {
        /// The source error.
        #[source]
        source: Inner,
    },
    /// A `#[from]` source field.
    #[error("converted")]
    #[fix(source)]
    Converted(#[from] Inner),
    /// A source whose error has no fix.
    #[error("quiet")]
    #[fix(source)]
    Quiet {
        /// The source error.
        #[source]
        source: Inner,
    },
}

#[test]
fn generic_enum_carries_its_generics() {
    assert_eq!(
        Wrapped::Bad { value: 5_u8 }.fix().as_deref(),
        Some("check 5")
    );
    assert_eq!(
        Wrapped::<String>::Bad {
            value: "path".to_owned()
        }
        .fix()
        .as_deref(),
        Some("check path")
    );
    let fine = Wrapped::<u8>::Fine.fix();
    assert!(matches!(fine, Some(Cow::Borrowed("nothing to do"))));
    assert_eq!(Wrapped::<u8>::Silent.fix(), None);
}

#[test]
fn source_attribute_delegates_to_the_source_field() {
    let failure = Outer::Delegated {
        source: Inner::Missing,
    };
    assert_eq!(failure.fix().as_deref(), Some("fix the inner thing"));
}

#[test]
fn from_source_delegates_to_the_converted_error() {
    let failure = Outer::from(Inner::Missing);
    assert_eq!(failure.fix().as_deref(), Some("fix the inner thing"));
}

#[test]
fn delegation_to_a_source_without_fix_yields_none() {
    let failure = Outer::Quiet {
        source: Inner::Opaque,
    };
    assert_eq!(failure.fix(), None);
}
