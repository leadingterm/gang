//! One-line operator guidance attached to error values.

use std::borrow::Cow;

/// Operator guidance for an error.
///
/// `#[derive(gang_types::Fix)]` implements this trait from `#[fix("text {field}")]`
/// attributes on enum variants. `#[fix(with = expression)]` computes an optional fix
/// using the variant's fields. A variant without the attribute has no fix and yields `None`.
pub trait Fix {
    /// The one-line action that helps the operator past this error, when there is one.
    fn fix(&self) -> Option<Cow<'static, str>>;
}
