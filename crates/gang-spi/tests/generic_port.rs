//! `#[port]` over methods with type parameters: the trait desugars, and generic arguments infer
//! through both a concrete and a generic call site.

use gang_spi::port;
use gang_testkit::must_ready as ready;

/// The marker a generic port method is parameterised over.
pub trait Tag: Send + Sync {
    /// The label the tag contributes to a description.
    fn label(&self) -> &'static str;
}

/// A tag labelled `alpha`.
pub struct Alpha;

impl Tag for Alpha {
    fn label(&self) -> &'static str {
        "alpha"
    }
}

/// A tag labelled `beta`.
pub struct Beta;

impl Tag for Beta {
    fn label(&self) -> &'static str {
        "beta"
    }
}

/// A port whose methods are generic over a tag, one asynchronous and one synchronous.
#[port]
pub trait Labeller: Send + Sync {
    /// Describes the tag asynchronously.
    async fn describe<T: Tag>(&self, tag: &T) -> String;

    /// Names the tag synchronously, under a `where` clause.
    fn name<T>(&self, tag: &T) -> String
    where
        T: Tag;
}

/// Upper-cases the tag and prefixes the name with `upper`.
pub struct Upper;

impl Labeller for Upper {
    async fn describe<T: Tag>(&self, tag: &T) -> String {
        tag.label().to_uppercase()
    }

    fn name<T>(&self, tag: &T) -> String
    where
        T: Tag,
    {
        format!("upper:{}", tag.label())
    }
}

/// Keeps the tag as written and prefixes the name with `lower`.
pub struct Lower;

impl Labeller for Lower {
    async fn describe<T: Tag>(&self, tag: &T) -> String {
        tag.label().to_owned()
    }

    fn name<T>(&self, tag: &T) -> String
    where
        T: Tag,
    {
        format!("lower:{}", tag.label())
    }
}

/// Calls a generic port method through a type parameter of the caller.
fn describe_through<L: Labeller, T: Tag>(labeller: &L, tag: &T) -> String {
    ready(labeller.describe(tag))
}

#[test]
fn generic_async_method_infers_its_type_parameter_for_each_implementor() {
    assert_eq!(ready(Upper.describe(&Alpha)), "ALPHA");
    assert_eq!(ready(Upper.describe(&Beta)), "BETA");
    assert_eq!(ready(Lower.describe(&Alpha)), "alpha");
    assert_eq!(ready(Lower.describe(&Beta)), "beta");
}

#[test]
fn generic_sync_method_with_where_clause_infers_its_type_parameter() {
    assert_eq!(Upper.name(&Alpha), "upper:alpha");
    assert_eq!(Lower.name(&Beta), "lower:beta");
}

#[test]
fn generic_method_called_through_a_generic_implementor_dispatches_to_it() {
    assert_eq!(describe_through(&Upper, &Beta), "BETA");
    assert_eq!(describe_through(&Lower, &Alpha), "alpha");
}
