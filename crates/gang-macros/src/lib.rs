//! Proc macros of the plugin contract (`port`, `settings`, `compose`, `FleetOptions`) and of the operator-fix derive (`Fix`); use the contract macros through `gang_spi::{port, compose}`.
//!
//! Proc-macro entry points must sit in the crate root; each hands off to its expansion module.

mod compose;
mod domain;
mod expansion;
mod fix;
mod fleet_options;
mod known;
mod names;
mod port;
mod port_failure;
mod settings;

/// Declares a capability port: a trait of `&self` methods, written with native `async fn`.
///
/// The attribute rewrites every `async fn` into `fn .. -> impl Future<Output = ..> + Send`
/// so that implementations may use `async fn` and callers may spawn the futures, and it
/// records the methods in a hidden `macro_rules!` named `__gang_delegate_<Trait>` that
/// `compose!` calls to implement the trait for a dispatch enum. The trait must be
/// re-exported from the root of the crate that declares it, and the types in its method
/// signatures must be nameable from that root or from `gang_types`.
///
/// Methods may declare type parameters (with bounds and `where` clauses); the delegating impl
/// repeats them. Lifetime and const parameters are not allowed.
///
/// Associated types: a port may declare associated types such as `type Scope: Debug + Send;`.
/// A method that takes `&Self::Scope` or `Self::Scope`, or a `Fleet<'_, Self::Options>`, or
/// returns `Result<Self::Scope, PortError>`, is narrowed by the dispatch enum; a value from
/// another plugin is a `PortError::Config`. `compose!` generates one enum per associated type.
///
/// Default bodies: a method may have a default body. `#[optional]` marks a method without a
/// body that returns `Result<_, PortError>` and defaults to `PortError::Unsupported { operation: "<method>" }`.
/// `PortError` must be in scope in the declaring module.
///
/// Every port is also implemented for `Arc<T>` of any plugin `T` that implements it.
#[proc_macro_attribute]
pub fn port(
    attribute: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    expansion::finish(port::expand(&attribute.into(), item.into()))
}

/// Declares a settings trait: configuration facts the host reads and sets on a plugin's section
/// without knowing the plugin, such as `gang_types::AuthenticationSettings`.
///
/// The attribute records the trait's methods in a hidden `macro_rules!` named
/// `__gang_delegate_<Trait>`, which `compose!` calls to implement the trait for a configuration
/// enum: every method that takes `&self` or `&mut self` calls the same method of the selected
/// plugin's settings, so a plugin's override is reached even where the trait has a default
/// body. A method that takes `self` by value keeps its default body, which must exist. Methods
/// take no generic parameters and their arguments are plain identifiers. The trait must be
/// re-exported from the root of the crate that declares it, and the types in its method
/// signatures must be nameable from that root.
#[proc_macro_attribute]
pub fn settings(
    attribute: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    expansion::finish(settings::expand(&attribute.into(), item.into()))
}

/// Composes the plugins of every domain into typed configuration enums and statically
/// dispatched plugin enums.
///
/// Each domain lists `key: PluginType` pairs; the key is the section name users write in
/// `gang.toml` and `PluginType` implements `gang_spi::Plugin` and the domain's ports.
/// Per domain the macro generates:
///
/// - `<Name>Config`, the section's configuration (tagged domains also get `key()`).
/// - `<Name>`, the dispatch enum implementing the domain's ports, with `DOMAIN` (the domain key)
///   and `build` returning `Result<Self, gang_spi::BuildError>`.
/// - one enum per associated type of the domain's ports, named `<Name><Assoc>`, with `as_<key>`
///   and `into_<key>` accessors; an associated type that delegates a `#[settings]` trait, such as the limits scope's `ActionScope`, has that trait implemented through its delegate macro.
/// - for `AnyOf` domains, a `<Name>Key` string enum made by `gang_types::string_enum!`.
/// - for authentication and scheduler domains, the configuration enum implements the domain's settings trait (`AuthenticationSettings` for authentication domains, `SchedulerSettings` for the scheduler domain) through the delegate macro of [`macro@settings`].
///
/// Finally `Plugins`, the `gang_config::PluginSet` of the composition.
#[proc_macro]
pub fn compose(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expansion::finish(compose::expand(input.into()))
}

/// Derives `gang_spi::FleetOptions` for a provisioner's `[fleets.options]` struct: one option
/// per field, its doc comment as the description, `Option<T>` fields optional. The struct also
/// derives `Deserialize` (with `deny_unknown_fields` and no other `serde` container attribute;
/// option names are never reserved platform property keys), and every value type implements
/// `Display`. `#[option(launch)]` marks a launch-only field: it is no platform property, so it
/// needs no `Display` and may be any `Deserialize` type. `#[fleet_options(validate = path)]`
/// names a `fn(&Self) -> Result<(), String>` checked when the configuration loads.
#[proc_macro_derive(FleetOptions, attributes(option, fleet_options))]
pub fn derive_fleet_options(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expansion::finish(fleet_options::expand(input.into()))
}

/// Derives `gang_types::Fix` for an error enum: the one-line action that helps the operator past
/// each variant, `None` for variants without one.
///
/// A variant carries `#[fix("text")]` or `#[fix("text {field} {}", expr, ..)]`, formatted like
/// `thiserror`'s `#[error(..)]`: inline `{field}` placeholders name fields (tuple fields are
/// `_0`, `_1`, ..) and format arguments may call methods on them. Text without placeholders or
/// arguments is `Cow::Borrowed`; anything else is formatted into a `Cow::Owned`.
/// A variant whose fix text names its fields uses named fields; tuple fields are bound as
/// `_0`, `_1`, but clippy's `used_underscore_binding` flags those placeholders, so prefer
/// named fields.
///
/// `#[fix(with = expression)]` evaluates an expression returning
/// `Option<std::borrow::Cow<'static, str>>`, with named fields or tuple fields `f0`, `f1`, ..
/// bound by reference.
///
/// A variant marked `#[fix(source)]` delegates to the Fix implementation of its `#[source]`, `#[from]` or transparent field.
///
/// ```ignore
/// #[derive(Debug, thiserror::Error, gang_types::Fix)]
/// enum SpawnError {
///     #[error("worker not found")]
///     #[fix("install the worker binary")]
///     NotFound,
///     #[error("cannot start {}", program.display())]
///     #[fix("make {} executable or check its path", program.display())]
///     Start { program: std::path::PathBuf },
///     #[error("exited with {0}")]
///     Exited(i32),
/// }
/// ```
#[proc_macro_derive(Fix, attributes(fix))]
pub fn derive_fix(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expansion::finish(fix::expand(input.into()))
}

/// Derives recursive recognition of wire values, treating `Unrecognized` variants as unknown.
#[proc_macro_derive(Known)]
pub fn derive_known(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expansion::finish(known::expand(input.into()))
}

/// Derives `From<Error> for gang_spi::PortError` for a plugin's error enum, classifying each
/// variant: `#[failure(config)]` (the plugin's configuration cannot be used),
/// `#[failure(unavailable)]` (the backend may answer on a later try), `#[failure(backend)]`
/// (trying again will not help; the default of a variant without the marker),
/// `#[failure(rejected(<Rejection variant>))]` (a refusal retaining the whole error as source),
/// `#[failure(not_found(<Subject variant>))]` (a missing subject containing the only field),
/// `#[failure(unsupported("<operation>"))]` (an unsupported operation), or
/// `#[failure(transparent)]` (convert the only field into a `PortError`). Configuration,
/// unavailable and backend errors also retain the whole error as their source.
///
/// ```ignore
/// #[derive(Debug, thiserror::Error, gang_spi::PortFailure)]
/// enum CgroupError {
///     #[error("the poll interval must not be zero")]
///     #[failure(config)]
///     ZeroPollInterval,
///     #[error("scope {0} is still populated")]
///     #[failure(unavailable)]
///     StillPopulated(String),
///     #[error("cgroup filesystem error")]
///     Io(#[source] std::io::Error),
/// }
/// ```
#[proc_macro_derive(PortFailure, attributes(failure))]
pub fn derive_port_failure(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expansion::finish(port_failure::expand(input.into()))
}
