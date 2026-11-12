//! `compose!`: typed configuration enums and static dispatch for the composed plugins.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Ident, Token, Type, braced};

use crate::domain::{Cardinality, DOMAINS, Domain, FLEET_OPTIONS};
use crate::names::{delegate_macro, pascal_case};

/// `key: PluginType`
struct PluginInput {
    key: Ident,
    plugin: Type,
}

impl Parse for PluginInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key = input.parse()?;
        input.parse::<Token![:]>()?;
        Ok(Self {
            key,
            plugin: input.parse()?,
        })
    }
}

/// `domain: Name { key: PluginType, .. }`
struct DomainInput {
    domain: Ident,
    name: Ident,
    plugins: Vec<PluginInput>,
}

impl Parse for DomainInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let domain = input.parse()?;
        input.parse::<Token![:]>()?;
        let name = input.parse()?;
        let body;
        braced!(body in input);
        let plugins = Punctuated::<PluginInput, Token![,]>::parse_terminated(&body)?
            .into_iter()
            .collect();
        Ok(Self {
            domain,
            name,
            plugins,
        })
    }
}

/// A validated domain with its plugins.
struct Composed {
    domain: &'static Domain,
    name: Ident,
    config: Ident,
    plugins: Vec<PluginInput>,
}

/// Matches every written domain against the known ones; each appears exactly once. A
/// `ZeroOrOne` domain may be omitted or written without plugins: its types are then uninhabited.
fn resolve(inputs: Vec<DomainInput>) -> syn::Result<Vec<Composed>> {
    let accepted = DOMAINS.map(|domain| domain.key).join(", ");
    let mut inputs: Vec<Option<DomainInput>> = inputs.into_iter().map(Some).collect();
    let mut composed = Vec::with_capacity(DOMAINS.len());
    for domain in &DOMAINS {
        let position = inputs.iter().position(|input| {
            input
                .as_ref()
                .is_some_and(|input| input.domain == domain.key)
        });
        let Some(input) = position.and_then(|position| inputs[position].take()) else {
            if domain.cardinality == Cardinality::ZeroOrOne {
                composed.push(Composed::without_plugins(domain));
                continue;
            }
            return Err(syn::Error::new(
                Span::call_site(),
                format!(
                    "compose! is missing the `{}` domain; expected {accepted}",
                    domain.key
                ),
            ));
        };
        if input.plugins.is_empty() && domain.cardinality != Cardinality::ZeroOrOne {
            return Err(syn::Error::new(
                input.domain.span(),
                "a domain needs at least one plugin",
            ));
        }
        for (index, plugin) in input.plugins.iter().enumerate() {
            if input.plugins[..index]
                .iter()
                .any(|earlier| earlier.key == plugin.key)
            {
                let message = format!("duplicate plugin key `{}`", plugin.key);
                return Err(syn::Error::new(plugin.key.span(), message));
            }
        }
        composed.push(Composed {
            domain,
            config: format_ident!("{}Config", input.name),
            name: input.name,
            plugins: input.plugins,
        });
    }
    if let Some(unknown) = inputs.into_iter().flatten().next() {
        return Err(syn::Error::new(
            unknown.domain.span(),
            format!("unknown domain `{}`; expected {accepted}", unknown.domain),
        ));
    }
    Ok(composed)
}

impl Composed {
    /// A `ZeroOrOne` domain the invocation leaves out: named after its associated type.
    fn without_plugins(domain: &'static Domain) -> Self {
        let name = format_ident!("{}", domain.associated_type);
        Self {
            domain,
            config: format_ident!("{name}Config"),
            name,
            plugins: Vec::new(),
        }
    }

    fn keys(&self) -> Vec<String> {
        self.plugins
            .iter()
            .map(|plugin| plugin.key.to_string())
            .collect()
    }

    fn variants(&self) -> Vec<Ident> {
        self.plugins
            .iter()
            .map(|plugin| pascal_case(&plugin.key))
            .collect()
    }

    /// The identifiers of the plugin keys: the fields of an `AnyOf` configuration.
    fn fields(&self) -> Vec<&Ident> {
        self.plugins.iter().map(|plugin| &plugin.key).collect()
    }

    fn plugin_types(&self) -> Vec<&Type> {
        self.plugins.iter().map(|plugin| &plugin.plugin).collect()
    }

    /// The documentation of each plugin's variant.
    fn variant_docs(&self) -> Vec<String> {
        self.keys()
            .iter()
            .map(|key| format!("The `{key}` plugin."))
            .collect()
    }

    /// The key enum of an `AnyOf` domain: `Sandbox` names `SandboxKey`.
    fn key_enum(&self) -> Ident {
        format_ident!("{}Key", self.name)
    }

    /// The dispatch enum of an associated type of the domain's ports: `Provisioner` and
    /// `FleetOptions` name `ProvisionerFleetOptions`.
    fn associated_enum(&self, associated: &str) -> Ident {
        format_ident!("{}{associated}", self.name)
    }

    /// The accessors of the associated enums that narrow a value to one plugin's, by key:
    /// `as_alpha` borrows it and `into_alpha` takes it.
    fn accessors(&self) -> (Vec<Ident>, Vec<Ident>) {
        self.plugins
            .iter()
            .map(|plugin| {
                (
                    format_ident!("as_{}", plugin.key),
                    format_ident!("into_{}", plugin.key),
                )
            })
            .unzip()
    }

    /// The `map_err` closure that names the domain in a plugin's build error.
    fn build_failure() -> TokenStream {
        quote!(|source| ::gang_spi::BuildError::new(Self::DOMAIN, source))
    }

    /// `impl Name { const DOMAIN }`: the domain's key, as a build error names it.
    fn domain_tokens(&self) -> TokenStream {
        let (name, key) = (&self.name, self.domain.key);
        let doc = format!("The key of the `{key}` domain, as a build error names it.");
        quote! {
            impl #name {
                #[doc = #doc]
                pub const DOMAIN: &'static str = #key;
            }
        }
    }

    /// The impls of the domain's ports for the dispatch enum. A port that declares associated
    /// types gets the dispatch enums of them.
    fn delegations(&self) -> TokenStream {
        let name = &self.name;
        let variants = self.variants();
        let (as_ref, into) = self.accessors();
        let delegations = self.domain.ports.iter().map(|port| {
            let delegate = delegate_macro(port.name);
            let associated = port
                .associated
                .iter()
                .map(|associated| self.associated_enum(associated.name));
            quote! {
                ::gang_spi::#delegate!(
                    self::#name { #( #variants => #as_ref, #into ),* }
                    [ #( #associated ),* ]
                );
            }
        });
        quote!(#(#delegations)*)
    }

    /// The documentation of an associated type's dispatch enum: the `[fleets.options]` of one
    /// fleet, or the `associated` of one call.
    fn associated_doc(&self, associated: &Ident) -> String {
        if associated == FLEET_OPTIONS {
            format!(
                "The `[fleets.options]` of one fleet, typed by the `{}` plugin that serves it.",
                self.domain.key
            )
        } else {
            format!(
                "The `{associated}` of one call, typed by the `{}` plugin that serves it.",
                self.domain.key
            )
        }
    }

    /// The delegate-macro invocations that make an associated type's dispatch enum implement
    /// each trait its port delegates, through each trait's `__gang_delegate_<Trait>` macro.
    fn delegated_impls(&self, enum_: &Ident, delegated: &[&str]) -> TokenStream {
        let variants = self.variants();
        let calls = delegated.iter().map(|config_trait| {
            let delegate = delegate_macro(config_trait);
            quote!(::gang_spi::#delegate!(self::#enum_ { #(#variants),* });)
        });
        quote!(#(#calls)*)
    }

    /// The enums holding the value an associated type of the domain's ports takes in whichever
    /// plugin the configuration selected, one variant per plugin, with the accessors that narrow
    /// them, and the traits each enum implements by delegation. Empty for a domain whose ports
    /// declare no associated types.
    fn associated_tokens(&self) -> TokenStream {
        let enums = self.domain.associated_types().map(|(port, associated)| {
            let (delegated, associated) = (associated.delegated, associated.name);
            let enum_ = self.associated_enum(associated);
            let port = format_ident!("{port}");
            let associated = format_ident!("{associated}");
            let (variants, types) = (self.variants(), self.plugin_types());
            let (as_ref, into) = self.accessors();
            let enum_doc = self.associated_doc(&associated);
            let variant_docs: Vec<String> = self
                .keys()
                .iter()
                .map(|key| format!("The `{associated}` of the `{key}` plugin."))
                .collect();
            let access_docs: Vec<(String, String)> = self
                .keys()
                .iter()
                .map(|key| {
                    (
                        format!("The `{associated}` of the `{key}` plugin, when this is one."),
                        format!("The `{associated}` of the `{key}` plugin, or the unchanged value."),
                    )
                })
                .collect();
            let others: Vec<Vec<&Ident>> = (0..variants.len())
                .map(|kept| {
                    variants
                        .iter()
                        .enumerate()
                        .filter_map(|(index, variant)| (index != kept).then_some(variant))
                        .collect()
                })
                .collect();
            let borrows = variants.iter().zip(&others).map(|(variant, others)| {
                if others.is_empty() {
                    quote!(Self::#variant(value) => ::core::option::Option::Some(value),)
                } else {
                    quote! {
                        Self::#variant(value) => ::core::option::Option::Some(value),
                        #( Self::#others(_) )|* => ::core::option::Option::None,
                    }
                }
            });
            let takes = variants.iter().zip(&others).map(|(variant, others)| {
                if others.is_empty() {
                    quote!(Self::#variant(value) => ::core::result::Result::Ok(value),)
                } else {
                    quote! {
                        Self::#variant(value) => ::core::result::Result::Ok(value),
                        other @ ( #( Self::#others(_) )|* ) => ::core::result::Result::Err(other),
                    }
                }
            });
            let (borrow_docs, take_docs): (Vec<_>, Vec<_>) = access_docs.into_iter().unzip();
            let accessors = if variants.is_empty() {
                quote!()
            } else {
                quote! {
                    impl #enum_ {
                        #(
                            #[doc = #borrow_docs]
                            #[must_use]
                            pub fn #as_ref(
                                &self,
                            ) -> ::core::option::Option<&<#types as ::gang_spi::#port>::#associated> {
                                match self { #borrows }
                            }

                            #[doc = #take_docs]
                            ///
                            /// # Errors
                            ///
                            /// The unchanged value when it belongs to another plugin.
                            pub fn #into(
                                self,
                            ) -> ::core::result::Result<<#types as ::gang_spi::#port>::#associated, Self> {
                                match self { #takes }
                            }
                        )*
                    }
                }
            };
            let delegate_calls = self.delegated_impls(&enum_, delegated);
            quote! {
                #[doc = #enum_doc]
                #[derive(::core::fmt::Debug)]
                pub enum #enum_ {
                    #(
                        #[doc = #variant_docs]
                        #variants(<#types as ::gang_spi::#port>::#associated),
                    )*
                }

                #accessors
                #delegate_calls
            }
        });
        quote!(#(#enums)*)
    }

    /// The `PluginSet` items that parse, validate and render the fleet options, including
    /// validation across each provisioner's fleets. Empty for a domain without fleet options.
    fn fleet_options_items(&self) -> TokenStream {
        if !self.domain.has_fleet_options() {
            return quote!();
        }
        let (config, options) = (&self.config, self.associated_enum(FLEET_OPTIONS));
        let selected = format_ident!("{}", self.domain.key);
        let variants = self.variants();
        let types = self.plugin_types();
        let (as_ref, _) = self.accessors();
        quote! {
            type FleetOptions = #options;

            fn fleet_options(
                #selected: &#config,
                table: ::gang_config::FleetOptionsTable,
            ) -> ::core::result::Result<
                #options,
                ::gang_config::FleetOptionsError,
            > {
                match #selected {
                    #(
                        #config::#variants(_) => {
                            ::gang_config::validated_fleet_options(
                                table,
                                ::gang_spi::FleetOptions::validate,
                            )
                            .map(#options::#variants)
                        }
                    )*
                }
            }

            /// Checks each provisioner's rules spanning its fleets.
            fn validate_fleets(
                fleets: &[(&::gang_spi::__private::gang_types::FleetName, &#options)],
            ) -> ::core::result::Result<(), ::std::string::String> {
                #(
                    let typed: ::std::vec::Vec<_> = fleets.iter()
                        .filter_map(|&(name, options)| {
                            ::core::option::Option::Some((name, options.#as_ref()?))
                        })
                        .collect();
                    <<#types as ::gang_spi::ProvisionerPort>::FleetOptions as ::gang_spi::FleetOptions>::validate_fleets(&typed)?;
                )*
                ::core::result::Result::Ok(())
            }

            fn option_properties(
                options: &#options,
            ) -> ::std::vec::Vec<(&'static str, ::std::string::String)> {
                match options {
                    #(
                        #options::#variants(options) => {
                            ::gang_spi::FleetOptions::properties(options)
                        }
                    )*
                }
            }
        }
    }

    /// The `impl` of each settings trait the domain's configuration enum implements, through
    /// the delegate macro `#[settings]` recorded on the trait. Empty for domains without config
    /// traits.
    fn config_trait_impls(&self) -> TokenStream {
        let (config, variants) = (&self.config, self.variants());
        let impls = self.domain.config_traits.iter().map(|config_trait| {
            let delegate = delegate_macro(config_trait);
            quote! {
                ::gang_spi::__private::gang_types::#delegate!(self::#config { #(#variants),* });
            }
        });
        quote!(#(#impls)*)
    }

    /// `Plugins::provisioner_options`: the options every plugin of the domain accepts, by key.
    fn options_listing(&self) -> TokenStream {
        let Some((port, associated)) = self
            .domain
            .associated_types()
            .find(|(_, associated)| associated.name == FLEET_OPTIONS)
        else {
            return quote!();
        };
        let (port, associated) = (
            format_ident!("{port}"),
            format_ident!("{}", associated.name),
        );
        let (keys, types) = (self.keys(), self.plugin_types());
        let listing = format_ident!("{}_options", self.domain.key);
        let doc = format!(
            "The options each `{}` plugin accepts in `[fleets.options]`, by plugin key.",
            self.domain.key
        );
        quote! {
            impl Plugins {
                #[doc = #doc]
                #[must_use]
                pub const fn #listing() -> &'static [(&'static str, &'static [::gang_spi::OptionSpec])] {
                    &[
                        #(
                            (
                                #keys,
                                <<#types as ::gang_spi::#port>::#associated
                                    as ::gang_spi::FleetOptions>::OPTIONS,
                            ),
                        )*
                    ]
                }
            }
        }
    }

    /// The `PluginSet` associated types the domain fills: its key for an `AnyOf` domain, then its configuration.
    fn assignments(&self) -> TokenStream {
        let associated = format_ident!("{}", self.domain.associated_type);
        let config = &self.config;
        let key = match self.domain.cardinality {
            Cardinality::ExactlyOne | Cardinality::ZeroOrOne => quote!(),
            Cardinality::AnyOf => {
                let key_associated = format_ident!("{}Key", self.domain.associated_type);
                let key_enum = self.key_enum();
                quote!(type #key_associated = #key_enum;)
            }
        };
        quote!(#key type #associated = #config;)
    }

    fn expand(&self) -> TokenStream {
        let dispatch = match self.domain.cardinality {
            Cardinality::ExactlyOne | Cardinality::ZeroOrOne => self.tagged_enum(),
            Cardinality::AnyOf => self.any_of(),
        };
        let associated = self.associated_tokens();
        let delegations = self.delegations();
        let config_traits = self.config_trait_impls();
        quote! {
            #dispatch
            #associated
            #delegations
            #config_traits
        }
    }

    /// An externally tagged enum: serde rejects several sections and lists the keys. A domain
    /// that is `ExactlyOne` rejects zero sections too; for `ZeroOrOne` the configuration makes
    /// the field optional.
    fn tagged_enum(&self) -> TokenStream {
        let (name, config) = (&self.name, &self.config);
        let (keys, variants, types) = (self.keys(), self.variants(), self.plugin_types());
        let how_many = match self.domain.cardinality {
            Cardinality::ZeroOrOne => "at most one plugin",
            Cardinality::ExactlyOne | Cardinality::AnyOf => "exactly one plugin",
        };
        let config_doc = format!("The `[{}.<plugin>]` section: {how_many}.", self.domain.key);
        let name_doc = format!("The selected `{}` plugin.", self.domain.key);
        let variant_docs = self.variant_docs();
        let build_doc = format!(
            "Builds the plugin the `{}` section selects.",
            self.domain.key
        );
        let build_failure = Self::build_failure();
        let build_body = if self.plugins.is_empty() {
            quote! {
                let _ = context;
                match config {}
            }
        } else {
            quote! {
                ::core::result::Result::Ok(match config {
                    #(
                        #config::#variants(config) => Self::#variants(
                            <#types as ::gang_spi::Plugin>::build(config, context)
                                .map_err(#build_failure)?,
                        ),
                    )*
                })
            }
        };
        let key_body = if self.plugins.is_empty() {
            quote!(match *self {})
        } else {
            quote! {
                match self {
                    #( Self::#variants(_) => #keys, )*
                }
            }
        };
        let key_doc = format!(
            "The key of the `[{}.<plugin>]` section this selects.",
            self.domain.key
        );
        let domain = self.domain_tokens();
        quote! {
            #[doc = #config_doc]
            #[derive(
                ::core::fmt::Debug,
                ::core::clone::Clone,
                ::gang_spi::__private::serde::Serialize,
                ::gang_spi::__private::serde::Deserialize,
            )]
            #[serde(crate = "::gang_spi::__private::serde")]
            pub enum #config {
                #(
                    #[doc = #variant_docs]
                    #[serde(rename = #keys)]
                    #variants(<#types as ::gang_spi::Plugin>::Config),
                )*
            }

            impl #config {
                #[doc = #key_doc]
                #[must_use]
                pub const fn key(&self) -> &'static str {
                    #key_body
                }
            }

            #[doc = #name_doc]
            pub enum #name {
                #(
                    #[doc = #variant_docs]
                    #variants(#types),
                )*
            }

            #domain

            impl #name {
                #[doc = #build_doc]
                ///
                /// # Errors
                ///
                /// The selected plugin's build error, naming the domain.
                pub fn build(
                    config: #config,
                    context: &::gang_spi::PluginContext<'_>,
                ) -> ::core::result::Result<Self, ::gang_spi::BuildError> {
                    #build_body
                }
            }
        }
    }

    /// The key enum of an `AnyOf` domain: a closed string enum of its keys.
    fn key_enum_tokens(&self) -> TokenStream {
        let key = self.key_enum();
        let (keys, variants) = (self.keys(), self.variants());
        let key_doc = format!(
            "Names one `[{}.<key>]` section, by its key.",
            self.domain.key
        );
        let variant_docs = self.variant_docs();
        let kind = self.domain.key;
        quote! {
            ::gang_spi::__private::string_enum! {
                #[doc = #key_doc]
                pub enum #key(#kind) {
                    #(
                        #[doc = #variant_docs]
                        #variants = #keys,
                    )*
                }
            }
        }
    }

    /// The `SandboxSelection` impl: which key names a configured section, and the programs of the section a key names.
    fn selection_tokens(&self) -> TokenStream {
        let (config, key) = (&self.config, self.key_enum());
        let (variants, fields) = (self.variants(), self.fields());
        let visit_binding = if fields.is_empty() {
            quote! { let _ = visit; }
        } else {
            quote! { let mut visit = visit; }
        };
        quote! {
            impl ::gang_config::SandboxSelection for #config {
                type Key = #key;

                fn is_configured(&self, key: #key) -> bool {
                    match key {
                        #( #key::#variants => self.#fields.is_some(), )*
                    }
                }

                fn try_for_each_program<E>(
                    &mut self,
                    key: #key,
                    visit: impl FnMut(&mut ::std::path::PathBuf) -> ::core::result::Result<(), E>,
                ) -> ::core::result::Result<(), E> {
                    #visit_binding
                    match key {
                        #(
                            #key::#variants => match &mut self.#fields {
                                ::core::option::Option::Some(section) => {
                                    ::gang_spi::SandboxPrograms::try_for_each_program(section, &mut visit)
                                }
                                ::core::option::Option::None => ::core::result::Result::Ok(()),
                            },
                        )*
                    }
                }
            }
        }
    }

    /// The `impl {name} { build }` of an `AnyOf` domain: builds the section the key names.
    fn build_tokens(&self) -> TokenStream {
        let (name, config, key) = (&self.name, &self.config, self.key_enum());
        let (variants, types) = (self.variants(), self.plugin_types());
        let fields = self.fields();
        let build_doc = format!(
            "Builds the plugin the `[{}.<key>]` section selects.",
            self.domain.key
        );
        let missing = format!(
            "{0} `{{key}}` has no [{0}.{{key}}] section",
            self.domain.key
        );
        let build_failure = Self::build_failure();
        quote! {
            impl #name {
                #[doc = #build_doc]
                ///
                /// # Errors
                ///
                /// The selected plugin's build error, or a missing section, naming the domain.
                pub fn build(
                    key: #key,
                    config: #config,
                    context: &::gang_spi::PluginContext<'_>,
                ) -> ::core::result::Result<Self, ::gang_spi::BuildError> {
                    match key {
                        #(
                            #key::#variants => match config.#fields {
                                ::core::option::Option::Some(section) => {
                                    ::core::result::Result::Ok(Self::#variants(
                                        <#types as ::gang_spi::Plugin>::build(section, context)
                                            .map_err(#build_failure)?,
                                    ))
                                }
                                ::core::option::Option::None => ::core::result::Result::Err(
                                    ::gang_spi::BuildError::new(
                                        Self::DOMAIN,
                                        ::gang_spi::PortError::config(::std::format!(#missing)),
                                    ),
                                ),
                            },
                        )*
                    }
                }
            }
        }
    }

    /// A struct with one optional section per plugin, several of which may be active; a key enum names each section.
    fn any_of(&self) -> TokenStream {
        let (name, config) = (&self.name, &self.config);
        let (keys, variants, types) = (self.keys(), self.variants(), self.plugin_types());
        let fields = self.fields();
        let config_doc = format!(
            "The `[{}.<plugin>]` sections: any selection of plugins.",
            self.domain.key
        );
        let field_docs = keys
            .iter()
            .map(|key| format!("The `[{}.{key}]` section.", self.domain.key));
        let variant_docs = self.variant_docs();
        let name_doc = format!("One built `{}` plugin.", self.domain.key);
        let config_struct = quote! {
            #[doc = #config_doc]
            #[derive(
                ::core::fmt::Debug,
                ::core::clone::Clone,
                ::gang_spi::__private::serde::Serialize,
                ::gang_spi::__private::serde::Deserialize,
            )]
            #[serde(crate = "::gang_spi::__private::serde", deny_unknown_fields)]
            pub struct #config {
                #(
                    #[doc = #field_docs]
                    #[serde(default, skip_serializing_if = "::core::option::Option::is_none")]
                    pub #fields: ::core::option::Option<<#types as ::gang_spi::Plugin>::Config>,
                )*
            }
        };
        let dispatch = quote! {
            #[doc = #name_doc]
            pub enum #name {
                #(
                    #[doc = #variant_docs]
                    #variants(#types),
                )*
            }
        };
        let key_enum = self.key_enum_tokens();
        let selection = if self.domain.sandbox_selection {
            self.selection_tokens()
        } else {
            quote!()
        };
        let domain = self.domain_tokens();
        let build = self.build_tokens();
        quote! {
            #config_struct
            #key_enum
            #selection
            #dispatch
            #domain
            #build
        }
    }
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let inputs = syn::parse::Parser::parse2(
        Punctuated::<DomainInput, Token![,]>::parse_terminated,
        input,
    )?;
    let composed = resolve(inputs.into_iter().collect())?;
    let domains = composed.iter().map(Composed::expand).collect::<Vec<_>>();
    let assignments = composed.iter().map(Composed::assignments);
    let fleet_items = composed.iter().map(Composed::fleet_options_items);
    let listings = composed.iter().map(Composed::options_listing);
    Ok(quote! {
        #(#domains)*

        /// The configuration types of this composition.
        #[derive(::core::fmt::Debug, ::core::clone::Clone, ::core::cmp::PartialEq)]
        pub struct Plugins;

        impl ::gang_config::PluginSet for Plugins {
            #(#assignments)*
            #(#fleet_items)*
        }

        #(#listings)*
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::*;

    fn rejection(input: TokenStream) -> String {
        expand(input)
            .expect_err("expansion is rejected")
            .to_string()
    }

    /// Every required domain with one plugin each, so that only the case under test fails.
    fn invocation(provisioner: &TokenStream) -> TokenStream {
        quote! {
            provisioner: Provisioner { #provisioner },
            cache: Cache { store: CacheStore },
            signer: Signer { signer: SignerImpl },
            scheduler: Scheduler { scheduler: SchedulerImpl },
            database: Database { database: DatabaseImpl },
            sandbox: Sandbox { sandbox: SandboxImpl },
            identity_provider: IdentityProvider { idp: IdpImpl },
            client_authentication: ClientAuthentication { auth: ClientAuthImpl },
            worker_authentication: WorkerAuthentication { auth: WorkerAuthImpl },
        }
    }

    #[test]
    fn generated_config_contracts_use_the_direct_dependency() {
        let output = expand(invocation(&quote! { a: P }))
            .expect("the invocation expands")
            .to_string();
        for item in [
            "PluginSet",
            "SandboxSelection",
            "FleetOptionsTable",
            "FleetOptionsError",
            "validated_fleet_options",
        ] {
            assert!(
                output.contains(&format!(":: gang_config :: {item}")),
                "{output}"
            );
        }
        assert!(!output.contains("__private :: gang_config"), "{output}");
    }

    #[test]
    fn any_of_without_sandbox_selection_does_not_implement_it() {
        static DOMAIN: Domain = Domain {
            key: "other",
            associated_type: "Other",
            cardinality: Cardinality::AnyOf,
            sandbox_selection: false,
            ports: &[],
            config_traits: &[],
        };
        let composed = Composed::without_plugins(&DOMAIN);
        let output = composed.expand().to_string();
        assert!(!output.contains("SandboxSelection"), "{output}");
    }

    #[test]
    fn rejects_duplicate_plugin_key() {
        let input = invocation(&quote! { a: P, a: Q });
        assert!(rejection(input).contains("duplicate plugin key `a`"));
    }

    #[test]
    fn rejects_domain_without_plugins() {
        let input = invocation(&quote! {});
        assert!(rejection(input).contains("a domain needs at least one plugin"));
    }

    #[test]
    fn rejects_unknown_domain() {
        let mut input = invocation(&quote! { a: P });
        input.extend(quote! { bogus: Bogus { a: A } });
        assert!(rejection(input).contains("unknown domain `bogus`"));
    }

    #[test]
    fn config_traits_delegate_to_the_selected_plugin() {
        let output = expand(invocation(&quote! { a: P }))
            .expect("the invocation expands")
            .to_string();
        for (config_trait, config, variant) in [
            ("SchedulerSettings", "SchedulerConfig", "Scheduler"),
            (
                "AuthenticationSettings",
                "ClientAuthenticationConfig",
                "Auth",
            ),
            (
                "AuthenticationSettings",
                "WorkerAuthenticationConfig",
                "Auth",
            ),
        ] {
            let delegation = format!(
                ":: gang_spi :: __private :: gang_types :: __gang_delegate_{config_trait} ! \
                 (self :: {config} {{ {variant} }})"
            );
            assert!(
                output.contains(&delegation),
                "{config_trait} for {config}: {output}"
            );
        }
        assert_eq!(
            output.matches("__gang_delegate_SchedulerSettings").count(),
            1
        );
        assert_eq!(
            output
                .matches("__gang_delegate_AuthenticationSettings")
                .count(),
            2
        );
    }

    #[test]
    fn limits_scope_delegates_action_scope_to_the_selected_plugins() {
        let mut input = invocation(&quote! { a: P });
        input.extend(quote! { limits: Limits { a: L, b: M } });
        let output = expand(input).expect("the invocation expands").to_string();
        assert!(
            output.contains(
                ":: gang_spi :: __gang_delegate_ActionScope ! (self :: LimitsScope { A , B })"
            ),
            "{output}"
        );
    }

    #[test]
    fn limits_scope_delegates_action_scope_without_plugins() {
        let output = expand(invocation(&quote! { a: P }))
            .expect("the invocation expands")
            .to_string();
        assert!(output.contains("pub enum LimitsScope"), "{output}");
        assert_eq!(
            output.matches("__gang_delegate_ActionScope").count(),
            1,
            "{output}"
        );
    }
}
