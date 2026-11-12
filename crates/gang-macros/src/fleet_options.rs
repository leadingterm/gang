//! `#[derive(FleetOptions)]`: the option vocabulary and platform properties of a provisioner's
//! `[fleets.options]` struct.
//! `#[fleet_options(validate = path)]` checks one fleet's options, and
//! `#[fleet_options(validate_fleets = path)]` checks rules spanning the provisioner's fleets.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Expr, ExprLit, Field, Fields, Lit, Meta, Type};

/// One field of the options struct: one option.
struct OptionField<'a> {
    field: &'a Field,
    key: String,
    doc: String,
    required: bool,
    launch: bool,
}

impl<'a> OptionField<'a> {
    fn try_from_field(field: &'a Field) -> syn::Result<Self> {
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new(field.span(), "fleet options have named fields"))?;
        if let Some(attribute) = field.attrs.iter().find(|attribute| is_serde(attribute)) {
            return Err(syn::Error::new(
                attribute.span(),
                "fleet option fields take no serde attributes: the key is the field name and \
                 only `Option` fields are optional",
            ));
        }
        let key = ident.unraw().to_string();
        let launch = launch_attribute(field)?;
        Ok(Self {
            field,
            key,
            doc: doc_text(field),
            required: !is_option(&field.ty),
            launch,
        })
    }

    /// The `OptionSpec` literal of the field.
    fn spec(&self) -> TokenStream {
        let (key, doc, required, launch) = (&self.key, &self.doc, self.required, self.launch);
        quote! {
            ::gang_spi::OptionSpec { key: #key, doc: #doc, required: #required, launch: #launch }
        }
    }

    /// A compile-time check that the option's key is no reserved platform property. The check
    /// runs where the struct is defined, against `gang_types::property`, which owns the list.
    fn reserved_guard(&self) -> TokenStream {
        let key = &self.key;
        let message = format!(
            "fleet option `{key}` is a reserved platform property (see gang_types::property::RESERVED)"
        );
        quote_spanned! {self.field.span()=>
            const _: () = ::core::assert!(
                !::gang_spi::__private::gang_types::property::is_reserved(#key),
                #message
            );
        }
    }

    /// The field's platform property, `None` for an unset optional option.
    fn property(&self) -> TokenStream {
        let (key, ident) = (&self.key, &self.field.ident);
        if self.required {
            quote! {
                ::core::option::Option::Some((
                    #key,
                    ::std::string::ToString::to_string(&self.#ident),
                ))
            }
        } else {
            quote! {
                self.#ident
                    .as_ref()
                    .map(|value| (#key, ::std::string::ToString::to_string(value)))
            }
        }
    }
}

/// Whether `attribute` is `#[serde(..)]`.
fn is_serde(attribute: &Attribute) -> bool {
    attribute.path().is_ident("serde")
}

/// Parses the field's `#[option(..)]` attributes: the only argument is `launch`, once.
fn launch_attribute(field: &Field) -> syn::Result<bool> {
    let mut launch = false;
    for attribute in field
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("option"))
    {
        attribute.parse_nested_meta(|meta| {
            if !meta.path.is_ident("launch") {
                return Err(meta.error(
                    "unknown option argument: the only one is `launch`, which marks a field \
                     that only shapes how instances are launched",
                ));
            }
            if launch {
                return Err(meta.error("duplicate `launch` argument"));
            }
            launch = true;
            Ok(())
        })?;
    }
    Ok(launch)
}

/// Parses the struct's `#[fleet_options(..)]` validation paths, each at most once.
fn validate_paths(input: &DeriveInput) -> syn::Result<(Option<syn::Path>, Option<syn::Path>)> {
    let mut validate = None;
    let mut validate_fleets = None;
    for attribute in input
        .attrs
        .iter()
        .filter(|attribute| attribute.path().is_ident("fleet_options"))
    {
        attribute.parse_nested_meta(|meta| {
            let (slot, key) = if meta.path.is_ident("validate") {
                (&mut validate, "validate")
            } else if meta.path.is_ident("validate_fleets") {
                (&mut validate_fleets, "validate_fleets")
            } else {
                return Err(meta.error(
                    "unknown fleet_options key: expected `validate = <path>` or `validate_fleets = <path>`",
                ));
            };
            if slot.is_some() {
                return Err(meta.error(format!("duplicate `{key}` key")));
            }
            *slot = Some(meta.value()?.parse::<syn::Path>()?);
            Ok(())
        })?;
    }
    Ok((validate, validate_fleets))
}

/// Requires the struct's only container `serde` item to be `deny_unknown_fields`: without it a
/// mistyped optional key would be accepted and silently stop selecting.
fn check_container_serde(input: &DeriveInput) -> syn::Result<()> {
    let mut denies_unknown_fields = false;
    for attribute in input.attrs.iter().filter(|attribute| is_serde(attribute)) {
        attribute.parse_nested_meta(|meta| {
            if !meta.path.is_ident("deny_unknown_fields") {
                return Err(syn::Error::new_spanned(
                    &meta.path,
                    "fleet option structs take only `#[serde(deny_unknown_fields)]`: the option \
                     key is the field name and only `Option` fields are optional",
                ));
            }
            denies_unknown_fields = true;
            Ok(())
        })?;
    }
    if denies_unknown_fields {
        return Ok(());
    }
    Err(syn::Error::new(
        input.span(),
        "FleetOptions structs derive `Deserialize` with `#[serde(deny_unknown_fields)]`, so \
         a mistyped option is an error instead of silently selecting nothing",
    ))
}

/// The field's doc comment lines, trimmed and joined with spaces.
fn doc_text(field: &Field) -> String {
    field
        .attrs
        .iter()
        .filter_map(|attribute| match &attribute.meta {
            Meta::NameValue(name_value) if name_value.path.is_ident("doc") => {
                match &name_value.value {
                    Expr::Lit(ExprLit {
                        lit: Lit::Str(text),
                        ..
                    }) => Some(text.value().trim().to_owned()),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `ty` is written `Option<..>`.
fn is_option(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "Option"))
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new(
            input.span(),
            "FleetOptions derives on structs",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new(
            input.span(),
            "FleetOptions derives on structs with named fields",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "FleetOptions structs are not generic",
        ));
    }
    check_container_serde(&input)?;
    let name = &input.ident;
    let options = fields
        .named
        .iter()
        .map(OptionField::try_from_field)
        .collect::<syn::Result<Vec<_>>>()?;
    let specs = options.iter().map(OptionField::spec);
    let guards = options.iter().map(OptionField::reserved_guard);
    let properties: Vec<TokenStream> = options
        .iter()
        .filter(|option| !option.launch)
        .map(OptionField::property)
        .collect();
    let count = properties.len();
    let (validate, validate_fleets) = validate_paths(&input)?;
    let validate = validate.map(|path| {
        quote! {
            fn validate(&self) -> ::core::result::Result<(), ::std::string::String> {
                #path(self)
            }
        }
    });
    let validate_fleets = validate_fleets.map(|path| {
        quote! {
            /// Checks rules spanning the provisioner's fleets.
            fn validate_fleets(
                fleets: &[(&::gang_spi::__private::gang_types::FleetName, &Self)],
            ) -> ::core::result::Result<(), ::std::string::String> {
                #path(fleets)
            }
        }
    });
    Ok(quote! {
        impl ::gang_spi::FleetOptions for #name {
            const OPTIONS: &'static [::gang_spi::OptionSpec] = &[#(#specs),*];

            fn properties(&self) -> ::std::vec::Vec<(&'static str, ::std::string::String)> {
                let properties: [::core::option::Option<(
                    &'static str,
                    ::std::string::String,
                )>; #count] = [#(#properties),*];
                properties.into_iter().flatten().collect()
            }

            #validate
            #validate_fleets
        }

        #(#guards)*
    })
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Span, TokenStream};
    use quote::quote;

    use super::{expand, is_option};

    fn rejection(input: TokenStream) -> String {
        expand(input)
            .expect_err("expansion is rejected")
            .to_string()
    }

    fn options_with_field(name: &str) -> TokenStream {
        let field = syn::Ident::new(name, Span::call_site());
        quote! {
            #[serde(deny_unknown_fields)]
            pub struct Options {
                #field: Option<String>,
            }
        }
    }

    #[test]
    fn accepts_deny_unknown_fields_struct() {
        let input = quote! {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            pub struct Options {
                /// doc
                capacity: Capacity,
                machine_class: Option<MachineClass>,
            }
        };
        assert!(expand(input).is_ok());
    }

    #[test]
    fn guards_every_key_against_the_reserved_properties() {
        let expansion = expand(options_with_field("network"))
            .expect("expansion is accepted")
            .to_string();
        assert!(
            expansion.contains(
                ":: gang_spi :: __private :: gang_types :: property :: is_reserved (\"network\")"
            ),
            "{expansion}"
        );
        assert!(
            expansion.contains("fleet option `network` is a reserved platform property"),
            "{expansion}"
        );
    }

    #[test]
    fn guards_the_unraw_key_of_a_raw_field_name() {
        let input = quote! {
            #[serde(deny_unknown_fields)]
            pub struct Options {
                r#fleet: Option<String>,
            }
        };
        let expansion = expand(input).expect("expansion is accepted").to_string();
        assert!(expansion.contains("is_reserved (\"fleet\")"), "{expansion}");
    }

    #[test]
    fn rejects_extra_container_serde_item() {
        let input = quote! {
            #[serde(deny_unknown_fields, rename_all = "kebab-case")]
            pub struct Options {
                capacity: Capacity,
            }
        };
        assert!(rejection(input).contains("deny_unknown_fields"));
    }

    #[test]
    fn rejects_container_serde_default() {
        let input = quote! {
            #[serde(deny_unknown_fields, default)]
            pub struct Options {
                capacity: Capacity,
            }
        };
        assert!(rejection(input).contains("deny_unknown_fields"));
    }

    #[test]
    fn rejects_missing_deny_unknown_fields() {
        let input = quote! {
            pub struct Options {
                capacity: Capacity,
            }
        };
        assert!(rejection(input).contains("deny_unknown_fields"));
    }

    #[test]
    fn rejects_field_serde_attribute() {
        let input = quote! {
            #[serde(deny_unknown_fields)]
            pub struct Options {
                #[serde(rename = "x")]
                capacity: Capacity,
            }
        };
        assert!(rejection(input).contains("serde"));
    }

    #[test]
    fn is_option_matches_a_fully_qualified_option_path_only() {
        let qualified: syn::Type = syn::parse_quote!(std::option::Option<u32>);
        let bare: syn::Type = syn::parse_quote!(Option<u32>);
        let other: syn::Type = syn::parse_quote!(MyOption<u32>);
        assert!(is_option(&qualified));
        assert!(is_option(&bare));
        assert!(!is_option(&other));
    }

    #[test]
    fn rejects_unknown_option_argument() {
        let input = quote! {
            #[serde(deny_unknown_fields)]
            pub struct Options {
                #[option(hidden)]
                capacity: Capacity,
            }
        };
        assert!(rejection(input).contains("launch"));
    }

    #[test]
    fn rejects_duplicate_launch_argument() {
        let input = quote! {
            #[serde(deny_unknown_fields)]
            pub struct Options {
                #[option(launch)]
                #[option(launch)]
                ssh: Ssh,
            }
        };
        assert!(rejection(input).contains("duplicate"));
    }

    #[test]
    fn rejects_unknown_fleet_options_key() {
        let input = quote! {
            #[serde(deny_unknown_fields)]
            #[fleet_options(hook = check_options)]
            pub struct Options {
                capacity: Capacity,
            }
        };
        assert!(rejection(input).contains("validate"));
    }

    #[test]
    fn rejects_duplicate_validate_key() {
        let input = quote! {
            #[serde(deny_unknown_fields)]
            #[fleet_options(validate = check_options)]
            #[fleet_options(validate = check_options)]
            pub struct Options {
                capacity: Capacity,
            }
        };
        assert!(rejection(input).contains("duplicate"));
    }

    #[test]
    fn accepts_validate_fleets_key() {
        let input = quote! {
            #[serde(deny_unknown_fields)]
            #[fleet_options(validate = check_options, validate_fleets = check_fleets)]
            pub struct Options {
                capacity: Capacity,
            }
        };
        let expansion = expand(input).expect("expansion is accepted").to_string();
        assert!(expansion.contains("check_fleets (fleets)"), "{expansion}");
    }

    #[test]
    fn rejects_duplicate_validate_fleets_key() {
        let input = quote! {
            #[serde(deny_unknown_fields)]
            #[fleet_options(validate_fleets = check_fleets)]
            #[fleet_options(validate_fleets = check_fleets)]
            pub struct Options {
                capacity: Capacity,
            }
        };
        assert!(rejection(input).contains("duplicate `validate_fleets`"));
    }
}
