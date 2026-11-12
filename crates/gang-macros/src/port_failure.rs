//! `#[derive(PortFailure)]`: classifies a plugin's error enum as a `gang_spi::PortError`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Fields, Ident, LitStr, Variant};

/// Whether the attribute is a `#[failure(..)]` marker.
fn is_failure(attribute: &Attribute) -> bool {
    attribute.path().is_ident("failure")
}

/// What a variant becomes in the `PortError` it converts to.
enum Class {
    /// `PortError::Config`: the plugin's configuration cannot be used.
    Config,
    /// `PortError::Unavailable`: the backend may answer on a later try.
    Unavailable,
    /// `PortError::Backend`: trying again will not help. The default.
    Backend,
    /// A refusal with the original error as its source.
    Rejected(Ident),
    /// A missing subject carrying the variant's only field.
    NotFound(Ident),
    /// An unsupported operation.
    Unsupported(LitStr),
    /// An already classified port error.
    Transparent,
}

impl Class {
    /// Reads the variant's optional failure marker, defaulting to `Backend`.
    fn of(variant: &Variant) -> syn::Result<Self> {
        if let Some(attribute) = variant
            .fields
            .iter()
            .flat_map(|field| &field.attrs)
            .find(|attribute| is_failure(attribute))
        {
            return Err(syn::Error::new(
                attribute.span(),
                "`#[failure]` goes on enum variants, not on fields",
            ));
        }
        let mut markers = variant
            .attrs
            .iter()
            .filter(|attribute| is_failure(attribute));
        let Some(marker) = markers.next() else {
            return Ok(Self::Backend);
        };
        if let Some(second) = markers.next() {
            return Err(syn::Error::new(
                second.span(),
                "a variant has at most one `#[failure(..)]`",
            ));
        }
        let class = Self::parse(marker)?;
        if matches!(class, Self::NotFound(_) | Self::Transparent) && variant.fields.len() != 1 {
            return Err(syn::Error::new(
                variant.fields.span(),
                "`not_found` and `transparent` require exactly one field",
            ));
        }
        Ok(class)
    }

    fn parse(marker: &Attribute) -> syn::Result<Self> {
        let mut class = None;
        marker.parse_nested_meta(|meta| {
            let chosen = if meta.path.is_ident("config") {
                Self::Config
            } else if meta.path.is_ident("unavailable") {
                Self::Unavailable
            } else if meta.path.is_ident("backend") {
                Self::Backend
            } else if meta.path.is_ident("rejected") {
                let content;
                syn::parenthesized!(content in meta.input);
                let reason = content.parse()?;
                if !content.is_empty() {
                    return Err(content.error("expected one rejection variant"));
                }
                Self::Rejected(reason)
            } else if meta.path.is_ident("not_found") {
                let content;
                syn::parenthesized!(content in meta.input);
                let subject = content.parse()?;
                if !content.is_empty() {
                    return Err(content.error("expected one subject variant"));
                }
                Self::NotFound(subject)
            } else if meta.path.is_ident("unsupported") {
                let content;
                syn::parenthesized!(content in meta.input);
                let operation = content.parse()?;
                if !content.is_empty() {
                    return Err(content.error("expected one operation string"));
                }
                Self::Unsupported(operation)
            } else if meta.path.is_ident("transparent") {
                Self::Transparent
            } else {
                return Err(meta.error("expected `config`, `unavailable`, `backend`, `rejected(<reason>)`, `not_found(<subject>)`, `unsupported(\"<operation>\")`, or `transparent`"));
            };
            if class.replace(chosen).is_some() {
                return Err(meta.error("a variant has one failure class"));
            }
            Ok(())
        })?;
        class.ok_or_else(|| {
            syn::Error::new(
                marker.span(),
                "expected `config`, `unavailable`, `backend`, `rejected(<reason>)`, `not_found(<subject>)`, `unsupported(\"<operation>\")`, or `transparent`",
            )
        })
    }

    /// The `PortError` this class builds from the bound error or field.
    fn tokens(&self) -> TokenStream {
        match self {
            Self::Config => quote!(::gang_spi::PortError::config(error)),
            Self::Unavailable => quote!(::gang_spi::PortError::unavailable(error)),
            Self::Backend => quote!(::gang_spi::PortError::backend(error)),
            Self::Rejected(reason) => {
                quote!(::gang_spi::PortError::rejected(::gang_spi::Rejection::#reason, error))
            }
            Self::NotFound(subject) => {
                quote!(::gang_spi::PortError::NotFound(::gang_spi::Subject::#subject(field)))
            }
            Self::Unsupported(operation) => {
                quote!(::gang_spi::PortError::Unsupported { operation: #operation })
            }
            Self::Transparent => quote!(::core::convert::Into::into(field)),
        }
    }
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let name = &input.ident;
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new(
            input.span(),
            "PortFailure derives on enums",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "PortFailure derives on enums without generics",
        ));
    }
    if let Some(attribute) = input.attrs.iter().find(|attribute| is_failure(attribute)) {
        return Err(syn::Error::new(
            attribute.span(),
            "`#[failure]` goes on enum variants, not on the enum",
        ));
    }
    let arms = data
        .variants
        .iter()
        .map(|variant| {
            let class = Class::of(variant)?;
            let ident = &variant.ident;
            let binds_field = matches!(class, Class::NotFound(_) | Class::Transparent);
            let pattern = if binds_field {
                match &variant.fields {
                    Fields::Unnamed(_) => quote!(#name::#ident(field)),
                    Fields::Named(fields) => {
                        let field_name = &fields.named[0].ident;
                        quote!(#name::#ident { #field_name: field })
                    }
                    Fields::Unit => {
                        return Err(syn::Error::new(
                            variant.span(),
                            "expected exactly one field",
                        ));
                    }
                }
            } else if matches!(class, Class::Unsupported(_)) {
                match &variant.fields {
                    Fields::Unit => quote!(#name::#ident),
                    Fields::Unnamed(_) => quote!(#name::#ident(..)),
                    Fields::Named(_) => quote!(#name::#ident { .. }),
                }
            } else {
                match &variant.fields {
                    Fields::Unit => quote!(error @ #name::#ident),
                    Fields::Unnamed(_) => quote!(error @ #name::#ident(..)),
                    Fields::Named(_) => quote!(error @ #name::#ident { .. }),
                }
            };
            let output = class.tokens();
            Ok(quote!(#pattern => #output,))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(quote! {
        impl ::core::convert::From<#name> for ::gang_spi::PortError {
            fn from(error: #name) -> Self {
                match error {
                    #(#arms)*
                }
            }
        }
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

    #[test]
    fn rejects_failure_on_the_enum() {
        let input = quote! { #[failure(config)] enum Failure { A } };
        assert!(rejection(input).contains("not on the enum"));
    }

    #[test]
    fn rejects_failure_on_a_field() {
        let tuple = quote! { enum Failure { A(#[failure(config)] String) } };
        assert!(rejection(tuple).contains("not on fields"));
        let named = quote! { enum Failure { A { #[failure(config)] code: u8 } } };
        assert!(rejection(named).contains("not on fields"));
    }

    #[test]
    fn rejects_two_markers_on_a_variant() {
        let input = quote! { enum Failure { #[failure(config)] #[failure(backend)] A } };
        assert!(rejection(input).contains("at most one"));
    }

    #[test]
    fn rejects_unknown_class() {
        let input = quote! { enum Failure { #[failure(retry)] A } };
        assert!(rejection(input).contains("expected `config`"));
    }

    #[test]
    fn rejected_and_other_classes_keep_the_error() {
        let input = quote! {
            enum Failure {
                #[failure(rejected(Quota))]
                A,
                #[failure(config)]
                B(String),
            }
        };
        let expansion = expand(input).expect("expansion is accepted").to_string();
        assert!(
            expansion.contains(
                ":: gang_spi :: PortError :: rejected (:: gang_spi :: Rejection :: Quota , error)"
            ),
            "{expansion}"
        );
        assert!(
            expansion.contains(":: gang_spi :: PortError :: config (error)"),
            "{expansion}"
        );
    }

    #[test]
    fn field_classes_require_one_field() {
        for input in [
            quote! { enum Failure { #[failure(transparent)] A } },
            quote! { enum Failure { #[failure(not_found(Provider))] A(u8, u8) } },
        ] {
            assert!(rejection(input).contains("exactly one field"));
        }
    }

    #[test]
    fn rejects_extra_attribute_arguments() {
        let input = quote! { enum Failure { #[failure(rejected(PolicyDenied, InvalidProof))] A } };
        assert!(rejection(input).contains("one rejection variant"));
    }
}
