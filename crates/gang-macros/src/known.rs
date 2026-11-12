//! Derives recursive recognition of wire values.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse2};

pub(crate) fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = parse2(input)?;
    let name = &input.ident;
    let mut generics = input.generics.clone();
    for param in generics.type_params_mut() {
        param.bounds.push(syn::parse_quote!(::gang_wire::Known));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let body = match &input.data {
        Data::Struct(data) => {
            let checks = data.fields.iter().enumerate().map(|(i, field)| {
                let member = field
                    .ident
                    .clone()
                    .map_or_else(|| syn::Member::Unnamed(i.into()), syn::Member::Named);
                quote!(::gang_wire::Known::is_known(&self.#member))
            });
            quote!(true #(&& #checks)*)
        }
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                if variant_name == "Unrecognized" {
                    return match &variant.fields {
                        Fields::Named(_) => quote!(Self::#variant_name { .. } => false),
                        Fields::Unnamed(_) => quote!(Self::#variant_name ( .. ) => false),
                        Fields::Unit => quote!(Self::#variant_name => false),
                    };
                }
                let names: Vec<_> = (0..variant.fields.len()).map(|i| format_ident!("field_{i}")).collect();
                let pattern = match &variant.fields {
                    Fields::Named(fields) => {
                        let members = fields.named.iter().map(|f| &f.ident);
                        quote!({ #(#members: #names),* })
                    }
                    Fields::Unnamed(_) => quote!((#(#names),*)),
                    Fields::Unit => quote!(),
                };
                quote!(Self::#variant_name #pattern => true #(&& ::gang_wire::Known::is_known(#names))*)
            });
            quote!(match self { #(#arms),* })
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                name,
                "Known cannot be derived for unions",
            ));
        }
    };
    Ok(quote! {
        impl #impl_generics ::gang_wire::Known for #name #ty_generics #where_clause {
            fn is_known(&self) -> bool { #body }
        }
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::*;

    fn expanded(item: TokenStream) -> String {
        expand(item).expect("expansion is accepted").to_string()
    }

    fn rejection(item: TokenStream) -> String {
        expand(item).expect_err("expansion is rejected").to_string()
    }

    #[test]
    fn named_struct_checks_each_field() {
        assert_eq!(
            expanded(quote! { struct Pair { first: A, second: B } }),
            quote! {
                impl ::gang_wire::Known for Pair {
                    fn is_known(&self) -> bool {
                        true && ::gang_wire::Known::is_known(&self.first)
                            && ::gang_wire::Known::is_known(&self.second)
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn tuple_struct_checks_each_field() {
        let first = syn::Index::from(0);
        let second = syn::Index::from(1);
        assert_eq!(
            expanded(quote! { struct Wrapper(A, B); }),
            quote! {
                impl ::gang_wire::Known for Wrapper {
                    fn is_known(&self) -> bool {
                        true && ::gang_wire::Known::is_known(&self.#first)
                            && ::gang_wire::Known::is_known(&self.#second)
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn unit_struct_is_known() {
        assert_eq!(
            expanded(quote! { struct Unit; }),
            quote! {
                impl ::gang_wire::Known for Unit {
                    fn is_known(&self) -> bool { true }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn generic_struct_bounds_type_parameters() {
        assert_eq!(
            expanded(quote! { struct Boxed<T> { value: T } }),
            quote! {
                impl<T: ::gang_wire::Known> ::gang_wire::Known for Boxed<T> {
                    fn is_known(&self) -> bool {
                        true && ::gang_wire::Known::is_known(&self.value)
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn enum_checks_fields_and_rejects_unrecognized_tuple() {
        assert_eq!(
            expanded(quote! {
                enum Message {
                    Ping,
                    Data(A, B),
                    Named { x: A },
                    Unrecognized(Raw),
                }
            }),
            quote! {
                impl ::gang_wire::Known for Message {
                    fn is_known(&self) -> bool {
                        match self {
                            Self::Ping => true,
                            Self::Data(field_0, field_1) => true
                                && ::gang_wire::Known::is_known(field_0)
                                && ::gang_wire::Known::is_known(field_1),
                            Self::Named { x: field_0 } => true
                                && ::gang_wire::Known::is_known(field_0),
                            Self::Unrecognized(..) => false
                        }
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn enum_rejects_unrecognized_named_variant() {
        assert_eq!(
            expanded(quote! { enum Message { Unrecognized { raw: Raw } } }),
            quote! {
                impl ::gang_wire::Known for Message {
                    fn is_known(&self) -> bool {
                        match self { Self::Unrecognized { .. } => false }
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn enum_rejects_unrecognized_unit_variant() {
        assert_eq!(
            expanded(quote! { enum Message { Unrecognized } }),
            quote! {
                impl ::gang_wire::Known for Message {
                    fn is_known(&self) -> bool {
                        match self { Self::Unrecognized => false }
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn rejects_unions() {
        assert_eq!(
            rejection(quote! { union Bits { a: u8 } }),
            "Known cannot be derived for unions"
        );
    }

    #[test]
    fn rejects_non_type_items() {
        rejection(quote! { fn not_a_type() {} });
    }
}
