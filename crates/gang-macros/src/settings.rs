//! `#[settings]`: records a settings trait's methods for `compose!`, which implements the trait
//! for a configuration enum by delegating to the selected plugin's settings.

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{FnArg, Ident, ItemTrait, Pat, Receiver, Signature, TraitItem, TraitItemFn};

use crate::names::delegate_macro;

/// One settings method the configuration enum delegates: one that borrows `self`.
struct Delegation<'a> {
    signature: &'a Signature,
    receiver: &'a Receiver,
    arguments: Vec<&'a Ident>,
}

impl<'a> Delegation<'a> {
    /// The delegation of `method`, or `None` for a method that takes `self` by value: such a
    /// method keeps its default body, which reaches the plugin through the delegated methods.
    fn of(method: &'a TraitItemFn) -> syn::Result<Option<Self>> {
        let signature = &method.sig;
        if let Some(generic) = signature.generics.params.first() {
            return Err(syn::Error::new(
                generic.span(),
                "settings methods take no generic parameters",
            ));
        }
        let receiver = signature
            .receiver()
            .ok_or_else(|| syn::Error::new(signature.span(), "settings methods take `self`"))?;
        if receiver.reference.is_none() {
            return match method.default {
                Some(_) => Ok(None),
                None => Err(syn::Error::new(
                    receiver.span(),
                    "a settings method that takes `self` by value needs a default body",
                )),
            };
        }
        let arguments = signature
            .inputs
            .iter()
            .filter_map(|input| match input {
                FnArg::Typed(typed) => Some(typed),
                FnArg::Receiver(_) => None,
            })
            .map(|typed| match &*typed.pat {
                Pat::Ident(binding) => Ok(&binding.ident),
                other => Err(syn::Error::new(
                    other.span(),
                    "settings arguments are plain identifiers",
                )),
            })
            .collect::<syn::Result<_>>()?;
        Ok(Some(Self {
            signature,
            receiver,
            arguments,
        }))
    }

    /// The method of the delegating impl: a `match` on the configuration enum that calls the
    /// same method of the selected plugin's settings.
    fn tokens(&self, trait_name: &Ident) -> TokenStream {
        let Self {
            signature,
            receiver,
            arguments,
        } = self;
        let name = &signature.ident;
        let binding = if receiver.mutability.is_some() {
            quote!(ref mut settings)
        } else {
            quote!(ref settings)
        };
        quote! {
            #signature {
                // `*self` and `ref`: an enum without variants only accepts a match on the place.
                match *self {
                    $(
                        Self::$variant(#binding) => {
                            #trait_name::#name(settings, #(#arguments),*)
                        }
                    )*
                }
            }
        }
    }
}

pub fn expand(attribute: &TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !attribute.is_empty() {
        return Err(syn::Error::new(
            attribute.span(),
            "#[settings] takes no arguments",
        ));
    }
    let settings: ItemTrait = syn::parse2(item)?;
    let trait_name = &settings.ident;
    let mut methods = Vec::new();
    for item in &settings.items {
        let TraitItem::Fn(method) = item else {
            return Err(syn::Error::new(
                item.span(),
                "settings traits contain only methods",
            ));
        };
        if let Some(delegation) = Delegation::of(method)? {
            methods.push(delegation.tokens(trait_name));
        }
    }
    let delegate_macro = delegate_macro(&trait_name.to_string());
    Ok(quote! {
        #settings

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #delegate_macro {
            ($enum:path { $($variant:ident),* $(,)? }) => {
                const _: () = {
                    // The trait and the types in its signatures, named from the declaring
                    // crate's root; naming the trait through the glob keeps the import used.
                    use $crate::*;

                    impl #trait_name for $enum {
                        #(#methods)*
                    }
                };
            };
        }
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::*;

    fn rejection(item: TokenStream) -> String {
        expand(&TokenStream::new(), item)
            .expect_err("expansion is rejected")
            .to_string()
    }

    fn expanded(item: TokenStream) -> String {
        expand(&TokenStream::new(), item)
            .expect("expansion is accepted")
            .to_string()
    }

    #[test]
    fn rejects_attribute_arguments() {
        let error = expand(
            &quote!(extra),
            quote!(
                trait Settings {}
            ),
        )
        .expect_err("expansion is rejected")
        .to_string();
        assert!(error.contains("takes no arguments"));
    }

    #[test]
    fn rejects_items_other_than_methods() {
        let input = quote! { trait Settings { type Value; } };
        assert!(rejection(input).contains("only methods"));
    }

    #[test]
    fn rejects_methods_without_a_receiver() {
        let input = quote! { trait Settings { fn make() -> Self; } };
        assert!(rejection(input).contains("take `self`"));
    }

    #[test]
    fn rejects_generic_methods() {
        let input = quote! { trait Settings { fn get<T>(&self) -> u8 { 0 } } };
        assert!(rejection(input).contains("no generic parameters"));
    }

    #[test]
    fn rejects_patterns_as_arguments() {
        let input = quote! { trait Settings { fn set(&mut self, (a, b): (u8, u8)) {} } };
        assert!(rejection(input).contains("plain identifiers"));
    }

    #[test]
    fn rejects_a_by_value_method_without_a_body() {
        let input = quote! { trait Settings { fn into_value(self) -> u8; } };
        assert!(rejection(input).contains("needs a default body"));
    }

    #[test]
    fn delegates_borrowing_methods_and_keeps_by_value_ones() {
        let expansion = expanded(quote! {
            pub trait Settings {
                fn level(&self) -> u8 { 0 }
                fn set_level(&mut self, level: u8) { let _ = level; }
                fn with_level(mut self, level: u8) -> Self where Self: Sized {
                    self.set_level(level);
                    self
                }
            }
        });
        assert!(
            expansion.contains("macro_rules ! __gang_delegate_Settings"),
            "{expansion}"
        );
        assert!(
            expansion
                .contains("Self :: $ variant (ref settings) => { Settings :: level (settings ,) }"),
            "{expansion}"
        );
        assert!(
            expansion.contains(
                "Self :: $ variant (ref mut settings) => { Settings :: set_level (settings , level) }"
            ),
            "{expansion}"
        );
        assert_eq!(
            expansion.matches("Self :: $ variant").count(),
            2,
            "{expansion}"
        );
    }
}
