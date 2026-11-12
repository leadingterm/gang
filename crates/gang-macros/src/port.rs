//! `#[port]`: desugars async port methods, records them for `compose!`, and forwards the port
//! through `Arc`.

use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};
use syn::spanned::Spanned;
use syn::{
    Block, FnArg, GenericArgument, GenericParam, Ident, ItemTrait, Pat, PathArguments, ReturnType,
    Signature, TraitItem, TraitItemFn, Type, TypePath, parse_quote,
};

use crate::names::delegate_macro;

/// How an argument carries an associated type that the dispatch enum narrows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Carrier {
    /// `Fleet<'_, Self::A>`: the value is the fleet's `options`, rebuilt with `with_options`.
    Fleet,
    /// `&Self::A` or `Self::A`: the argument is the value itself.
    Value { by_reference: bool },
}

/// An argument whose type is, or carries, an associated type of the port.
struct Narrowing {
    argument: Ident,
    assoc: Ident,
    carrier: Carrier,
}

/// One port method as the dispatch enum delegates it.
struct Delegation {
    /// The method exactly as written, `async fn` included.
    signature: Signature,
    arguments: Vec<Ident>,
    /// The arguments the dispatch enum narrows to the selected plugin's value before the call.
    narrowings: Vec<Narrowing>,
    /// The associated type in `Result<Self::A, _>`: the dispatch enum wraps the plugin's value.
    returned: Option<Ident>,
}

/// `Self::Name` names the associated type `Name`.
fn self_assoc(ty: &Type) -> Option<&Ident> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };
    let mut segments = path.segments.iter();
    match (segments.next(), segments.next(), segments.next()) {
        (Some(first), Some(second), None)
            if first.ident == "Self" && first.arguments.is_none() && second.arguments.is_none() =>
        {
            Some(&second.ident)
        }
        _ => None,
    }
}

/// `Fleet<.., Self::Name>` carries the associated type `Name` as its options.
fn fleet_assoc(ty: &Type) -> Option<&Ident> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };
    let segment = path
        .segments
        .last()
        .filter(|segment| segment.ident == "Fleet")?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(ty) => self_assoc(ty),
        _ => None,
    })
}

/// The associated type in `Result<Self::Name, _>`.
fn returned_assoc(output: &ReturnType) -> Option<&Ident> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };
    let Type::Path(TypePath { qself: None, path }) = &**ty else {
        return None;
    };
    let segment = path
        .segments
        .last()
        .filter(|segment| segment.ident == "Result")?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    match arguments.args.first() {
        Some(GenericArgument::Type(ty)) => self_assoc(ty),
        _ => None,
    }
}

/// The name of the `macro_rules!` variable that carries the dispatch enum of an associated type.
fn assoc_variable(assoc: &Ident) -> Ident {
    format_ident!("assoc_{assoc}")
}

impl Delegation {
    fn try_from_method(method: &TraitItemFn) -> syn::Result<Self> {
        let signature = method.sig.clone();
        if let Some(generic) = signature
            .generics
            .params
            .iter()
            .find(|param| !matches!(param, GenericParam::Type(_)))
        {
            return Err(syn::Error::new(
                generic.span(),
                "port method generics are type parameters only",
            ));
        }
        let receiver = signature
            .receiver()
            .ok_or_else(|| syn::Error::new(signature.span(), "port methods take `&self`"))?;
        if receiver.reference.is_none() || receiver.mutability.is_some() {
            return Err(syn::Error::new(
                receiver.span(),
                "port methods take `&self`",
            ));
        }
        let typed = signature.inputs.iter().filter_map(|input| match input {
            FnArg::Typed(typed) => Some(typed),
            FnArg::Receiver(_) => None,
        });
        let mut arguments = Vec::new();
        let mut narrowings = Vec::new();
        for argument in typed {
            let Pat::Ident(binding) = &*argument.pat else {
                return Err(syn::Error::new(
                    argument.pat.span(),
                    "port arguments are plain identifiers",
                ));
            };
            let narrowing = if let Some(assoc) = fleet_assoc(&argument.ty) {
                Some((assoc, Carrier::Fleet))
            } else if let Type::Reference(reference) = &*argument.ty {
                self_assoc(&reference.elem)
                    .map(|assoc| (assoc, Carrier::Value { by_reference: true }))
            } else {
                self_assoc(&argument.ty).map(|assoc| {
                    (
                        assoc,
                        Carrier::Value {
                            by_reference: false,
                        },
                    )
                })
            };
            if let Some((assoc, carrier)) = narrowing {
                narrowings.push(Narrowing {
                    argument: binding.ident.clone(),
                    assoc: assoc.clone(),
                    carrier,
                });
            }
            arguments.push(binding.ident.clone());
        }
        let returned = returned_assoc(&signature.output).cloned();
        Ok(Self {
            signature,
            arguments,
            narrowings,
            returned,
        })
    }

    /// Checks that the method only names associated types the port declares, and that a method
    /// that narrows or wraps them can report a mismatch.
    fn validate(&self, declared: &[Ident]) -> syn::Result<()> {
        let named = self
            .narrowings
            .iter()
            .map(|narrowing| &narrowing.assoc)
            .chain(&self.returned);
        for assoc in named {
            if !declared.contains(assoc) {
                return Err(syn::Error::new(
                    assoc.span(),
                    format!("the port declares no associated type `{assoc}`"),
                ));
            }
        }
        if (!self.narrowings.is_empty() || self.returned.is_some())
            && !is_port_result(&self.signature.output)
        {
            return Err(syn::Error::new(
                self.signature.output.span(),
                "a method that takes or returns an associated type returns `Result<_, PortError>`",
            ));
        }
        Ok(())
    }

    /// The text of the error a mismatched associated value reports.
    fn mismatch(&self) -> String {
        match self.narrowings.first() {
            Some(Narrowing {
                carrier: Carrier::Fleet,
                ..
            }) => "the fleet's options belong to another provisioner".to_owned(),
            Some(narrowing) => format!(
                "the {} belongs to another plugin",
                narrowing.assoc.to_string().to_lowercase()
            ),
            None => String::new(),
        }
    }

    /// The method of the delegating impl: `match` on the dispatch enum's variants. A method
    /// that takes associated values narrows each to the selected plugin's through the dispatch
    /// enum's `as_<key>`/`into_<key>` accessors, which `compose!` generates; a method that
    /// returns one wraps the plugin's in the dispatch enum's variant.
    fn tokens(&self) -> TokenStream {
        let Self {
            signature,
            arguments,
            narrowings,
            returned,
        } = self;
        let name = &signature.ident;
        let wrap = returned.as_ref().map(|assoc| {
            let enum_ = assoc_variable(assoc);
            quote!(.map($#enum_::$variant))
        });
        let narrowed = |argument: &Ident| format_ident!("narrowed_{argument}");
        let call_arguments = arguments.iter().map(|argument| {
            match narrowings
                .iter()
                .find(|narrowing| &narrowing.argument == argument)
            {
                Some(Narrowing {
                    carrier: Carrier::Fleet,
                    ..
                }) => {
                    let value = narrowed(argument);
                    quote!(#argument.with_options(#value))
                }
                Some(_) => {
                    let value = narrowed(argument);
                    quote!(#value)
                }
                None => quote!(#argument),
            }
        });
        let call = quote!(plugin.#name(#(#call_arguments),*));
        let call = if signature.asyncness.is_some() {
            quote!(#call.await)
        } else {
            call
        };
        let mismatch = self.mismatch();
        let narrow = narrowings.iter().map(|narrowing| {
            let Narrowing {
                argument,
                assoc,
                carrier,
            } = narrowing;
            let (enum_, value) = (assoc_variable(assoc), narrowed(argument));
            match carrier {
                Carrier::Fleet => quote! {
                    let ::core::option::Option::Some(#value) = $#enum_::$as_ref(#argument.options)
                    else {
                        return ::core::result::Result::Err(PortError::config(#mismatch));
                    };
                },
                Carrier::Value { by_reference: true } => quote! {
                    let ::core::option::Option::Some(#value) = $#enum_::$as_ref(#argument)
                    else {
                        return ::core::result::Result::Err(PortError::config(#mismatch));
                    };
                },
                Carrier::Value {
                    by_reference: false,
                } => quote! {
                    let ::core::result::Result::Ok(#value) = $#enum_::$into(#argument) else {
                        return ::core::result::Result::Err(PortError::config(#mismatch));
                    };
                },
            }
        });
        quote! {
            #signature {
                // `*self` and `ref`: a `ZeroOrOne` domain without plugins has an empty enum,
                // which only a match on the place itself accepts without arms.
                match *self {
                    $(
                        Self::$variant(ref plugin) => {
                            #(#narrow)*
                            #call #wrap
                        }
                    )*
                }
            }
        }
    }
}

/// Strips the `#[optional]` markers of a method and reports whether it carried one.
fn take_optional(method: &mut TraitItemFn) -> syn::Result<bool> {
    let (markers, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut method.attrs)
        .into_iter()
        .partition(|attribute| attribute.path().is_ident("optional"));
    method.attrs = kept;
    for marker in &markers {
        marker.meta.require_path_only()?;
    }
    Ok(!markers.is_empty())
}

/// Whether the type is written `Result<_, PortError>`.
fn is_port_result(output: &ReturnType) -> bool {
    let ReturnType::Type(_, ty) = output else {
        return false;
    };
    let Type::Path(TypePath { qself: None, path }) = &**ty else {
        return false;
    };
    let Some(segment) = path.segments.last() else {
        return false;
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return false;
    };
    let mut arguments = arguments.args.iter();
    let (Some(_), Some(GenericArgument::Type(Type::Path(error))), None) =
        (arguments.next(), arguments.next(), arguments.next())
    else {
        return false;
    };
    segment.ident == "Result"
        && error
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "PortError")
}

/// The default body of an `#[optional]` method: it reports the operation as unsupported.
fn unsupported_body(method: &TraitItemFn, delegation: &Delegation) -> syn::Result<Block> {
    if let Some(body) = &method.default {
        return Err(syn::Error::new(
            body.span(),
            "an optional method has no body",
        ));
    }
    if !is_port_result(&method.sig.output) {
        return Err(syn::Error::new(
            method.sig.ident.span(),
            "an optional method returns `Result<_, PortError>`",
        ));
    }
    let name = &method.sig.ident;
    let arguments = &delegation.arguments;
    // Reads the arguments so that the default body leaves them unused without a lint allowance.
    let read = if arguments.is_empty() {
        quote!()
    } else {
        quote!(let _ = (#(&#arguments,)*);)
    };
    Ok(parse_quote!({
        #read
        ::core::result::Result::Err(PortError::Unsupported {
            operation: ::core::stringify!(#name),
        })
    }))
}

/// `async fn f(..) -> T { body }` becomes
/// `fn f(..) -> impl Future<Output = T> + Send { async move { body } }`; a method without a
/// body only has its signature rewritten.
fn desugar(method: &mut TraitItemFn) {
    if method.sig.asyncness.take().is_none() {
        return;
    }
    let output = match &method.sig.output {
        ReturnType::Default => quote!(()),
        ReturnType::Type(_, ty) => ty.to_token_stream(),
    };
    method.sig.output =
        parse_quote!(-> impl ::core::future::Future<Output = #output> + ::core::marker::Send);
    if let Some(body) = method.default.take() {
        method.default = Some(parse_quote!({ async move #body }));
    }
}

/// An associated type of a port: any name, with bounds, no generics and no default. The
/// dispatch enum of the composed plugins carries one variant per plugin, so it needs `Debug`.
fn declared_assoc(associated: &syn::TraitItemType) -> syn::Result<Ident> {
    if !associated.generics.params.is_empty() || associated.default.is_some() {
        return Err(syn::Error::new(
            associated.span(),
            "a port's associated types have neither generics nor a default",
        ));
    }
    Ok(associated.ident.clone())
}

/// Expands `#[port]` on a trait: desugars its async methods, emits the hidden delegation macro
/// that `compose!` uses to implement the port for a dispatch enum, and implements the port for
/// `Arc<T>` of every plugin `T`, so a plugin may be shared without a wrapper.
///
/// A method may have a default body; a plugin that does not override it gets the default, also
/// through the dispatch enum. A method marked `#[optional]` has no body and returns
/// `Result<_, PortError>`; the macro supplies a default body that fails with
/// `PortError::Unsupported` naming the method. `PortError` is written unqualified there and in
/// the delegation, so the module that declares the port must have `PortError` in scope.
///
/// A port may declare associated types (`type Scope: Debug + Send;`). A method that takes
/// `&Self::Scope` or `Self::Scope`, or a `Fleet<'_, Self::Options>`, or returns
/// `Result<Self::Scope, PortError>`, is narrowed by the dispatch enum: `compose!` generates one
/// enum per associated type with one variant per plugin, and a value from another plugin is a
/// `PortError::Config`.
pub fn expand(attribute: &TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !attribute.is_empty() {
        return Err(syn::Error::new(
            attribute.span(),
            "#[port] takes no arguments",
        ));
    }
    let mut port: ItemTrait = syn::parse2(item)?;
    let mut delegations = Vec::new();
    let mut forwards = Vec::new();
    let mut associated = Vec::new();
    for item in &mut port.items {
        match item {
            TraitItem::Fn(method) => {
                let optional = take_optional(method)?;
                let delegation = Delegation::try_from_method(method)?;
                if optional {
                    method.default = Some(unsupported_body(method, &delegation)?);
                }
                desugar(method);
                let (signature, arguments, name) =
                    (&method.sig, &delegation.arguments, &method.sig.ident);
                forwards.push(quote! {
                    #signature {
                        (**self).#name(#(#arguments),*)
                    }
                });
                delegations.push(delegation);
            }
            TraitItem::Type(declared) => associated.push(declared_assoc(declared)?),
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "port traits contain only methods and associated types",
                ));
            }
        }
    }
    for delegation in &delegations {
        delegation.validate(&associated)?;
    }
    let port_name = &port.ident;
    let delegate_macro = delegate_macro(&port_name.to_string());
    let methods = delegations.iter().map(Delegation::tokens);
    let variables: Vec<Ident> = associated.iter().map(assoc_variable).collect();
    // The dispatch enums of the associated types are imported by name, so that no item of the
    // glob imports below can shadow them.
    Ok(quote! {
        #port

        impl<GangPlugin: #port_name> #port_name for ::std::sync::Arc<GangPlugin> {
            #( type #associated = <GangPlugin as #port_name>::#associated; )*
            #(#forwards)*
        }

        #[doc(hidden)]
        #[macro_export]
        macro_rules! #delegate_macro {
            (
                $enum:path {
                    $($variant:ident => $as_ref:ident, $into:ident),* $(,)?
                }
                [ #( $#variables:ident ),* $(,)? ]
            ) => {
                const _: () = {
                    use $crate::*;
                    use $crate::__private::*;
                    #( use self::$#variables; )*

                    // Named through the glob import above, which the port's trait (re-exported at the crate root) always uses.
                    impl #port_name for $enum {
                        #( type #associated = $#variables; )*
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

    #[test]
    fn rejects_attribute_arguments() {
        let error = expand(
            &quote!(extra),
            quote!(
                trait Port {}
            ),
        )
        .expect_err("expansion is rejected")
        .to_string();
        assert!(error.contains("takes no arguments"));
    }

    #[test]
    fn rejects_lifetime_generics_on_a_method() {
        let input = quote! { trait Port { fn load<'a>(&self); } };
        assert!(rejection(input).contains("type parameters only"));
    }

    #[test]
    fn rejects_method_without_receiver() {
        let input = quote! { trait Port { async fn load() -> u8; } };
        assert!(rejection(input).contains("take `&self`"));
    }

    #[test]
    fn rejects_method_taking_self_by_value() {
        let input = quote! { trait Port { fn load(self) -> u8; } };
        assert!(rejection(input).contains("take `&self`"));
    }

    #[test]
    fn rejects_method_taking_mutable_self() {
        let input = quote! { trait Port { fn load(&mut self) -> u8; } };
        assert!(rejection(input).contains("take `&self`"));
    }

    #[test]
    fn rejects_non_identifier_argument_pattern() {
        let input = quote! { trait Port { fn load(&self, (a, b): (u8, u8)); } };
        assert!(rejection(input).contains("plain identifiers"));
    }

    #[test]
    fn rejects_undeclared_associated_type_in_argument() {
        let input = quote! {
            trait Port {
                fn save(&self, scope: &Self::Scope) -> Result<(), PortError>;
            }
        };
        assert!(rejection(input).contains("declares no associated type `Scope`"));
    }

    #[test]
    fn rejects_undeclared_associated_type_in_result() {
        let input = quote! {
            trait Port {
                fn open(&self) -> Result<Self::Scope, PortError>;
            }
        };
        assert!(rejection(input).contains("declares no associated type `Scope`"));
    }

    #[test]
    fn rejects_associated_value_method_without_port_result() {
        let input = quote! {
            trait Port {
                type Scope;
                fn save(&self, scope: &Self::Scope) -> u8;
            }
        };
        assert!(rejection(input).contains("takes or returns an associated type"));
    }

    #[test]
    fn rejects_associated_type_with_generics() {
        let input = quote! { trait Port { type Scope<'a>; } };
        assert!(rejection(input).contains("neither generics nor a default"));
    }

    #[test]
    fn rejects_associated_type_with_default() {
        let input = quote! { trait Port { type Scope = u8; } };
        assert!(rejection(input).contains("neither generics nor a default"));
    }

    #[test]
    fn rejects_constants_in_port() {
        let input = quote! { trait Port { const LIMIT: u8; } };
        assert!(rejection(input).contains("only methods and associated types"));
    }

    #[test]
    fn rejects_optional_marker_with_arguments() {
        let input = quote! {
            trait Port {
                #[optional(extra)]
                fn load(&self) -> Result<u8, PortError>;
            }
        };
        assert!(rejection(input).contains("unexpected token in attribute"));
    }

    #[test]
    fn rejects_optional_method_with_body() {
        let input = quote! {
            trait Port {
                #[optional]
                fn load(&self) -> Result<u8, PortError> { Ok(1) }
            }
        };
        assert!(rejection(input).contains("an optional method has no body"));
    }

    #[test]
    fn rejects_optional_method_without_port_result() {
        let input = quote! {
            trait Port {
                #[optional]
                fn load(&self) -> u8;
            }
        };
        assert!(rejection(input).contains("an optional method returns"));
    }
}
