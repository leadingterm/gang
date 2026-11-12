//! `#[derive(Fix)]`: the operator-facing fix of each variant of an error enum.

use std::collections::BTreeSet;

use proc_macro2::{Ident, Spacing, Span, TokenStream, TokenTree};
use quote::quote;
use syn::parse::ParseStream;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Field, Fields, LitStr, Meta, Token, Variant};

/// The parsed `#[fix("text", args..)]` of a variant.
struct FixText {
    literal: LitStr,
    args: TokenStream,
}

impl FixText {
    fn parse(attribute: &Attribute) -> syn::Result<Self> {
        if !matches!(attribute.meta, Meta::List(_)) {
            return Err(syn::Error::new(
                attribute.span(),
                "expected `#[fix(\"text\", args..)]`",
            ));
        }
        attribute.parse_args_with(|input: ParseStream| {
            let literal: LitStr = input.parse().map_err(|error| {
                syn::Error::new(
                    error.span(),
                    "the first argument of `#[fix]` must be a string literal",
                )
            })?;
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
            Ok(Self {
                literal,
                args: input.parse()?,
            })
        })
    }

    /// The names of the fields the fix mentions, in the literal or in the format arguments.
    fn mentions(&self) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        let mut labels = BTreeSet::new();
        scan_arguments(self.args.clone(), true, &mut names, &mut labels);
        let mut placeholders = BTreeSet::new();
        scan_literal(&self.literal.value(), &mut placeholders);
        names.extend(placeholders.difference(&labels).cloned());
        names
    }

    /// The `Cow` of the fix: borrowed for static text, otherwise formatted.
    fn value(&self) -> TokenStream {
        let literal = &self.literal;
        if self.args.is_empty() {
            if let Some(text) = scan_literal(&literal.value(), &mut BTreeSet::new()) {
                return quote! { ::std::borrow::Cow::Borrowed(#text) };
            }
            return quote! { ::std::borrow::Cow::Owned(::std::format!(#literal)) };
        }
        let args = &self.args;
        quote! { ::std::borrow::Cow::Owned(::std::format!(#literal, #args)) }
    }
}

/// The parsed `#[fix(..)]` of a variant.
enum Directive {
    /// `#[fix("text", args..)]`.
    Text(FixText),
    /// `#[fix(source)]`, carrying the span of the attribute.
    Source(Span),
    /// `#[fix(with = expression)]`.
    With(syn::Expr),
}

impl Directive {
    fn parse(attribute: &Attribute) -> syn::Result<Self> {
        if is_source_directive(attribute) {
            return Ok(Self::Source(attribute.span()));
        }
        if let Meta::List(list) = &attribute.meta {
            let mut tokens = list.tokens.clone().into_iter();
            if matches!(tokens.next(), Some(TokenTree::Ident(ident)) if ident == "with")
                && matches!(tokens.next(), Some(TokenTree::Punct(punct)) if punct.as_char() == '=')
            {
                return attribute.parse_args_with(|input: ParseStream| {
                    input.parse::<Ident>()?;
                    input.parse::<Token![=]>()?;
                    Ok(Self::With(input.parse()?))
                });
            }
        }
        FixText::parse(attribute).map(Self::Text)
    }
}

/// Whether the attribute is the bare `#[fix(source)]`.
fn is_source_directive(attribute: &Attribute) -> bool {
    let Meta::List(list) = &attribute.meta else {
        return false;
    };
    syn::parse2::<Ident>(list.tokens.clone()).is_ok_and(|ident| ident == "source")
}

/// Whether the variant is `#[error(transparent)]`, so that its field is its source.
fn is_transparent(variant: &Variant) -> bool {
    variant.attrs.iter().any(|attribute| {
        attribute.path().is_ident("error")
            && attribute
                .parse_args::<Ident>()
                .is_ok_and(|ident| ident == "transparent")
    })
}

/// Whether the field is a source: marked `#[source]` or `#[from]`, named `source`, or in a
/// transparent variant.
fn is_source(field: &Field, transparent: bool) -> bool {
    transparent
        || field.ident.as_ref().is_some_and(|ident| ident == "source")
        || field.attrs.iter().any(|attribute| {
            attribute.path().is_ident("source") || attribute.path().is_ident("from")
        })
}

/// Collects the identifiers of format arguments that may name bound fields.
///
/// An identifier after a single `.` is a method or field access, and a top-level `name =` is a
/// named argument's label; neither refers to a binding.
fn scan_arguments(
    stream: TokenStream,
    top_level: bool,
    names: &mut BTreeSet<String>,
    labels: &mut BTreeSet<String>,
) {
    let mut tokens = stream.into_iter().peekable();
    let mut dots = 0_usize;
    while let Some(token) = tokens.next() {
        let is_dot = matches!(&token, TokenTree::Punct(punct) if punct.as_char() == '.');
        match token {
            TokenTree::Group(group) => scan_arguments(group.stream(), false, names, labels),
            TokenTree::Ident(ident) => {
                let is_label = top_level
                    && matches!(
                        tokens.peek(),
                        Some(TokenTree::Punct(punct))
                            if punct.as_char() == '=' && punct.spacing() == Spacing::Alone
                    );
                if is_label {
                    labels.insert(ident.to_string());
                } else if dots != 1 {
                    names.insert(ident.to_string());
                }
            }
            TokenTree::Punct(_) | TokenTree::Literal(_) => {}
        }
        dots = if is_dot { dots + 1 } else { 0 };
    }
}

/// Collects the names the placeholders of a format string refer to into `names` and returns
/// the text with doubled braces unescaped when it has no placeholder at all.
fn scan_literal(text: &str, names: &mut BTreeSet<String>) -> Option<String> {
    let mut unescaped = String::with_capacity(text.len());
    let mut is_static = true;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                unescaped.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                unescaped.push('}');
            }
            '{' => {
                is_static = false;
                let mut placeholder = String::new();
                for inner in chars.by_ref() {
                    if inner == '}' {
                        break;
                    }
                    placeholder.push(inner);
                }
                scan_placeholder(&placeholder, names);
            }
            '}' => is_static = false,
            other => unescaped.push(other),
        }
    }
    is_static.then_some(unescaped)
}

/// Collects the argument name of one `name:spec` placeholder and the names the spec takes
/// widths and precisions from (`width$`).
fn scan_placeholder(placeholder: &str, names: &mut BTreeSet<String>) {
    let (argument, spec) = placeholder.split_once(':').unwrap_or((placeholder, ""));
    let argument = argument.trim();
    if is_identifier(argument) {
        names.insert(argument.to_owned());
    }
    for (end, _) in spec.match_indices('$') {
        let before = &spec[..end];
        let start = before
            .char_indices()
            .rev()
            .find(|&(_, c)| !is_identifier_char(c))
            .map_or(0, |(index, c)| index + c.len_utf8());
        let name = &before[start..];
        if is_identifier(name) {
            names.insert(name.to_owned());
        }
    }
}

fn is_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_identifier(text: &str) -> bool {
    let body = text.strip_prefix("r#").unwrap_or(text);
    let mut chars = body.chars();
    chars
        .next()
        .is_some_and(|first| first.is_alphabetic() || first == '_')
        && chars.all(is_identifier_char)
}

fn is_fix(attribute: &Attribute) -> bool {
    attribute.path().is_ident("fix")
}

/// The match arm of one variant.
fn arm(variant: &Variant) -> syn::Result<TokenStream> {
    if let Some(attribute) = variant
        .fields
        .iter()
        .flat_map(|field| &field.attrs)
        .find(|attribute| is_fix(attribute))
    {
        return Err(syn::Error::new(
            attribute.span(),
            "`#[fix]` goes on enum variants, not on fields",
        ));
    }
    let mut attributes = variant.attrs.iter().filter(|attribute| is_fix(attribute));
    let directive = attributes.next().map(Directive::parse).transpose()?;
    if let Some(extra) = attributes.next() {
        return Err(syn::Error::new(
            extra.span(),
            "a variant takes at most one `#[fix]`",
        ));
    }
    let name = &variant.ident;
    let Some(directive) = directive else {
        let pattern = match &variant.fields {
            Fields::Named(_) => quote! { { .. } },
            Fields::Unnamed(_) => quote! { (..) },
            Fields::Unit => TokenStream::new(),
        };
        return Ok(quote! { Self::#name #pattern => ::core::option::Option::None });
    };
    let fix = match directive {
        Directive::Text(fix) => fix,
        Directive::Source(span) => return source_arm(variant, span),
        Directive::With(expression) => return Ok(with_arm(variant, &expression)),
    };
    let mentions = fix.mentions();
    let pattern = match &variant.fields {
        Fields::Named(fields) => {
            let bound: Vec<&Ident> = fields
                .named
                .iter()
                .filter_map(|field| field.ident.as_ref())
                .filter(|ident| mentions.contains(&ident.to_string()))
                .collect();
            quote! { { #(#bound,)* .. } }
        }
        Fields::Unnamed(fields) => {
            let positions = (0..fields.unnamed.len()).map(|index| {
                let name = format!("_{index}");
                if mentions.contains(&name) {
                    let ident = Ident::new(&name, fix.literal.span());
                    quote! { #ident }
                } else {
                    quote! { _ }
                }
            });
            quote! { ( #(#positions),* ) }
        }
        Fields::Unit => TokenStream::new(),
    };
    let value = fix.value();
    Ok(quote! {
        Self::#name #pattern => ::core::option::Option::Some(#value)
    })
}

/// The match arm of a variant whose guidance is computed from its fields.
fn with_arm(variant: &Variant, expression: &syn::Expr) -> TokenStream {
    let name = &variant.ident;
    let (pattern, bindings) = match &variant.fields {
        Fields::Named(fields) => {
            let bindings: Vec<_> = fields
                .named
                .iter()
                .filter_map(|field| field.ident.as_ref())
                .cloned()
                .collect();
            (quote! { { #(#bindings),* } }, bindings)
        }
        Fields::Unnamed(fields) => {
            let bindings: Vec<_> = (0..fields.unnamed.len())
                .map(|index| Ident::new(&format!("f{index}"), variant.ident.span()))
                .collect();
            (quote! { ( #(#bindings),* ) }, bindings)
        }
        Fields::Unit => (TokenStream::new(), Vec::new()),
    };
    quote! {
        Self::#name #pattern => {
            let _ = (#(&#bindings,)*);
            #expression
        }
    }
}

/// The match arm of a variant whose fix is the fix of its source field.
fn source_arm(variant: &Variant, span: Span) -> syn::Result<TokenStream> {
    let transparent = is_transparent(variant);
    let sources: Vec<(usize, &Field)> = variant
        .fields
        .iter()
        .enumerate()
        .filter(|(_, field)| is_source(field, transparent))
        .collect();
    let [(index, field)] = sources.as_slice() else {
        let message = if sources.is_empty() {
            "a variant with #[fix(source)] needs a #[source], #[from], `source`-named or transparent field"
        } else {
            "a variant with #[fix(source)] needs exactly one #[source], #[from], `source`-named or transparent field"
        };
        return Err(syn::Error::new(span, message));
    };
    let name = &variant.ident;
    if let Some(ident) = &field.ident {
        return Ok(quote! {
            Self::#name { #ident, .. } => ::gang_types::Fix::fix(#ident)
        });
    }
    let binding = Ident::new("source", field.span());
    let positions = (0..variant.fields.len()).map(|position| {
        if position == *index {
            quote! { #binding }
        } else {
            quote! { _ }
        }
    });
    Ok(quote! {
        Self::#name ( #(#positions),* ) => ::gang_types::Fix::fix(#binding)
    })
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new(input.ident.span(), "Fix derives on enums"));
    };
    if let Some(attribute) = input.attrs.iter().find(|attribute| is_fix(attribute)) {
        return Err(syn::Error::new(
            attribute.span(),
            "`#[fix]` goes on enum variants, not on the enum",
        ));
    }
    let arms = data
        .variants
        .iter()
        .map(arm)
        .collect::<syn::Result<Vec<_>>>()?;
    let body = if arms.is_empty() {
        quote! { match *self {} }
    } else {
        quote! { match self { #(#arms,)* } }
    };
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::gang_types::Fix for #name #ty_generics #where_clause {
            fn fix(&self) -> ::core::option::Option<::std::borrow::Cow<'static, str>> {
                #body
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

    fn expanded(input: TokenStream) -> String {
        expand(input).expect("expansion is accepted").to_string()
    }

    #[test]
    fn rejects_structs() {
        assert_eq!(
            rejection(quote! { struct Failure; }),
            "Fix derives on enums"
        );
    }

    #[test]
    fn rejects_attribute_without_list() {
        let input = quote! { enum Failure { #[fix] Missing } };
        assert!(rejection(input).contains("expected `#[fix("));
    }

    #[test]
    fn rejects_non_literal_first_argument() {
        let input = quote! { enum Failure { #[fix(text)] Missing } };
        assert!(rejection(input).contains("string literal"));
    }

    #[test]
    fn rejects_duplicate_attribute() {
        let input = quote! { enum Failure { #[fix("a")] #[fix("b")] Missing } };
        assert!(rejection(input).contains("at most one"));
    }

    #[test]
    fn rejects_attribute_on_field_and_enum() {
        let field = quote! { enum Failure { Missing(#[fix("a")] String) } };
        assert!(rejection(field).contains("not on fields"));
        let container = quote! { #[fix("a")] enum Failure { Missing } };
        assert!(rejection(container).contains("not on the enum"));
    }

    #[test]
    fn source_directive_delegates_to_the_source_field() {
        let input = quote! {
            enum Failure {
                #[fix(source)]
                Named { code: u8, #[source] inner: Inner },
                #[error(transparent)]
                #[fix(source)]
                Transparent(Inner),
                #[fix(source)]
                Tuple(u8, #[from] Inner),
            }
        };
        let expansion = expanded(input);
        assert!(
            expansion.contains("{ inner , .. } => :: gang_types :: Fix :: fix (inner)"),
            "{expansion}"
        );
        assert!(
            expansion.contains("(source) => :: gang_types :: Fix :: fix (source)"),
            "{expansion}"
        );
        assert!(
            expansion.contains("(_ , source) => :: gang_types :: Fix :: fix (source)"),
            "{expansion}"
        );
    }

    #[test]
    fn source_directive_accepts_a_field_named_source() {
        let input = quote! { enum Failure { #[fix(source)] Io { path: String, source: Inner } } };
        let expansion = expanded(input);
        assert!(
            expansion.contains("{ source , .. } => :: gang_types :: Fix :: fix (source)"),
            "{expansion}"
        );
    }

    #[test]
    fn rejects_source_directive_without_source_field() {
        let named = quote! { enum Failure { #[fix(source)] Missing { code: u8 } } };
        assert!(
            rejection(named)
                .contains("needs a #[source], #[from], `source`-named or transparent field")
        );
        let unit = quote! { enum Failure { #[fix(source)] Missing } };
        assert!(
            rejection(unit)
                .contains("needs a #[source], #[from], `source`-named or transparent field")
        );
    }

    #[test]
    fn rejects_source_directive_with_two_source_fields() {
        let input = quote! {
            enum Failure {
                #[fix(source)]
                Both(#[from] Inner, #[source] Other),
            }
        };
        assert!(rejection(input).contains("exactly one"));
        let named = quote! {
            enum Failure {
                #[fix(source)]
                Both { source: Inner, #[from] other: Other },
            }
        };
        assert!(rejection(named).contains("exactly one"));
    }
}
