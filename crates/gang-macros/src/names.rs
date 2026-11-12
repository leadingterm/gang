//! Identifiers derived from user-written names.

use proc_macro2::Span;
use syn::Ident;

/// `aws` -> `Aws`, `github_actions` -> `GithubActions`.
pub fn pascal_case(key: &Ident) -> Ident {
    let name: String = key
        .to_string()
        .split('_')
        .filter(|word| !word.is_empty())
        .flat_map(|word| {
            let mut characters = word.chars();
            let first = characters.next().map(|first| first.to_ascii_uppercase());
            first.into_iter().chain(characters)
        })
        .collect();
    Ident::new(&name, key.span())
}

/// The hidden `macro_rules!` that `#[port]` generates for a port trait.
pub fn delegate_macro(port: &str) -> Ident {
    Ident::new(&format!("__gang_delegate_{port}"), Span::call_site())
}
