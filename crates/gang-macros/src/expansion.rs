use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;

pub(crate) fn finish(result: syn::Result<TokenStream2>) -> TokenStream {
    result.unwrap_or_else(syn::Error::into_compile_error).into()
}
