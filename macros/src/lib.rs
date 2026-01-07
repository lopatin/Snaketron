mod serde_wasm_bindgen;
use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn serde_wasm_bindgen(attr: TokenStream, item: TokenStream) -> TokenStream {
    serde_wasm_bindgen::expand_macro(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
