use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{spanned::Spanned, Error, Item};

pub fn expand_macro(serde_attr: TokenStream, tokens: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<Item>(tokens)?;
    match item {
        Item::Struct(it) => inner_macro(it.ident.clone(), serde_attr, it.to_token_stream()),
        Item::Enum(it) => inner_macro(it.ident.clone(), serde_attr, it.to_token_stream()),
        item => Err(Error::new(
            item.span(),
            "serde_wasm_bindgen macro can only be applied to structs or enums",
        )),
    }
}

fn inner_macro(
    ident: proc_macro2::Ident,
    serde_attr: TokenStream,
    tokens: TokenStream,
) -> Result<TokenStream, Error> {
    let pound = syn::Token![#](tokens.span()).to_token_stream();

    Ok(quote! {
      #pound[derive(Serialize, Deserialize)]
      #pound[serde(#serde_attr)]
      #tokens


      impl From<#ident> for JsValue {
        fn from(val: #ident) -> Self {
            serde_wasm_bindgen::to_value(&val).unwrap()
        }
      }

      impl TryFrom<JsValue> for #ident {
        type Error = serde_wasm_bindgen::Error;

        fn try_from(value: JsValue) -> Result<Self, Self::Error> {
            serde_wasm_bindgen::from_value(value)
        }
      }

      impl wasm_bindgen::describe::WasmDescribe for #ident {
        fn describe() {
            JsValue::describe()
        }
      }

      impl wasm_bindgen::convert::IntoWasmAbi for #ident {
        type Abi = <JsValue as IntoWasmAbi>::Abi;

        fn into_abi(self) -> Self::Abi {
            Into::<JsValue>::into(self).into_abi()
        }
      }

      impl wasm_bindgen::convert::FromWasmAbi for #ident {
        type Abi = <JsValue as FromWasmAbi>::Abi;

        unsafe fn from_abi(js: Self::Abi) -> Self {
            JsValue::from_abi(js).try_into().unwrap()
        }
      }
    })
}
