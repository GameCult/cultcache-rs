use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;
use syn::LitStr;
use syn::parse_macro_input;

#[proc_macro_derive(DatabaseEntry, attributes(cultcache))]
pub fn derive_database_entry(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let mut entry_type: Option<LitStr> = None;
    let mut schema_name: Option<LitStr> = None;

    for attribute in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cultcache"))
    {
        let parse_result = attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("type") {
                entry_type = Some(meta.value()?.parse()?);
                Ok(())
            } else if meta.path.is_ident("schema") {
                schema_name = Some(meta.value()?.parse()?);
                Ok(())
            } else {
                Err(meta.error("unsupported cultcache attribute"))
            }
        });
        if let Err(error) = parse_result {
            return error.to_compile_error().into();
        }
    }

    let Some(entry_type) = entry_type else {
        return syn::Error::new_spanned(
            ident,
            "DatabaseEntry derive requires #[cultcache(type = \"...\")]",
        )
        .to_compile_error()
        .into();
    };
    let schema_name = schema_name.unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));

    quote! {
        impl ::cultcache_rs::DatabaseEntry for #ident {
            const TYPE: &'static str = #entry_type;
            const SCHEMA_NAME: &'static str = #schema_name;
        }
    }
    .into()
}
