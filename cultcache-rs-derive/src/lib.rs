use proc_macro::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Data;
use syn::DeriveInput;
use syn::Fields;
use syn::LitInt;
use syn::LitStr;
use syn::Type;
use syn::parse_macro_input;

#[proc_macro_derive(DatabaseEntry, attributes(cultcache))]
pub fn derive_database_entry(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident.clone();
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

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    ident,
                    "DatabaseEntry derive requires a named struct",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(ident, "DatabaseEntry derive requires a struct")
                .to_compile_error()
                .into();
        }
    };

    let mut slots: Vec<CultCacheSlot> = Vec::new();
    for field in fields {
        let Some(field_ident) = field.ident else {
            return syn::Error::new_spanned(field.ty, "DatabaseEntry derive requires named fields")
                .to_compile_error()
                .into();
        };
        let mut key: Option<usize> = None;
        let mut default = false;
        for attribute in field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("cultcache"))
        {
            let parse_result = attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("key") {
                    let value: LitInt = meta.value()?.parse()?;
                    key = Some(value.base10_parse()?);
                    Ok(())
                } else if meta.path.is_ident("default") {
                    default = true;
                    Ok(())
                } else {
                    Err(meta.error("unsupported field cultcache attribute"))
                }
            });
            if let Err(error) = parse_result {
                return error.to_compile_error().into();
            }
        }
        let Some(key) = key else {
            return syn::Error::new_spanned(
                field_ident,
                "DatabaseEntry fields require #[cultcache(key = N)]",
            )
            .to_compile_error()
            .into();
        };
        if slots.iter().any(|slot| slot.key == key) {
            return syn::Error::new_spanned(
                field_ident,
                format!("duplicate DatabaseEntry slot {key}"),
            )
            .to_compile_error()
            .into();
        }
        slots.push(CultCacheSlot {
            key,
            ident: field_ident,
            ty: field.ty,
            default,
        });
    }
    slots.sort_by_key(|slot| slot.key);
    let max_slot = slots.iter().map(|slot| slot.key).max().unwrap_or(0);

    let serialize_elements = (0..=max_slot).map(|slot| {
        if let Some(field) = slots.iter().find(|field| field.key == slot) {
            let field_ident = &field.ident;
            quote! {
                tuple.serialize_element(&self.#field_ident)?;
            }
        } else {
            quote! {
                tuple.serialize_element(&())?;
            }
        }
    });

    let option_declarations = slots.iter().map(|field| {
        let variable = format_ident!("slot_{}", field.key);
        let ty = &field.ty;
        quote! {
            let mut #variable: ::std::option::Option<#ty> = ::std::option::Option::None;
        }
    });

    let deserialize_match_arms = (0..=max_slot).map(|slot| {
        if let Some(field) = slots.iter().find(|field| field.key == slot) {
            let variable = format_ident!("slot_{}", field.key);
            let ty = &field.ty;
            quote! {
                #slot => {
                    #variable = seq.next_element::<#ty>()?;
                }
            }
        } else {
            quote! {
                #slot => {
                    let _ = seq.next_element::<::serde::de::IgnoredAny>()?;
                }
            }
        }
    });

    let build_fields = slots.iter().map(|field| {
        let field_ident = &field.ident;
        let variable = format_ident!("slot_{}", field.key);
        if field.default {
            quote! {
                #field_ident: #variable.unwrap_or_default()
            }
        } else {
            let field_name = field_ident.to_string();
            quote! {
                #field_ident: #variable.ok_or_else(|| ::serde::de::Error::missing_field(#field_name))?
            }
        }
    });

    quote! {
        impl ::cultcache_rs::DatabaseEntry for #ident {
            const TYPE: &'static str = #entry_type;
            const SCHEMA_NAME: &'static str = #schema_name;
        }

        impl ::serde::Serialize for #ident {
            fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                use ::serde::ser::SerializeTuple;
                let mut tuple = serializer.serialize_tuple(#max_slot + 1)?;
                #(#serialize_elements)*
                tuple.end()
            }
        }

        impl<'de> ::serde::Deserialize<'de> for #ident {
            fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                struct CultCacheVisitor;

                impl<'de> ::serde::de::Visitor<'de> for CultCacheVisitor {
                    type Value = #ident;

                    fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                        formatter.write_str(concat!("a CultCache array for ", stringify!(#ident)))
                    }

                    fn visit_seq<A>(self, mut seq: A) -> ::std::result::Result<Self::Value, A::Error>
                    where
                        A: ::serde::de::SeqAccess<'de>,
                    {
                        #(#option_declarations)*
                        let mut index = 0usize;
                        while index <= #max_slot {
                            match index {
                                #(#deserialize_match_arms)*
                                _ => unreachable!(),
                            }
                            index += 1;
                        }
                        while let ::std::option::Option::Some(_) = seq.next_element::<::serde::de::IgnoredAny>()? {}
                        Ok(#ident {
                            #(#build_fields),*
                        })
                    }
                }

                deserializer.deserialize_tuple(#max_slot + 1, CultCacheVisitor)
            }
        }
    }
    .into()
}

struct CultCacheSlot {
    key: usize,
    ident: syn::Ident,
    ty: Type,
    default: bool,
}
