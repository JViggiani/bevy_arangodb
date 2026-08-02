//! `#[persist(...)]` attribute expansion.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Item, LitStr, Meta, parse_macro_input, punctuated::Punctuated, spanned::Spanned, token::Comma,
};

use crate::crate_path::get_crate_path;

#[derive(Clone, Copy, PartialEq, Eq)]
enum PersistKind {
    Component,
    Resource,
    Relationship,
}

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
    let kind = match parse_persist_kind(&metas) {
        Ok(kind) => kind,
        Err(err) => return err.to_compile_error().into(),
    };

    let mut ast = parse_macro_input!(item as Item);
    let name = match item_ident(&ast) {
        Some(ident) => ident.clone(),
        None => {
            return syn::Error::new(
                ast.span(),
                "`#[persist]` can only be applied to structs or enums",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut single_field_serde: Option<proc_macro2::TokenStream> = None;

    match &mut ast {
        Item::Struct(s) => {
            let is_single_field = is_single_field_struct(s);
            match kind {
                PersistKind::Component | PersistKind::Resource => {
                    let bevy_derive = match kind {
                        PersistKind::Component => quote!(::bevy::prelude::Component),
                        PersistKind::Resource => quote!(::bevy::prelude::Resource),
                        PersistKind::Relationship => unreachable!(),
                    };
                    if is_single_field {
                        s.attrs
                            .push(syn::parse_quote!(#[derive(#bevy_derive)]));
                        single_field_serde = single_field_serde_tokens(s);
                    } else {
                        s.attrs.push(syn::parse_quote!(
                            #[derive(#bevy_derive, ::serde::Serialize, ::serde::Deserialize)]
                        ));
                    }
                }
                PersistKind::Relationship => {
                    // Native Bevy relationships contain Entity and must not derive serde
                    // when the many-relationship-edges feature is off. Single-field
                    // payloads use custom serde (same as component/resource); multi-field
                    // use derive only under the feature.
                    if is_single_field {
                        #[cfg(feature = "bevy_many_relationship_edges")]
                        {
                            single_field_serde = single_field_serde_tokens(s);
                        }
                    } else {
                        s.attrs.push(syn::parse_quote!(
                            #[cfg_attr(
                                feature = "bevy_many_relationship_edges",
                                derive(::serde::Serialize, ::serde::Deserialize)
                            )]
                        ));
                    }
                }
            }
        }
        Item::Enum(e) => {
            let derive_list = match kind {
                PersistKind::Component => {
                    quote! { ::bevy::prelude::Component, ::serde::Serialize, ::serde::Deserialize }
                }
                PersistKind::Resource => {
                    quote! { ::bevy::prelude::Resource, ::serde::Serialize, ::serde::Deserialize }
                }
                PersistKind::Relationship => {
                    quote! { ::serde::Serialize, ::serde::Deserialize }
                }
            };
            e.attrs.push(syn::parse_quote!(#[derive(#derive_list)]));
        }
        _ => unreachable!("validated by item_ident"),
    }

    let crate_path = get_crate_path();

    // Native Bevy relationships (feature off) contain Entity and cannot implement
    // Persist (serde supertraits). Gate the impl when kind is relationship.
    let impl_persist = match kind {
        PersistKind::Relationship => quote! {
            #[cfg(feature = "bevy_many_relationship_edges")]
            impl #crate_path::core::persist::Persist for #name {
                fn name() -> &'static str {
                    stringify!(#name)
                }
            }
        },
        PersistKind::Component | PersistKind::Resource => quote! {
            impl #crate_path::core::persist::Persist for #name {
                fn name() -> &'static str {
                    stringify!(#name)
                }
            }
        },
    };

    let field_methods = match &ast {
        Item::Struct(s) => {
            let methods = s.fields.iter().filter_map(|f| f.ident.as_ref()).map(|ident| {
                let field_str = LitStr::new(&ident.to_string(), proc_macro2::Span::call_site());
                quote! {
                    pub fn #ident() -> #crate_path::core::query::filter_expression::FilterExpression {
                        #crate_path::core::query::filter_expression::FilterExpression::field(
                            <Self as #crate_path::core::persist::Persist>::name(),
                            #field_str,
                        )
                    }
                }
            });
            let methods_body = quote! {
                impl #name {
                    #(#methods)*
                }
            };
            match kind {
                PersistKind::Relationship => quote! {
                    #[cfg(feature = "bevy_many_relationship_edges")]
                    #methods_body
                },
                PersistKind::Component | PersistKind::Resource => methods_body,
            }
        }
        _ => quote! {},
    };

    let register_fn = format_ident!("__persist_register_{}", name);
    let ctor_fn = format_ident!("__persist_ctor_{}", name);
    let register_call = match kind {
        PersistKind::Component => {
            quote! { #crate_path::bevy::registration::register_persist_component::<#name>(app); }
        }
        PersistKind::Resource => {
            quote! { #crate_path::bevy::registration::register_persist_resource::<#name>(app); }
        }
        PersistKind::Relationship => quote! {
            #[cfg(not(feature = "bevy_many_relationship_edges"))]
            #crate_path::bevy::registration::register_persist_bevy_relationship::<#name>(app);
            #[cfg(feature = "bevy_many_relationship_edges")]
            #crate_path::bevy::registration::register_persist_many_relationship::<#name>(app);
        },
    };
    let auto_registration = quote! {
        #[allow(non_snake_case)]
        fn #register_fn(app: &mut ::bevy::app::App) {
            #register_call
        }

        #[allow(non_snake_case)]
        #[::ctor::ctor]
        fn #ctor_fn() {
            #crate_path::bevy::registration::COMPONENT_REGISTRY
                .lock()
                .unwrap()
                .push(#register_fn);
        }
    };

    TokenStream::from(quote! {
        #ast
        #single_field_serde
        #impl_persist
        #field_methods
        #auto_registration
    })
}

fn parse_persist_kind(metas: &Punctuated<Meta, Comma>) -> syn::Result<PersistKind> {
    let mut kind = None;
    for meta in metas {
        let Meta::Path(path) = meta else {
            return Err(syn::Error::new(
                meta.span(),
                "expected `component`, `resource`, or `relationship`",
            ));
        };
        let ident = path
            .get_ident()
            .ok_or_else(|| syn::Error::new(path.span(), "expected a simple ident"))?;
        let next = if ident == "component" {
            PersistKind::Component
        } else if ident == "resource" {
            PersistKind::Resource
        } else if ident == "relationship" {
            PersistKind::Relationship
        } else {
            return Err(syn::Error::new(
                ident.span(),
                "expected `component`, `resource`, or `relationship`",
            ));
        };
        if kind.replace(next).is_some() {
            return Err(syn::Error::new(
                ident.span(),
                "`#[persist]` accepts exactly one of `component`, `resource`, or `relationship`",
            ));
        }
    }
    kind.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "`#[persist]` requires `component`, `resource`, or `relationship`",
        )
    })
}

fn item_ident(item: &Item) -> Option<&syn::Ident> {
    match item {
        Item::Struct(s) => Some(&s.ident),
        Item::Enum(e) => Some(&e.ident),
        _ => None,
    }
}

fn is_single_field_struct(s: &syn::ItemStruct) -> bool {
    match &s.fields {
        syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => true,
        syn::Fields::Named(fields) if fields.named.len() == 1 => true,
        _ => false,
    }
}

/// Single-field persisted types always serialize as `{ "field": value }` so adding
/// more fields later does not flip the on-disk shape from scalar to object.
/// Deserialization also accepts the legacy bare scalar produced by the old
/// `#[serde(transparent)]` representation for backward compatibility.
///
/// Serde impls are emitted in the **enclosing module** (not a nested `mod`). Nested
/// helpers broke short field-type paths like `use foo::Bar; struct Wrap(pub Bar);`
/// because `#field_ty` was pasted into a child module where the parent's `use`
/// imports are not in scope.
fn single_field_serde_tokens(s: &syn::ItemStruct) -> Option<proc_macro2::TokenStream> {
    let (field_ident, field_name, field_ty) = match &s.fields {
        syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
            let field = fields.unnamed.first()?;
            (
                syn::Member::Unnamed(syn::Index::from(0)),
                "0".to_string(),
                &field.ty,
            )
        }
        syn::Fields::Named(fields) if fields.named.len() == 1 => {
            let field = fields.named.first()?;
            let ident = field.ident.as_ref()?;
            (
                syn::Member::Named(ident.clone()),
                ident.to_string(),
                &field.ty,
            )
        }
        _ => return None,
    };

    let struct_ident = &s.ident;
    let field_name_lit = syn::LitStr::new(&field_name, proc_macro2::Span::call_site());

    Some(quote! {
        impl ::serde::Serialize for #struct_ident {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                use ::serde::ser::SerializeStruct;
                let mut state = serializer.serialize_struct(stringify!(#struct_ident), 1)?;
                state.serialize_field(#field_name_lit, &self.#field_ident)?;
                state.end()
            }
        }

        impl<'de> ::serde::Deserialize<'de> for #struct_ident {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                use ::serde::de::Error as _;
                use ::serde::Deserialize;

                // Compact / postcard (and other non-JSON) backends are not human-readable
                // and cannot round-trip through `serde_json::Value`. Use a normal struct
                // shape there. JSON keeps the legacy bare-scalar / object dual path.
                if !deserializer.is_human_readable() {
                    #[derive(Deserialize)]
                    struct __PersistHelper {
                        #[serde(rename = #field_name_lit)]
                        value: #field_ty,
                    }
                    let helper = __PersistHelper::deserialize(deserializer)?;
                    return Ok(#struct_ident {
                        #field_ident: helper.value,
                    });
                }

                let raw = ::serde_json::Value::deserialize(deserializer)?;
                let inner = if let Some(obj) = raw.as_object() {
                    obj.get(#field_name_lit)
                        .cloned()
                        .unwrap_or(raw)
                } else {
                    raw
                };
                let field_value: #field_ty =
                    ::serde_json::from_value(inner).map_err(D::Error::custom)?;
                Ok(#struct_ident {
                    #field_ident: field_value,
                })
            }
        }
    })
}
