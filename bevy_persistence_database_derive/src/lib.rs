#![allow(non_snake_case)]

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Item, ItemFn, LitStr, Meta, parse_macro_input, punctuated::Punctuated, token::Comma};

// New attribute macro: single annotation for derive + registration
#[proc_macro_attribute]
pub fn persist(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse component/resource list: e.g. #[persist(component,resource)]
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
    let is_comp = metas
        .iter()
        .any(|m| matches!(m, Meta::Path(p) if p.is_ident("component")));
    let is_res = metas
        .iter()
        .any(|m| matches!(m, Meta::Path(p) if p.is_ident("resource")));
    let is_rel = metas
        .iter()
        .any(|m| matches!(m, Meta::Path(p) if p.is_ident("relationship")));

    // No effect if neither
    if !is_comp && !is_res && !is_rel {
        return item;
    }

    // Parse the input item (struct or enum)
    let mut ast = parse_macro_input!(item as Item);

    // Derive Component/Resource and serde for non-unit types, skip serde for unit structs to allow manual impls
    let mut single_field_serde: Option<proc_macro2::TokenStream> = None;

    if let Item::Struct(s) = &mut ast {
        let is_single_field = match &s.fields {
            syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => true,
            syn::Fields::Named(fields) if fields.named.len() == 1 => true,
            _ => false,
        };

        if is_comp {
            if is_single_field {
                s.attrs.push(syn::parse_quote!(#[derive(::bevy::prelude::Component)]));
            } else {
                s.attrs.push(syn::parse_quote!(
                    #[derive(::bevy::prelude::Component, ::serde::Serialize, ::serde::Deserialize)]
                ));
            }
            single_field_serde = single_field_serde_tokens(s, is_single_field);
        } else if is_res {
            if is_single_field {
                s.attrs.push(syn::parse_quote!(#[derive(::bevy::prelude::Resource)]));
            } else {
                s.attrs.push(syn::parse_quote!(
                    #[derive(::bevy::prelude::Resource, ::serde::Serialize, ::serde::Deserialize)]
                ));
            }
            single_field_serde = single_field_serde_tokens(s, is_single_field);
        } else {
            // Relationship: Serialize/Deserialize only needed for the bevy_many_relationship_edges
            // payload path. The native Bevy relationship path (feature off) uses structs that
            // contain Entity and must NOT derive serde.
            s.attrs.push(syn::parse_quote!(
                #[cfg_attr(feature = "bevy_many_relationship_edges", derive(::serde::Serialize, ::serde::Deserialize))]
            ));
            #[cfg(feature = "bevy_many_relationship_edges")]
            {
                single_field_serde = single_field_serde_tokens(s, true);
            }
        }
    } else if let Item::Enum(e) = &mut ast {
        // enums always derive serde
        let derive_list = if is_comp {
            quote! { ::bevy::prelude::Component, ::serde::Serialize, ::serde::Deserialize }
        } else if is_res {
            quote! { ::bevy::prelude::Resource, ::serde::Serialize, ::serde::Deserialize }
        } else {
            quote! { ::serde::Serialize, ::serde::Deserialize }
        };
        e.attrs.push(syn::parse_quote!(#[derive(#derive_list)]));
    }

    // Registration boilerplate
    let name = match &ast {
        Item::Struct(s) => &s.ident,
        Item::Enum(e) => &e.ident,
        _ => unreachable!(),
    };
    let crate_path = get_crate_path();

    // Implement the Persist trait, providing the name() method.
    // For native Bevy relationships (non-feature path), the struct contains Entity and cannot
    // implement Serialize/Deserialize, so we skip the Persist impl (which has serde supertraits).
    let impl_persist = if is_rel {
        quote! {
            #[cfg(feature = "bevy_many_relationship_edges")]
            impl #crate_path::core::persist::Persist for #name {
                fn name() -> &'static str {
                    stringify!(#name)
                }
            }
        }
    } else {
        quote! {
            impl #crate_path::core::persist::Persist for #name {
                fn name() -> &'static str {
                    stringify!(#name)
                }
            }
        }
    };

    // Generate field accessor methods for structs (only meaningful for payload types that have Persist).
    let mut field_methods = quote! {};
    if let Item::Struct(ref s) = ast {
        let methods = s.fields.iter().filter_map(|f| f.ident.as_ref()).map(|ident| {
            let field_str = LitStr::new(&ident.to_string(), proc_macro2::Span::call_site());
            quote! {
                pub fn #ident() -> #crate_path::core::query::filter_expression::FilterExpression {
                    #crate_path::core::query::filter_expression::FilterExpression::field(<Self as #crate_path::core::persist::Persist>::name(), #field_str)
                }
            }
        });
        let methods_body = quote! {
            impl #name {
                #(#methods)*
            }
        };
        field_methods = if is_rel {
            quote! {
                #[cfg(feature = "bevy_many_relationship_edges")]
                #methods_body
            }
        } else {
            methods_body
        };
    }

    // Generate ctor-based auto-registration for component, resource, and relationship types.
    let auto_registration = if is_comp || is_res || is_rel {
        let register_fn = format_ident!("__persist_register_{}", name);
        let ctor_fn = format_ident!("__persist_ctor_{}", name);

        let register_call = if is_comp {
            quote! { #crate_path::bevy::registration::register_persist_component::<#name>(app); }
        } else if is_res {
            quote! { #crate_path::bevy::registration::register_persist_resource::<#name>(app); }
        } else {
            // Relationships use one of two functions depending on the feature flag.
            quote! {
                #[cfg(not(feature = "bevy_many_relationship_edges"))]
                #crate_path::bevy::registration::register_persist_bevy_relationship::<#name>(app);
                #[cfg(feature = "bevy_many_relationship_edges")]
                #crate_path::bevy::registration::register_persist_many_relationship::<#name>(app);
            }
        };

        quote! {
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
        }
    // We return early above when none of the flags are set, so this is unreachable.
    } else {
        unreachable!()
    };

    // Combine all generated code
    TokenStream::from(quote! {
        #ast
        #single_field_serde
        #impl_persist
        #field_methods
        #auto_registration
    })
}

/// Single-field persisted types always serialize as `{ "field": value }` so adding
/// more fields later does not flip the on-disk shape from scalar to object.
/// Deserialization also accepts the legacy bare scalar produced by the old
/// `#[serde(transparent)]` representation for backward compatibility.
fn single_field_serde_tokens(
    s: &syn::ItemStruct,
    enabled: bool,
) -> Option<proc_macro2::TokenStream> {
    if !enabled {
        return None;
    }

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
            (syn::Member::Named(ident.clone()), ident.to_string(), &field.ty)
        }
        _ => return None,
    };

    let struct_ident = &s.ident;
    let field_name_lit = syn::LitStr::new(&field_name, proc_macro2::Span::call_site());
    let serde_mod = format_ident!("__persist_serde_{}", struct_ident);

    Some(quote! {
        #[allow(non_snake_case)]
        mod #serde_mod {
            use super::#struct_ident;
            use ::serde::de::Error as _;
            use ::serde::{Deserialize, Deserializer, Serialize, Serializer};
            use ::serde::ser::SerializeStruct;

            pub fn serialize<S>(value: &#struct_ident, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                let mut state = serializer.serialize_struct(stringify!(#struct_ident), 1)?;
                state.serialize_field(#field_name_lit, &value.#field_ident)?;
                state.end()
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<#struct_ident, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
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

        impl ::serde::Serialize for #struct_ident {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                #serde_mod::serialize(self, serializer)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for #struct_ident {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                #serde_mod::deserialize(deserializer)
            }
        }
    })
}

#[proc_macro_attribute]
pub fn db_matrix_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let vis = &input.vis;
    let sig = &input.sig;
    let body = &input.block;
    let orig_name = &sig.ident;

    // Preserve attributes except #[test]; we'll add #[test] to generated fns
    let passthrough_attrs: Vec<syn::Attribute> = input
        .attrs
        .into_iter()
        .filter(|a| !a.path().is_ident("test"))
        .collect();

    // Helper to build a backend-gated test
    let gen_backend_test = |suffix: &str,
                            backend_expr: proc_macro2::TokenStream,
                            cfg: proc_macro2::TokenStream| {
        let fn_name = format_ident!("{}_{}", orig_name, suffix);
        let attrs = &passthrough_attrs;

        // Runtime skip if env excludes this backend
        let skip_check = quote! {
            let wants = std::env::var("bevy_persistence_database_TEST_BACKENDS").unwrap_or_default();
            if !wants.is_empty() {
                let mut enabled = false;
                for token in wants.split(',').map(|s| s.trim().to_ascii_lowercase()).filter(|s| !s.is_empty()) {
                    if token == #suffix {
                        enabled = true;
                        break;
                    }
                }
                if !enabled {
                    eprintln!("skipping {} due to bevy_persistence_database_TEST_BACKENDS={}", stringify!(#fn_name), wants);
                    return;
                }
            }
        };

        quote! {
            #cfg
            #(#attrs)*
            #[test]
            #vis fn #fn_name() {
                #skip_check
                let backend = #backend_expr;
                let setup = || crate::common::setup_backend(backend);
                #body
            }
        }
    };

    // Optional: rust-analyzer-friendly fallback via feature, no unknown cfg used
    #[cfg(feature = "ra-fallback")]
    {
        let attrs = &passthrough_attrs;
        let expanded = quote! {
            #(#attrs)*
            #[test]
            #vis fn #orig_name() {
                // Pick one backend that is enabled; prefer postgres if available
                #[allow(unused_mut)]
                let mut backend_opt = None;
                #[cfg(feature = "postgres")]
                { backend_opt = Some(crate::common::TestBackend::Postgres); }
                #[cfg(all(not(feature = "postgres"), feature = "arango"))]
                { backend_opt = Some(crate::common::TestBackend::Arango); }
                let backend = backend_opt.expect("No backend feature enabled");
                let setup = || crate::common::setup_backend(backend);
                #body
            }
        };
        return TokenStream::from(expanded);
    }

    // Default: real per-backend tests (no cfg(rust_analyzer) usage)
    #[cfg(not(feature = "ra-fallback"))]
    {
        let arango_test = gen_backend_test(
            "db_arango",
            quote!(crate::common::TestBackend::Arango),
            quote!(#[cfg(feature = "arango")]),
        );

        let postgres_test = gen_backend_test(
            "db_postgres",
            quote!(crate::common::TestBackend::Postgres),
            quote!(#[cfg(feature = "postgres")]),
        );

        let expanded = quote! {
            #arango_test
            #postgres_test
        };
        return TokenStream::from(expanded);
    }
}

fn get_crate_path() -> proc_macro2::TokenStream {
    use proc_macro_crate::{FoundCrate, crate_name};

    // First check if we're in the bevy_persistence_database crate
    if let Ok(FoundCrate::Itself) = crate_name("bevy_persistence_database") {
        return quote!(crate);
    }

    // Then check if bevy_persistence_database is a dependency
    if let Ok(FoundCrate::Name(name)) = crate_name("bevy_persistence_database") {
        let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
        return quote!(::#ident);
    }

    // If neither case matched, default to bevy_persistence_database
    quote!(::bevy_persistence_database)
}
