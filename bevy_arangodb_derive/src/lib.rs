#![allow(non_snake_case)]
extern crate ctor;

use proc_macro::TokenStream;
use quote::{quote, format_ident};
use syn::{
    parse_macro_input,
    punctuated::Punctuated,
    token::Comma,
    Item, Meta,
    LitStr,
};

// New attribute macro: single annotation for derive + registration
#[proc_macro_attribute]
pub fn persist(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse component/resource list: e.g. #[persist(component,resource)]
    let metas = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
    let is_comp = metas.iter().any(|m| matches!(m, Meta::Path(p) if p.is_ident("component")));
    let is_res  = metas.iter().any(|m| matches!(m, Meta::Path(p) if p.is_ident("resource")));

    // No effect if neither
    if !is_comp && !is_res {
        return item;
    }

    // Parse the input item (struct or enum)
    let mut ast = parse_macro_input!(item as Item);

    // Derive Component/Resource and serde for non-unit types, skip serde for unit structs to allow manual impls
    if let Item::Struct(s) = &mut ast {
        let derive_list = if is_comp {
            quote! { ::bevy::prelude::Component, ::serde::Serialize, ::serde::Deserialize }
        } else {
            quote! { ::bevy::prelude::Resource, ::serde::Serialize, ::serde::Deserialize }
        };
        s.attrs.push(syn::parse_quote!(#[derive(#derive_list)]));

        // Add `#[serde(transparent)]` to single-field tuple structs
        if let syn::Fields::Unnamed(fields) = &s.fields {
            if fields.unnamed.len() == 1 {
                s.attrs.push(syn::parse_quote!(#[serde(transparent)]));
            }
        }
    } else if let Item::Enum(e) = &mut ast {
        // enums always derive serde
        let derive_list = if is_comp {
            quote! { ::bevy::prelude::Component, ::serde::Serialize, ::serde::Deserialize }
        } else {
            quote! { ::bevy::prelude::Resource, ::serde::Serialize, ::serde::Deserialize }
        };
        e.attrs.push(syn::parse_quote!(#[derive(#derive_list)]));
    }

    // Registration boilerplate
    let name       = match &ast {
        Item::Struct(s) => &s.ident,
        Item::Enum(e)   => &e.ident,
        _ => unreachable!(),
    };
    let register_fn = format_ident!("__persist_register_{}", name);
    let ctor_fn = format_ident!("__persist_ctor_{}", name);
    let crate_path  = get_crate_path();

    let registration = if is_comp {
        quote! {
            #[allow(non_snake_case)]
            fn #register_fn(app: &mut bevy::app::App) {
                use bevy::prelude::IntoScheduleConfigs;
                let type_id = std::any::TypeId::of::<#name>();
                let mut registered = app.world_mut().resource_mut::<#crate_path::persistence_plugin::RegisteredPersistTypes>();
                if registered.0.insert(type_id) {
                    app.world_mut()
                        .resource_mut::<#crate_path::PersistenceSession>()
                        .register_component::<#name>();
                    app.add_systems(
                        bevy::app::PostUpdate,
                        #crate_path::persistence_plugin::auto_dirty_tracking_entity_system::<#name>
                            .in_set(#crate_path::persistence_plugin::PersistenceSystemSet::PreCommit),
                    );
                }
            }
        }
    } else {
        quote! {
            #[allow(non_snake_case)]
            fn #register_fn(app: &mut bevy::app::App) {
                use bevy::prelude::IntoScheduleConfigs;
                let type_id = std::any::TypeId::of::<#name>();
                let mut registered = app.world_mut().resource_mut::<#crate_path::persistence_plugin::RegisteredPersistTypes>();
                if registered.0.insert(type_id) {
                    app.world_mut()
                        .resource_mut::<#crate_path::PersistenceSession>()
                        .register_resource::<#name>();
                    app.add_systems(
                        bevy::app::PostUpdate,
                        #crate_path::persistence_plugin::auto_dirty_tracking_resource_system::<#name>
                            .in_set(#crate_path::persistence_plugin::PersistenceSystemSet::PreCommit),
                    );
                }
            }
        }
    };

    let ctor_registration = quote! {
        #[ctor::ctor]
        #[allow(non_snake_case)]
        fn #ctor_fn() {
            #crate_path::registration::COMPONENT_REGISTRY
                .lock()
                .unwrap()
                .push(#register_fn);
        }
    };

    // Implement the Persist trait, providing the name() method
    let impl_persist = quote! {
        impl #crate_path::Persist for #name {
            fn name() -> &'static str {
                stringify!(#name)
            }
        }
    };

    // Generate field accessor methods for structs
    let mut field_methods = quote! {};
    if let Item::Struct(ref s) = ast {
        // For each named field, create a static method returning an Expression::Field
        let methods = s.fields.iter().filter_map(|f| f.ident.as_ref()).map(|ident| {
            let field_str = LitStr::new(&ident.to_string(), proc_macro2::Span::call_site());
            quote! {
                pub fn #ident() -> #crate_path::dsl::Expression {
                    #crate_path::dsl::Expression::Field { component_name: <Self as #crate_path::Persist>::name(), field_name: #field_str }
                }
            }
        });
        field_methods = quote! {
            impl #name {
                #(#methods)*
            }
        };
    }

    // Combine all generated code
    TokenStream::from(quote! {
        #ast
        #registration
        #ctor_registration
        #impl_persist
        #field_methods
    })
}

fn get_crate_path() -> proc_macro2::TokenStream {
    use proc_macro_crate::{crate_name, FoundCrate};

    // First check if we're in the bevy_arangodb crate (integration tests)
    if let Ok(FoundCrate::Itself) = crate_name("bevy_arangodb") {
        return quote!(::bevy_arangodb::bevy_arangodb_core);
    }

    // Then check for bevy_arangodb_core
    match crate_name("bevy_arangodb_core") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_) => {
            // Try to find bevy_arangodb as a dependency
            if let Ok(FoundCrate::Name(name)) = crate_name("bevy_arangodb") {
                let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
                quote!(::#ident::bevy_arangodb_core)
            } else {
                quote!(::bevy_arangodb_core)
            }
        }
    }
}