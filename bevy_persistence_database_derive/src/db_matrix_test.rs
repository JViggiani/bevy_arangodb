//! `#[db_matrix_test]` attribute expansion.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, parse_macro_input, spanned::Spanned};

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "`#[db_matrix_test]` does not take arguments",
        )
        .to_compile_error()
        .into();
    }

    let input = parse_macro_input!(item as ItemFn);
    if !input.sig.inputs.is_empty() {
        return syn::Error::new(
            input.sig.inputs.span(),
            "`#[db_matrix_test]` test functions must take no arguments",
        )
        .to_compile_error()
        .into();
    }
    if input.sig.asyncness.is_some() {
        return syn::Error::new(
            input.sig.asyncness.span(),
            "`#[db_matrix_test]` does not support async test functions",
        )
        .to_compile_error()
        .into();
    }

    let vis = &input.vis;
    let body = &input.block;
    let orig_name = &input.sig.ident;

    // Preserve attributes except #[test]; we'll add #[test] to generated fns
    let passthrough_attrs: Vec<syn::Attribute> = input
        .attrs
        .into_iter()
        .filter(|a| !a.path().is_ident("test"))
        .collect();

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

    #[cfg(not(feature = "ra-fallback"))]
    {
        let arango_test = backend_test(
            orig_name,
            vis,
            &passthrough_attrs,
            body,
            "db_arango",
            quote!(crate::common::TestBackend::Arango),
            quote!(#[cfg(feature = "arango")]),
        );
        let postgres_test = backend_test(
            orig_name,
            vis,
            &passthrough_attrs,
            body,
            "db_postgres",
            quote!(crate::common::TestBackend::Postgres),
            quote!(#[cfg(feature = "postgres")]),
        );
        TokenStream::from(quote! {
            #arango_test
            #postgres_test
        })
    }
}

#[cfg(not(feature = "ra-fallback"))]
fn backend_test(
    orig_name: &syn::Ident,
    vis: &syn::Visibility,
    attrs: &[syn::Attribute],
    body: &syn::Block,
    suffix: &str,
    backend_expr: proc_macro2::TokenStream,
    cfg: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let fn_name = format_ident!("{}_{}", orig_name, suffix);
    quote! {
        #cfg
        #(#attrs)*
        #[test]
        #vis fn #fn_name() {
            let wants = std::env::var("bevy_persistence_database_TEST_BACKENDS").unwrap_or_default();
            if !wants.is_empty() {
                let enabled = wants
                    .split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .filter(|s| !s.is_empty())
                    .any(|token| token == #suffix);
                if !enabled {
                    eprintln!(
                        "skipping {} due to bevy_persistence_database_TEST_BACKENDS={}",
                        stringify!(#fn_name),
                        wants
                    );
                    return;
                }
            }
            let backend = #backend_expr;
            let setup = || crate::common::setup_backend(backend);
            #body
        }
    }
}
