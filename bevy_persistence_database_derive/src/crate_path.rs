//! Resolve the `bevy_persistence_database` crate path for generated code.

use quote::quote;

pub(crate) fn get_crate_path() -> proc_macro2::TokenStream {
    use proc_macro_crate::{FoundCrate, crate_name};

    match crate_name("bevy_persistence_database") {
        Ok(FoundCrate::Itself) => quote!(crate),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#ident)
        }
        Err(_) => quote!(::bevy_persistence_database),
    }
}
