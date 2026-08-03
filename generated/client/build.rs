use std::{env, fs, path::PathBuf};

use quote::{format_ident, quote};
use syn::{FnArg, ImplItem, ImplItemFn, Item, Pat, Type};

fn main() {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let source_path = manifest.join("../../src/db/main.rs");
    println!("cargo:rerun-if-changed=../../src/db/",);
    println!("cargo:rerun-if-changed=../../libs/shared_types/src");

    let source = fs::read_to_string(&source_path).unwrap_or_else(|error| {
        panic!(
            "cannot read database source {}: {error}",
            source_path.display()
        )
    });
    let file = syn::parse_file(&source).expect("cannot parse database source");
    let mut methods = Vec::new();

    for item in file.items {
        let Item::Impl(item) = item else { continue };
        for item in item.items {
            let ImplItem::Fn(method) = item else { continue };
            if !method.attrs.iter().any(|attr| attr.path().is_ident("ipc")) {
                continue;
            }
            methods.push(method);
        }
    }

    let functions = methods.iter().map(|method| {
        let (client_name, request_variant) = ipc_options(method);
        let docs = method
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("doc"));
        let args = arguments(method);
        let definitions = args.iter().map(|(name, ty)| {
            let ty = owned_type(ty);
            quote!(#name: #ty)
        });
        let names = args.iter().map(|(name, _)| name);
        let return_type = match &method.sig.output {
            syn::ReturnType::Default => quote!(Result<(), Box<dyn std::error::Error>>),
            syn::ReturnType::Type(_, ty) => quote!(Result<#ty, Box<dyn std::error::Error>>),
        };
        let function = client_name.unwrap_or_else(|| method.sig.ident.clone());
        let variant = request_variant.unwrap_or_else(|| pascal_case(&method.sig.ident));
        quote! {
            #(#docs)*
            pub fn #function(#(#definitions),*) -> #return_type {
                crate::init_data_request(&crate::SupportedDBRequests::#variant(#(#names),*))
            }
        }
    });

    let tokens = quote! {
        //! Generated from `src/db/main.rs`; do not edit manually.
        #![allow(dead_code)]
        use std::collections::{HashMap, HashSet};
        use shared_types::*;
        #(#functions)*

        /// Returns whether the host application is shutting down.
        ///
        /// This lifecycle request is generated alongside the database client
        /// because plugins use it to stop long-running work cleanly.
        pub fn should_exit() -> Result<bool, Box<dyn std::error::Error>> {
            crate::init_data_request(&crate::SupportedDBRequests::ShouldExit)
        }
    };
    let parsed = syn::parse2(tokens).expect("generated client is invalid Rust");
    let output = prettyplease::unparse(&parsed);
    let output_path =
    PathBuf::from(std::env::var_os("OUT_DIR").unwrap())
        .join("generated_api.rs");
    let unchanged = fs::read_to_string(&output_path)
        .map(|existing| existing == output)
        .unwrap_or(false);
    if !unchanged {
        let temporary_path = output_path.with_extension("rs.tmp");
        fs::write(&temporary_path, output).unwrap_or_else(|error| {
            panic!(
                "cannot write generated client {}: {error}",
                output_path.display()
            )
        });
        fs::rename(&temporary_path, &output_path).unwrap_or_else(|error| {
            panic!(
                "cannot replace generated client {}: {error}",
                output_path.display()
            )
        });
    }
}

fn ipc_options(method: &ImplItemFn) -> (Option<syn::Ident>, Option<syn::Ident>) {
    let mut name = None;
    let mut request = None;
    for attr in &method.attrs {
        if !attr.path().is_ident("ipc") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value: syn::LitStr = meta.value()?.parse()?;
                name = Some(syn::parse_str(&value.value()).unwrap());
            } else if meta.path.is_ident("request") {
                let value: syn::LitStr = meta.value()?.parse()?;
                request = Some(syn::parse_str(&value.value()).unwrap());
            }
            Ok(())
        });
    }
    (name, request)
}

fn arguments(method: &ImplItemFn) -> Vec<(&syn::Ident, &Type)> {
    method
        .sig
        .inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Receiver(_) => None,
            FnArg::Typed(argument) => match argument.pat.as_ref() {
                Pat::Ident(pattern) => Some((&pattern.ident, argument.ty.as_ref())),
                _ => None,
            },
        })
        .collect()
}

fn owned_type(ty: &Type) -> Type {
    let Type::Reference(reference) = ty else {
        return ty.clone();
    };
    if let Type::Slice(slice) = reference.elem.as_ref() {
        let element = &slice.elem;
        syn::parse_quote!(Vec<#element>)
    } else if let Type::Path(path) = reference.elem.as_ref()
        && path.path.is_ident("str")
    {
        syn::parse_quote!(String)
    } else {
        (*reference.elem).clone()
    }
}

fn pascal_case(name: &syn::Ident) -> syn::Ident {
    let mut output = String::new();
    let mut upper = true;
    for c in name.to_string().chars() {
        if !c.is_ascii_alphanumeric() {
            upper = true;
            continue;
        }
        if upper {
            output.extend(c.to_uppercase());
            upper = false
        } else {
            output.push(c)
        }
    }
    format_ident!("{output}")
}
