extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, format_ident, quote};
use std::{fs, path::PathBuf};
use syn::{
    Attribute, File, FnArg, ImplItem, ImplItemFn, ItemImpl, Meta, Pat, ReturnType, Type,
    parse_macro_input,
};

/// Generates an external Rust client file from methods marked `#[ipc]`.
///
/// The generated functions preserve the database method documentation and
/// names. Use `name = "..."` and `request = "..."` when the public client
/// name or existing request variant differs from the database method name.
#[proc_macro_attribute]
pub fn export_ipc(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let path = match parse_client_path(attr) {
        Ok(path) => path,
        Err(error) => return error.into_compile_error().into(),
    };

    match expand(input, path) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn parse_client_path(attr: TokenStream) -> syn::Result<String> {
    let args = syn::parse::Parser::parse(
        syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        attr,
    )?;
    for argument in args {
        let Meta::NameValue(value) = argument else {
            return Err(syn::Error::new_spanned(
                argument,
                "expected `client_path = \"...\"`",
            ));
        };
        if !value.path.is_ident("client_path") {
            return Err(syn::Error::new_spanned(
                value.path,
                "expected `client_path`",
            ));
        }
        let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(path),
            ..
        }) = value.value
        else {
            return Err(syn::Error::new_spanned(
                value,
                "client_path must be a string literal",
            ));
        };
        return Ok(path.value());
    }
    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "missing `client_path`",
    ))
}

#[derive(Default)]
struct IpcOptions {
    client_name: Option<syn::Ident>,
    request_variant: Option<syn::Ident>,
}

fn take_ipc_attribute(method: &mut ImplItemFn) -> syn::Result<Option<IpcOptions>> {
    let mut result = None;
    let mut error = None;
    method.attrs.retain(|attribute| {
        if !attribute.path().is_ident("ipc") {
            return true;
        }
        match parse_ipc_options(attribute) {
            Ok(options) => result = Some(options),
            Err(err) => error = Some(err),
        }
        false
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(result)
}

fn parse_ipc_options(attribute: &Attribute) -> syn::Result<IpcOptions> {
    let mut options = IpcOptions::default();
    let Meta::List(list) = &attribute.meta else {
        return Ok(options);
    };
    let args = syn::parse::Parser::parse2(
        syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        list.tokens.clone(),
    )?;
    for arg in args {
        let Meta::NameValue(value) = arg else {
            return Err(syn::Error::new_spanned(
                arg,
                "expected `name = \"...\"` or `request = \"...\"`",
            ));
        };
        let target = if value.path.is_ident("name") {
            &mut options.client_name
        } else if value.path.is_ident("request") {
            &mut options.request_variant
        } else {
            return Err(syn::Error::new_spanned(value.path, "unknown ipc option"));
        };
        let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(name),
            ..
        }) = value.value
        else {
            return Err(syn::Error::new_spanned(
                value,
                "IPC option must be a string literal",
            ));
        };
        *target =
            Some(syn::parse_str(&name.value()).map_err(|_| {
                syn::Error::new_spanned(name, "IPC option must be a Rust identifier")
            })?);
    }
    Ok(options)
}

fn expand(mut input: ItemImpl, client_path: String) -> syn::Result<TokenStream2> {
    let mut methods = Vec::new();
    for item in &mut input.items {
        let ImplItem::Fn(method) = item else { continue };
        let Some(options) = take_ipc_attribute(method)? else {
            continue;
        };
        if !matches!(method.vis, syn::Visibility::Public(_)) {
            return Err(syn::Error::new_spanned(
                &method.sig.ident,
                "IPC methods must be public",
            ));
        }
        if method.sig.asyncness.is_some() {
            return Err(syn::Error::new_spanned(
                &method.sig.ident,
                "async IPC methods are not supported",
            ));
        }
        validate_arguments(method)?;
        methods.push((method.clone(), options));
    }
    write_client_file(&client_path, &methods)?;
    Ok(quote! { #input })
}

fn method_arguments(method: &ImplItemFn) -> syn::Result<Vec<(&syn::Ident, &Type)>> {
    method
        .sig
        .inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Receiver(_) => None,
            FnArg::Typed(argument) => {
                let Pat::Ident(pattern) = argument.pat.as_ref() else {
                    return Some(Err(syn::Error::new_spanned(
                        &argument.pat,
                        "IPC arguments must be named",
                    )));
                };
                Some(Ok((&pattern.ident, argument.ty.as_ref())))
            }
        })
        .collect()
}

fn validate_arguments(method: &ImplItemFn) -> syn::Result<()> {
    method_arguments(method).map(|_| ())
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

fn return_type(output: &ReturnType) -> TokenStream2 {
    match output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    }
}

fn client_return_type(output: &ReturnType) -> TokenStream2 {
    match output {
        ReturnType::Default => quote! { Result<(), Box<dyn std::error::Error>> },
        ReturnType::Type(_, ty) => quote! { Result<#ty, Box<dyn std::error::Error>> },
    }
}

fn documentation(attributes: &[Attribute]) -> Vec<Attribute> {
    attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("doc"))
        .cloned()
        .collect()
}

fn pascal_case(name: &syn::Ident) -> syn::Ident {
    let mut output = String::new();
    let mut uppercase = true;
    for character in name.to_string().chars() {
        if !character.is_ascii_alphanumeric() {
            uppercase = true;
            continue;
        }
        if uppercase {
            output.extend(character.to_uppercase());
            uppercase = false;
        } else {
            output.push(character);
        }
    }
    format_ident!("{output}")
}

fn write_client_file(path: &str, methods: &[(ImplItemFn, IpcOptions)]) -> syn::Result<()> {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "CARGO_MANIFEST_DIR is unavailable",
        )
    })?;
    let output = PathBuf::from(manifest).join(path);
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(io_error)?;
    }

    let functions = methods.iter().map(|(method, options)| {
        let docs = documentation(&method.attrs);
        let default_name = method.sig.ident.clone();
        let name = options.client_name.as_ref().unwrap_or(&default_name);
        let default_variant = pascal_case(&method.sig.ident);
        let variant = options.request_variant.as_ref().unwrap_or(&default_variant);
        let args = method_arguments(method).expect("validated IPC method");
        let definitions = args.iter().map(|(name, ty)| {
            let ty = owned_type(ty);
            quote! { #name: #ty }
        });
        let names = args.iter().map(|(name, _)| name);
        let ret = client_return_type(&method.sig.output);
        quote! {
            #(#docs)*
            pub fn #name(#(#definitions),*) -> #ret {
                crate::init_data_request(&crate::SupportedDBRequests::#variant(#(#names),*))
            }
        }
    });

    let source_tokens = quote! {
        //! Generated by `ipc_macro`; do not edit manually.
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
    let source_file: File = syn::parse2(source_tokens).map_err(|error| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("failed to parse generated client: {error}"),
        )
    })?;
    let source = prettyplease::unparse(&source_file);
    write_if_changed(&output, &source)?;
    Ok(())
}

fn write_if_changed(path: &std::path::Path, contents: &str) -> syn::Result<()> {
    if let Ok(existing) = fs::read_to_string(path)
        && existing == contents
    {
        return Ok(());
    }

    let temporary = path.with_extension("rs.tmp");
    fs::write(&temporary, contents).map_err(io_error)?;
    fs::rename(temporary, path).map_err(io_error)?;
    Ok(())
}

fn io_error(error: std::io::Error) -> syn::Error {
    syn::Error::new(proc_macro2::Span::call_site(), error.to_string())
}
