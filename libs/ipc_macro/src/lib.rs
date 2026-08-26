extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use std::{
    fs,
    io::Write as _,
    path::PathBuf,
    process::{Command, Stdio},
};
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
    input
        .items
        .push(ImplItem::Fn(server_dispatch_method(&methods)));
    Ok(quote! { #input })
}

fn server_dispatch_method(methods: &[(ImplItemFn, IpcOptions)]) -> ImplItemFn {
    let arms = methods.iter().map(|(method, options)| {
        let default_variant = pascal_case(&method.sig.ident);
        let variant = options.request_variant.as_ref().unwrap_or(&default_variant);
        let method_name = &method.sig.ident;
        let args = method_arguments(method).expect("validated IPC method");
        let names = args.iter().map(|(name, _)| name).collect::<Vec<_>>();
        let pattern = quote! { client::SupportedDBRequests::#variant(#(#names),*) };
        let call_args = args.iter().map(|(name, ty)| {
            if matches!(ty, Type::Reference(_)) {
                quote! { &#name }
            } else {
                quote! { #name }
            }
        });

        quote! {
            #pattern => Some(
                client::data_size_to_b(&self.#method_name(#(#call_args),*))
            ),
        }
    });

    syn::parse_quote! {
        pub fn dispatch_ipc_request(
            &self,
            request: client::SupportedDBRequests,
        ) -> Option<Vec<u8>> {
            match request {
                #(#arms)*
                _ => None,
            }
        }
    }
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

fn client_return_type(output: &ReturnType) -> TokenStream2 {
    match output {
        ReturnType::Default => quote! { Result<(), Box<dyn std::error::Error>> },
        ReturnType::Type(_, ty) => quote! { Result<#ty, Box<dyn std::error::Error>> },
    }
}

fn async_client_return_type(output: &ReturnType) -> TokenStream2 {
    match output {
        ReturnType::Default => quote! { Result<(), Box<dyn std::error::Error + Send + Sync>> },
        ReturnType::Type(_, ty) => quote! { Result<#ty, Box<dyn std::error::Error + Send + Sync>> },
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
        let definitions = args
            .iter()
            .map(|(name, ty)| {
                let ty = owned_type(ty);
                quote! { #name: #ty }
            })
            .collect::<Vec<_>>();
        let names = args.iter().map(|(name, _)| name).collect::<Vec<_>>();
        let ret = client_return_type(&method.sig.output);
        let async_ret = async_client_return_type(&method.sig.output);
        let async_name = format_ident!("{}_async", name);
        let request = quote! { crate::SupportedDBRequests::#variant(#(#names),*) };
        let async_request = quote! { crate::SupportedDBRequests::#variant(#(#names),*) };
        quote! {
            #(#docs)*
            pub fn #name(#(#definitions),*) -> #ret {
                crate::init_data_request(#request)
            }

            #(#docs)*
            pub fn #async_name(#(#definitions),*) -> impl std::future::Future<Output = #async_ret> {
                crate::init_data_request_async(#async_request)
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
            crate::init_data_request(crate::SupportedDBRequests::ShouldExit)
        }

        /// Writes to the host log without printing to stdout.
        pub fn log_silent(log: String) -> Result<bool, Box<dyn std::error::Error>> {
            crate::init_data_request(crate::SupportedDBRequests::LoggingNoPrint(log))
        }

        /// Asynchronously returns whether the host application is shutting down.
        pub fn should_exit_async() -> impl std::future::Future<Output = Result<bool, Box<dyn std::error::Error + Send + Sync>>> {
            crate::init_data_request_async(crate::SupportedDBRequests::ShouldExit)
        }

        /// Asynchronously writes to the host log without printing to stdout.
        pub fn log_silent_async(log: String) -> impl std::future::Future<Output = Result<bool, Box<dyn std::error::Error + Send + Sync>>> {
            crate::init_data_request_async(crate::SupportedDBRequests::LoggingNoPrint(log))
        }
    };
    let source_file: File = syn::parse2(source_tokens).map_err(|error| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("failed to parse generated client: {error}"),
        )
    })?;
    let source = format_source(&prettyplease::unparse(&source_file))?;
    write_if_changed(&output, &source)?;
    Ok(())
}

fn format_source(source: &str) -> syn::Result<String> {
    let mut formatter = Command::new("rustfmt")
        .args(["--edition", "2024", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(io_error)?;

    formatter
        .stdin
        .take()
        .ok_or_else(|| {
            syn::Error::new(proc_macro2::Span::call_site(), "rustfmt stdin unavailable")
        })?
        .write_all(source.as_bytes())
        .map_err(io_error)?;

    let output = formatter.wait_with_output().map_err(io_error)?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("rustfmt failed: {error}"),
        ));
    }

    String::from_utf8(output.stdout).map_err(|error| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("rustfmt returned invalid UTF-8: {error}"),
        )
    })
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
