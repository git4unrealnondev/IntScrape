use std::{env, error::Error, fs, path::PathBuf};

use quote::quote;
use syn::{Attribute, FnArg, ImplItem, Item, Meta, Pat, Type};

fn main() -> Result<(), Box<dyn Error>> {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let source_path = manifest.join("../../src/db/main.rs");
    println!("cargo:rerun-if-changed={}", source_path.display());
    println!("cargo:rerun-if-changed=../../libs/shared_types/src");

    let source = fs::read_to_string(source_path)?;
    let file = syn::parse_file(&source)
        .map_err(|error| format!("failed to parse database source: {error}"))?;
    let mut variants = Vec::new();

    for item in file.items {
        let Item::Impl(item) = item else { continue };
        for item in item.items {
            let ImplItem::Fn(method) = item else { continue };
            let Some(request) = ipc_request(&method).map_err(|error| {
                format!(
                    "failed to parse IPC attribute on {}: {error}",
                    method.sig.ident
                )
            })?
            else {
                continue;
            };
            let fields = arguments(&method)
                .into_iter()
                .map(|(_, ty)| owned_type(ty))
                .collect::<Vec<_>>();
            variants.push(quote! { #request(#(#fields),*) });
        }
    }

    variants.extend([
        quote! { ExternalPluginCall(String, shared_types::CallbackInfoInput) },
        quote! { LoggingNoPrint(String) },
        quote! { ShouldExit },
    ]);

    let tokens = quote! {
        #[derive(Debug, serde::Serialize, serde::Deserialize, bitcode::Encode, bitcode::Decode)]
        pub enum SupportedDBRequests {
            #(#variants),*
        }
    };
    let parsed = syn::parse2(tokens)
        .map_err(|error| format!("failed to parse generated request enum: {error}"))?;
    let output = prettyplease::unparse(&parsed);
    let output_path =
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("supported_db_requests.rs");
    write_if_changed(&output_path, &output)?;

    Ok(())
}

fn ipc_request(method: &syn::ImplItemFn) -> syn::Result<Option<syn::Ident>> {
    for attribute in &method.attrs {
        if !attribute.path().is_ident("ipc") {
            continue;
        }

        let mut request = None;
        let Attribute { meta, .. } = attribute;
        let Meta::List(list) = meta else {
            return Ok(Some(pascal_case(&method.sig.ident)));
        };
        let args = syn::parse::Parser::parse2(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            list.tokens.clone(),
        )?;
        for arg in args {
            if let Meta::NameValue(value) = arg
                && value.path.is_ident("request")
            {
                let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(value),
                    ..
                }) = value.value
                else {
                    return Err(syn::Error::new_spanned(
                        value,
                        "request must be a string literal",
                    ));
                };
                request = Some(syn::parse_str(&value.value()).map_err(|_| {
                    syn::Error::new_spanned(value, "request must be a Rust identifier")
                })?);
            }
        }
        return Ok(Some(
            request.unwrap_or_else(|| pascal_case(&method.sig.ident)),
        ));
    }
    Ok(None)
}

fn arguments(method: &syn::ImplItemFn) -> Vec<(&syn::Ident, &Type)> {
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
    syn::parse_str(&output).expect("generated request variant must be an identifier")
}

fn write_if_changed(path: &std::path::Path, contents: &str) -> Result<(), Box<dyn Error>> {
    if fs::read_to_string(path)
        .map(|existing| existing == contents)
        .unwrap_or(false)
    {
        return Ok(());
    }
    let temporary = path.with_extension("rs.tmp");
    fs::write(&temporary, contents)?;
    fs::rename(temporary, path)?;
    Ok(())
}
