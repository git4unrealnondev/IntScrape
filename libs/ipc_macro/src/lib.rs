extern crate proc_macro;
use proc_macro::TokenStream;
use quote::ToTokens;
use quote::{format_ident, quote};
use syn::{
    FnArg, ImplItem, ItemImpl, PatType, ReturnType, Type, Visibility, parse_macro_input, token::Pub,
};
#[proc_macro_attribute]
pub fn export_ipc(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let struct_name = &input.self_ty;

    let struct_name_str = struct_name.into_token_stream().to_string();
    let base_path = struct_name_str.to_lowercase();
    let mut match_arms = vec![];

    // Loop through the items in the implementation block
    for item in &input.items {
        if let syn::ImplItem::Fn(m) = item {
            // Only export public functions
            if !matches!(m.vis, syn::Visibility::Public(_)) {
                continue;
            }

            let fn_name = &m.sig.ident;
            // The unique method string identifier (e.g. "my_struct/my_function")
            let route_name = format!("{}/{}", base_path, fn_name);

            // Collect argument names/types for the function signature
            let mut arg_names = vec![];
            let mut fn_arg_types = vec![];

            for input_arg in &m.sig.inputs {
                if let FnArg::Typed(PatType { pat, ty, .. }) = input_arg {
                    arg_names.push(pat);
                    fn_arg_types.push(ty.clone());
                }
            }

            // Determine the *owned* types for Serde JSON tuple deserialization (e.g., convert '&str' -> 'String')
            let closure_arg_types: Vec<Type> = fn_arg_types
                .iter()
                .map(|ty| {
                    if let Type::Reference(type_ref) = ty.as_ref() {
                        // Convert '&T' to 'T'
                        (*type_ref.elem).clone()
                    } else {
                        *(*ty).clone()
                    }
                })
                .collect();

            // Borrow the owned data if the underlying function expects a reference
            let borrowed_fn_args: Vec<_> = fn_arg_types
                .iter()
                .zip(arg_names.iter())
                .map(|(ty, name)| {
                    if let Type::Reference(_) = ty.as_ref() {
                        quote! { & #name }
                    } else {
                        quote! { #name }
                    }
                })
                .collect();

            // Return type
            let ret_type = match &m.sig.output {
                ReturnType::Default => quote! { () },
                ReturnType::Type(_, ty) => quote! { #ty },
            };

            // Generate the matching arm for this specific routing identifier
            let match_arm = if !arg_names.is_empty() {
                // Method WITH arguments (expects a parameters tuple inside the JSON wrapper)
                quote! {
                    #route_name => {
                        let params: (#(#closure_arg_types),*) = serde_json::from_value(req.params)
                            .map_err(|e| format!("Invalid parameters: {}", e))?;
                        let (#(#arg_names),*) = params;

                        let res: #ret_type = instance.#fn_name(#(#borrowed_fn_args),*);
                        serde_json::to_vec(&res).map_err(|e| e.to_string())?
                    }
                }
            } else {
                // Method WITHOUT arguments
                quote! {
                    #route_name => {
                        let res: #ret_type = instance.#fn_name();
                        serde_json::to_vec(&res).map_err(|e| e.to_string())?
                    }
                }
            };

            match_arms.push(match_arm);
        }
    }

    // Expand the code
    let expanded = quote! {
        #input

        // Struct to safely deserialize the incoming IPC frame
        #[derive(serde::Deserialize)]
        struct IpcRequestEnvelope {
            method: String,
            #[serde(default)]
            params: serde_json::Value,
        }

        // This separate impl block holds the generated handler method
        impl #struct_name {
            pub fn handle_ipc_call(&self, raw_payload: &[u8]) -> Result<Vec<u8>, String> {
                let req: IpcRequestEnvelope = serde_json::from_slice(raw_payload)
                    .map_err(|e| format!("IPC Deserialization payload error: {}", e))?;

                let instance = self;

                let response_bytes = match req.method.as_str() {
                    #( #match_arms, )*
                    _ => return Err(format!("Unknown IPC route: {}", req.method)),
                };

                Ok(response_bytes)
            }
        }
    };
    TokenStream::from(expanded)
}
