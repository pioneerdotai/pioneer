use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input, parse_quote};

/// Run a Tokio test with a cleanup scope for PostgreSQL fixture builders.
/// Arguments are forwarded to the Tokio test attribute.
#[proc_macro_attribute]
pub fn postgres_test(arguments: TokenStream, item: TokenStream) -> TokenStream {
    let mut function = parse_macro_input!(item as ItemFn);
    let arguments = proc_macro2::TokenStream::from(arguments);
    let body = function.block;
    function.block = parse_quote!({
        ::pioneer_postgres_test::with_test_databases(async #body)
            .await
            .expect("close isolated PostgreSQL test databases")
    });
    quote!(
        #[::tokio::test(#arguments)]
        #function
    )
    .into()
}
