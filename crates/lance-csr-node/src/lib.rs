//! The private native module behind the `lance-csr` npm package.
//!
//! Users see the TypeScript classes in `nodejs/src/`, which wrap these and carry the
//! documentation and types. Document Node behaviour there, not here.

mod convert;
mod engine;
mod query;
mod test_utils;

#[napi_derive::napi]
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
