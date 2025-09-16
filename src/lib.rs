// Copyright © 2024 Pathway

#![warn(clippy::pedantic)]
#![warn(clippy::cargo)]
#![allow(clippy::must_use_candidate)] // too noisy
// FIXME:
#![allow(clippy::result_large_err)] // Too noisy, around 250 warnings across the codebase. Fix gradually.
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

pub mod connectors;
pub mod deepcopy;
pub mod engine;
pub mod external_integration;
pub mod persistence;
pub mod python_api;

pub mod async_runtime;
mod env;
mod fs_helpers;
mod mat_mul;
mod pipe;
mod retry;
mod timestamp;

#[cfg(all(not(feature = "standard-allocator"), unix))]
mod jemalloc {
    use jemallocator::Jemalloc;

    #[global_allocator]
    static GLOBAL_ALLOCATOR: Jemalloc = Jemalloc;
}
