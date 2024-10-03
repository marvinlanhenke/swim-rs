//! # SWIM API Module
//!
//! This module provides the primary API for interacting with the SWIM protocol implementation.
//! It includes configuration options and the main entry point for creating and managing SWIM clusters.
//!
//! ## Modules
//!
//! - [`config`]: Contains configuration structures and builders for customizing SWIM nodes.
//! - [`swim`]: Provides the `SwimCluster` struct for initializing and running SWIM nodes.
//!
//! ## Tracing Initialization
//!
//! The `init_tracing` function initializes the tracing subscriber for logging purposes.

use lazy_static::lazy_static;
use tracing_subscriber::EnvFilter;

pub mod config;
pub mod swim;

lazy_static! {
    static ref TRACING: () = {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    };
}

/// Initializes the tracing subscriber for logging.
///
/// This function ensures that the tracing subscriber is only initialized once.
/// It should be called before running the SWIM node to enable logging output.
fn init_tracing() {
    lazy_static::initialize(&TRACING);
}
