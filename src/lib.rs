use lazy_static::lazy_static;
use tracing_subscriber::EnvFilter;

pub mod api;

mod core;
pub use core::{member::MembershipList, node::SwimNode};

mod error;
pub use error::Result;

#[cfg(any(test, feature = "test-util"))]
#[path = "./test-utils/mod.rs"]
#[doc(hidden)]
mod test_utils;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/swim.rs"));
}

lazy_static! {
    static ref TRACING: () = {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    };
}

fn init_tracing() {
    lazy_static::initialize(&TRACING);
}
