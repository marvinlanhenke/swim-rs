pub mod api;

mod core;

mod error;
pub use error::Result;

#[cfg(test)]
#[path = "./test-utils/mod.rs"]
#[doc(hidden)]
mod test_utils;

mod pb {
    include!(concat!(env!("OUT_DIR"), "/swim.rs"));
}
pub use pb::gossip::Event;
