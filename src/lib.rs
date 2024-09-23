pub mod config;
pub mod core;
pub mod error;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/swim.rs"));
}
