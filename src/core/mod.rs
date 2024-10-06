//! # Core Module
//!
//! The `core` module contains the main components of the SWIM protocol implementation.
//! It includes sub-modules for failure detection, dissemination of gossip messages,
//! membership list management, message handling, node orchestration, transport abstraction,
//! type definitions, and utility functions.
mod detection;
mod disseminate;
pub(crate) mod member;
mod message;
pub(crate) mod node;
pub(crate) mod transport;
mod types;
mod utils;
