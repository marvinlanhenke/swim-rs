//! # Error Handling Module
//!
//! This module defines the error types and result aliases used throughout the crate.
//! It utilizes the `snafu` crate for error handling and provides a convenient `Result` type alias.
use snafu::Snafu;

use crate::pb::gossip::Event;

/// A type alias for `Result<T, Error>`
pub type Result<T> = std::result::Result<T, Error>;

/// The error type used throughout the crate.
///
/// This enum defines various error variants that can occur during the execution
/// of the SWIM protocol implementation. It leverages the `snafu` crate for generating
/// error contexts and backtraces.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("InternalError: {message}, {location}"))]
    Internal {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("InvalidDataError: {message}, {location}"))]
    InvalidData {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("IoError: {message}, {location}"))]
    Io {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("ProstEncodeError: {message}, {location}"))]
    ProstEncode {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("ProstDecodeError: {message}, {location}"))]
    ProstDecode {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("ProstUnknownEnumValueError: {message}, {location}"))]
    ProstUnknownEnumValue {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("AddrParseError: {message}, {location}"))]
    AddrParse {
        message: String,
        location: snafu::Location,
    },
    #[snafu(display("SendEventError: {message}, {location}"))]
    SendEvent {
        message: String,
        location: snafu::Location,
    },
}

trait SnafuLocationExt {
    fn to_snafu_location(&'static self) -> snafu::Location;
}

impl SnafuLocationExt for std::panic::Location<'static> {
    fn to_snafu_location(&'static self) -> snafu::Location {
        snafu::Location::new(self.file(), self.line(), self.column())
    }
}

macro_rules! make_error_from {
    ($from: ty, $to: ident) => {
        impl From<$from> for Error {
            fn from(value: $from) -> Self {
                Self::$to {
                    message: value.to_string(),
                    location: std::panic::Location::caller().to_snafu_location(),
                }
            }
        }
    };
}

make_error_from!(std::io::Error, Io);
make_error_from!(prost::UnknownEnumValue, ProstUnknownEnumValue);
make_error_from!(prost::DecodeError, ProstDecode);
make_error_from!(prost::EncodeError, ProstEncode);
make_error_from!(std::net::AddrParseError, AddrParse);
make_error_from!(tokio::sync::broadcast::error::SendError<Event>, SendEvent);
