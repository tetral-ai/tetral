#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod bootstrap;
pub mod connection;
pub mod drive_transfer;
pub mod errors;
pub mod field;
pub mod id;
pub mod local;
pub mod path;
pub mod ports;
pub mod strict;
pub mod time;
pub mod tool;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

pub use strict::{StrictError, StrictErrorKind};

/// The connection protocol version, sent in every envelope and refused when
/// different. It is not the CLI's `cli_version`, and neither derives from the
/// other.
pub const PROTOCOL_VERSION: i64 = 1;

/// A message that the protocol defines as one JSON value.
pub trait Message: Serialize + DeserializeOwned {
    /// The largest size of the message as received, in bytes. A downstream
    /// event, whose limit depends on what it carries, has the largest here and
    /// the narrower ones in [`Message::check`]; a tool-status report is read
    /// under its media limit whatever it holds.
    const MAX_BYTES: usize;

    /// The rules beyond the schema that the protocol states: the narrower
    /// size limit of a downstream event, and relations between members.
    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        Ok(())
    }
}

/// A message as it was read, unknown members included.
pub struct Received<'a> {
    pub value: &'a Value,
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("the message holds {size} bytes, over its limit of {limit}")]
    TooLarge { size: usize, limit: usize },
    #[error(transparent)]
    Strict(#[from] StrictError),
    #[error("the message does not match its schema: {0}")]
    Shape(String),
    #[error("the message breaks a protocol rule: {0}")]
    Rule(&'static str),
}

/// Reads one message: its size, the strict pass, the typed members, then the
/// rules of [`Message::check`].
pub fn decode<M: Message>(input: &[u8]) -> Result<M, DecodeError> {
    let (message, _) = decode_value::<M>(input)?;
    Ok(message)
}

/// [`decode`], also returning the message as read.
pub fn decode_value<M: Message>(input: &[u8]) -> Result<(M, Value), DecodeError> {
    if input.len() > M::MAX_BYTES {
        return Err(DecodeError::TooLarge {
            size: input.len(),
            limit: M::MAX_BYTES,
        });
    }
    let negative_zeros = strict::read(input)?;
    let value: Value = if negative_zeros.is_empty() {
        serde_json::from_slice(input)
    } else {
        // `serde_json` reads the integer `-0` as the float `-0.0`, which an
        // integer member refuses, while the schemas read it as the integer 0;
        // dropping its sign keeps the offsets and reads it as 0.
        let mut unsigned = input.to_vec();
        for at in negative_zeros {
            unsigned[at] = b' ';
        }
        serde_json::from_slice(&unsigned)
    }
    .map_err(|error| DecodeError::Shape(error.to_string()))?;
    let message = M::deserialize(&value).map_err(|error| DecodeError::Shape(error.to_string()))?;
    message.check(&Received {
        value: &value,
        bytes: input.len(),
    })?;
    Ok((message, value))
}

/// A response that the protocol relates to the request it answers.
pub trait Answer<Request>: Message {
    fn check_answer(&self, request: &Request) -> Result<(), DecodeError>;
}

/// Reads a response as [`decode`] does, then checks it against `request`.
pub fn decode_answer<M: Answer<R>, R>(input: &[u8], request: &R) -> Result<M, DecodeError> {
    let message = decode::<M>(input)?;
    message.check_answer(request)?;
    Ok(message)
}

/// Writes a message as compact UTF-8 JSON.
pub fn encode<M: Serialize>(message: &M) -> Vec<u8> {
    serde_json::to_vec(message).expect("protocol types always serialize")
}

pub(crate) fn rule(holds: bool, broken: &'static str) -> Result<(), DecodeError> {
    if holds {
        Ok(())
    } else {
        Err(DecodeError::Rule(broken))
    }
}
