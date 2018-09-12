//! brokkr error classes.

// TODO: Look into the error-chain crate.

use redis;
use serde_json;
use uuid;

use std::convert::From;
use std::error::Error as _Error;
use std::fmt;
use std::result::Result as _Result;
use std::str;

/// Main error type
#[derive(Debug)]
pub enum Error {
  /// Tried to move a job to an invalid state.
  InvalidTransition(String),
  /// Wrap a Redis error
  RedisError(redis::RedisError),
  /// Wrap a serde error occuring when de-/serializing data structures.
  SerdeError(serde_json::Error),
  UuidError(uuid::parser::ParseError),
  Utf8Error(str::Utf8Error),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    f.write_str(self.description())
  }
}

impl _Error for Error {
  fn description(&self) -> &str {
    match self {
      Error::InvalidTransition(s) => &s,
      Error::RedisError(e) => e.description(),
      Error::SerdeError(e) => e.description(),
      Error::UuidError(e) => e.description(),
      Error::Utf8Error(e) => e.description(),
    }
  }

  fn cause(&self) -> Option<&_Error> {
    match self {
      Error::InvalidTransition(_) => None,
      Error::RedisError(e) => Some(e),
      Error::SerdeError(e) => Some(e),
      Error::UuidError(e) => Some(e),
      Error::Utf8Error(e) => Some(e),
    }
  }
}

macro_rules! _from {
  ($s:ty, $w:ident) => {
    impl From<$s> for Error {
      fn from(e: $s) -> Self {
        Error::$w(e)
      }
    }
  };
}

_from!(redis::RedisError, RedisError);
_from!(serde_json::Error, SerdeError);
// _from!(uuid::parser::ParseError, UuidError);
_from!(str::Utf8Error, Utf8Error);

pub type Result<V> = _Result<V, Error>;

#[macro_export]
macro_rules! invalid_transition {
  ($to: expr, $from: expr) => {
    Err(Error::InvalidTransition(format!(
      "Cannot move job to {:?} from {:?}.",
      $to, $from
    )))
  };
}
