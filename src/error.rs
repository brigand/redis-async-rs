/*
 * Copyright 2017-2018 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

//! Error handling

use std::{error, fmt, io};

use futures::sync::{mpsc, oneshot};

use lwactors::ActorError;

use resp;

#[derive(Debug)]
pub enum Error {
    /// A non-specific internal error that prevented an operation from completing
    Internal(String),

    /// An IO error occurred
    IO(io::Error),

    /// A RESP parsing/serialising error occurred
    RESP(String, Option<resp::RespValue>),

    /// A remote error
    Remote(String),

    /// End of stream - not necesserially an error if you're anticipating it
    EndOfStream,

    /// An unexpected error.  In this context "unexpected" means
    /// "unexpected because we check ahead of time", it used to maintain the type signature of
    /// chains of futures; but it occurring at runtime should be considered a catastrophic
    /// failure.
    ///
    /// If any error is propagated this way that needs to be handled, then it should be made into
    /// a proper option.
    Unexpected(String),
}

pub fn internal<T: Into<String>>(msg: T) -> Error {
    Error::Internal(msg.into())
}

pub fn resp<T: Into<String>>(msg: T, resp: resp::RespValue) -> Error {
    Error::RESP(msg.into(), Some(resp))
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<oneshot::Canceled> for Error {
    fn from(err: oneshot::Canceled) -> Error {
        Error::Unexpected(format!("Oneshot was cancelled before use: {}", err))
    }
}

impl<T: 'static + Send> From<mpsc::SendError<T>> for Error {
    fn from(err: mpsc::SendError<T>) -> Error {
        Error::Unexpected(format!("Cannot write to channel: {}", err))
    }
}

impl From<ActorError> for Error {
    fn from(err: ActorError) -> Error {
        internal(format!("Internal actor failed: {}", err))
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Internal(ref s) => s,
            Error::IO(ref err) => err.description(),
            Error::RESP(ref s, _) => s,
            Error::Remote(ref s) => s,
            Error::EndOfStream => "End of Stream",
            Error::Unexpected(ref err) => err,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Internal(_) => None,
            Error::IO(ref err) => Some(err),
            Error::RESP(_, _) => None,
            Error::Remote(_) => None,
            Error::EndOfStream => None,
            Error::Unexpected(_) => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::error::Error;
        fmt::Display::fmt(self.description(), f)
    }
}
