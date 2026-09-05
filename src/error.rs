//! Everything this crate can fail on.

use thiserror::Error;

/// Anything that can go wrong while talking to a DecSync directory.
#[derive(Error, Debug)]
pub enum DecSyncError {
    /// A file could not be read or written.
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    /// A line in an entry file did not parse as an entry.
    #[error("invalid entry")]
    InvalidEntry(#[from] serde_json::Error),
    /// `.decsync-info` was missing a usable version, or was not JSON.
    #[error("invalid DecSync info")]
    InvalidInfo,
    /// `.decsync-info` names a DecSync version this crate does not implement.
    #[error("unsupported DecSync version {found}, supported version is {supported}")]
    UnsupportedVersion {
        /// The version this crate is built for.
        supported: i64,
        /// The version found in the directory.
        found: i64,
    },
    /// Reserved; currently unused.
    #[error("unknown DecSync error")]
    Unknown,
}
