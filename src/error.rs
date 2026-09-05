use thiserror::Error;

#[derive(Error, Debug)]
pub enum DecSyncError {
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("invalid entry")]
    InvalidEntry(#[from] serde_json::Error),
    #[error("invalid DecSync info")]
    InvalidInfo,
    #[error("unsupported DecSync version {found}, supported version is {supported}")]
    UnsupportedVersion { supported: i64, found: i64 },
    #[error("unknown DecSync error")]
    Unknown,
}
