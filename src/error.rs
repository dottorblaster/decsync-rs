use thiserror::Error;

#[derive(Error, Debug)]
pub enum DecSyncError {
    #[error("I/O error")]
    IOError(#[from] std::io::Error),
    #[error("invalid entry")]
    InvalidEntry(#[from] serde_json::Error),
    #[error("unknown DecSync error")]
    Unknown,
}
