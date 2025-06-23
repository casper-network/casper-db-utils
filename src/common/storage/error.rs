use std::{fmt::Debug, io, path::PathBuf};

use thiserror::Error;

use casper_storage::block_store::BlockStoreError;
use casper_types::{BlockValidationError, bytesrepr};

/// A fatal storage component error.
///
/// An error of this kinds indicates that storage is corrupted or otherwise irrecoverably broken, at
/// least for the moment. It should usually be followed by swift termination of the node.
#[derive(Debug, Error)]
pub enum FatalStorageError {
    /// Failure to create the root database directory.
    #[error("failed to create database directory `{}`: {}", .0.display(), .1)]
    CreateDatabaseDirectory(PathBuf, io::Error),
    /// Error when validating a block.
    #[error(transparent)]
    BlockValidation(#[from] BlockValidationError),
    /// `ToBytes` serialization failure of an item that should never fail to serialize.
    #[error("unexpected serialization failure: {0}")]
    UnexpectedSerializationFailure(bytesrepr::Error),
    /// `ToBytes` deserialization failure of an item that should never fail to serialize.
    #[error("unexpected deserialization failure: {0}")]
    UnexpectedDeserializationFailure(bytesrepr::Error),

    /// BlockStoreError
    #[error(transparent)]
    BlockStoreError(#[from] BlockStoreError),
}

impl From<Box<BlockValidationError>> for FatalStorageError {
    fn from(err: Box<BlockValidationError>) -> Self {
        Self::BlockValidation(*err)
    }
}

#[derive(Debug, Error)]
pub(super) enum GetRequestError {
    /// A fatal error occurred.
    #[error(transparent)]
    Fatal(#[from] FatalStorageError),
}
