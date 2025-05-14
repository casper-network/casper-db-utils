mod compact;
mod helpers;
#[cfg(test)]
pub(crate) mod tests;
// All code in the `utils` mod was copied from `casper-node` because it isn't available in the
// public interface.
mod utils;

use std::{io::Error as IoError, path::PathBuf};

use anyhow::Error as AnyError;
use casper_types::Digest;
use clap::ArgMatches;
use lmdb::Error as LmdbError;
use thiserror::Error as ThisError;

use compact::DestinationOptions;
pub use helpers::copy_state_root;
pub use utils::{create_execution_engine, load_execution_engine};

pub const COMMAND_NAME: &str = "compact-trie";
const APPEND: &str = "append";
const DESTINATION_TRIE_STORE_PATH: &str = "dest-trie";
const OVERWRITE: &str = "overwrite";
const MAX_DB_SIZE: &str = "max-db-size";
pub const DEFAULT_MAX_DB_SIZE: &str = "483183820800"; // 450 gb
const SOURCE_TRIE_STORE_PATH: &str = "src-trie";
const STORAGE_PATH: &str = "storage-path";

/// Possible errors caught while compacting the trie store.
#[derive(Debug, ThisError)]
pub enum Error {
    /// Error copying the state root with a specific digest.
    #[error("Error copying state root {0}: {1}")]
    CopyStateRoot(Digest, AnyError),
    /// Error creating the execution engine for the destination trie.
    #[error("Error loading the execution engine: {0}")]
    CreateDestTrie(AnyError),
    /// Error working with the destination trie path.
    #[error("Invalid destination: {0}")]
    InvalidDest(String),
    /// Path cannot be created/resolved.
    #[error("Path {0} cannot be created/resolved: {1}")]
    InvalidPath(PathBuf, IoError),
    /// Error while operating on LMDB.
    #[error("Error while operating on LMDB: {0}")]
    LmdbOperation(LmdbError),
    /// A block of specific height is missing from the storage.
    #[error("Storage database is missing block {0}")]
    MissingBlock(u64),
    /// Error creating the execution engine for the source trie.
    #[error("Error creating the execution engine: {0}")]
    OpenSourceTrie(AnyError),
    #[error("Error when accessing the database layer: {0}")]
    DbLayer(#[from] crate::common::db::Error),
}

pub fn run(matches: &ArgMatches) -> Result<(), Error> {
    let storage_path = matches.value_of(STORAGE_PATH).unwrap();
    let source_trie_path = matches.value_of(SOURCE_TRIE_STORE_PATH).unwrap();
    let destination_trie_path = matches.value_of(DESTINATION_TRIE_STORE_PATH).unwrap();
    // Prettier than C style if/else.
    let dest_opt = match matches {
        _ if matches.is_present(APPEND) => DestinationOptions::Append,
        _ if matches.is_present(OVERWRITE) => DestinationOptions::Overwrite,
        _ => DestinationOptions::New,
    };
    let max_db_size = matches
        .value_of(MAX_DB_SIZE)
        .unwrap()
        .parse()
        .expect("Value of \"--max-db-size\" must be an integer.");

    compact::trie_compact(
        storage_path,
        source_trie_path,
        destination_trie_path,
        dest_opt,
        max_db_size,
    )
}
