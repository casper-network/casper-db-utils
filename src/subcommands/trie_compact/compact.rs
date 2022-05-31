use std::{
    collections::HashSet,
    fmt::{Display, Formatter, Result as FmtResult},
    fs::{self, OpenOptions},
    io::Error as IoError,
    path::{Path, PathBuf},
};

use anyhow::Error as AnyError;
use lmdb::Error as LmdbError;
use log::info;
use thiserror::Error as ThisError;

use casper_hashing::Digest;
use casper_node::storage::Error as StorageError;

use super::utils::{create_execution_engine, create_storage, load_execution_engine};

pub(crate) const TRIE_STORE_FILE_NAME: &str = "data.lmdb";

/// Defines behavior for opening destination trie store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DestinationOptions {
    /// `data.lmdb` in destination directory will be appended if it exists.
    Append,
    /// `data.lmdb` in destination directory will be overwritten if it exists.
    Overwrite,
    /// `data.lmdb` must not exist in destination directory.
    New,
}

/// Possible errors caught while compacting the trie store.
#[derive(Debug, ThisError)]
pub enum Error {
    /// Error copying the state root with a specific digest.
    CopyStateRoot(Digest, AnyError),
    /// Error creating the execution engine for the source trie.
    OpenSourceTrie(AnyError),
    /// Error opening the block/deploys LMDB store.
    OpenStorage(AnyError),
    /// Error working with the destination trie path.
    InvalidDest(String),
    /// Path cannot be created/resolved.
    InvalidPath(PathBuf, IoError),
    /// Error while operating on LMDB.
    LmdbOperation(LmdbError),
    /// Error creating the execution engine for the destination trie.
    CreateDestTrie(AnyError),
    /// A block of specific height is missing from the storage.
    MissingBlock(u64),
    /// Error while getting a block of specific height from storage.
    Storage(u64, StorageError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::CopyStateRoot(digest, err) => {
                write!(f, "Error copying state root {}: {}", digest, err)
            }
            Self::CreateDestTrie(err) => {
                write!(f, "Error loading the execution engine: {}", err)
            }
            Self::InvalidDest(msg) => write!(f, "Invalid destination: {}", msg),
            Self::InvalidPath(path, err) => write!(
                f,
                "Path {} cannot be created/resolved: {}",
                path.to_string_lossy(),
                err
            ),
            Self::LmdbOperation(err) => write!(f, "Error operation on LMDB: {}.", err),
            Self::MissingBlock(idx) => write!(f, "Storage database is missing block {}.", idx),
            Self::OpenSourceTrie(err) => {
                write!(f, "Error creating the execution engine: {}", err)
            }
            Self::OpenStorage(err) => {
                write!(f, "Error opening the block/deploy storage: {}", err)
            }
            Self::Storage(idx, err) => write!(
                f,
                "Storage error while trying to retrieve block {}: {}",
                idx, err
            ),
        }
    }
}

fn validate_trie_paths<P: AsRef<Path>>(
    source_trie_path: P,
    destination_trie_path: P,
    dest_opt: DestinationOptions,
) -> Result<(), Error> {
    let dest_path_exists = destination_trie_path.as_ref().exists();

    if !dest_path_exists {
        match dest_opt {
            DestinationOptions::New => {
                fs::create_dir_all(destination_trie_path.as_ref()).map_err(|err| {
                    Error::InvalidPath(destination_trie_path.as_ref().to_owned(), err)
                })?;
            }
            DestinationOptions::Append => {
                return Err(Error::InvalidDest(
                    "No destination trie to append. Consider not using \"--append\".".to_string(),
                ));
            }
            DestinationOptions::Overwrite => {
                return Err(Error::InvalidDest(
                    "No destination trie to overwrite. Consider not using \"--overwrite\"."
                        .to_string(),
                ));
            }
        }
    } else {
        let dest_data_exists = destination_trie_path
            .as_ref()
            .join(TRIE_STORE_FILE_NAME)
            .exists();
        match dest_opt {
            DestinationOptions::New => {
                if dest_data_exists {
                    return Err(Error::InvalidDest(format!(
                        "Output file \"data.lmdb\" already exists at destination \"{}\". \
                    Run the program with `--overwrite` to overwrite file or `--append` \
                    to write alongside existing contents.",
                        destination_trie_path
                            .as_ref()
                            .join(TRIE_STORE_FILE_NAME)
                            .to_string_lossy()
                    )));
                }
            }
            DestinationOptions::Append => {
                if !dest_data_exists {
                    return Err(Error::InvalidDest(format!(
                        "Nothing to append to, output file \"data.lmdb\" doesn't exist at \
                    destination \"{}\". Run the program without `--append`",
                        destination_trie_path
                            .as_ref()
                            .join(TRIE_STORE_FILE_NAME)
                            .to_string_lossy()
                    )));
                }
            }
            DestinationOptions::Overwrite => {
                if dest_data_exists {
                    let _f: std::fs::File = OpenOptions::new()
                        .truncate(true)
                        .write(true)
                        .open(destination_trie_path.as_ref().join(TRIE_STORE_FILE_NAME))
                        .map_err(|io_err| {
                            Error::InvalidDest(format!(
                                "Couldn't overwrite destination file: {}",
                                io_err
                            ))
                        })?;
                } else {
                    return Err(Error::InvalidDest(format!(
                        "Nothing to overwrite, output file \"data.lmdb\" doesn't exist at \
                        destination \"{}\". Run the program without `--overwrite`",
                        destination_trie_path
                            .as_ref()
                            .join(TRIE_STORE_FILE_NAME)
                            .to_string_lossy()
                    )));
                }
            }
        }
    }

    // Replace `canonicalize` with `fs::absolute` when it becomes stable.
    if source_trie_path
        .as_ref()
        .canonicalize()
        .map_err(|err| Error::InvalidPath(source_trie_path.as_ref().to_path_buf(), err))?
        == destination_trie_path
            .as_ref()
            .canonicalize()
            .map_err(|err| Error::InvalidPath(destination_trie_path.as_ref().to_path_buf(), err))?
    {
        return Err(Error::InvalidDest(
            "Destination trie can't be in the same LMDB file as the source.".to_string(),
        ));
    };

    Ok(())
}

/// Compacts a source trie and outputs the result to the destination trie.
///
/// The function first retrieves the highest block hash from storage and
/// compacting starts from that state root hash. Each descendant of that
/// block's hash is copied to the destination trie. This process is repeated
/// for all the remaining blocks, from highest to lowest.
pub fn trie_compact(
    storage_path: PathBuf,
    source_trie_path: PathBuf,
    destination_trie_path: PathBuf,
    dest_opt: DestinationOptions,
    max_db_size: usize,
) -> Result<(), Error> {
    validate_trie_paths(&source_trie_path, &destination_trie_path, dest_opt)?;

    let (source_state, _env) =
        load_execution_engine(source_trie_path, max_db_size, Digest::default(), true)
            .map_err(Error::OpenSourceTrie)?;

    let (destination_state, _env) =
        create_execution_engine(destination_trie_path, max_db_size, true)
            .map_err(Error::CreateDestTrie)?;

    // Create a separate lmdb for block/deploy storage at chain_download_path.
    let storage = create_storage(&storage_path).map_err(Error::OpenStorage)?;

    let mut block = match storage
        .read_highest_block()
        .map_err(|err| Error::Storage(0, err))?
    {
        Some(block) => block,
        None => {
            info!("No blocks found in storage, exiting.");
            return Ok(());
        }
    };
    let mut visited_roots = HashSet::new();
    let mut block_height;

    info!("Copying state roots from source to destination.");
    loop {
        block_height = block.height();
        let state_root = *block.take_header().state_root_hash();
        if !visited_roots.contains(&state_root) {
            super::helpers::copy_state_root(state_root, &source_state, &destination_state)
                .map_err(|err| Error::CopyStateRoot(state_root, err))?;
            destination_state
                .flush_environment()
                .map_err(Error::LmdbOperation)?;
            visited_roots.insert(state_root);
        }
        if block_height == 0 {
            break;
        }
        block = storage
            .read_block_by_height(block_height - 1)
            .map_err(|storage_err| Error::Storage(block_height - 1, storage_err))?
            .ok_or(Error::MissingBlock(block_height - 1))?;
    }
    info!(
        "Finished copying {} state roots to new database.",
        visited_roots.len()
    );

    Ok(())
}
