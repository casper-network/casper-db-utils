mod helpers;
mod utils;

use std::{path::PathBuf, collections::HashSet, fmt::{Display, Formatter, Result as FmtResult}};

use anyhow::Error as AnyError;
use log::info;
use thiserror::Error as ThisError;

use casper_hashing::Digest;

use utils::{create_execution_engine, create_storage, load_execution_engine};

const DEFAULT_MAX_DB_SIZE: usize = 483_183_820_800; // 450 gb

#[derive(Debug, ThisError)]
pub enum Error {
    CreateExecutionEngine(AnyError),
    InvalidDest,
    LoadExecutionEngine(AnyError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::CreateExecutionEngine(err) => write!(f, "Error creating the execution engine: {}", err),
            Self::InvalidDest => write!(f, "Destination trie can't be in the same LMDB file as the source."),
            Self::LoadExecutionEngine(err) => write!(f, "Error loading the execution engine: {}", err),
        }
    }
}

pub fn trie_compact(
    storage_path: PathBuf,
    source_trie_path: PathBuf,
    destination_trie_path: PathBuf
) -> Result<(), Error> {
    if source_trie_path == destination_trie_path {
        return Err(Error::InvalidDest);
    }

    let (source_state, _env) = load_execution_engine(
        source_trie_path,
        DEFAULT_MAX_DB_SIZE,
        Digest::default(),
        true,
    ).map_err(|err| Error::LoadExecutionEngine(err))?;

    let (destination_state, _env) = create_execution_engine(
        destination_trie_path,
        DEFAULT_MAX_DB_SIZE,
        true,
    ).map_err(|err| Error::CreateExecutionEngine(err))?;

    // Create a separate lmdb for block/deploy storage at chain_download_path.
    let storage = create_storage(&storage_path)
        .expect("should create storage");

    let mut block = storage.read_highest_block().unwrap().unwrap();
    let mut visited_roots = HashSet::new();
    let mut block_height;

    info!("Copying state roots from source to destination.");
    loop {
        block_height = block.height();
        let state_root = *block.take_header().state_root_hash();
        if !visited_roots.contains(&state_root) {
            helpers::copy_state_root(state_root, &source_state, &destination_state)
                .expect("should copy state root");
            destination_state
                .flush_environment()
                .expect("should flush to lmdb");
            visited_roots.insert(state_root);
        }
        if block_height == 0 {
            break;
        }
        block = storage
            .read_block_by_height(block_height - 1)
            .unwrap()
            .unwrap();
    }
    info!(
        "Finished copying {} state roots to new database.",
        visited_roots.len()
    );

    Ok(())
}
