use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use log::info;

use casper_execution_engine::{
    core::engine_state::{EngineConfig, EngineState},
    storage::{
        global_state::lmdb::LmdbGlobalState, transaction_source::lmdb::LmdbEnvironment,
        trie_store::lmdb::LmdbTrieStore,
    },
};
use casper_hashing::Digest;
use casper_node::{storage::Storage, StorageConfig, WithDir};
use casper_types::ProtocolVersion;
use lmdb::DatabaseFlags;

use crate::common::db::TRIE_STORE_FILE_NAME;

/// LMDB max readers
///
/// The default value is chosen to be the same as the node itself.
const DEFAULT_MAX_READERS: u32 = 512;

/// Loads an existing execution engine.
pub fn load_execution_engine(
    ee_lmdb_path: impl AsRef<Path>,
    default_max_db_size: usize,
    state_root_hash: Digest,
    manual_sync_enabled: bool,
) -> Result<(Arc<EngineState<LmdbGlobalState>>, Arc<LmdbEnvironment>), anyhow::Error> {
    let lmdb_data_file = ee_lmdb_path.as_ref().join(TRIE_STORE_FILE_NAME);
    if !ee_lmdb_path.as_ref().join(TRIE_STORE_FILE_NAME).exists() {
        return Err(anyhow::anyhow!(
            "lmdb data file not found at: {}",
            lmdb_data_file.display()
        ));
    }
    let lmdb_environment =
        create_lmdb_environment(&ee_lmdb_path, default_max_db_size, manual_sync_enabled)?;
    let lmdb_trie_store = Arc::new(LmdbTrieStore::open(&lmdb_environment, None)?);
    let global_state = LmdbGlobalState::new(
        Arc::clone(&lmdb_environment),
        lmdb_trie_store,
        state_root_hash,
    );
    Ok((
        Arc::new(EngineState::new(global_state, EngineConfig::default())),
        lmdb_environment,
    ))
}

/// Create an lmdb environment at a given path.
fn create_lmdb_environment(
    lmdb_path: impl AsRef<Path>,
    default_max_db_size: usize,
    manual_sync_enabled: bool,
) -> Result<Arc<LmdbEnvironment>, anyhow::Error> {
    let lmdb_environment = Arc::new(LmdbEnvironment::new(
        &lmdb_path,
        default_max_db_size,
        DEFAULT_MAX_READERS,
        manual_sync_enabled,
    )?);
    Ok(lmdb_environment)
}

/// Creates a new execution engine.
pub fn create_execution_engine(
    ee_lmdb_path: impl AsRef<Path>,
    default_max_db_size: usize,
    manual_sync_enabled: bool,
) -> Result<(Arc<EngineState<LmdbGlobalState>>, Arc<LmdbEnvironment>), anyhow::Error> {
    if !ee_lmdb_path.as_ref().exists() {
        info!(
            "creating new lmdb data dir {}",
            ee_lmdb_path.as_ref().display()
        );
        fs::create_dir_all(&ee_lmdb_path)?;
    }
    fs::create_dir_all(&ee_lmdb_path)?;
    let lmdb_environment =
        create_lmdb_environment(&ee_lmdb_path, default_max_db_size, manual_sync_enabled)?;
    lmdb_environment.env().sync(true)?;

    let lmdb_trie_store = Arc::new(LmdbTrieStore::new(
        &lmdb_environment,
        None,
        DatabaseFlags::empty(),
    )?);
    let global_state = LmdbGlobalState::empty(Arc::clone(&lmdb_environment), lmdb_trie_store)?;

    Ok((
        Arc::new(EngineState::new(global_state, EngineConfig::default())),
        lmdb_environment,
    ))
}

pub fn create_storage(chain_download_path: impl AsRef<Path>) -> Result<Storage, anyhow::Error> {
    let chain_download_path = normalize_path(chain_download_path)?;
    let mut storage_config = StorageConfig::default();
    storage_config.path = chain_download_path.clone();
    Ok(Storage::new(
        &WithDir::new(chain_download_path, storage_config),
        None,
        ProtocolVersion::from_parts(0, 0, 0),
        false,
        // Works around needing to add "network name" to the path, instead a caller can
        // reference the exact directory.
        #[cfg(not(test))]
        ".",
        #[cfg(test)]
        "casper",
    )?)
}

pub fn normalize_path<P: AsRef<Path>>(path: P) -> Result<PathBuf, anyhow::Error> {
    let path = path.as_ref();
    let path = if path.is_absolute() {
        path.into()
    } else {
        env::current_dir()?.join(path)
    };
    Ok(fs::canonicalize(path)?)
}
